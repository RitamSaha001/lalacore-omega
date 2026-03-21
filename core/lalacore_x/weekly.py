from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from core.lalacore_x.calibration import ConfidenceCalibrator
from core.lalacore_x.meta_verification import MetaVerificationLayer
from core.lalacore_x.mini_distillation import LC9DistillationHub
from core.lalacore_x.mini_evolution import MiniEvolutionEngine
from core.lalacore_x.replay import FailureReplayMemory
from core.lalacore_x.retrieval import ConceptVault
from core.lalacore_x.routing import ProviderStatsMemory
from core.lalacore_x.telemetry import DEFAULT_TELEMETRY
from core.lalacore_x.token_budget import TokenBudgetGuardian


class WeeklyEvolutionJob:
    """
    Runs autonomous weekly evolution cycle:
    - recalibrate risk model
    - generate replay set
    - expand trap vault
    - output evolution report
    """

    def __init__(self):
        self.telemetry = DEFAULT_TELEMETRY
        self.calibrator = ConfidenceCalibrator()
        self.replay = FailureReplayMemory()
        self.vault = ConceptVault()
        self.stats = ProviderStatsMemory()
        self.mini_evolution = MiniEvolutionEngine()
        self.distillation = LC9DistillationHub()
        self.meta_verification = MetaVerificationLayer()
        self.token_guardian = TokenBudgetGuardian()
        self.drift_state_path = Path("data/metrics/drift_state.json")
        self.drift_state_path.parent.mkdir(parents=True, exist_ok=True)

    def run(self) -> Dict:
        events = self.telemetry.read_events()
        calibration_rows = self._build_calibration_rows(events)
        self.calibrator.fit_from_rows(calibration_rows, epochs=8, lr=0.03)

        replay_fraction = max(0.05, min(0.4, self.mini_evolution.replay_intensity() * self.token_guardian.replay_intensity_scale()))
        replay_report = self.replay.build_weekly_replay(top_fraction=replay_fraction)
        replay_rows = self.replay.read_failures()
        trap_report = self._expand_trap_vault(events)

        self.stats.auto_tune_thresholds()
        ranking = self.stats.weekly_recompute_rankings()
        routing_thresholds = self.stats.data.get("routing_thresholds", {})
        prompt_effectiveness = self.distillation.analyze_prompt_effectiveness()
        dataset_report = self.distillation.finalize_weekly_dataset(replay_rows=replay_rows)
        drift_report = self._detect_statistical_drift(events)
        weekly_mini = self.mini_evolution.weekly_adjustments()
        synthetic_report = self.distillation.generate_synthetic_expansion(
            source_rows=self._synthetic_source_rows(),
            reliable_clusters=self._reliable_clusters(),
        )
        error_summary = self.meta_verification.summarize()

        report = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "calibration_rows": len(calibration_rows),
            "replay": replay_report,
            "trap_report": trap_report,
            "provider_rankings": ranking,
            "routing_thresholds": routing_thresholds,
            "drift_detection": drift_report,
            "synthetic_expansion": synthetic_report,
            "error_memory": error_summary,
            "distillation": {
                "dataset": dataset_report,
                "prompt_effectiveness_rows": len(prompt_effectiveness.get("rows", [])),
            },
            "mini_evolution": {
                "drift_score": self.mini_evolution.drift_score(),
                "replay_intensity": self.mini_evolution.replay_intensity(),
                "weekly_adjustments": weekly_mini,
            },
            "token_budget": self.token_guardian.summary(),
        }

        out = Path("data/reports/weekly_evolution_report.json")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2), encoding="utf-8")

        return report

    def _build_calibration_rows(self, events: List[Dict]) -> List[Dict]:
        rows: List[Dict] = []

        for event in events:
            if event.get("event_type") != "solve_result":
                continue

            rows.append(
                {
                    "verification_fail": 0.0 if event.get("verified") else 1.0,
                    "disagreement": float(event.get("disagreement", 0.0)),
                    "retrieval_strength": float(event.get("retrieval_strength", 0.0)),
                    "critic_score": float(event.get("critic_score", 0.5)),
                    "provider_reliability": float(event.get("provider_reliability", 0.5)),
                    "trap_probability": float(event.get("trap_probability", 0.0)),
                    "entropy": float(event.get("entropy", 0.0)),
                    "bt_margin": float(event.get("bt_margin", 0.0)),
                    "disagreement_cluster_size": float(event.get("disagreement_cluster_size", 0.0)),
                    "deterministic_dominance": 1.0 if event.get("deterministic_dominance") else 0.0,
                    "uncertainty": float(event.get("uncertainty", 1.0)),
                    "target_wrong": 0.0 if event.get("verified") else 1.0,
                }
            )

        return rows

    def _expand_trap_vault(self, events: List[Dict]) -> Dict:
        failure_phrases = Counter()

        for event in events:
            if event.get("event_type") != "solve_result":
                continue
            if event.get("verified"):
                continue

            reason = str(event.get("failure_reason", "")).lower()
            for phrase in (
                "domain",
                "extraneous",
                "unit",
                "sign",
                "boundary",
                "wrong root",
            ):
                if phrase in reason:
                    failure_phrases[phrase] += 1

        added = 0
        for phrase, count in failure_phrases.items():
            if count < 2:
                continue
            pattern = phrase.replace(" ", "\\s+")
            hint = f"Historical failures indicate '{phrase}' errors. Double-check this before final answer."
            self.vault.add_trap(pattern=pattern, hint=hint, weight=min(2.0, 0.5 + 0.1 * count))
            added += 1

        return {"trap_patterns_added": added, "candidate_phrases": dict(failure_phrases)}

    def _provider_ranking_snapshot(self) -> List[Dict]:
        return self.stats.weekly_recompute_rankings()

    def _detect_statistical_drift(self, events: List[Dict]) -> Dict:
        rows = [e for e in events if e.get("event_type") == "solve_result"]
        if not rows:
            return {"drift_detected": False, "reason": "no_rows"}

        baseline = self._load_drift_state()

        provider_acc = {}
        provider_counts = {}
        for row in rows:
            provider = str(row.get("provider", "unknown"))
            provider_counts[provider] = provider_counts.get(provider, 0) + 1
            provider_acc[provider] = provider_acc.get(provider, 0.0) + (1.0 if row.get("verified") else 0.0)
        for provider in provider_acc:
            provider_acc[provider] /= max(1, provider_counts.get(provider, 1))

        entropy_vals = [float(r.get("entropy", 0.0)) for r in rows]
        risk_vals = [float(r.get("risk", 1.0)) for r in rows]
        entropy_mean = sum(entropy_vals) / max(1, len(entropy_vals))
        risk_mean = sum(risk_vals) / max(1, len(risk_vals))
        mini_reliability = float(self.mini_evolution.state.get("global", {}).get("ema_reliability", 0.5))

        drift_flags = []
        provider_deltas = {}
        prev_provider_acc = baseline.get("provider_accuracy", {})
        for provider, acc in provider_acc.items():
            prev = float(prev_provider_acc.get(provider, acc))
            delta = acc - prev
            provider_deltas[provider] = round(delta, 6)
            if delta <= -0.08 and provider_counts.get(provider, 0) >= 8:
                drift_flags.append(f"provider_accuracy_drop:{provider}")

        prev_entropy = float(baseline.get("entropy_mean", entropy_mean))
        prev_risk = float(baseline.get("risk_mean", risk_mean))
        prev_mini = float(baseline.get("mini_reliability", mini_reliability))
        prev_det = float(baseline.get("deterministic_pass_rate", 0.0))
        det_pass_rate = (
            sum(1.0 for r in rows if bool(r.get("deterministic_dominance"))) / max(1, len(rows))
        )

        if entropy_mean - prev_entropy >= 0.16:
            drift_flags.append("entropy_shift_up")
        if risk_mean - prev_risk >= 0.10:
            drift_flags.append("risk_shift_up")
        if mini_reliability - prev_mini <= -0.08:
            drift_flags.append("mini_reliability_drop")
        if det_pass_rate - prev_det <= -0.10:
            drift_flags.append("deterministic_pass_rate_drop")

        drift_detected = bool(drift_flags)
        recommendation = None
        if drift_detected:
            recommendation = self._prepare_routing_rebalance_recommendation(drift_flags)
            self.telemetry.append_event(
                {
                    "event_type": "drift_detection",
                    "drift_flags": drift_flags,
                    "provider_deltas": provider_deltas,
                    "entropy_mean": entropy_mean,
                    "risk_mean": risk_mean,
                    "recommendation": recommendation,
                }
            )

        new_state = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "provider_accuracy": provider_acc,
            "provider_counts": provider_counts,
            "entropy_mean": entropy_mean,
            "risk_mean": risk_mean,
            "mini_reliability": mini_reliability,
            "deterministic_pass_rate": det_pass_rate,
        }
        self._save_drift_state(new_state)

        return {
            "drift_detected": drift_detected,
            "flags": drift_flags,
            "provider_deltas": provider_deltas,
            "entropy_mean": round(entropy_mean, 6),
            "risk_mean": round(risk_mean, 6),
            "mini_reliability": round(mini_reliability, 6),
            "deterministic_pass_rate": round(det_pass_rate, 6),
            "routing_rebalance_recommended": bool(recommendation),
            "routing_rebalance_plan": recommendation,
        }

    def _prepare_routing_rebalance_recommendation(self, flags: List[str]) -> Dict:
        thresholds = self.stats.data.setdefault("routing_thresholds", {})
        proposal = {
            "high_confidence_score": max(0.05, float(thresholds.get("high_confidence_score", 0.18)) - 0.02),
            "gap_for_two_provider_mode": min(0.30, float(thresholds.get("gap_for_two_provider_mode", 0.10)) + 0.01),
            "default_arena_size": min(4, max(2, int(thresholds.get("default_arena_size", 3)))),
            "flags": list(flags),
        }
        out = Path("data/metrics/routing_rebalance_recommendation.json")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps({"ts": datetime.now(timezone.utc).isoformat(), "proposal": proposal}, indent=2), encoding="utf-8")
        return proposal

    def _load_drift_state(self) -> Dict:
        if not self.drift_state_path.exists():
            return {}
        try:
            return json.loads(self.drift_state_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save_drift_state(self, payload: Dict) -> None:
        self.drift_state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _reliable_clusters(self) -> List[str]:
        clusters = []
        for cluster, row in self.mini_evolution.state.get("cluster_stats", {}).items():
            if float(row.get("ema_reliability", 0.5)) >= 0.80 and int(row.get("total", 0)) >= 16:
                clusters.append(cluster)
        return sorted(clusters)

    def _synthetic_source_rows(self) -> List[Dict]:
        rows = []
        rows.extend(self.distillation._read_jsonl(self.distillation.training_path))
        rows.extend(self.distillation._read_jsonl(self.distillation.disagreement_path))
        return rows
