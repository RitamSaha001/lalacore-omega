"""Offline shadow traffic rollout simulator for promotion-risk estimation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))


def _norm_question(question: str) -> str:
    return " ".join(str(question or "").strip().lower().split())


class MiniTrafficSimulator:
    """Simulates staged traffic rollout (10/25/50%) using offline logs and checkpoints."""

    def __init__(self, *, output_dir: str = "data/mini_training/traffic_simulation") -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def simulate_from_rows(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        candidate_model_state: Mapping[str, Any],
        rollout_percents: Sequence[float] = (0.10, 0.25, 0.50),
    ) -> Dict[str, Any]:
        memory = dict(candidate_model_state.get("memory", {}))
        default_answer = str(candidate_model_state.get("default_answer", ""))
        default_conf = _clamp(float(candidate_model_state.get("default_confidence", 0.50)))

        total = len(rows)
        rollouts: List[Dict[str, Any]] = []
        aggregate_risk = 0.0
        for pct in rollout_percents:
            n = max(1, int(round(total * float(pct)))) if total > 0 else 0
            subset = list(rows[:n])
            if not subset:
                rollouts.append(
                    {
                        "rollout_percent": float(pct),
                        "samples": 0,
                        "regression_risk": 1.0,
                        "hard_case_regression": 1.0,
                        "calibration_shift": 1.0,
                        "concept_degradation": 1.0,
                    }
                )
                aggregate_risk += 1.0
                continue

            subset_metrics = self._evaluate_subset(
                subset,
                memory=memory,
                default_answer=default_answer,
                default_conf=default_conf,
            )
            baseline_acc = float(subset_metrics["baseline_accuracy"])
            candidate_acc = float(subset_metrics["candidate_accuracy"])
            hard_regression = float(subset_metrics["hard_case_regression"])
            calibration_shift = float(subset_metrics["calibration_shift"])
            concept_degradation = float(subset_metrics["concept_degradation"])
            regression_risk = float(subset_metrics["regression_risk"])
            aggregate_risk += regression_risk

            rollouts.append(
                {
                    "rollout_percent": float(pct),
                    "samples": int(len(subset)),
                    "baseline_accuracy": baseline_acc,
                    "candidate_accuracy": candidate_acc,
                    "regression_risk": regression_risk,
                    "hard_case_regression": hard_regression,
                    "calibration_shift": calibration_shift,
                    "concept_degradation": concept_degradation,
                    "rare_concept_degradation": concept_degradation,
                }
            )

        mean_risk = (aggregate_risk / len(rollouts)) if rollouts else 1.0
        safe = bool(all(float(item.get("regression_risk", 1.0)) <= 0.35 for item in rollouts))
        stress = self._simulate_stress_scenarios(
            rows,
            memory=memory,
            default_answer=default_answer,
            default_conf=default_conf,
        )
        stress_resilience = float(stress.get("stress_resilience_score", 0.0))
        promotion_risk_factor = _clamp(0.55 * float(mean_risk) + 0.45 * (1.0 - stress_resilience))
        report = {
            "rollouts": rollouts,
            "safe_to_promote": bool(safe),
            "regression_risk_score": float(_clamp(mean_risk)),
            "stress_scenarios": list(stress.get("scenarios", [])),
            "stress_resilience_score": stress_resilience,
            "promotion_risk_factor": float(promotion_risk_factor),
        }
        self.write_report(report)
        self.write_report(report, filename="stress_simulation_report.json")
        return report

    def simulate_from_shadow_log(
        self,
        shadow_log_path: str,
        *,
        candidate_model_state: Mapping[str, Any],
        rollout_percents: Sequence[float] = (0.10, 0.25, 0.50),
    ) -> Dict[str, Any]:
        rows = self._read_jsonl(Path(shadow_log_path))
        return self.simulate_from_rows(rows, candidate_model_state=candidate_model_state, rollout_percents=rollout_percents)

    def write_report(self, report: Mapping[str, Any], filename: str = "traffic_simulation_report.json") -> Path:
        path = self.output_dir / filename
        path.write_text(json.dumps(dict(report), indent=2, sort_keys=True), encoding="utf-8")
        return path

    def _evaluate_subset(
        self,
        subset: Sequence[Mapping[str, Any]],
        *,
        memory: Mapping[str, Any],
        default_answer: str,
        default_conf: float,
    ) -> Dict[str, float]:
        baseline_correct = 0
        candidate_correct = 0
        baseline_confidences: List[float] = []
        candidate_confidences: List[float] = []
        hard_total = 0
        baseline_hard = 0
        candidate_hard = 0
        concept_totals: Dict[str, List[int]] = {}

        for row in subset:
            question = _norm_question(str(row.get("question", "")))
            winner_answer = str(row.get("arena_winner_answer", row.get("final_answer", ""))).strip()
            winner_verified = bool(row.get("winner_verified", row.get("verified", True)))
            mini_answer = str(row.get("mini_answer", row.get("final_answer", ""))).strip()

            baseline_ok = 1 if (winner_verified and mini_answer == winner_answer) else 0
            baseline_correct += baseline_ok
            baseline_conf = _clamp(float(row.get("mini_confidence", row.get("confidence", 1.0 - float(row.get("risk", 0.5))))))
            baseline_confidences.append(baseline_conf)

            slot = memory.get(question, {})
            cand_answer = str(slot.get("final_answer", default_answer)).strip()
            cand_conf = _clamp(float(slot.get("confidence", default_conf)))
            cand_ok = 1 if (winner_verified and cand_answer == winner_answer) else 0
            candidate_correct += cand_ok
            candidate_confidences.append(cand_conf)

            hard_case = str(row.get("difficulty", "unknown")).lower() == "hard" or float(row.get("risk", 0.0)) >= 0.65
            if hard_case:
                hard_total += 1
                baseline_hard += baseline_ok
                candidate_hard += cand_ok

            clusters = row.get("concept_cluster", [])
            if isinstance(clusters, list):
                for cluster in clusters:
                    key = str(cluster).strip().lower()
                    if not key:
                        continue
                    concept_totals.setdefault(key, [0, 0, 0, 0])
                    concept_totals[key][0] += baseline_ok
                    concept_totals[key][1] += 1
                    concept_totals[key][2] += cand_ok
                    concept_totals[key][3] += 1

        size = max(1, len(subset))
        baseline_acc = baseline_correct / size
        candidate_acc = candidate_correct / size
        baseline_hard_acc = (baseline_hard / hard_total) if hard_total else baseline_acc
        candidate_hard_acc = (candidate_hard / hard_total) if hard_total else candidate_acc
        hard_regression = max(0.0, baseline_hard_acc - candidate_hard_acc)
        calibration_shift = abs(
            (sum(candidate_confidences) / len(candidate_confidences))
            - (sum(baseline_confidences) / len(baseline_confidences))
        ) if baseline_confidences and candidate_confidences else 0.0
        concept_drops: List[float] = []
        for values in concept_totals.values():
            base = (values[0] / values[1]) if values[1] else 0.0
            cand = (values[2] / values[3]) if values[3] else 0.0
            concept_drops.append(max(0.0, base - cand))
        concept_degradation = (sum(concept_drops) / len(concept_drops)) if concept_drops else 0.0
        regression = max(0.0, baseline_acc - candidate_acc)
        regression_risk = _clamp(
            0.45 * regression
            + 0.25 * hard_regression
            + 0.15 * calibration_shift
            + 0.15 * concept_degradation
        )
        return {
            "baseline_accuracy": float(baseline_acc),
            "candidate_accuracy": float(candidate_acc),
            "regression_risk": float(regression_risk),
            "hard_case_regression": float(hard_regression),
            "calibration_shift": float(calibration_shift),
            "concept_degradation": float(concept_degradation),
        }

    def _simulate_stress_scenarios(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        memory: Mapping[str, Any],
        default_answer: str,
        default_conf: float,
    ) -> Dict[str, Any]:
        scenarios = self._build_stress_scenarios(rows)
        scored: List[Dict[str, Any]] = []
        for scenario in scenarios:
            subset = list(scenario.get("rows", []))
            if not subset:
                scored.append(
                    {
                        "name": str(scenario.get("name", "unknown")),
                        "samples": 0,
                        "regression_risk": 1.0,
                        "resilience": 0.0,
                    }
                )
                continue

            metrics = self._evaluate_subset(
                subset,
                memory=memory,
                default_answer=default_answer,
                default_conf=default_conf,
            )
            risk = float(metrics["regression_risk"])
            scored.append(
                {
                    "name": str(scenario.get("name", "unknown")),
                    "samples": int(len(subset)),
                    "regression_risk": float(risk),
                    "resilience": float(_clamp(1.0 - risk)),
                    "baseline_accuracy": float(metrics["baseline_accuracy"]),
                    "candidate_accuracy": float(metrics["candidate_accuracy"]),
                }
            )

        if not scored:
            return {"scenarios": [], "stress_resilience_score": 0.0}
        mean_risk = sum(float(item.get("regression_risk", 1.0)) for item in scored) / len(scored)
        return {
            "scenarios": scored,
            "stress_resilience_score": float(_clamp(1.0 - mean_risk)),
        }

    def _build_stress_scenarios(self, rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        items = list(rows)
        if not items:
            return []

        frequency = self._cluster_frequency(items)
        rare_clusters = self._rare_clusters(frequency)
        by_entropy = sorted(items, key=lambda row: float(row.get("entropy", 0.0)), reverse=True)
        n_entropy = max(1, int(round(len(items) * 0.35)))
        n_hard = max(1, int(round(len(items) * 0.35)))
        n_disagreement = max(1, int(round(len(items) * 0.35)))

        entropy_spike = by_entropy[:n_entropy]
        rare_burst = [
            row for row in items if isinstance(row.get("concept_cluster"), list)
            and any(str(cluster).strip().lower() in rare_clusters for cluster in row.get("concept_cluster", []))
        ]
        hard_burst = [
            row for row in items
            if str(row.get("difficulty", "unknown")).lower() == "hard" or float(row.get("risk", 0.0)) >= 0.65
        ][:n_hard]
        disagreement_burst = [
            row for row in items
            if float(row.get("disagreement", 0.0)) > 0.0 or bool(row.get("agreement_with_winner") is False)
        ][:n_disagreement]
        combinatorics_wave = [
            row for row in items if self._is_adversarial_combinatorics(str(row.get("question", "")))
        ]

        return [
            {"name": "entropy_spikes", "rows": entropy_spike},
            {"name": "rare_cluster_bursts", "rows": rare_burst},
            {"name": "hard_case_burst", "rows": hard_burst},
            {"name": "disagreement_burst", "rows": disagreement_burst},
            {"name": "adversarial_combinatorics_wave", "rows": combinatorics_wave},
        ]

    def _cluster_frequency(self, rows: Sequence[Mapping[str, Any]]) -> Dict[str, int]:
        freq: Dict[str, int] = {}
        for row in rows:
            clusters = row.get("concept_cluster", [])
            if not isinstance(clusters, list):
                continue
            for cluster in clusters:
                key = str(cluster).strip().lower()
                if not key:
                    continue
                freq[key] = freq.get(key, 0) + 1
        return freq

    def _rare_clusters(self, frequency: Mapping[str, int]) -> set[str]:
        if not frequency:
            return set()
        values = sorted(int(v) for v in frequency.values())
        cutoff = values[max(0, int(len(values) * 0.25) - 1)] if values else 0
        return {cluster for cluster, freq in frequency.items() if int(freq) <= int(max(1, cutoff))}

    def _is_adversarial_combinatorics(self, question: str) -> bool:
        q = str(question or "").lower()
        needles = (
            "how many",
            "subset",
            "permutation",
            "arrangement",
            "no repetition",
            "exactly one even",
            "greater than",
            "books",
            "digit numbers",
        )
        return any(token in q for token in needles)

    def _read_jsonl(self, path: Path) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not path.exists():
            return rows
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                text = line.strip()
                if not text:
                    continue
                payload = json.loads(text)
                if isinstance(payload, dict):
                    rows.append(payload)
        return rows
