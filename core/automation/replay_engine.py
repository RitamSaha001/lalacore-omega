from __future__ import annotations

import hashlib
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence

from core.automation.logging import AutomationLogger
from core.automation.state_manager import AutomationStateManager
from core.lalacore_x.mini_distillation import LC9DistillationHub
from core.lalacore_x.mini_evolution import MiniEvolutionEngine
from core.lalacore_x.recovery import retry_async
from core.lalacore_x.replay import FailureReplayMemory
from core.lalacore_x.telemetry import DEFAULT_TELEMETRY
from core.lalacore_x.token_budget import TokenBudgetGuardian
from core.solver import solve_question


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_ts(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None


def _norm(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


class AutomatedReplayEngine:
    """
    Weekly replay selector + optional pipeline replay executor.

    Uses existing stores only:
    - failure replay memory
    - disagreement memory
    - runtime telemetry solve_result rows
    - feeder replay cases
    """

    def __init__(
        self,
        *,
        replay_runs_path: str = "data/replay/weekly_replay_runs.jsonl",
        feeder_replay_path: str = "data/replay/feeder_cases.jsonl",
        logger: AutomationLogger | None = None,
        state_manager: AutomationStateManager | None = None,
        replay_memory: FailureReplayMemory | None = None,
        distillation: LC9DistillationHub | None = None,
        mini_evolution: MiniEvolutionEngine | None = None,
        token_guardian: TokenBudgetGuardian | None = None,
    ):
        self.replay_runs_path = Path(replay_runs_path)
        self.replay_runs_path.parent.mkdir(parents=True, exist_ok=True)

        self.feeder_replay_path = Path(feeder_replay_path)
        self.feeder_replay_path.parent.mkdir(parents=True, exist_ok=True)

        self.logger = logger or AutomationLogger()
        self.state = state_manager or AutomationStateManager()
        self.replay_memory = replay_memory or FailureReplayMemory()
        self.distillation = distillation or LC9DistillationHub()
        self.mini_evolution = mini_evolution or MiniEvolutionEngine()
        self.token_guardian = token_guardian or TokenBudgetGuardian()

        self.replay_batch_cap = max(1, int(os.getenv("LC9_REPLAY_BATCH_CAP", "18")))
        self.per_bucket_cap = max(1, int(os.getenv("LC9_REPLAY_BUCKET_CAP", "4")))
        self.calibration_risk_threshold = float(os.getenv("LC9_REPLAY_CALIBRATION_RISK", "0.60"))

    async def run_weekly_replay(
        self,
        *,
        max_items: int | None = None,
        execute_pipeline: bool = True,
        trigger: str = "scheduled",
    ) -> Dict[str, Any]:
        checkpoint_ts = self.state.get_checkpoint_value("replay", "last_replay_checkpoint")
        since = _parse_ts(checkpoint_ts)

        requested = self.replay_batch_cap if max_items is None else max(1, int(max_items))
        scaled = max(1, int(round(requested * float(self.token_guardian.replay_intensity_scale()))))
        budget = min(self.replay_batch_cap, scaled)

        candidates = self._build_candidates(since=since)
        selected = self._select_replay_batch(candidates, max_items=budget)

        runs = []
        failures = 0
        if execute_pipeline:
            for row in selected:
                try:
                    result = await retry_async(
                        lambda: solve_question(str(row.get("question", ""))),
                        component="automation_replay",
                        operation="solve_question",
                        telemetry=self._runtime_telemetry_proxy(),
                        max_attempts=2,
                        base_delay_s=0.2,
                    )
                    summary = self._result_summary(result)
                    runs.append(
                        {
                            "ts": _utc_now(),
                            "question_hash": row.get("question_hash"),
                            "question": row.get("question"),
                            "subject": row.get("subject"),
                            "difficulty": row.get("difficulty"),
                            "concept_clusters": row.get("concept_clusters", []),
                            "source": row.get("source"),
                            "priority": float(row.get("priority", 0.0)),
                            "verified": bool(summary.get("verified", False)),
                            "winner_provider": summary.get("winner_provider", ""),
                            "risk": float(summary.get("risk", 1.0)),
                            "entropy": float(summary.get("entropy", 0.0)),
                        }
                    )
                except Exception as exc:
                    failures += 1
                    runs.append(
                        {
                            "ts": _utc_now(),
                            "question_hash": row.get("question_hash"),
                            "question": row.get("question"),
                            "subject": row.get("subject"),
                            "difficulty": row.get("difficulty"),
                            "concept_clusters": row.get("concept_clusters", []),
                            "source": row.get("source"),
                            "priority": float(row.get("priority", 0.0)),
                            "verified": False,
                            "winner_provider": "",
                            "risk": 1.0,
                            "entropy": float(row.get("entropy", 0.0)),
                            "error": str(exc)[:400],
                        }
                    )
        else:
            for row in selected:
                runs.append(
                    {
                        "ts": _utc_now(),
                        "question_hash": row.get("question_hash"),
                        "question": row.get("question"),
                        "subject": row.get("subject"),
                        "difficulty": row.get("difficulty"),
                        "concept_clusters": row.get("concept_clusters", []),
                        "source": row.get("source"),
                        "priority": float(row.get("priority", 0.0)),
                        "dry_run": True,
                    }
                )

        for run in runs:
            self._append_jsonl(self.replay_runs_path, run)

        self.state.checkpoint(
            "replay",
            last_replay_checkpoint=_utc_now(),
            last_replay_count=len(selected),
            last_replay_failures=failures,
            last_replay_trigger=str(trigger),
        )

        source_counts = defaultdict(int)
        for row in selected:
            source_counts[str(row.get("source", "unknown"))] += 1

        report = {
            "requested": requested,
            "budget": budget,
            "candidate_pool": len(candidates),
            "selected": len(selected),
            "executed": bool(execute_pipeline),
            "execution_failures": failures,
            "source_mix": dict(source_counts),
            "output": str(self.replay_runs_path),
        }
        self.logger.event("weekly_replay_run", {"trigger": str(trigger), **report})
        return report

    def build_weekly_dataset_inputs(self, *, since: datetime | None = None, max_rows: int = 4000) -> List[Dict[str, Any]]:
        rows = self._build_candidates(since=since)
        rows.sort(key=lambda r: float(r.get("priority", 0.0)), reverse=True)
        return rows[: max(1, int(max_rows))]

    # -----------------------------
    # Candidate collection
    # -----------------------------

    def _build_candidates(self, *, since: datetime | None) -> List[Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []
        candidates.extend(self._from_failures())
        candidates.extend(self._from_disagreements(since=since))
        candidates.extend(self._from_calibration_risk(since=since))
        candidates.extend(self._from_feeder_cases(since=since))

        dedup: Dict[str, Dict[str, Any]] = {}
        for row in candidates:
            q = str(row.get("question", "")).strip()
            if not q:
                continue
            qh = row.get("question_hash") or self._question_hash(q)
            row["question_hash"] = qh
            current = dedup.get(qh)
            if current is None or float(row.get("priority", 0.0)) > float(current.get("priority", 0.0)):
                dedup[qh] = row

        out = list(dedup.values())
        out.sort(key=lambda r: float(r.get("priority", 0.0)), reverse=True)
        return out

    def _from_failures(self) -> List[Dict[str, Any]]:
        rows = self.replay_memory.read_failures()
        out = []
        for row in rows:
            question = str(row.get("question", "")).strip()
            if not question:
                continue
            subject = str(row.get("subject", "general")).lower().strip() or "general"
            difficulty = str(row.get("difficulty", "unknown")).lower().strip() or "unknown"
            clusters = self._clusters(row.get("concept_clusters", []))
            risk = float(row.get("risk", 1.0) or 1.0)
            entropy = float(row.get("entropy", 0.0) or 0.0)
            disagreement = float(row.get("disagreement", 0.0) or 0.0)
            det_fail = bool(row.get("deterministic_fail", True))
            priority = self._priority(
                deterministic_fail=det_fail,
                risk=risk,
                entropy=entropy,
                disagreement=disagreement,
                source="failure_memory",
                concept_clusters=clusters,
            )
            out.append(
                {
                    "question": question,
                    "subject": subject,
                    "difficulty": difficulty,
                    "concept_clusters": clusters,
                    "source": "failure_memory",
                    "priority": priority,
                    "risk": risk,
                    "entropy": entropy,
                    "disagreement": disagreement,
                    "deterministic_fail": det_fail,
                    "ts": row.get("ts"),
                }
            )
        return out

    def _from_disagreements(self, *, since: datetime | None) -> List[Dict[str, Any]]:
        rows = self._read_jsonl(self.distillation.disagreement_path)
        out = []
        for row in rows:
            ts = _parse_ts(row.get("ts"))
            if since is not None and ts is not None and ts < since:
                continue
            question = str(row.get("question", "")).strip()
            if not question:
                continue
            subject = str(row.get("subject", "general")).lower().strip() or "general"
            difficulty = str(row.get("difficulty", "unknown")).lower().strip() or "unknown"
            clusters = self._clusters(row.get("concept_cluster", []))
            entropy = float(row.get("entropy", 0.0) or 0.0)
            risk = float(row.get("uncertainty", 0.7) or 0.7)
            disagreement = 1.0 if row.get("case") else 0.5
            det_fail = not bool(row.get("winner_verified", False))
            priority = self._priority(
                deterministic_fail=det_fail,
                risk=risk,
                entropy=entropy,
                disagreement=disagreement,
                source="disagreement_memory",
                concept_clusters=clusters,
            )
            out.append(
                {
                    "question": question,
                    "subject": subject,
                    "difficulty": difficulty,
                    "concept_clusters": clusters,
                    "source": "disagreement_memory",
                    "priority": priority,
                    "risk": risk,
                    "entropy": entropy,
                    "disagreement": disagreement,
                    "deterministic_fail": det_fail,
                    "ts": row.get("ts"),
                }
            )
        return out

    def _from_calibration_risk(self, *, since: datetime | None) -> List[Dict[str, Any]]:
        rows = DEFAULT_TELEMETRY.read_events(limit=30000)
        out = []
        for row in rows:
            if str(row.get("event_type", "")) != "solve_result":
                continue
            ts = _parse_ts(row.get("ts"))
            if since is not None and ts is not None and ts < since:
                continue

            risk = float(row.get("risk", 1.0) or 1.0)
            if risk < self.calibration_risk_threshold:
                continue

            question = str(row.get("question", "")).strip()
            if not question:
                continue
            subject = str(row.get("subject", "general")).lower().strip() or "general"
            difficulty = str(row.get("difficulty", "unknown")).lower().strip() or "unknown"
            entropy = float(row.get("entropy", 0.0) or 0.0)
            disagreement = float(row.get("disagreement", 0.0) or 0.0)
            det_fail = not bool(row.get("verified", False))
            priority = self._priority(
                deterministic_fail=det_fail,
                risk=risk,
                entropy=entropy,
                disagreement=disagreement,
                source="calibration_risk",
                concept_clusters=[],
            )
            out.append(
                {
                    "question": question,
                    "subject": subject,
                    "difficulty": difficulty,
                    "concept_clusters": [],
                    "source": "calibration_risk",
                    "priority": priority,
                    "risk": risk,
                    "entropy": entropy,
                    "disagreement": disagreement,
                    "deterministic_fail": det_fail,
                    "ts": row.get("ts"),
                }
            )
        return out

    def _from_feeder_cases(self, *, since: datetime | None) -> List[Dict[str, Any]]:
        rows = self._read_jsonl(self.feeder_replay_path)
        out = []
        for row in rows:
            ts = _parse_ts(row.get("ts"))
            if since is not None and ts is not None and ts < since:
                continue
            question = str(row.get("question", "")).strip()
            if not question:
                continue
            subject = str(row.get("subject", "general")).lower().strip() or "general"
            difficulty = str(row.get("difficulty", "unknown")).lower().strip() or "unknown"
            clusters = self._clusters(row.get("concept_clusters", []))
            risk = float(row.get("risk", 1.0) or 1.0)
            entropy = float(row.get("entropy", 0.0) or 0.0)
            disagreement = float(row.get("disagreement", 0.0) or 0.0)
            det_fail = bool(row.get("deterministic_fail", False))
            base_priority = float(row.get("replay_priority", 0.0) or 0.0)
            priority = max(
                base_priority,
                self._priority(
                    deterministic_fail=det_fail,
                    risk=risk,
                    entropy=entropy,
                    disagreement=disagreement,
                    source="feeder",
                    concept_clusters=clusters,
                ),
            )
            out.append(
                {
                    "question": question,
                    "subject": subject,
                    "difficulty": difficulty,
                    "concept_clusters": clusters,
                    "source": "feeder",
                    "priority": priority,
                    "risk": risk,
                    "entropy": entropy,
                    "disagreement": disagreement,
                    "deterministic_fail": det_fail,
                    "ts": row.get("ts"),
                }
            )
        return out

    def _select_replay_batch(self, rows: Sequence[Dict[str, Any]], *, max_items: int) -> List[Dict[str, Any]]:
        if not rows:
            return []

        buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            key = f"{row.get('subject', 'general')}|{row.get('difficulty', 'unknown')}"
            buckets[key].append(row)

        for key in buckets:
            buckets[key].sort(key=lambda r: float(r.get("priority", 0.0)), reverse=True)

        selected: List[Dict[str, Any]] = []
        per_bucket = defaultdict(int)
        keys = sorted(buckets.keys())
        cursor = 0

        while len(selected) < max_items and keys:
            key = keys[cursor % len(keys)]
            cursor += 1
            if per_bucket[key] >= self.per_bucket_cap:
                if all(per_bucket[k] >= self.per_bucket_cap or not buckets[k] for k in keys):
                    break
                continue
            if not buckets[key]:
                if all(not buckets[k] for k in keys):
                    break
                continue
            row = buckets[key].pop(0)
            selected.append(row)
            per_bucket[key] += 1

        if len(selected) < max_items:
            leftovers = []
            for key in keys:
                leftovers.extend(buckets[key])
            leftovers.sort(key=lambda r: float(r.get("priority", 0.0)), reverse=True)
            for row in leftovers:
                if len(selected) >= max_items:
                    break
                selected.append(row)

        return selected

    # -----------------------------
    # Helpers
    # -----------------------------

    def _priority(
        self,
        *,
        deterministic_fail: bool,
        risk: float,
        entropy: float,
        disagreement: float,
        source: str,
        concept_clusters: Sequence[str],
    ) -> float:
        source_bonus = {
            "failure_memory": 0.55,
            "disagreement_memory": 0.45,
            "calibration_risk": 0.35,
            "feeder": 0.40,
        }.get(str(source), 0.25)

        cluster_weak = self._cluster_weakness(concept_clusters)
        score = 0.0
        score += 1.35 if deterministic_fail else 0.20
        score += 1.00 * max(0.0, min(1.0, risk))
        score += 0.72 * max(0.0, min(1.0, entropy))
        score += 0.48 * max(0.0, min(1.0, disagreement))
        score += 0.60 * cluster_weak
        score += source_bonus
        return round(score, 6)

    def _cluster_weakness(self, concept_clusters: Sequence[str]) -> float:
        if not concept_clusters:
            return 0.5
        stats = self.mini_evolution.state.get("cluster_stats", {})
        vals = []
        for cluster in concept_clusters:
            row = stats.get(str(cluster).lower().strip(), {})
            reliability = float(row.get("ema_reliability", 0.5))
            vals.append(1.0 - max(0.0, min(1.0, reliability)))
        if not vals:
            return 0.5
        return max(0.0, min(1.0, sum(vals) / len(vals)))

    def _clusters(self, raw: Sequence[str] | Any) -> List[str]:
        if not isinstance(raw, (list, tuple)):
            return []
        out = []
        for val in raw:
            c = str(val).strip().lower()
            if not c:
                continue
            if c not in out:
                out.append(c)
        return out[:16]

    def _question_hash(self, question: str) -> str:
        return hashlib.sha1(_norm(question).encode("utf-8")).hexdigest()

    def _result_summary(self, result: Dict[str, Any]) -> Dict[str, Any]:
        arena = result.get("arena", {}) if isinstance(result, dict) else {}
        verification = result.get("verification", {}) if isinstance(result, dict) else {}
        return {
            "verified": bool(verification.get("verified", False)),
            "risk": float(verification.get("risk_score", verification.get("risk", 1.0)) or 1.0),
            "winner_provider": str(result.get("winner_provider", "") or ""),
            "entropy": float(arena.get("entropy", 0.0) or 0.0),
        }

    def _read_jsonl(self, path: Path) -> List[Dict[str, Any]]:
        if not path.exists():
            return []
        out: List[Dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return out

    def _append_jsonl(self, path: Path, row: Dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")

    def _runtime_telemetry_proxy(self):
        class _Proxy:
            @staticmethod
            def log_recovery_attempt(**kwargs):
                return None

        return _Proxy()
