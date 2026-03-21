from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List

from core.automation.logging import AutomationLogger
from core.automation.replay_engine import AutomatedReplayEngine
from core.automation.state_manager import AutomationStateManager
from core.lalacore_x.mini_distillation import LC9DistillationHub


class AutomationDatasetDistiller:
    """
    Weekly automation distiller.

    Curates dataset inputs from replay/disagreement/feeder memories, then
    delegates final export to LC9DistillationHub for backward compatibility.
    """

    def __init__(
        self,
        *,
        distillation: LC9DistillationHub | None = None,
        replay_engine: AutomatedReplayEngine | None = None,
        logger: AutomationLogger | None = None,
        state_manager: AutomationStateManager | None = None,
    ):
        self.distillation = distillation or LC9DistillationHub()
        self.replay_engine = replay_engine or AutomatedReplayEngine()
        self.logger = logger or AutomationLogger()
        self.state = state_manager or AutomationStateManager()

    def run_weekly(
        self,
        *,
        trigger: str = "scheduled",
        max_replay_rows: int = 5000,
        min_margin: float = 0.06,
        max_uncertainty: float = 0.72,
    ) -> Dict[str, Any]:
        since = self._checkpoint_ts()
        raw_inputs = self.replay_engine.build_weekly_dataset_inputs(since=since, max_rows=max_replay_rows)
        replay_rows = self._to_distillation_replay_rows(raw_inputs)

        report = self.distillation.finalize_weekly_dataset(
            replay_rows=replay_rows,
            min_margin=min_margin,
            max_uncertainty=max_uncertainty,
        )

        by_source = Counter(str(r.get("source", "unknown")) for r in raw_inputs)
        self.state.checkpoint(
            "dataset",
            last_export_ts=datetime.now(timezone.utc).isoformat(),
            last_export_count=int(report.get("total", 0)),
            last_replay_rows=len(replay_rows),
        )

        out = {
            **report,
            "input_rows": len(raw_inputs),
            "filtered_replay_rows": len(replay_rows),
            "input_source_mix": dict(by_source),
        }

        self.logger.event("automation_dataset_distilled", {"trigger": str(trigger), **out})
        return out

    def _checkpoint_ts(self):
        ts = self.state.get_checkpoint_value("dataset", "last_export_ts")
        if not ts:
            return None
        try:
            return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            return None

    def _to_distillation_replay_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []

        for row in rows:
            entropy = float(row.get("entropy", 0.0) or 0.0)
            risk = float(row.get("risk", 1.0) or 1.0)
            priority = float(row.get("priority", 0.0) or 0.0)

            # Drop highly unstable rows from distillation inputs.
            if entropy > 0.95:
                continue
            if risk > 0.98 and priority < 1.3:
                continue

            out.append(
                {
                    "ts": row.get("ts"),
                    "question": row.get("question", ""),
                    "subject": row.get("subject", "general"),
                    "difficulty": row.get("difficulty", "unknown"),
                    "concept_clusters": list(row.get("concept_clusters", [])),
                    "risk": risk,
                    "disagreement": float(row.get("disagreement", 0.0) or 0.0),
                    "entropy": entropy,
                    "provider": row.get("winner_provider", row.get("source", "unknown")),
                    "final_answer": row.get("final_answer", ""),
                    "curriculum_level": int(row.get("curriculum_level", 1) or 1),
                    "source": row.get("source", "unknown"),
                }
            )

        return out
