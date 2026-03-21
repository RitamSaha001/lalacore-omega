from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class AutomationLogger:
    """
    Structured automation logger.
    Stores entries in LC9_AUTOMATION_LOGS.
    """

    def __init__(self, path: str = "data/lc9/LC9_AUTOMATION_LOGS.jsonl"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def job_start(self, *, job: str, trigger: str, run_id: str, extra: Dict | None = None) -> None:
        self._append(
            {
                "ts": _now(),
                "event_type": "job_start",
                "job": str(job),
                "trigger": str(trigger),
                "run_id": str(run_id),
                "extra": extra or {},
            }
        )

    def job_complete(self, *, job: str, run_id: str, duration_s: float, items_processed: int, extra: Dict | None = None) -> None:
        self._append(
            {
                "ts": _now(),
                "event_type": "job_complete",
                "job": str(job),
                "run_id": str(run_id),
                "duration_s": float(duration_s),
                "items_processed": int(items_processed),
                "extra": extra or {},
            }
        )

    def job_failure(self, *, job: str, run_id: str, duration_s: float, error_type: str, message: str, extra: Dict | None = None) -> None:
        self._append(
            {
                "ts": _now(),
                "event_type": "job_failure",
                "job": str(job),
                "run_id": str(run_id),
                "duration_s": float(duration_s),
                "error_type": str(error_type),
                "message": str(message)[:500],
                "extra": extra or {},
            }
        )

    def event(self, event_type: str, payload: Dict) -> None:
        self._append({"ts": _now(), "event_type": str(event_type), **(payload or {})})

    def _append(self, row: Dict) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")

