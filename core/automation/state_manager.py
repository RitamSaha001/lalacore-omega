from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_ts(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None


class AutomationStateManager:
    """
    Persistent automation state store for crash-resume and checkpointing.

    Backward-compatible additive store:
    - LC9_AUTOMATION_STATE.json
    """

    def __init__(self, path: str = "data/lc9/LC9_AUTOMATION_STATE.json"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self.state = self._load()

    def _default_state(self) -> Dict[str, Any]:
        return {
            "version": 1,
            "jobs": {},
            "checkpoints": {
                "feeder": {
                    "last_processed_id": 0,
                    "last_checkpoint_ts": None,
                    "last_processed_hash": None,
                },
                "replay": {
                    "last_replay_checkpoint": None,
                    "last_replay_count": 0,
                },
                "dataset": {
                    "last_export_ts": None,
                    "last_export_count": 0,
                },
                "scheduler": {
                    "last_tick_ts": None,
                },
            },
        }

    def _load(self) -> Dict[str, Any]:
        base = self._default_state()
        if not self.path.exists():
            return base
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                base.update(payload)
        except Exception:
            pass

        jobs = base.setdefault("jobs", {})
        if not isinstance(jobs, dict):
            base["jobs"] = {}

        checkpoints = base.setdefault("checkpoints", {})
        for key, row in self._default_state()["checkpoints"].items():
            cur = checkpoints.setdefault(key, {})
            if isinstance(cur, dict):
                merged = dict(row)
                merged.update(cur)
                checkpoints[key] = merged
            else:
                checkpoints[key] = dict(row)

        return base

    def _save(self) -> None:
        self.path.write_text(json.dumps(self.state, indent=2, sort_keys=True), encoding="utf-8")

    def _job_row(self, job: str) -> Dict[str, Any]:
        jobs = self.state.setdefault("jobs", {})
        row = jobs.setdefault(
            str(job),
            {
                "status": "idle",
                "last_run_id": None,
                "last_trigger": None,
                "last_start_ts": None,
                "last_end_ts": None,
                "last_duration_s": 0.0,
                "last_error": None,
                "current_stage": None,
                "completed_stages": [],
                "retry_count": 0,
            },
        )
        row.setdefault("completed_stages", [])
        return row

    def start_job(self, job: str, run_id: str, trigger: str, *, resume: bool = False) -> Dict[str, Any]:
        with self._lock:
            row = self._job_row(job)
            if not resume:
                row["completed_stages"] = []
                row["retry_count"] = 0
                row["current_stage"] = None
            row["status"] = "running"
            row["last_run_id"] = str(run_id)
            row["last_trigger"] = str(trigger)
            row["last_start_ts"] = _utc_now()
            row["last_error"] = None
            self._save()
            return dict(row)

    def mark_stage_complete(self, job: str, stage: str) -> None:
        with self._lock:
            row = self._job_row(job)
            stage = str(stage)
            row["current_stage"] = stage
            completed = list(row.get("completed_stages", []))
            if stage not in completed:
                completed.append(stage)
            row["completed_stages"] = completed
            self._save()

    def mark_job_complete(self, job: str, *, duration_s: float) -> Dict[str, Any]:
        with self._lock:
            row = self._job_row(job)
            row["status"] = "completed"
            row["last_end_ts"] = _utc_now()
            row["last_duration_s"] = float(duration_s)
            row["last_error"] = None
            self._save()
            return dict(row)

    def mark_job_failure(self, job: str, *, error: str, duration_s: float | None = None) -> Dict[str, Any]:
        with self._lock:
            row = self._job_row(job)
            row["status"] = "failed"
            row["last_end_ts"] = _utc_now()
            if duration_s is not None:
                row["last_duration_s"] = float(duration_s)
            row["last_error"] = str(error)[:500]
            row["retry_count"] = int(row.get("retry_count", 0)) + 1
            self._save()
            return dict(row)

    def recover_stale_job(self, job: str, *, stale_after_minutes: int = 240) -> bool:
        with self._lock:
            row = self._job_row(job)
            if str(row.get("status")) != "running":
                return False
            started = _parse_ts(row.get("last_start_ts"))
            if started is None:
                row["status"] = "failed"
                row["last_error"] = "recovered_stale_running_state"
                self._save()
                return True

            age = datetime.now(timezone.utc) - started
            if age < timedelta(minutes=max(1, int(stale_after_minutes))):
                return False

            row["status"] = "failed"
            row["last_end_ts"] = _utc_now()
            row["last_error"] = "recovered_stale_running_state"
            row["retry_count"] = int(row.get("retry_count", 0)) + 1
            self._save()
            return True

    def get_job(self, job: str) -> Dict[str, Any]:
        with self._lock:
            return dict(self._job_row(job))

    def completed_stages(self, job: str) -> list[str]:
        with self._lock:
            row = self._job_row(job)
            return [str(x) for x in row.get("completed_stages", [])]

    def checkpoint(self, scope: str, **updates: Any) -> Dict[str, Any]:
        with self._lock:
            checkpoints = self.state.setdefault("checkpoints", {})
            row = checkpoints.setdefault(str(scope), {})
            row.update(updates)
            row["updated_ts"] = _utc_now()
            self._save()
            return dict(row)

    def checkpoint_row(self, scope: str) -> Dict[str, Any]:
        with self._lock:
            row = self.state.setdefault("checkpoints", {}).setdefault(str(scope), {})
            return dict(row)

    def get_checkpoint_value(self, scope: str, key: str, default: Any = None) -> Any:
        with self._lock:
            row = self.state.setdefault("checkpoints", {}).setdefault(str(scope), {})
            return row.get(str(key), default)

    def should_run_weekly(self, job: str, *, now: datetime | None = None, min_interval_days: int = 7) -> bool:
        now = now or datetime.now(timezone.utc)
        with self._lock:
            row = self._job_row(job)
            last_success = _parse_ts(row.get("last_end_ts")) if str(row.get("status")) == "completed" else None
            if last_success is None:
                return True
            return (now - last_success) >= timedelta(days=max(1, int(min_interval_days)))

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps(self.state))
