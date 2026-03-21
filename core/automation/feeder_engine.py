from __future__ import annotations

import asyncio
import hashlib
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence

from core.automation.logging import AutomationLogger
from core.automation.state_manager import AutomationStateManager
from core.lalacore_x.mini_evolution import MiniEvolutionEngine
from core.lalacore_x.recovery import retry_async
from core.lalacore_x.solve_pipeline import should_mark_completed
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


class FeederEngine:
    """
    Manual question feeder for Mini evolution.

    Guarantees:
    - Feeder never bypasses solve pipeline.
    - Idempotent enqueue by content hash.
    - Crash-safe retries with bounded attempts.
    - Additive stores only.
    """

    STATUS_PENDING = "Pending"
    STATUS_PROCESSING = "Processing"
    STATUS_COMPLETED = "Completed"
    STATUS_FAILED = "Failed"

    def __init__(
        self,
        queue_path: str = "data/lc9/LC9_FEEDER_QUEUE.jsonl",
        training_cases_path: str = "data/lc9/LC9_FEEDER_CASES.jsonl",
        replay_cases_path: str = "data/replay/feeder_cases.jsonl",
        *,
        logger: AutomationLogger | None = None,
        state_manager: AutomationStateManager | None = None,
        mini_evolution: MiniEvolutionEngine | None = None,
        token_guardian: TokenBudgetGuardian | None = None,
    ):
        self.queue_path = Path(queue_path)
        self.queue_path.parent.mkdir(parents=True, exist_ok=True)

        self.training_cases_path = Path(training_cases_path)
        self.training_cases_path.parent.mkdir(parents=True, exist_ok=True)

        self.replay_cases_path = Path(replay_cases_path)
        self.replay_cases_path.parent.mkdir(parents=True, exist_ok=True)

        self.logger = logger or AutomationLogger()
        self.state = state_manager or AutomationStateManager()
        self.mini_evolution = mini_evolution or MiniEvolutionEngine()
        self.token_guardian = token_guardian or TokenBudgetGuardian()

        self.max_attempts = max(1, int(os.getenv("LC9_FEEDER_MAX_ATTEMPTS", "3")))
        self.processing_timeout_minutes = max(5, int(os.getenv("LC9_FEEDER_PROCESSING_TIMEOUT_MIN", "45")))
        self.daily_cap = max(1, int(os.getenv("LC9_FEEDER_DAILY_CAP", "30")))
        self.queue_cap = max(500, int(os.getenv("LC9_FEEDER_QUEUE_CAP", "50000")))

        self._lock = threading.Lock()

    # -----------------------------
    # Public API
    # -----------------------------

    def enqueue_question(
        self,
        *,
        question: str,
        subject: str = "general",
        difficulty: str = "unknown",
        concept_cluster: Sequence[str] | None = None,
        source_tag: str = "manual",
    ) -> Dict[str, Any]:
        question = str(question or "").strip()
        if not question:
            raise ValueError("question cannot be empty")

        subject = str(subject or "general").strip().lower() or "general"
        difficulty = str(difficulty or "unknown").strip().lower() or "unknown"
        clusters = self._normalize_clusters(concept_cluster)
        item_hash = self._item_hash(question, subject, difficulty, clusters)

        with self._lock:
            rows = self._read_jsonl(self.queue_path)
            existing = self._find_by_hash(rows, item_hash)
            if existing is not None:
                return {
                    "added": False,
                    "duplicate": True,
                    "queue_item": self._public_row(existing),
                }

            row = {
                "id": self._next_id(rows),
                "item_hash": item_hash,
                "question": question,
                "subject": subject,
                "difficulty": difficulty,
                "concept_cluster": clusters,
                "source_tag": str(source_tag or "manual"),
                "status": self.STATUS_PENDING,
                "attempts": 0,
                "max_attempts": self.max_attempts,
                "created_ts": _utc_now(),
                "updated_ts": _utc_now(),
                "last_error": None,
                "processed_ts": None,
                "result_summary": {},
            }
            rows.append(row)
            rows = self._apply_queue_cap(rows)
            self._write_jsonl(self.queue_path, rows)

        self.logger.event(
            "feeder_enqueue",
            {
                "feeder_id": row["id"],
                "item_hash": item_hash,
                "subject": subject,
                "difficulty": difficulty,
                "source_tag": row["source_tag"],
            },
        )
        return {
            "added": True,
            "duplicate": False,
            "queue_item": self._public_row(row),
        }

    async def process_pending(
        self,
        *,
        max_items: int = 10,
        trigger: str = "manual",
    ) -> Dict[str, Any]:
        max_items = max(1, int(max_items))

        self._recover_stale_processing()

        budget_scale = float(self.token_guardian.replay_intensity_scale())
        scaled_max = max(1, int(round(max_items * budget_scale)))
        remaining_daily = max(0, self.daily_cap - self._processed_today_count())
        budget = min(scaled_max, remaining_daily)

        if budget <= 0:
            return {
                "processed": 0,
                "completed": 0,
                "failed": 0,
                "skipped_daily_cap": True,
                "daily_cap": self.daily_cap,
                "remaining_daily": remaining_daily,
            }

        processed = 0
        completed = 0
        failed = 0
        start_ts = datetime.now(timezone.utc)

        while processed < budget:
            row = self._reserve_next_pending()
            if row is None:
                break

            processed += 1
            feeder_id = int(row["id"])
            try:
                result = await retry_async(
                    lambda: solve_question(str(row.get("question", ""))),
                    component="automation_feeder",
                    operation="solve_question",
                    telemetry=self._runtime_telemetry_proxy(),
                    max_attempts=2,
                    base_delay_s=0.2,
                )
                quality_gate = should_mark_completed(result)
                if bool(quality_gate.get("complete", False)):
                    self._mark_completed(row, result)
                    completed += 1
                    self.state.checkpoint(
                        "feeder",
                        last_processed_id=feeder_id,
                        last_processed_hash=str(row.get("item_hash")),
                        last_checkpoint_ts=_utc_now(),
                    )
                else:
                    reasons = list(quality_gate.get("reasons", []))
                    reason_text = ",".join(reasons) if reasons else "quality_gate_failed"
                    force_terminal = any(reason in {"plausibility_failed", "verification_failed_high_risk"} for reason in reasons)
                    row = self._mark_failed(row, f"quality_gate:{reason_text}", force_terminal=force_terminal)
                    if str(row.get("status")) == self.STATUS_FAILED:
                        failed += 1
            except Exception as exc:
                row = self._mark_failed(row, str(exc))
                if str(row.get("status")) == self.STATUS_FAILED:
                    failed += 1

        duration_s = max(0.0, (datetime.now(timezone.utc) - start_ts).total_seconds())
        self.logger.event(
            "feeder_process_summary",
            {
                "trigger": str(trigger),
                "processed": processed,
                "completed": completed,
                "failed": failed,
                "duration_s": round(duration_s, 6),
                "budget": budget,
                "daily_cap": self.daily_cap,
            },
        )

        return {
            "processed": processed,
            "completed": completed,
            "failed": failed,
            "duration_s": round(duration_s, 6),
            "budget": budget,
            "daily_cap": self.daily_cap,
            "remaining_daily": max(0, self.daily_cap - self._processed_today_count()),
        }

    def status(self, *, limit: int = 20) -> Dict[str, Any]:
        rows = self._read_jsonl(self.queue_path)
        counts = {
            self.STATUS_PENDING: 0,
            self.STATUS_PROCESSING: 0,
            self.STATUS_COMPLETED: 0,
            self.STATUS_FAILED: 0,
        }
        for row in rows:
            counts[str(row.get("status", self.STATUS_PENDING))] = counts.get(str(row.get("status", self.STATUS_PENDING)), 0) + 1

        recent = rows[-max(1, int(limit)) :]
        return {
            "total": len(rows),
            "counts": counts,
            "recent": [self._public_row(r) for r in recent],
            "daily_cap": self.daily_cap,
            "processed_today": self._processed_today_count(),
        }

    # -----------------------------
    # Internal queue transitions
    # -----------------------------

    def _reserve_next_pending(self) -> Dict[str, Any] | None:
        with self._lock:
            rows = self._read_jsonl(self.queue_path)
            rows = self._normalize_processing_timeouts(rows)

            candidate_idx = None
            candidate = None
            for idx, row in enumerate(rows):
                status = str(row.get("status", self.STATUS_PENDING))
                attempts = int(row.get("attempts", 0))
                max_attempts = int(row.get("max_attempts", self.max_attempts))
                if status != self.STATUS_PENDING:
                    continue
                if attempts >= max_attempts:
                    continue
                candidate_idx = idx
                candidate = row
                break

            if candidate is None or candidate_idx is None:
                if rows:
                    self._write_jsonl(self.queue_path, rows)
                return None

            candidate["status"] = self.STATUS_PROCESSING
            candidate["attempts"] = int(candidate.get("attempts", 0)) + 1
            candidate["updated_ts"] = _utc_now()
            rows[candidate_idx] = candidate
            self._write_jsonl(self.queue_path, rows)
            return dict(candidate)

    def _mark_completed(self, reserved_row: Dict[str, Any], result: Dict[str, Any]) -> None:
        with self._lock:
            rows = self._read_jsonl(self.queue_path)
            rid = int(reserved_row.get("id", -1))
            idx = self._find_idx_by_id(rows, rid)
            if idx is None:
                return

            result_summary = self._result_summary(result)
            rows[idx]["status"] = self.STATUS_COMPLETED
            rows[idx]["updated_ts"] = _utc_now()
            rows[idx]["processed_ts"] = _utc_now()
            rows[idx]["result_summary"] = result_summary
            rows[idx]["last_error"] = None
            self._write_jsonl(self.queue_path, rows)

            final_row = dict(rows[idx])

        self._append_training_case(final_row, result=result, error=None)
        self._append_replay_case(final_row, result=result, error=None)
        self._flag_failure_for_mini_replay(final_row, result)
        self.logger.event(
            "feeder_item_completed",
            {
                "feeder_id": final_row.get("id"),
                "subject": final_row.get("subject"),
                "difficulty": final_row.get("difficulty"),
                "winner_provider": result_summary.get("winner_provider"),
                "verified": bool(result_summary.get("verified")),
            },
        )

    def _mark_failed(self, reserved_row: Dict[str, Any], error: str, *, force_terminal: bool = False) -> Dict[str, Any]:
        with self._lock:
            rows = self._read_jsonl(self.queue_path)
            rid = int(reserved_row.get("id", -1))
            idx = self._find_idx_by_id(rows, rid)
            if idx is None:
                return dict(reserved_row)

            row = rows[idx]
            attempts = int(row.get("attempts", 0))
            max_attempts = int(row.get("max_attempts", self.max_attempts))
            if force_terminal:
                attempts = max_attempts
                row["attempts"] = max_attempts

            if attempts >= max_attempts:
                row["status"] = self.STATUS_FAILED
                row["processed_ts"] = _utc_now()
            else:
                row["status"] = self.STATUS_PENDING
            row["updated_ts"] = _utc_now()
            row["last_error"] = str(error)[:500]
            rows[idx] = row
            self._write_jsonl(self.queue_path, rows)
            out = dict(row)

        self.logger.event(
            "feeder_item_failed",
            {
                "feeder_id": out.get("id"),
                "subject": out.get("subject"),
                "difficulty": out.get("difficulty"),
                "status": out.get("status"),
                "attempts": out.get("attempts"),
                "max_attempts": out.get("max_attempts"),
                "error": str(error)[:250],
            },
        )
        if str(out.get("status")) == self.STATUS_FAILED:
            self._append_training_case(out, result=None, error=error)
            self._append_replay_case(out, result=None, error=error)
        else:
            self.logger.event(
                "feeder_item_retry_scheduled",
                {
                    "feeder_id": out.get("id"),
                    "attempts": out.get("attempts"),
                    "max_attempts": out.get("max_attempts"),
                },
            )
        return out

    def _recover_stale_processing(self) -> None:
        with self._lock:
            rows = self._read_jsonl(self.queue_path)
            updated = self._normalize_processing_timeouts(rows)
            self._write_jsonl(self.queue_path, updated)

    def _normalize_processing_timeouts(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        changed = False

        for row in rows:
            if str(row.get("status")) != self.STATUS_PROCESSING:
                continue
            updated_ts = _parse_ts(row.get("updated_ts"))
            if updated_ts is None:
                row["status"] = self.STATUS_PENDING
                row["updated_ts"] = _utc_now()
                changed = True
                continue

            age = now - updated_ts
            if age < timedelta(minutes=self.processing_timeout_minutes):
                continue

            attempts = int(row.get("attempts", 0))
            max_attempts = int(row.get("max_attempts", self.max_attempts))
            if attempts >= max_attempts:
                row["status"] = self.STATUS_FAILED
                row["processed_ts"] = _utc_now()
            else:
                row["status"] = self.STATUS_PENDING
            row["updated_ts"] = _utc_now()
            row["last_error"] = "processing_timeout_recovered"
            changed = True

        if changed:
            rows = self._apply_queue_cap(rows)
        return rows

    # -----------------------------
    # Logging + replay flagging
    # -----------------------------

    def _append_training_case(self, row: Dict[str, Any], *, result: Dict[str, Any] | None, error: str | None) -> None:
        summary = self._result_summary(result or {})
        payload = {
            "ts": _utc_now(),
            "feeder_id": int(row.get("id", 0)),
            "item_hash": str(row.get("item_hash", "")),
            "question": row.get("question", ""),
            "subject": row.get("subject", "general"),
            "difficulty": row.get("difficulty", "unknown"),
            "concept_cluster": list(row.get("concept_cluster", [])),
            "source_tag": row.get("source_tag", "manual"),
            "status": row.get("status", self.STATUS_PENDING),
            "verified": bool(summary.get("verified", False)),
            "winner_provider": summary.get("winner_provider", ""),
            "risk": float(summary.get("risk", 1.0)),
            "winner_margin": float(summary.get("winner_margin", 0.0)),
            "entropy": float(summary.get("entropy", 0.0)),
            "degraded_mode": bool(summary.get("degraded_mode", False)),
            "error": (str(error)[:500] if error else None),
        }
        self._append_jsonl(self.training_cases_path, payload)

    def _append_replay_case(self, row: Dict[str, Any], *, result: Dict[str, Any] | None, error: str | None) -> None:
        summary = self._result_summary(result or {})
        verified = bool(summary.get("verified", False))
        risk = float(summary.get("risk", 1.0))
        entropy = float(summary.get("entropy", 0.0))
        disagreement = float(summary.get("disagreement", 0.0))

        replay_priority = 0.0
        replay_priority += 1.2 if not verified else 0.25
        replay_priority += 0.9 * max(0.0, min(1.0, risk))
        replay_priority += 0.6 * max(0.0, min(1.0, entropy))
        replay_priority += 0.4 * max(0.0, min(1.0, disagreement))
        if error:
            replay_priority += 0.8

        payload = {
            "ts": _utc_now(),
            "feeder_id": int(row.get("id", 0)),
            "item_hash": str(row.get("item_hash", "")),
            "question": row.get("question", ""),
            "subject": row.get("subject", "general"),
            "difficulty": row.get("difficulty", "unknown"),
            "concept_clusters": list(row.get("concept_cluster", [])),
            "source_tag": row.get("source_tag", "manual"),
            "status": row.get("status", self.STATUS_PENDING),
            "deterministic_fail": (not verified) or bool(error),
            "verified": verified,
            "risk": risk,
            "entropy": entropy,
            "disagreement": disagreement,
            "winner_provider": summary.get("winner_provider", ""),
            "final_answer": summary.get("final_answer", ""),
            "replay_priority": round(replay_priority, 6),
            "error": (str(error)[:500] if error else None),
        }
        self._append_jsonl(self.replay_cases_path, payload)

    def _flag_failure_for_mini_replay(self, row: Dict[str, Any], result: Dict[str, Any]) -> None:
        summary = self._result_summary(result)
        verified = bool(summary.get("verified", False))
        risk = float(summary.get("risk", 1.0))
        entropy = float(summary.get("entropy", 0.0))
        disagreement = float(summary.get("disagreement", 0.0))

        if verified and risk <= 0.55 and entropy <= 0.60:
            return

        payload = {
            "question": row.get("question", ""),
            "subject": row.get("subject", "general"),
            "difficulty": row.get("difficulty", "unknown"),
            "provider": summary.get("winner_provider", "unknown"),
            "risk": risk,
            "calibration_risk": risk,
            "deterministic_fail": not verified,
            "entropy": entropy,
            "mini_disagreement": disagreement,
            "reason": "feeder_priority",
            "final_answer": summary.get("final_answer", ""),
            "disagreement": disagreement,
            "concept_clusters": list(row.get("concept_cluster", [])),
            "reinforced_clusters": list(row.get("concept_cluster", [])),
            "error_type": "unknown",
            "error_weight": 1.0,
        }
        self.mini_evolution.enqueue_failure(payload)

    # -----------------------------
    # Helpers
    # -----------------------------

    def _result_summary(self, result: Dict[str, Any]) -> Dict[str, Any]:
        arena = result.get("arena", {}) if isinstance(result, dict) else {}
        verification = result.get("verification", {}) if isinstance(result, dict) else {}
        engine = result.get("engine", {}) if isinstance(result, dict) else {}
        return {
            "verified": bool(verification.get("verified", False)),
            "risk": float(verification.get("risk_score", verification.get("risk", 1.0)) or 1.0),
            "winner_provider": str(result.get("winner_provider", "") or ""),
            "winner_margin": float(arena.get("winner_margin", 0.0) or 0.0),
            "entropy": float(arena.get("entropy", 0.0) or 0.0),
            "disagreement": float(arena.get("disagreement", 0.0) or 0.0),
            "final_answer": str(result.get("final_answer", "") or ""),
            "degraded_mode": bool(engine.get("degraded_mode", False)),
        }

    def _normalize_clusters(self, clusters: Sequence[str] | None) -> List[str]:
        if clusters is None:
            return []
        out = []
        for cluster in clusters:
            c = str(cluster or "").strip().lower()
            if not c:
                continue
            if c not in out:
                out.append(c)
        return out[:16]

    def _item_hash(self, question: str, subject: str, difficulty: str, clusters: Sequence[str]) -> str:
        base = {
            "question": _norm(question),
            "subject": _norm(subject),
            "difficulty": _norm(difficulty),
            "concept_cluster": [str(c).lower().strip() for c in clusters],
        }
        text = json.dumps(base, sort_keys=True, separators=(",", ":"))
        return hashlib.sha1(text.encode("utf-8")).hexdigest()

    def _find_by_hash(self, rows: Sequence[Dict[str, Any]], item_hash: str) -> Dict[str, Any] | None:
        for row in rows:
            if str(row.get("item_hash", "")) != str(item_hash):
                continue
            status = str(row.get("status", self.STATUS_PENDING))
            if status in {
                self.STATUS_PENDING,
                self.STATUS_PROCESSING,
                self.STATUS_COMPLETED,
            }:
                return row
        return None

    def _next_id(self, rows: Sequence[Dict[str, Any]]) -> int:
        best = 0
        for row in rows:
            try:
                best = max(best, int(row.get("id", 0)))
            except Exception:
                continue
        return best + 1

    def _find_idx_by_id(self, rows: Sequence[Dict[str, Any]], rid: int) -> int | None:
        for idx, row in enumerate(rows):
            try:
                if int(row.get("id", -1)) == int(rid):
                    return idx
            except Exception:
                continue
        return None

    def _processed_today_count(self) -> int:
        rows = self._read_jsonl(self.queue_path)
        today = datetime.now(timezone.utc).date()
        count = 0
        for row in rows:
            if str(row.get("status")) != self.STATUS_COMPLETED:
                continue
            ts = _parse_ts(row.get("processed_ts"))
            if ts is None:
                continue
            if ts.date() == today:
                count += 1
        return count

    def _apply_queue_cap(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if len(rows) <= self.queue_cap:
            return rows

        # Keep all active rows and trim oldest completed/failed first.
        active = [r for r in rows if str(r.get("status")) in {self.STATUS_PENDING, self.STATUS_PROCESSING}]
        done = [r for r in rows if str(r.get("status")) in {self.STATUS_COMPLETED, self.STATUS_FAILED}]
        keep_done = max(0, self.queue_cap - len(active))
        done = done[-keep_done:]
        return active + done

    def _public_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row.get("id", 0)),
            "item_hash": str(row.get("item_hash", "")),
            "question": str(row.get("question", "")),
            "subject": str(row.get("subject", "general")),
            "difficulty": str(row.get("difficulty", "unknown")),
            "concept_cluster": list(row.get("concept_cluster", [])),
            "source_tag": str(row.get("source_tag", "manual")),
            "status": str(row.get("status", self.STATUS_PENDING)),
            "attempts": int(row.get("attempts", 0)),
            "max_attempts": int(row.get("max_attempts", self.max_attempts)),
            "created_ts": row.get("created_ts"),
            "updated_ts": row.get("updated_ts"),
            "processed_ts": row.get("processed_ts"),
            "last_error": row.get("last_error"),
            "result_summary": row.get("result_summary", {}),
        }

    def _append_jsonl(self, path: Path, row: Dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")

    def _read_jsonl(self, path: Path) -> List[Dict[str, Any]]:
        if not path.exists():
            return []

        rows: List[Dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return rows

    def _write_jsonl(self, path: Path, rows: Sequence[Dict[str, Any]]) -> None:
        tmp = path.with_name(f"{path.name}.{os.getpid()}.{int(time.time() * 1_000_000)}.tmp")
        with tmp.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=True) + "\n")
        try:
            os.replace(tmp, path)
        except FileNotFoundError:
            with path.open("w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row, ensure_ascii=True) + "\n")

    def _runtime_telemetry_proxy(self):
        # Minimal interface expected by retry_async.
        class _Proxy:
            @staticmethod
            def log_recovery_attempt(**kwargs):
                return None

        return _Proxy()
