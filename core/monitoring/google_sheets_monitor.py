from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence


KNOWN_PROVIDERS = ("mini", "openrouter", "groq", "gemini", "hf", "huggingface")
SHEET_DASHBOARD = "Dashboard"
SHEET_QUEUE_MINUTE = "QueueMinute"
SHEET_QUESTION_STATUS = "QuestionStatusLog"
SHEET_QUESTION_STATUS_CURRENT = "QuestionStatusCurrent"
SHEET_PROVIDER_HEALTH = "ProviderHealthMinute"
SHEET_TOKEN_USAGE = "TokenMinute"
SHEET_SOLVER_EVENTS = "SolverEvents"
SHEET_RUNTIME = "RuntimeIncidents"
SHEET_WEEKLY = "WeeklyHealth"

DEFAULT_STATE = {
    "last_sync_ts": None,
    "last_queue_ts": None,
    "last_debug_ts": None,
    "last_runtime_ts": None,
}

DASHBOARD_HEADERS = [
    "snapshot_ts",
    "queue_total",
    "pending",
    "processing",
    "completed",
    "failed",
    "processed_today",
    "weekly_completed_pct",
    "weekly_escalated_pct",
    "weekly_failed_pct",
    "weekly_avg_entropy",
    "weekly_avg_plausibility",
    "weekly_provider_win_distribution",
    "token_week",
    "weekly_total_tokens",
    "weekly_limit_tokens",
    "weekly_remaining_tokens",
    "token_pressure",
    "providers_seen",
    "runtime_incidents_24h",
    "debug_events_synced",
    "runtime_events_synced",
    "question_rows_synced",
]

QUEUE_MINUTE_HEADERS = [
    "snapshot_ts",
    "total",
    "pending",
    "processing",
    "completed",
    "failed",
    "processed_today",
    "pending_pct",
    "processing_pct",
    "completed_pct",
    "failed_pct",
    "avg_risk_completed",
    "avg_entropy_completed",
    "avg_disagreement_completed",
]

QUESTION_STATUS_HEADERS = [
    "snapshot_ts",
    "id",
    "item_hash",
    "status",
    "attempts",
    "max_attempts",
    "subject",
    "difficulty",
    "source_tag",
    "created_ts",
    "updated_ts",
    "processed_ts",
    "verified",
    "risk",
    "entropy",
    "disagreement",
    "winner_provider",
    "final_answer",
    "last_error",
    "question",
]

PROVIDER_HEALTH_HEADERS = [
    "snapshot_ts",
    "provider",
    "configured",
    "key_slots",
    "circuit_state",
    "open_for_s",
    "requests",
    "success",
    "failures",
    "success_rate",
    "consecutive_failures",
    "timeout",
    "invalid_response",
    "empty_response",
    "rate_limit",
    "auth",
    "generic",
    "ema_reliability",
    "calibration_error",
    "brier_score",
    "avg_tokens_ema",
    "total_tokens",
    "gain_per_1k_tokens_ema",
    "total_cases",
    "verified_pass",
]

TOKEN_USAGE_HEADERS = [
    "snapshot_ts",
    "week",
    "provider",
    "provider_sessions",
    "provider_total_tokens",
    "provider_avg_tokens",
    "weekly_sessions",
    "weekly_total_tokens",
    "weekly_limit",
    "weekly_remaining",
    "pressure",
]

SOLVER_EVENT_HEADERS = [
    "snapshot_ts",
    "event_ts",
    "event_type",
    "question_hash",
    "provider",
    "question",
    "extracted_answer",
    "verification",
    "risk",
    "entropy",
    "plausible",
    "plausibility_score",
    "final_status",
    "escalate",
    "quality_reasons",
    "raw_output_preview",
    "tokens_used",
    "rationale",
]

RUNTIME_HEADERS = [
    "snapshot_ts",
    "event_ts",
    "event_type",
    "component",
    "operation",
    "status",
    "exception_type",
    "module",
    "function",
    "provider",
    "reason",
    "input_size",
    "entropy",
    "active_providers",
    "mini_eligible",
    "token_total",
    "extra",
]

WEEKLY_HEADERS = [
    "snapshot_ts",
    "window_start",
    "window_end",
    "total_decisions",
    "completed_pct",
    "escalated_pct",
    "failed_pct",
    "avg_entropy",
    "avg_plausibility_score",
    "provider_win_distribution",
    "over_escalation_rate",
    "mini_win_rate",
    "plausibility_fail_rate",
]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        ts = datetime.fromisoformat(text)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)
    except Exception:
        return None


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _ratio(numer: float, denom: float) -> float:
    if denom <= 0:
        return 0.0
    return numer / denom


def _mean(values: Iterable[float]) -> float:
    data = [float(v) for v in values]
    if not data:
        return 0.0
    return sum(data) / max(1, len(data))


def _truncate(value: Any, limit: int = 600) -> str:
    text = str(value or "")
    return text if len(text) <= limit else text[:limit]


def _read_json(path: Path, default: Dict[str, Any] | None = None) -> Dict[str, Any]:
    if not path.exists():
        return default or {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return default or {}


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _ts_gt(left: Any, right: Any) -> bool:
    if right in {None, ""}:
        return _parse_ts(left) is not None
    lts = _parse_ts(left)
    rts = _parse_ts(right)
    if lts is None or rts is None:
        return False
    return lts > rts


def _max_ts(*values: Any) -> str | None:
    best: datetime | None = None
    for value in values:
        ts = _parse_ts(value)
        if ts is None:
            continue
        if best is None or ts > best:
            best = ts
    return best.isoformat() if best is not None else None


def _week_key(ts: datetime | None = None) -> str:
    now = ts or _utc_now()
    year, week, _ = now.isocalendar()
    return f"{year}-W{week:02d}"


def _sorted_json(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=True)
    except Exception:
        return "{}"


def extract_spreadsheet_id(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "/spreadsheets/d/" not in text:
        return text
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9\-_]+)", text)
    if match:
        return str(match.group(1))
    return ""


def _provider_env_map() -> Dict[str, str]:
    return {
        "openrouter": "OPENROUTER_KEYS",
        "groq": "GROQ_KEYS",
        "gemini": "GEMINI_KEYS",
        "hf": "HF_KEYS",
        "huggingface": "HF_KEYS",
    }


def _configured_key_slots() -> Dict[str, int]:
    out: Dict[str, int] = {}
    for provider, env_key in _provider_env_map().items():
        raw = os.getenv(env_key, "")
        parts = [x.strip() for x in str(raw).split(",") if x.strip()]
        out[provider] = len(parts)
    out["mini"] = 1
    return out


class GoogleSheetsClient:
    """
    Thin Google Sheets adapter with automatic tab/header provisioning.
    """

    def __init__(self, *, spreadsheet_id: str, service_account_file: str):
        self.spreadsheet_id = str(spreadsheet_id).strip()
        self.service_account_file = Path(service_account_file).expanduser()
        if not self.spreadsheet_id:
            raise ValueError("spreadsheet_id is required")
        if not self.service_account_file.exists():
            raise FileNotFoundError(f"service account file not found: {self.service_account_file}")

        try:
            import gspread  # type: ignore
        except Exception as exc:
            raise RuntimeError("Missing dependency 'gspread'. Install: pip install gspread google-auth") from exc

        self._gspread = gspread
        self._client = gspread.service_account(filename=str(self.service_account_file))
        self._sheet = self._client.open_by_key(self.spreadsheet_id)
        self._cache: Dict[str, Any] = {}

    def replace_rows(self, *, title: str, headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> int:
        ws = self._worksheet(title=title, headers=headers)
        values = [list(headers)]
        values.extend([list(row) for row in rows])
        ws.clear()
        ws.update("A1", values, value_input_option="RAW")
        return max(0, len(rows))

    def append_rows(self, *, title: str, headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> int:
        ws = self._worksheet(title=title, headers=headers)
        if not rows:
            return 0
        ws.append_rows([list(row) for row in rows], value_input_option="RAW")
        return len(rows)

    def _worksheet(self, *, title: str, headers: Sequence[str]):
        if title in self._cache:
            return self._cache[title]
        try:
            ws = self._sheet.worksheet(title)
        except self._gspread.exceptions.WorksheetNotFound:
            cols = max(26, len(headers) + 4)
            ws = self._sheet.add_worksheet(title=title, rows=4000, cols=cols)

        first_row = ws.row_values(1)
        if list(first_row) != list(headers):
            ws.clear()
            ws.update("A1", [list(headers)], value_input_option="RAW")
        self._cache[title] = ws
        return ws


class MonitoringAggregator:
    """
    Read existing local telemetry artifacts and emit feeder-compatible health views.
    """

    def __init__(
        self,
        *,
        queue_path: str = "data/lc9/LC9_FEEDER_QUEUE.jsonl",
        debug_path: str = "data/lc9/LC9_SOLVER_DEBUG.jsonl",
        runtime_path: str = "data/lc9/LC9_RUNTIME_TELEMETRY.jsonl",
        provider_stats_path: str = "data/metrics/provider_stats.json",
        provider_circuit_path: str = "data/metrics/provider_circuit.json",
        token_budget_path: str = "data/metrics/token_budget.json",
        debug_event_limit: int = 2000,
        runtime_event_limit: int = 2000,
        question_row_limit: int = 2000,
        weekly_days: int = 7,
        weekly_limit_tokens: int = 1_000_000,
    ):
        self.queue_path = Path(queue_path)
        self.debug_path = Path(debug_path)
        self.runtime_path = Path(runtime_path)
        self.provider_stats_path = Path(provider_stats_path)
        self.provider_circuit_path = Path(provider_circuit_path)
        self.token_budget_path = Path(token_budget_path)
        self.debug_event_limit = max(100, int(debug_event_limit))
        self.runtime_event_limit = max(100, int(runtime_event_limit))
        self.question_row_limit = max(100, int(question_row_limit))
        self.weekly_days = max(1, int(weekly_days))
        self.weekly_limit_tokens = max(1, int(weekly_limit_tokens))

    def collect(self, *, state: Dict[str, Any] | None = None, snapshot_ts: str | None = None) -> Dict[str, Any]:
        state = {**DEFAULT_STATE, **(state or {})}
        snapshot_ts = snapshot_ts or _utc_now_iso()

        queue_rows = _read_jsonl(self.queue_path)
        debug_rows = _read_jsonl(self.debug_path)
        runtime_rows = _read_jsonl(self.runtime_path)
        provider_stats = _read_json(self.provider_stats_path, default={"providers": {}})
        provider_circuit = _read_json(self.provider_circuit_path, default={"providers": {}})
        token_budget = _read_json(self.token_budget_path, default={"weekly": {}})

        queue_snapshot = self._queue_snapshot(queue_rows)
        question_rows, max_queue_ts = self._question_rows(
            snapshot_ts=snapshot_ts,
            queue_rows=queue_rows,
            since_ts=state.get("last_queue_ts"),
        )
        question_rows_current = self._question_rows_current(
            snapshot_ts=snapshot_ts,
            queue_rows=queue_rows,
        )
        solver_rows, max_debug_ts = self._solver_event_rows(
            snapshot_ts=snapshot_ts,
            debug_rows=debug_rows,
            since_ts=state.get("last_debug_ts"),
        )
        runtime_export_rows, max_runtime_ts = self._runtime_rows(
            snapshot_ts=snapshot_ts,
            runtime_rows=runtime_rows,
            since_ts=state.get("last_runtime_ts"),
        )
        provider_rows = self._provider_rows(
            snapshot_ts=snapshot_ts,
            provider_stats=provider_stats,
            provider_circuit=provider_circuit,
        )
        token_rows, token_summary = self._token_rows(snapshot_ts=snapshot_ts, token_budget=token_budget)
        weekly_row, weekly_stats = self._weekly_row(
            snapshot_ts=snapshot_ts,
            debug_rows=debug_rows,
            queue_rows=queue_rows,
        )

        providers_seen = sorted(
            {
                str(row[1])
                for row in provider_rows
                if len(row) > 1 and str(row[1]).strip()
            }
        )
        runtime_24h = self._runtime_count_24h(runtime_rows, snapshot_ts=snapshot_ts)
        dashboard_row = [
            snapshot_ts,
            queue_snapshot["total"],
            queue_snapshot["pending"],
            queue_snapshot["processing"],
            queue_snapshot["completed"],
            queue_snapshot["failed"],
            queue_snapshot["processed_today"],
            round(weekly_stats["completed_pct"] * 100.0, 4),
            round(weekly_stats["escalated_pct"] * 100.0, 4),
            round(weekly_stats["failed_pct"] * 100.0, 4),
            round(weekly_stats["avg_entropy"], 6),
            round(weekly_stats["avg_plausibility"], 6),
            weekly_stats["provider_win_distribution"],
            token_summary["week"],
            round(token_summary["weekly_total_tokens"], 6),
            round(token_summary["weekly_limit"], 6),
            round(token_summary["weekly_remaining"], 6),
            round(token_summary["pressure"], 6),
            ",".join(providers_seen),
            runtime_24h,
            len(solver_rows),
            len(runtime_export_rows),
            len(question_rows),
        ]

        next_state = dict(state)
        next_state["last_sync_ts"] = snapshot_ts
        next_state["last_queue_ts"] = _max_ts(state.get("last_queue_ts"), max_queue_ts)
        next_state["last_debug_ts"] = _max_ts(state.get("last_debug_ts"), max_debug_ts)
        next_state["last_runtime_ts"] = _max_ts(state.get("last_runtime_ts"), max_runtime_ts)

        return {
            "snapshot_ts": snapshot_ts,
            "dashboard_row": dashboard_row,
            "queue_minute_row": self._queue_minute_row(snapshot_ts=snapshot_ts, snapshot=queue_snapshot),
            "question_rows": question_rows,
            "question_rows_current": question_rows_current,
            "provider_rows": provider_rows,
            "token_rows": token_rows,
            "solver_event_rows": solver_rows,
            "runtime_rows": runtime_export_rows,
            "weekly_row": weekly_row,
            "next_state": next_state,
            "queue_snapshot": queue_snapshot,
            "weekly_stats": weekly_stats,
            "token_summary": token_summary,
        }

    def _queue_snapshot(self, queue_rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        counts = {
            "Pending": 0,
            "Processing": 0,
            "Completed": 0,
            "Failed": 0,
        }
        now_date = _utc_now().date()
        processed_today = 0
        risks: List[float] = []
        entropies: List[float] = []
        disagreements: List[float] = []

        for row in queue_rows:
            status = str(row.get("status", "Pending"))
            if status not in counts:
                counts[status] = counts.get(status, 0) + 1
            else:
                counts[status] += 1

            if status == "Completed":
                summary = row.get("result_summary", {}) if isinstance(row.get("result_summary"), dict) else {}
                risks.append(_to_float(summary.get("risk"), 0.0))
                entropies.append(_to_float(summary.get("entropy"), 0.0))
                disagreements.append(_to_float(summary.get("disagreement"), 0.0))
                pts = _parse_ts(row.get("processed_ts"))
                if pts is not None and pts.date() == now_date:
                    processed_today += 1

        total = len(queue_rows)
        return {
            "total": total,
            "pending": counts.get("Pending", 0),
            "processing": counts.get("Processing", 0),
            "completed": counts.get("Completed", 0),
            "failed": counts.get("Failed", 0),
            "processed_today": processed_today,
            "avg_risk_completed": _mean(risks),
            "avg_entropy_completed": _mean(entropies),
            "avg_disagreement_completed": _mean(disagreements),
        }

    def _queue_minute_row(self, *, snapshot_ts: str, snapshot: Dict[str, Any]) -> List[Any]:
        total = float(max(1, int(snapshot.get("total", 0))))
        pending = float(snapshot.get("pending", 0))
        processing = float(snapshot.get("processing", 0))
        completed = float(snapshot.get("completed", 0))
        failed = float(snapshot.get("failed", 0))

        return [
            snapshot_ts,
            int(snapshot.get("total", 0)),
            int(pending),
            int(processing),
            int(completed),
            int(failed),
            int(snapshot.get("processed_today", 0)),
            round(_ratio(pending, total) * 100.0, 4),
            round(_ratio(processing, total) * 100.0, 4),
            round(_ratio(completed, total) * 100.0, 4),
            round(_ratio(failed, total) * 100.0, 4),
            round(_to_float(snapshot.get("avg_risk_completed"), 0.0), 6),
            round(_to_float(snapshot.get("avg_entropy_completed"), 0.0), 6),
            round(_to_float(snapshot.get("avg_disagreement_completed"), 0.0), 6),
        ]

    def _question_rows(
        self,
        *,
        snapshot_ts: str,
        queue_rows: Sequence[Dict[str, Any]],
        since_ts: str | None,
    ) -> tuple[list[list[Any]], str | None]:
        changed: List[Dict[str, Any]] = []
        max_seen = since_ts

        for row in queue_rows:
            change_ts = row.get("updated_ts") or row.get("processed_ts") or row.get("created_ts")
            max_seen = _max_ts(max_seen, change_ts)
            if since_ts and not _ts_gt(change_ts, since_ts):
                continue
            changed.append(row)

        changed.sort(key=lambda r: _parse_ts(r.get("updated_ts") or r.get("processed_ts") or r.get("created_ts")) or datetime.min.replace(tzinfo=timezone.utc))
        if len(changed) > self.question_row_limit:
            changed = changed[-self.question_row_limit :]

        out: List[List[Any]] = []
        for row in changed:
            summary = row.get("result_summary", {}) if isinstance(row.get("result_summary"), dict) else {}
            out.append(
                [
                    snapshot_ts,
                    _to_int(row.get("id"), 0),
                    str(row.get("item_hash", "")),
                    str(row.get("status", "Pending")),
                    _to_int(row.get("attempts"), 0),
                    _to_int(row.get("max_attempts"), 0),
                    str(row.get("subject", "")),
                    str(row.get("difficulty", "")),
                    str(row.get("source_tag", "")),
                    str(row.get("created_ts", "")),
                    str(row.get("updated_ts", "")),
                    str(row.get("processed_ts", "")),
                    bool(summary.get("verified", False)),
                    round(_to_float(summary.get("risk"), 0.0), 6),
                    round(_to_float(summary.get("entropy"), 0.0), 6),
                    round(_to_float(summary.get("disagreement"), 0.0), 6),
                    str(summary.get("winner_provider", "")),
                    _truncate(summary.get("final_answer", ""), 700),
                    _truncate(row.get("last_error", ""), 700),
                    _truncate(row.get("question", ""), 1000),
                ]
            )
        return out, max_seen

    def _question_rows_current(
        self,
        *,
        snapshot_ts: str,
        queue_rows: Sequence[Dict[str, Any]],
    ) -> list[list[Any]]:
        rows = list(queue_rows)
        rows.sort(
            key=lambda r: (
                _to_int(r.get("id"), 0),
                _parse_ts(r.get("updated_ts") or r.get("processed_ts") or r.get("created_ts"))
                or datetime.min.replace(tzinfo=timezone.utc),
            )
        )
        if len(rows) > self.question_row_limit:
            rows = rows[-self.question_row_limit :]

        out: List[List[Any]] = []
        for row in rows:
            summary = row.get("result_summary", {}) if isinstance(row.get("result_summary"), dict) else {}
            out.append(
                [
                    snapshot_ts,
                    _to_int(row.get("id"), 0),
                    str(row.get("item_hash", "")),
                    str(row.get("status", "Pending")),
                    _to_int(row.get("attempts"), 0),
                    _to_int(row.get("max_attempts"), 0),
                    str(row.get("subject", "")),
                    str(row.get("difficulty", "")),
                    str(row.get("source_tag", "")),
                    str(row.get("created_ts", "")),
                    str(row.get("updated_ts", "")),
                    str(row.get("processed_ts", "")),
                    bool(summary.get("verified", False)),
                    round(_to_float(summary.get("risk"), 0.0), 6),
                    round(_to_float(summary.get("entropy"), 0.0), 6),
                    round(_to_float(summary.get("disagreement"), 0.0), 6),
                    str(summary.get("winner_provider", "")),
                    _truncate(summary.get("final_answer", ""), 700),
                    _truncate(row.get("last_error", ""), 700),
                    _truncate(row.get("question", ""), 1000),
                ]
            )
        return out

    def _solver_event_rows(
        self,
        *,
        snapshot_ts: str,
        debug_rows: Sequence[Dict[str, Any]],
        since_ts: str | None,
    ) -> tuple[list[list[Any]], str | None]:
        accepted = {"routing_decision", "provider_output", "plausibility_check", "final_status", "extraction_failure"}
        selected: List[Dict[str, Any]] = []
        max_seen = since_ts

        for row in debug_rows:
            ts = row.get("ts")
            max_seen = _max_ts(max_seen, ts)
            if since_ts and not _ts_gt(ts, since_ts):
                continue
            if str(row.get("event_type", "")) not in accepted:
                continue
            selected.append(row)

        selected.sort(key=lambda r: _parse_ts(r.get("ts")) or datetime.min.replace(tzinfo=timezone.utc))
        if len(selected) > self.debug_event_limit:
            selected = selected[-self.debug_event_limit :]

        rows: List[List[Any]] = []
        for row in selected:
            event_type = str(row.get("event_type", ""))
            report = row.get("report", {}) if isinstance(row.get("report"), dict) else {}
            plausible = None
            plausibility_score = None
            if event_type == "plausibility_check":
                plausible = bool(report.get("plausible", False))
                plausibility_score = _to_float(report.get("score"), 0.0)
            elif event_type == "final_status":
                plausible = bool(row.get("plausible", False))
                plausibility_score = _to_float(row.get("plausibility_score"), 0.0)

            quality_reasons = row.get("quality_reasons", [])
            if isinstance(quality_reasons, list):
                quality_reasons_text = ",".join(str(x) for x in quality_reasons)
            else:
                quality_reasons_text = str(quality_reasons or "")

            rows.append(
                [
                    snapshot_ts,
                    str(row.get("ts", "")),
                    event_type,
                    str(row.get("question_hash", "")),
                    str(row.get("provider", "")),
                    _truncate(row.get("question", ""), 1000),
                    _truncate(row.get("extracted_answer", row.get("answer", "")), 500),
                    row.get("verification"),
                    round(_to_float(row.get("risk"), 0.0), 6) if row.get("risk") is not None else "",
                    round(_to_float(row.get("entropy"), 0.0), 6) if row.get("entropy") is not None else "",
                    plausible if plausible is not None else "",
                    round(float(plausibility_score), 6) if plausibility_score is not None else "",
                    str(row.get("final_status", "")),
                    bool(row.get("escalate", False)) if row.get("escalate") is not None else "",
                    _truncate(quality_reasons_text, 1000),
                    _truncate(row.get("raw_output", ""), 900),
                    _to_int(row.get("tokens_used"), 0),
                    _truncate(row.get("rationale", ""), 1000),
                ]
            )
        return rows, max_seen

    def _runtime_rows(
        self,
        *,
        snapshot_ts: str,
        runtime_rows: Sequence[Dict[str, Any]],
        since_ts: str | None,
    ) -> tuple[list[list[Any]], str | None]:
        selected: List[Dict[str, Any]] = []
        max_seen = since_ts
        for row in runtime_rows:
            ts = row.get("ts")
            max_seen = _max_ts(max_seen, ts)
            if since_ts and not _ts_gt(ts, since_ts):
                continue
            selected.append(row)

        selected.sort(key=lambda r: _parse_ts(r.get("ts")) or datetime.min.replace(tzinfo=timezone.utc))
        if len(selected) > self.runtime_event_limit:
            selected = selected[-self.runtime_event_limit :]

        out: List[List[Any]] = []
        for row in selected:
            extra = row.get("extra", {}) if isinstance(row.get("extra"), dict) else {}
            incident = extra.get("incident", {}) if isinstance(extra.get("incident"), dict) else {}
            provider = str(row.get("provider") or incident.get("provider") or extra.get("provider") or "")
            reason = str(row.get("reason") or incident.get("reason") or "")
            token_usage = row.get("token_usage", {}) if isinstance(row.get("token_usage"), dict) else {}
            out.append(
                [
                    snapshot_ts,
                    str(row.get("ts", "")),
                    str(row.get("event_type", "")),
                    str(row.get("component", "")),
                    str(row.get("operation", "")),
                    str(row.get("status", "")),
                    str(row.get("exception_type", "")),
                    str(row.get("module", "")),
                    str(row.get("function", "")),
                    provider,
                    reason,
                    _to_int(row.get("input_size"), 0),
                    round(_to_float(row.get("entropy"), 0.0), 6) if row.get("entropy") is not None else "",
                    ",".join(str(x) for x in (row.get("active_providers") or [])),
                    row.get("mini_eligible", ""),
                    round(_to_float(token_usage.get("total_tokens"), 0.0), 6),
                    _truncate(_sorted_json(extra), 1000),
                ]
            )
        return out, max_seen

    def _provider_rows(
        self,
        *,
        snapshot_ts: str,
        provider_stats: Dict[str, Any],
        provider_circuit: Dict[str, Any],
    ) -> List[List[Any]]:
        stats_by_provider = provider_stats.get("providers", {}) if isinstance(provider_stats.get("providers"), dict) else {}
        circuit_by_provider = provider_circuit.get("providers", {}) if isinstance(provider_circuit.get("providers"), dict) else {}
        key_slots = _configured_key_slots()

        providers = set(KNOWN_PROVIDERS)
        providers.update(str(k) for k in stats_by_provider.keys())
        providers.update(str(k) for k in circuit_by_provider.keys())

        rows: List[List[Any]] = []
        now = time.time()
        provider_list = sorted(providers, key=lambda p: (0 if p == "mini" else 1, p))
        for provider in provider_list:
            stats = stats_by_provider.get(provider, {}) if isinstance(stats_by_provider.get(provider), dict) else {}
            circuit = circuit_by_provider.get(provider, {}) if isinstance(circuit_by_provider.get(provider), dict) else {}
            token_stats = stats.get("token_stats", {}) if isinstance(stats.get("token_stats"), dict) else {}
            open_until = _to_float(circuit.get("open_until"), 0.0)
            open_for_s = max(0.0, open_until - now) if str(circuit.get("state", "closed")) == "open" else 0.0
            requests = _to_int(circuit.get("requests"), 0)
            success = _to_int(circuit.get("success"), 0)
            failures = _to_int(circuit.get("failures"), 0)
            success_rate = _ratio(float(success), float(requests))
            slot_count = _to_int(key_slots.get(provider), 0)
            configured = provider == "mini" or slot_count > 0

            rows.append(
                [
                    snapshot_ts,
                    provider,
                    bool(configured),
                    slot_count,
                    str(circuit.get("state", "closed")),
                    round(open_for_s, 6),
                    requests,
                    success,
                    failures,
                    round(success_rate, 6),
                    _to_int(circuit.get("consecutive_failures"), 0),
                    _to_int(circuit.get("timeout"), 0),
                    _to_int(circuit.get("invalid_response"), 0),
                    _to_int(circuit.get("empty_response"), 0),
                    _to_int(circuit.get("rate_limit"), 0),
                    _to_int(circuit.get("auth"), 0),
                    _to_int(circuit.get("generic"), 0),
                    round(_to_float(stats.get("ema_reliability"), 0.0), 6),
                    round(_to_float(stats.get("calibration_error"), 0.0), 6),
                    round(_to_float(stats.get("brier_score"), 0.0), 6),
                    round(_to_float(token_stats.get("avg_tokens_ema"), 0.0), 6),
                    round(_to_float(token_stats.get("total_tokens"), 0.0), 6),
                    round(_to_float(token_stats.get("gain_per_1k_tokens_ema"), 0.0), 6),
                    _to_int(stats.get("total"), 0),
                    _to_int(stats.get("verified_pass"), 0),
                ]
            )
        return rows

    def _token_rows(self, *, snapshot_ts: str, token_budget: Dict[str, Any]) -> tuple[list[list[Any]], dict[str, Any]]:
        weekly = token_budget.get("weekly", {}) if isinstance(token_budget.get("weekly"), dict) else {}
        week = _week_key()
        current = weekly.get(week, {}) if isinstance(weekly.get(week), dict) else {}
        providers = current.get("providers", {}) if isinstance(current.get("providers"), dict) else {}

        weekly_total = _to_float(current.get("total_tokens"), 0.0)
        weekly_sessions = _to_int(current.get("sessions"), 0)
        remaining = max(0.0, float(self.weekly_limit_tokens) - weekly_total)
        pressure = _ratio(weekly_total, float(self.weekly_limit_tokens))

        rows: List[List[Any]] = []
        if providers:
            for provider in sorted(providers.keys()):
                prow = providers.get(provider, {}) if isinstance(providers.get(provider), dict) else {}
                rows.append(
                    [
                        snapshot_ts,
                        week,
                        str(provider),
                        _to_int(prow.get("sessions"), 0),
                        round(_to_float(prow.get("total_tokens"), 0.0), 6),
                        round(_to_float(prow.get("avg_tokens"), 0.0), 6),
                        weekly_sessions,
                        round(weekly_total, 6),
                        self.weekly_limit_tokens,
                        round(remaining, 6),
                        round(pressure, 6),
                    ]
                )
        else:
            rows.append(
                [
                    snapshot_ts,
                    week,
                    "all",
                    0,
                    0.0,
                    0.0,
                    weekly_sessions,
                    round(weekly_total, 6),
                    self.weekly_limit_tokens,
                    round(remaining, 6),
                    round(pressure, 6),
                ]
            )

        summary = {
            "week": week,
            "weekly_total_tokens": weekly_total,
            "weekly_limit": float(self.weekly_limit_tokens),
            "weekly_remaining": remaining,
            "pressure": pressure,
        }
        return rows, summary

    def _weekly_row(
        self,
        *,
        snapshot_ts: str,
        debug_rows: Sequence[Dict[str, Any]],
        queue_rows: Sequence[Dict[str, Any]],
    ) -> tuple[list[Any], dict[str, Any]]:
        end_ts = _parse_ts(snapshot_ts) or _utc_now()
        start_ts = end_ts - timedelta(days=self.weekly_days)

        final_events: List[Dict[str, Any]] = []
        for row in debug_rows:
            if str(row.get("event_type", "")) != "final_status":
                continue
            rts = _parse_ts(row.get("ts"))
            if rts is None or rts < start_ts:
                continue
            final_events.append(row)

        total = len(final_events)
        completed = 0
        escalated = 0
        failed = 0
        entropy_values: List[float] = []
        plausibility_values: List[float] = []
        provider_wins: Dict[str, int] = {}
        over_escalated = 0
        mini_wins = 0
        plausibility_fails = 0

        for row in final_events:
            status = str(row.get("final_status", ""))
            provider = str(row.get("winner_provider", "")).strip()
            if status == "Completed":
                completed += 1
            if status == "Failed":
                failed += 1
            if bool(row.get("escalate", False)):
                escalated += 1
                if status == "Completed":
                    over_escalated += 1
            if provider:
                provider_wins[provider] = provider_wins.get(provider, 0) + 1
                if provider == "mini":
                    mini_wins += 1
            if row.get("entropy") is not None:
                entropy_values.append(_to_float(row.get("entropy"), 0.0))
            if row.get("plausibility_score") is not None:
                plausibility_values.append(_to_float(row.get("plausibility_score"), 0.0))
            if bool(row.get("plausible", True)) is False:
                plausibility_fails += 1

        if total == 0:
            for row in queue_rows:
                pts = _parse_ts(row.get("processed_ts"))
                if pts is None or pts < start_ts:
                    continue
                if str(row.get("status", "")) == "Completed":
                    completed += 1
                if str(row.get("status", "")) == "Failed":
                    failed += 1
                summary = row.get("result_summary", {}) if isinstance(row.get("result_summary"), dict) else {}
                if summary:
                    provider = str(summary.get("winner_provider", "")).strip()
                    if provider:
                        provider_wins[provider] = provider_wins.get(provider, 0) + 1
                        if provider == "mini":
                            mini_wins += 1
                    entropy_values.append(_to_float(summary.get("entropy"), 0.0))
                total += 1

        completed_pct = _ratio(float(completed), float(total))
        escalated_pct = _ratio(float(escalated), float(total))
        failed_pct = _ratio(float(failed), float(total))
        avg_entropy = _mean(entropy_values)
        avg_plausibility = _mean(plausibility_values)
        over_escalation = _ratio(float(over_escalated), float(max(1, escalated)))
        mini_win_rate = _ratio(float(mini_wins), float(max(1, total)))
        plaus_fail_rate = _ratio(float(plausibility_fails), float(max(1, total)))

        win_distribution = _sorted_json(provider_wins)

        row = [
            snapshot_ts,
            start_ts.isoformat(),
            end_ts.isoformat(),
            total,
            round(completed_pct * 100.0, 4),
            round(escalated_pct * 100.0, 4),
            round(failed_pct * 100.0, 4),
            round(avg_entropy, 6),
            round(avg_plausibility, 6),
            win_distribution,
            round(over_escalation, 6),
            round(mini_win_rate, 6),
            round(plaus_fail_rate, 6),
        ]
        stats = {
            "completed_pct": completed_pct,
            "escalated_pct": escalated_pct,
            "failed_pct": failed_pct,
            "avg_entropy": avg_entropy,
            "avg_plausibility": avg_plausibility,
            "provider_win_distribution": win_distribution,
            "over_escalation_rate": over_escalation,
            "mini_win_rate": mini_win_rate,
            "plausibility_fail_rate": plaus_fail_rate,
        }
        return row, stats

    def _runtime_count_24h(self, runtime_rows: Sequence[Dict[str, Any]], *, snapshot_ts: str) -> int:
        now = _parse_ts(snapshot_ts) or _utc_now()
        low = now - timedelta(hours=24)
        total = 0
        for row in runtime_rows:
            ts = _parse_ts(row.get("ts"))
            if ts is None:
                continue
            if ts >= low:
                total += 1
        return total


class GoogleSheetsSyncService:
    """
    Sync orchestration from local telemetry -> Google Sheets tabs.
    """

    def __init__(
        self,
        *,
        sheet_client: Any,
        aggregator: MonitoringAggregator | None = None,
        state_path: str | None = None,
        sync_log_path: str | None = None,
    ):
        self.sheet_client = sheet_client
        self.aggregator = aggregator or MonitoringAggregator()

        # 🔥 Detect project root safely
        project_root = Path(__file__).resolve().parents[2]

        # 🔥 Absolute safe paths
        self.state_path = Path(state_path) if state_path else project_root / "data/lc9/LC9_GOOGLE_SHEETS_SYNC_STATE.json"
        self.sync_log_path = Path(sync_log_path) if sync_log_path else project_root / "data/lc9/LC9_GOOGLE_SHEETS_SYNC_LOG.jsonl"

        # Ensure directories exist
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.sync_log_path.parent.mkdir(parents=True, exist_ok=True)

    def sync_once(self, *, snapshot_ts: str | None = None) -> Dict[str, Any]:
        state = self._load_state()
        bundle = self.aggregator.collect(state=state, snapshot_ts=snapshot_ts)

        self.sheet_client.replace_rows(
            title=SHEET_DASHBOARD,
            headers=DASHBOARD_HEADERS,
            rows=[bundle["dashboard_row"]],
        )

        counts: Dict[str, int] = {}
        counts[SHEET_QUEUE_MINUTE] = self.sheet_client.append_rows(
            title=SHEET_QUEUE_MINUTE,
            headers=QUEUE_MINUTE_HEADERS,
            rows=[bundle["queue_minute_row"]],
        )
        counts[SHEET_QUESTION_STATUS] = self.sheet_client.append_rows(
            title=SHEET_QUESTION_STATUS,
            headers=QUESTION_STATUS_HEADERS,
            rows=bundle["question_rows"],
        )
        counts[SHEET_QUESTION_STATUS_CURRENT] = self.sheet_client.replace_rows(
            title=SHEET_QUESTION_STATUS_CURRENT,
            headers=QUESTION_STATUS_HEADERS,
            rows=bundle["question_rows_current"],
        )
        counts[SHEET_PROVIDER_HEALTH] = self.sheet_client.append_rows(
            title=SHEET_PROVIDER_HEALTH,
            headers=PROVIDER_HEALTH_HEADERS,
            rows=bundle["provider_rows"],
        )
        counts[SHEET_TOKEN_USAGE] = self.sheet_client.append_rows(
            title=SHEET_TOKEN_USAGE,
            headers=TOKEN_USAGE_HEADERS,
            rows=bundle["token_rows"],
        )
        counts[SHEET_SOLVER_EVENTS] = self.sheet_client.append_rows(
            title=SHEET_SOLVER_EVENTS,
            headers=SOLVER_EVENT_HEADERS,
            rows=bundle["solver_event_rows"],
        )
        counts[SHEET_RUNTIME] = self.sheet_client.append_rows(
            title=SHEET_RUNTIME,
            headers=RUNTIME_HEADERS,
            rows=bundle["runtime_rows"],
        )
        counts[SHEET_WEEKLY] = self.sheet_client.append_rows(
            title=SHEET_WEEKLY,
            headers=WEEKLY_HEADERS,
            rows=[bundle["weekly_row"]],
        )

        self._save_state(bundle["next_state"])

        out = {
            "snapshot_ts": bundle["snapshot_ts"],
            "synced_rows": counts,
            "next_state": bundle["next_state"],
            "queue_snapshot": bundle["queue_snapshot"],
            "weekly_stats": bundle["weekly_stats"],
            "token_summary": bundle["token_summary"],
        }
        self._append_sync_log(out)
        return out

    def _load_state(self) -> Dict[str, Any]:
        if not self.state_path.exists():
            return dict(DEFAULT_STATE)
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return {**DEFAULT_STATE, **payload}
        except Exception:
            pass
        return dict(DEFAULT_STATE)

    def _save_state(self, state: Dict[str, Any]) -> None:
        self.state_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")

    def _append_sync_log(self, payload: Dict[str, Any]) -> None:
        row = {"ts": _utc_now_iso(), **payload}
        with self.sync_log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")
