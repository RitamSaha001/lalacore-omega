from __future__ import annotations

from core.lalacore_x.telemetry import DEFAULT_TELEMETRY


LOG_FILE = "data/logs/runtime_log.json"


def log_solve(data):
    DEFAULT_TELEMETRY.append_event({"event_type": "solver_legacy_log", "payload": data})
