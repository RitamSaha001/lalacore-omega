from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Sequence


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class RuntimeTelemetry:
    """
    Structured runtime telemetry (non-sensitive).
    """

    def __init__(self, path: str = "data/lc9/LC9_RUNTIME_TELEMETRY.jsonl"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log_exception(
        self,
        *,
        exception_type: str,
        module: str,
        function: str,
        input_size: int,
        entropy: float | None,
        active_providers: Sequence[str],
        mini_eligible: bool | None,
        token_usage: Dict[str, float] | None = None,
        extra: Dict | None = None,
    ) -> None:
        row = {
            "ts": _now(),
            "event_type": "runtime_exception",
            "exception_type": str(exception_type),
            "module": str(module),
            "function": str(function),
            "input_size": int(max(0, input_size)),
            "entropy": float(entropy) if entropy is not None else None,
            "active_providers": [str(p) for p in active_providers],
            "mini_eligible": bool(mini_eligible) if mini_eligible is not None else None,
            "token_usage": self._sanitize_token_usage(token_usage or {}),
            "extra": extra or {},
        }
        self._append(row)

    def log_incident(self, name: str, payload: Dict) -> None:
        self._append({"ts": _now(), "event_type": str(name), **(payload or {})})

    def log_recovery_attempt(
        self,
        *,
        component: str,
        operation: str,
        attempt: int,
        max_attempts: int,
        delay_s: float,
        status: str,
        error_type: str | None = None,
    ) -> None:
        self._append(
            {
                "ts": _now(),
                "event_type": "recovery_attempt",
                "component": str(component),
                "operation": str(operation),
                "attempt": int(attempt),
                "max_attempts": int(max_attempts),
                "delay_s": float(delay_s),
                "status": str(status),
                "error_type": str(error_type) if error_type else None,
            }
        )

    def _sanitize_token_usage(self, token_usage: Dict[str, float]) -> Dict[str, float]:
        out = {}
        for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
            if key in token_usage:
                out[key] = float(max(0.0, token_usage.get(key, 0.0)))
        return out

    def _append(self, row: Dict) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")

