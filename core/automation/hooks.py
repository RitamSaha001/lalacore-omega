from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from core.automation.logging import AutomationLogger


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class AutomationHooks:
    """
    Non-blocking post-session automation hook sink.

    This module does not execute model calls; it only captures structured hook
    signals so downstream automation can consume them asynchronously.
    """

    def __init__(
        self,
        *,
        path: str = "data/lc9/LC9_AUTOMATION_HOOK_EVENTS.jsonl",
        logger: AutomationLogger | None = None,
    ):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.logger = logger or AutomationLogger()

    async def emit_post_arena(self, payload: Dict[str, Any]) -> None:
        row = {
            "ts": _utc_now(),
            "event_type": "post_arena_hooks",
            **(payload or {}),
        }
        self._append(row)
        self.logger.event(
            "post_arena_hooks",
            {
                "winner_provider": row.get("winner_provider"),
                "subject": row.get("subject"),
                "difficulty": row.get("difficulty"),
                "disagreement_case_count": int(row.get("disagreement_case_count", 0)),
                "mini_shadow": bool(row.get("mini_shadow", False)),
            },
        )

    def _append(self, row: Dict[str, Any]) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")


def dispatch_post_arena_hooks(payload: Dict[str, Any]) -> None:
    hooks = AutomationHooks()

    async def _run() -> None:
        try:
            await hooks.emit_post_arena(payload)
        except Exception:
            return

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No event loop context; run inline safely.
        try:
            asyncio.run(_run())
        except Exception:
            return
        return

    try:
        loop.create_task(_run())
    except Exception:
        return
