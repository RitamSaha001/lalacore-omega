from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict


def _week_key(ts: datetime | None = None) -> str:
    ts = ts or datetime.now(timezone.utc)
    year, week, _ = ts.isocalendar()
    return f"{year}-W{week:02d}"


class TokenBudgetGuardian:
    """
    Free-tier token budget pressure monitor.
    """

    def __init__(
        self,
        path: str = "data/metrics/token_budget.json",
        weekly_limit: int = 1_000_000,
    ):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.weekly_limit = int(weekly_limit)
        self.state = self._load()

    def record_session(self, token_usage_by_provider: Dict[str, Dict[str, float]], debate_triggered: bool) -> None:
        key = _week_key()
        weekly = self.state.setdefault("weekly", {}).setdefault(
            key,
            {
                "total_tokens": 0.0,
                "sessions": 0,
                "debate_sessions": 0,
                "providers": {},
            },
        )
        total = 0.0
        for provider, usage in token_usage_by_provider.items():
            t = float(usage.get("total_tokens", 0.0))
            total += t
            prow = weekly["providers"].setdefault(provider, {"total_tokens": 0.0, "sessions": 0, "avg_tokens": 0.0})
            prow["total_tokens"] += t
            prow["sessions"] += 1
            prow["avg_tokens"] = prow["total_tokens"] / max(1, prow["sessions"])

        weekly["total_tokens"] += total
        weekly["sessions"] += 1
        if debate_triggered:
            weekly["debate_sessions"] += 1
        self._save()

    def pressure(self) -> float:
        weekly = self._current_week()
        total = float(weekly.get("total_tokens", 0.0))
        if self.weekly_limit <= 0:
            return 0.0
        ratio = total / float(self.weekly_limit)
        return max(0.0, min(1.5, ratio))

    def allow_debate(self) -> bool:
        p = self.pressure()
        return p < 0.90

    def arena_iteration_scale(self) -> float:
        p = self.pressure()
        if p >= 1.10:
            return 0.55
        if p >= 0.95:
            return 0.75
        return 1.0

    def replay_intensity_scale(self) -> float:
        p = self.pressure()
        if p >= 1.10:
            return 0.50
        if p >= 0.95:
            return 0.75
        return 1.0

    def summary(self) -> Dict:
        weekly = self._current_week()
        return {
            "week": _week_key(),
            "weekly_total_tokens": float(weekly.get("total_tokens", 0.0)),
            "weekly_limit": self.weekly_limit,
            "pressure": self.pressure(),
        }

    def _current_week(self) -> Dict:
        return self.state.setdefault("weekly", {}).setdefault(_week_key(), {"total_tokens": 0.0, "sessions": 0, "debate_sessions": 0, "providers": {}})

    def _load(self) -> Dict:
        base = {"weekly": {}}
        if not self.path.exists():
            return base
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                base.update(payload)
        except Exception:
            pass
        return base

    def _save(self) -> None:
        self.path.write_text(json.dumps(self.state, indent=2, sort_keys=True), encoding="utf-8")

