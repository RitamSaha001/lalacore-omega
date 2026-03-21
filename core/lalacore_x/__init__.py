from __future__ import annotations

from typing import Any


__all__ = ["LalaCoreXEngine"]


def __getattr__(name: str) -> Any:
    if name == "LalaCoreXEngine":
        from core.lalacore_x.engine import LalaCoreXEngine

        return LalaCoreXEngine
    raise AttributeError(name)
