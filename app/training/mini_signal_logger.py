from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict


class MiniSignalLogger:
    def __init__(self, path: str = "data/zaggle/mini_signals.jsonl"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, payload: Dict) -> None:
        row = {"ts": datetime.now(timezone.utc).isoformat(), **payload}
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")


DEFAULT_MINI_SIGNAL_LOGGER = MiniSignalLogger()
