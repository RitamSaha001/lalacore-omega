from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List


class SelfHealingTelemetry:
    """
    Resilient JSONL telemetry sink with auto-healing and rotation.
    """

    def __init__(self, path: str = "data/logs/runtime_log.json", rotate_bytes: int = 5_000_000):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.rotate_bytes = rotate_bytes
        self._lock = threading.Lock()

    def append_event(self, event: Dict) -> None:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            **event,
        }

        with self._lock:
            self._maybe_rotate()
            with self.path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=True) + "\n")

    def read_events(self, limit: int | None = None) -> List[Dict]:
        self.heal_file()
        rows: List[Dict] = []

        if not self.path.exists():
            return rows

        with self.path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

        if limit is not None and limit > 0:
            return rows[-limit:]

        return rows

    def heal_file(self) -> None:
        """
        Rewrites file to keep only valid JSON entries.
        Corrupted records are preserved as diagnostic events.
        """
        with self._lock:
            if not self.path.exists():
                return

            healed: List[Dict] = []
            broken = 0

            with self.path.open("r", encoding="utf-8") as f:
                for line in f:
                    raw = line.strip()
                    if not raw:
                        continue
                    try:
                        healed.append(json.loads(raw))
                    except json.JSONDecodeError:
                        broken += 1
                        healed.append({
                            "ts": datetime.now(timezone.utc).isoformat(),
                            "event_type": "telemetry_heal",
                            "status": "recovered_corrupt_line",
                            "raw_preview": raw[:300],
                        })

            tmp_path = self.path.with_suffix(".tmp")
            with tmp_path.open("w", encoding="utf-8") as f:
                for row in healed:
                    f.write(json.dumps(row, ensure_ascii=True) + "\n")

            try:
                os.replace(tmp_path, self.path)
            except FileNotFoundError:
                # Rare race/FS edge case: fallback to direct rewrite.
                with self.path.open("w", encoding="utf-8") as f:
                    for row in healed:
                        f.write(json.dumps(row, ensure_ascii=True) + "\n")

            if broken:
                with self.path.open("a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "ts": datetime.now(timezone.utc).isoformat(),
                        "event_type": "telemetry_heal_summary",
                        "broken_lines": broken,
                    }) + "\n")

    def _maybe_rotate(self) -> None:
        if not self.path.exists():
            return

        if self.path.stat().st_size < self.rotate_bytes:
            return

        stamp = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
        rotated = self.path.with_name(f"{self.path.stem}_{stamp}{self.path.suffix}")
        os.replace(self.path, rotated)


DEFAULT_TELEMETRY = SelfHealingTelemetry()
