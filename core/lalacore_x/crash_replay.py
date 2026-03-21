from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class CrashReplayRecorder:
    """
    Records fatal snapshots for manual crash replay.
    """

    def __init__(self, path: str = "data/lc9/LC9_CRASH_SNAPSHOTS.jsonl", max_entries: int = 5000):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.max_entries = max_entries

    def record(self, *, exception_type: str, message: str, snapshot: Dict) -> None:
        row = {
            "ts": _now(),
            "exception_type": str(exception_type),
            "message": str(message)[:500],
            "snapshot": self._sanitize_snapshot(snapshot),
        }
        self._append(row)
        self._trim_if_needed()

    def read(self, limit: int = 100) -> List[Dict]:
        if not self.path.exists():
            return []
        rows = []
        with self.path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return rows[-max(1, int(limit)):]

    def _sanitize_snapshot(self, snapshot: Dict) -> Dict:
        responses = []
        for row in snapshot.get("responses", [])[:8]:
            provider = str(row.get("provider", "unknown"))
            answer = str(row.get("final_answer", ""))[:120]
            responses.append(
                {
                    "provider": provider,
                    "final_answer": answer,
                    "deterministic_pass": bool(row.get("deterministic_pass")),
                    "confidence": float(row.get("confidence", 0.0)),
                }
            )

        question_hash = str(snapshot.get("question_hash", "")).strip()
        if not question_hash:
            question_hash = hashlib.sha1(str(snapshot.get("question", "")).encode("utf-8")).hexdigest()

        return {
            "question_hash": question_hash,
            "responses": responses,
            "entropy": float(snapshot.get("entropy", 0.0)),
            "matches": snapshot.get("matches", [])[:64],
            "bt_thetas": snapshot.get("bt_thetas", {}),
            "mini_eligible": bool(snapshot.get("mini_eligible", False)),
            "active_providers": [str(p) for p in snapshot.get("active_providers", [])][:8],
        }

    def _append(self, row: Dict) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")

    def _trim_if_needed(self) -> None:
        if not self.path.exists():
            return
        try:
            lines = self.path.read_text(encoding="utf-8").splitlines()
        except Exception:
            return
        if len(lines) <= self.max_entries:
            return
        keep = lines[-self.max_entries :]
        self.path.write_text("\n".join(keep) + "\n", encoding="utf-8")

