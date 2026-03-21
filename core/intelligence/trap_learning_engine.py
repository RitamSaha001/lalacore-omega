from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


class TrapLearningEngine:
    """
    Failure-based trap learning.

    Maintains concept_trap_stats:
    {
      concept_id,
      failure_count,
      success_count,
      trap_frequency_ema
    }
    """

    def __init__(self, path: str = "data/lc9/concept_trap_stats.json"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()

    def _load_state(self) -> Dict:
        if not self.path.exists():
            return {"concept_trap_stats": {}}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                payload.setdefault("concept_trap_stats", {})
                return payload
        except Exception:
            pass
        return {"concept_trap_stats": {}}

    def _save_state(self) -> None:
        self.path.write_text(json.dumps(self.state, indent=2, sort_keys=True), encoding="utf-8")

    def record_outcome(
        self,
        concept_ids: Iterable[str],
        *,
        success: bool,
        trap_signals: Iterable[str] | None = None,
    ) -> Dict:
        trap_signals = [str(x).strip() for x in (trap_signals or []) if str(x).strip()]
        failed = (not bool(success)) or bool(trap_signals)
        indicator = 1.0 if failed else 0.0

        rows = self.state.setdefault("concept_trap_stats", {})
        changed = {}
        for concept_id in concept_ids:
            cid = str(concept_id or "").strip()
            if not cid:
                continue
            row = rows.setdefault(
                cid,
                {
                    "concept_id": cid,
                    "failure_count": 0,
                    "success_count": 0,
                    "trap_frequency_ema": 0.0,
                    "updated_ts": None,
                },
            )

            if failed:
                row["failure_count"] = int(row.get("failure_count", 0)) + 1
            else:
                row["success_count"] = int(row.get("success_count", 0)) + 1
            prev = float(row.get("trap_frequency_ema", 0.0))
            row["trap_frequency_ema"] = round(_clamp(0.8 * prev + 0.2 * indicator, 0.0, 1.0), 6)
            row["updated_ts"] = _utc_now()
            changed[cid] = dict(row)

        self._save_state()
        return {
            "failed": failed,
            "updated": changed,
        }

    def record_failure(self, concept_ids: Iterable[str], trap_signals: Iterable[str] | None = None) -> Dict:
        return self.record_outcome(concept_ids, success=False, trap_signals=trap_signals)

    def record_success(self, concept_ids: Iterable[str]) -> Dict:
        return self.record_outcome(concept_ids, success=True, trap_signals=[])

    def trap_frequency_map(self, concept_ids: Iterable[str] | None = None) -> Dict[str, float]:
        rows = self.state.get("concept_trap_stats", {})
        if concept_ids is None:
            return {cid: float(row.get("trap_frequency_ema", 0.0)) for cid, row in rows.items()}

        out = {}
        for concept_id in concept_ids:
            cid = str(concept_id or "").strip()
            if not cid:
                continue
            out[cid] = float(rows.get(cid, {}).get("trap_frequency_ema", 0.0))
        return out

    def table(self) -> List[Dict]:
        rows = self.state.get("concept_trap_stats", {})
        return sorted(
            [dict(value) for value in rows.values()],
            key=lambda row: float(row.get("trap_frequency_ema", 0.0)),
            reverse=True,
        )
