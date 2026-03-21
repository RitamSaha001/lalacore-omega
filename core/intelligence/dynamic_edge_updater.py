from __future__ import annotations

from collections import defaultdict
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _edge_key(from_concept: str, to_concept: str, relation_type: str) -> str:
    return f"{from_concept}||{to_concept}||{relation_type}"


def _is_dynamic_relation(relation_type: str) -> bool:
    return str(relation_type) in {"cross_subject_bridge", "structural_dependency"}


def _unit_slug_from_concept_id(concept_id: str) -> str:
    parts = str(concept_id or "").split("::")
    if len(parts) >= 2:
        return str(parts[1])
    return "unknown_unit"


def normalize_edge_weights(
    edges: Iterable[Dict],
    *,
    variance_threshold: float = 0.035,
) -> List[Dict]:
    """
    Stabilize dynamic edge weights:
    1) outgoing normalization per source node
    2) clamp to [0.4, 1.3]
    3) slow decay toward base weight
    4) variance smoothing within unit slices
    """
    out = [dict(edge) for edge in edges]

    # Ensure base weights are always carried.
    for edge in out:
        if "base_weight" not in edge:
            edge["base_weight"] = float(edge.get("weight", 0.8))

    dynamic_by_src: Dict[str, List[int]] = defaultdict(list)
    for idx, edge in enumerate(out):
        if not _is_dynamic_relation(str(edge.get("relation_type", ""))):
            continue
        src = str(edge.get("from_concept", ""))
        dynamic_by_src[src].append(idx)

    # 1) Normalize outgoing dynamic weights per source.
    for idxs in dynamic_by_src.values():
        total = sum(max(0.0, float(out[i].get("weight", 0.0))) for i in idxs)
        if total <= 1e-12:
            uniform = 1.0 / max(1, len(idxs))
            for i in idxs:
                out[i]["weight"] = uniform
        else:
            for i in idxs:
                out[i]["weight"] = max(0.0, float(out[i].get("weight", 0.0))) / total

    # 2) Clamp and 3) decay toward base weight.
    for idx, edge in enumerate(out):
        if not _is_dynamic_relation(str(edge.get("relation_type", ""))):
            continue
        base_weight = float(edge.get("base_weight", edge.get("weight", 0.8)))
        clamped = _clamp(float(edge.get("weight", 0.8)), 0.4, 1.3)
        decayed = 0.97 * clamped + 0.03 * base_weight
        out[idx]["weight"] = round(_clamp(decayed, 0.4, 1.3), 6)
        out[idx]["base_weight"] = float(base_weight)

    # 4) Unit-level variance smoothing.
    unit_groups: Dict[str, List[int]] = defaultdict(list)
    for idx, edge in enumerate(out):
        if not _is_dynamic_relation(str(edge.get("relation_type", ""))):
            continue
        unit_groups[_unit_slug_from_concept_id(str(edge.get("from_concept", "")))].append(idx)

    for idxs in unit_groups.values():
        if len(idxs) < 2:
            continue
        weights = [float(out[i].get("weight", 0.8)) for i in idxs]
        mean_weight = sum(weights) / len(weights)
        variance = sum((w - mean_weight) ** 2 for w in weights) / len(weights)
        if variance <= float(variance_threshold):
            continue
        for i in idxs:
            smoothed = 0.9 * float(out[i].get("weight", 0.8)) + 0.1 * mean_weight
            out[i]["weight"] = round(_clamp(smoothed, 0.4, 1.3), 6)

    return out


class DynamicEdgeUpdater:
    """
    Dynamic edge reweighting:
    new_weight = old_weight + 0.05 * (failure_rate - 0.5)
    clamp [0.4, 1.3]
    """

    def __init__(self, path: str = "data/lc9/dynamic_edge_stats.json"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()

    def _load_state(self) -> Dict:
        if not self.path.exists():
            return {"edges": {}}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                payload.setdefault("edges", {})
                return payload
        except Exception:
            pass
        return {"edges": {}}

    def _save_state(self) -> None:
        self.path.write_text(json.dumps(self.state, indent=2, sort_keys=True), encoding="utf-8")

    def register_outcome(
        self,
        *,
        from_concept: str,
        to_concept: str,
        relation_type: str,
        failed: bool,
    ) -> Dict:
        key = _edge_key(from_concept, to_concept, relation_type)
        rows = self.state.setdefault("edges", {})
        row = rows.setdefault(
            key,
            {
                "from_concept": str(from_concept),
                "to_concept": str(to_concept),
                "relation_type": str(relation_type),
                "failure_count": 0,
                "success_count": 0,
                "failure_rate_ema": 0.5,
                "updated_ts": None,
            },
        )

        if failed:
            row["failure_count"] = int(row.get("failure_count", 0)) + 1
        else:
            row["success_count"] = int(row.get("success_count", 0)) + 1

        indicator = 1.0 if failed else 0.0
        prev = float(row.get("failure_rate_ema", 0.5))
        row["failure_rate_ema"] = round(_clamp(0.8 * prev + 0.2 * indicator, 0.0, 1.0), 6)
        row["updated_ts"] = _utc_now()

        self._save_state()
        return dict(row)

    def apply(self, edges: Iterable[Dict]) -> List[Dict]:
        rows = self.state.get("edges", {})
        updated: List[Dict] = []
        for edge in edges:
            out = dict(edge)
            relation = str(edge.get("relation_type", ""))
            old_weight = float(edge.get("weight", 0.8))
            out["base_weight"] = float(edge.get("base_weight", old_weight))

            if _is_dynamic_relation(relation):
                key = _edge_key(str(edge.get("from_concept", "")), str(edge.get("to_concept", "")), relation)
                stat = rows.get(key)
                failure_rate = 0.5
                if isinstance(stat, dict):
                    failure_rate = float(stat.get("failure_rate_ema", 0.5))
                new_weight = old_weight + 0.05 * (failure_rate - 0.5)
                out["weight"] = round(_clamp(new_weight, 0.4, 1.5), 6)
            updated.append(out)

        normalized = self.normalize_edge_weights(updated)
        self.state["last_normalization_ts"] = _utc_now()
        return normalized

    def normalize_edge_weights(self, edges: Iterable[Dict], *, variance_threshold: float = 0.035) -> List[Dict]:
        return normalize_edge_weights(edges, variance_threshold=variance_threshold)

    def edge_failure_rate(self, from_concept: str, to_concept: str, relation_type: str) -> float:
        key = _edge_key(from_concept, to_concept, relation_type)
        row = self.state.get("edges", {}).get(key, {})
        return float(row.get("failure_rate_ema", 0.5))
