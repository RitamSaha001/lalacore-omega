from __future__ import annotations

import re
from typing import Dict, List, Set, Tuple


def _norm(text: str) -> str:
    return " ".join(str(text or "").strip().lower().split())


def _slug(text: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", str(text or "").strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "na"


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


class EdgeBuilder:
    """
    Builds weighted edges for the concept graph.
    """

    def __init__(self, syllabus: Dict[str, Dict[str, Dict]], concept_nodes: List[Dict]):
        self.syllabus = syllabus
        self.nodes = concept_nodes

    def build_edges(self) -> List[Dict]:
        edges: List[Dict] = []
        seen: Set[Tuple[str, str, str]] = set()

        unit_tier_nodes: Dict[Tuple[str, str, str], List[str]] = {}
        core_id_by_unit: Dict[Tuple[str, str], str] = {}

        for node in self.nodes:
            subject = str(node.get("subject", ""))
            unit = str(node.get("unit", ""))
            tier = str(node.get("tier", ""))
            node_id = str(node.get("id", ""))
            unit_tier_nodes.setdefault((subject, unit, tier), []).append(node_id)
            if tier == "core":
                core_id_by_unit[(subject, unit)] = node_id

        def add_edge(from_id: str, to_id: str, relation: str, weight: float) -> None:
            key = (from_id, to_id, relation)
            if from_id == to_id or key in seen:
                return
            seen.add(key)
            edges.append(
                {
                    "from_concept": from_id,
                    "to_concept": to_id,
                    "relation_type": relation,
                    "weight": float(round(weight, 6)),
                }
            )

        for subject, units in self.syllabus.items():
            for unit_name, spec in units.items():
                core_id = core_id_by_unit.get((subject, unit_name))
                if not core_id:
                    continue

                micro_ids = unit_tier_nodes.get((subject, unit_name, "micro"), [])
                structural_ids = unit_tier_nodes.get((subject, unit_name, "structural"), [])
                trap_ids = unit_tier_nodes.get((subject, unit_name, "trap"), [])

                for micro_id in micro_ids:
                    add_edge(core_id, micro_id, "extension", 0.8)
                    add_edge(micro_id, core_id, "extension", 0.8)

                for structural_id in structural_ids:
                    add_edge(core_id, structural_id, "structural_dependency", 0.9)
                    for micro_id in micro_ids[:6]:
                        add_edge(micro_id, structural_id, "structural_dependency", 0.9)

                for trap_id in trap_ids:
                    add_edge(core_id, trap_id, "trap_link", 1.2)
                    for structural_id in structural_ids:
                        add_edge(structural_id, trap_id, "trap_link", 1.2)

                prereqs = spec.get("prerequisite_units", [])
                for prereq_name in prereqs:
                    prereq_core = self._resolve_core_by_unit_name(core_id_by_unit, prereq_name)
                    if prereq_core:
                        add_edge(prereq_core, core_id, "prerequisite", 1.0)

        core_nodes = [
            node
            for node in self.nodes
            if str(node.get("tier", "")) == "core"
        ]
        for i in range(len(core_nodes)):
            for j in range(i + 1, len(core_nodes)):
                a = core_nodes[i]
                b = core_nodes[j]
                a_subject = str(a.get("subject", ""))
                b_subject = str(b.get("subject", ""))
                if not a_subject or not b_subject:
                    continue

                a_spec = self.syllabus.get(a_subject, {}).get(str(a.get("unit", "")), {})
                b_spec = self.syllabus.get(b_subject, {}).get(str(b.get("unit", "")), {})

                shared_tools = self._shared(a_spec.get("tools", []), b_spec.get("tools", []))
                shared_patterns = self._shared(a_spec.get("structural_patterns", []), b_spec.get("structural_patterns", []))
                shared_archetypes = self._shared(a_spec.get("reasoning_archetypes", []), b_spec.get("reasoning_archetypes", []))

                if a_subject == b_subject:
                    if shared_patterns or shared_archetypes:
                        add_edge(str(a["id"]), str(b["id"]), "structural_dependency", 0.9)
                        add_edge(str(b["id"]), str(a["id"]), "structural_dependency", 0.9)
                    continue

                if not (shared_tools or shared_patterns or shared_archetypes):
                    continue

                weight = 0.6
                weight += 0.12 * min(2, len(shared_tools))
                weight += 0.08 * min(2, len(shared_patterns))
                weight += 0.07 * min(2, len(shared_archetypes))
                weight = _clamp(weight, 0.6, 1.1)

                add_edge(str(a["id"]), str(b["id"]), "cross_subject_bridge", weight)
                add_edge(str(b["id"]), str(a["id"]), "cross_subject_bridge", weight)

        return edges

    def _resolve_core_by_unit_name(self, core_map: Dict[Tuple[str, str], str], unit_name: str) -> str | None:
        normalized = _norm(unit_name)
        for (subject, unit), node_id in core_map.items():
            if _norm(unit) == normalized:
                return node_id
        return None

    def _shared(self, a: List[str], b: List[str]) -> Set[str]:
        a_set = {_norm(x) for x in a if _norm(x)}
        b_set = {_norm(x) for x in b if _norm(x)}
        return a_set.intersection(b_set)
