from __future__ import annotations

import re
from typing import Dict, List, Tuple

from core.intelligence.syllabus_graph import build_syllabus_hierarchy


def _slug(text: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", str(text or "").strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "na"


def _subject_weight(subject: str) -> float:
    base = {
        "Mathematics": 1.00,
        "Physics": 1.05,
        "Chemistry": 1.00,
    }
    return float(base.get(subject, 1.0))


class ConceptGraphGenerator:
    """
    Auto-generates a large concept graph from syllabus hierarchy.
    """

    def __init__(self, syllabus: Dict[str, Dict[str, Dict]] | None = None):
        self.syllabus = syllabus or build_syllabus_hierarchy()

    def generate(self) -> Dict:
        nodes: List[Dict] = []
        unit_core_ids: Dict[Tuple[str, str], str] = {}

        tool_to_subjects: Dict[str, set] = {}
        for subject, units in self.syllabus.items():
            for _, spec in units.items():
                for tool in spec.get("tools", []):
                    key = str(tool).strip().lower()
                    if not key:
                        continue
                    bucket = tool_to_subjects.setdefault(key, set())
                    bucket.add(subject)

        for subject, units in self.syllabus.items():
            subject_w = _subject_weight(subject)
            for unit_name, spec in units.items():
                unit_slug = _slug(unit_name)
                core_id = f"{_slug(subject)}::{unit_slug}::core::core"
                unit_core_ids[(subject, unit_name)] = core_id

                prereq_count = len(spec.get("prerequisite_units", []))
                difficulty_base = min(1.8, subject_w + 0.08 * prereq_count)

                cross_subject_links = sorted(
                    {
                        other_subject
                        for tool in spec.get("tools", [])
                        for other_subject in tool_to_subjects.get(str(tool).strip().lower(), set())
                        if other_subject != subject
                    }
                )

                nodes.append(
                    self._node(
                        node_id=core_id,
                        name=f"{unit_name} Core",
                        subject=subject,
                        unit=unit_name,
                        tier="core",
                        difficulty_weight=round(difficulty_base, 6),
                        trap_base_frequency=0.12,
                        keywords=spec.get("core_topics", []) + spec.get("tools", []),
                        cross_subject_links=cross_subject_links,
                    )
                )

                for subtopic in spec.get("subtopics", []):
                    node_id = f"{_slug(subject)}::{unit_slug}::micro::{_slug(subtopic)}"
                    nodes.append(
                        self._node(
                            node_id=node_id,
                            name=str(subtopic),
                            subject=subject,
                            unit=unit_name,
                            tier="micro",
                            difficulty_weight=round(min(2.0, difficulty_base + 0.12), 6),
                            trap_base_frequency=0.10,
                            keywords=[subtopic] + spec.get("core_topics", []),
                            cross_subject_links=cross_subject_links,
                        )
                    )

                for pattern in spec.get("structural_patterns", []):
                    node_id = f"{_slug(subject)}::{unit_slug}::structural::{_slug(pattern)}"
                    nodes.append(
                        self._node(
                            node_id=node_id,
                            name=str(pattern),
                            subject=subject,
                            unit=unit_name,
                            tier="structural",
                            difficulty_weight=round(min(2.1, difficulty_base + 0.15), 6),
                            trap_base_frequency=0.16,
                            keywords=[pattern] + spec.get("reasoning_archetypes", []),
                            cross_subject_links=cross_subject_links,
                        )
                    )

                for trap in spec.get("common_traps", []):
                    node_id = f"{_slug(subject)}::{unit_slug}::trap::{_slug(trap)}"
                    nodes.append(
                        self._node(
                            node_id=node_id,
                            name=str(trap),
                            subject=subject,
                            unit=unit_name,
                            tier="trap",
                            difficulty_weight=round(min(2.3, difficulty_base + 0.20), 6),
                            trap_base_frequency=0.32,
                            keywords=[trap] + spec.get("common_traps", []),
                            cross_subject_links=cross_subject_links,
                        )
                    )

        return {
            "concept_nodes": nodes,
            "unit_core_ids": unit_core_ids,
            "node_count": len(nodes),
        }

    def _node(
        self,
        *,
        node_id: str,
        name: str,
        subject: str,
        unit: str,
        tier: str,
        difficulty_weight: float,
        trap_base_frequency: float,
        keywords: List[str],
        cross_subject_links: List[str],
    ) -> Dict:
        return {
            "id": str(node_id),
            "name": str(name),
            "subject": str(subject),
            "unit": str(unit),
            "tier": str(tier),
            "difficulty_weight": float(difficulty_weight),
            "trap_base_frequency": float(trap_base_frequency),
            "cross_subject_links": list(cross_subject_links),
            "keywords": [str(item).strip().lower() for item in keywords if str(item).strip()],
        }
