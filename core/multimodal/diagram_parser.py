from __future__ import annotations

import re
from typing import Any, Dict, List, Sequence


class DiagramParser:
    """
    Geometry-diagram aware parser from OCR text/layout hints.
    """

    _POINT_PATTERN = re.compile(r"\b([A-Z])\b")
    _SEGMENT_PATTERN = re.compile(r"\b(?:segment|line)\s*([A-Z])\s*([A-Z])\b", flags=re.IGNORECASE)
    _ANGLE_PATTERN = re.compile(r"(?:∠|angle\s*)([A-Z])\s*([A-Z])\s*([A-Z])", flags=re.IGNORECASE)
    _CIRCLE_PATTERN = re.compile(r"circle\s*([A-Z])", flags=re.IGNORECASE)

    def parse(self, detected_text: str, layout_blocks: Sequence[Dict[str, Any]] | None = None) -> Dict[str, Any]:
        text = str(detected_text or "")
        joined_layout = "\n".join(str(block.get("text", "")) for block in (layout_blocks or []))
        combined = "\n".join(part for part in (text, joined_layout) if part).strip()

        points = sorted(set(self._POINT_PATTERN.findall(combined)))
        segments = self._extract_segments(combined)
        angles = self._extract_angles(combined)
        circles = self._extract_circles(combined)
        relationships = self._extract_relationships(combined)

        geometry_detected = bool(
            points
            and (
                segments
                or angles
                or circles
                or any(word in combined.lower() for word in ("triangle", "parallel", "perpendicular", "chord", "radius"))
            )
        )

        abstraction = {
            "points": points,
            "segments": segments,
            "angles": angles,
            "circles": circles,
            "relationships": relationships,
        }

        return {
            "is_geometry": geometry_detected,
            "points": points,
            "segments": segments,
            "angles": angles,
            "circles": circles,
            "relationships": relationships,
            "abstraction": abstraction,
        }

    def _extract_segments(self, text: str) -> List[Dict[str, str]]:
        found = []
        for a, b in self._SEGMENT_PATTERN.findall(text):
            found.append({"a": a.upper(), "b": b.upper()})

        # Also capture compact AB notations inside common geometry syntax.
        for token in re.findall(r"\b([A-Z]{2})\b", text):
            if token in {"IF", "TO", "OF", "IN", "ON", "AT", "BY"}:
                continue
            found.append({"a": token[0], "b": token[1]})

        return self._dedupe_pair_dicts(found)

    def _extract_angles(self, text: str) -> List[Dict[str, str]]:
        out = []
        for a, b, c in self._ANGLE_PATTERN.findall(text):
            out.append({"a": a.upper(), "b": b.upper(), "c": c.upper()})

        for token in re.findall(r"\b([A-Z]{3})\b", text):
            if token in {"SIN", "COS", "TAN", "LOG", "LHS", "RHS"}:
                continue
            if "angle" in text.lower() or "∠" in text:
                out.append({"a": token[0], "b": token[1], "c": token[2]})

        dedup: List[Dict[str, str]] = []
        seen = set()
        for row in out:
            key = (row["a"], row["b"], row["c"])
            if key in seen:
                continue
            seen.add(key)
            dedup.append(row)
        return dedup

    def _extract_circles(self, text: str) -> List[Dict[str, str]]:
        out = []
        for center in self._CIRCLE_PATTERN.findall(text):
            out.append({"center": center.upper()})
        if "circle" in text.lower() and not out:
            out.append({"center": "O"})
        return out

    def _extract_relationships(self, text: str) -> List[Dict[str, str]]:
        lower = text.lower()
        relationships: List[Dict[str, str]] = []

        mapping = {
            "parallel": "parallel",
            "perpendicular": "perpendicular",
            "bisector": "bisector",
            "tangent": "tangent",
            "chord": "chord",
            "diameter": "diameter",
            "radius": "radius",
            "congruent": "congruent",
            "similar": "similar",
        }

        for needle, label in mapping.items():
            if needle in lower:
                relationships.append({"type": label, "statement": needle})

        return relationships

    def _dedupe_pair_dicts(self, rows: Sequence[Dict[str, str]]) -> List[Dict[str, str]]:
        out: List[Dict[str, str]] = []
        seen = set()
        for row in rows:
            a = str(row.get("a", "")).upper()
            b = str(row.get("b", "")).upper()
            if not a or not b:
                continue
            key = tuple(sorted((a, b)))
            if key in seen:
                continue
            seen.add(key)
            out.append({"a": a, "b": b})
        return out
