from __future__ import annotations

import re
from typing import Any, Dict, List

from core.multimodal.diagram_parser import DiagramParser as CoreDiagramParser


class DiagramParser:
    """
    High-level diagram understanding wrapper for chat reasoning graphs.
    """

    def __init__(self, base_parser: CoreDiagramParser | None = None) -> None:
        self.base_parser = base_parser or CoreDiagramParser()

    def parse(self, text: str, vision_analysis: Dict[str, Any] | None = None) -> Dict[str, Any]:
        raw = str(text or "")
        ocr_payload: Dict[str, Any] = {}
        if isinstance(vision_analysis, dict):
            candidate_ocr = vision_analysis.get("ocr")
            if isinstance(candidate_ocr, dict):
                ocr_payload = dict(candidate_ocr)
        ocr_text = str(ocr_payload.get("clean_text") or ocr_payload.get("math_normalized_text") or ocr_payload.get("raw_text", ""))
        merged_text = "\n".join(part for part in (raw, ocr_text) if str(part or "").strip()).strip()
        layout_blocks = []
        if isinstance(ocr_payload.get("layout_blocks"), list):
            layout_blocks = [row for row in ocr_payload.get("layout_blocks", []) if isinstance(row, dict)]

        base = self.base_parser.parse(merged_text or raw, layout_blocks)
        geometry_objects = {}
        if isinstance(vision_analysis, dict):
            geometry_objects = dict(vision_analysis.get("geometry_objects") or {})
        if not geometry_objects and isinstance(base.get("abstraction"), dict):
            geometry_objects = dict(base.get("abstraction") or {})

        diagram_type = self._detect_diagram_type(merged_text or raw, base, geometry_objects)
        objects = self._extract_objects(merged_text or raw, base, geometry_objects, diagram_type)
        labels = sorted({str(obj.get("label", "")).strip() for obj in objects if str(obj.get("label", "")).strip()})
        angles = self._extract_angles(base, geometry_objects)
        connections = self._extract_connections(base, geometry_objects, diagram_type, objects)

        nodes = [{"id": f"n{idx+1}", "label": str(obj.get("label") or obj.get("type")), "type": str(obj.get("type", "object"))} for idx, obj in enumerate(objects)]
        edge_list = [{"from": str(conn.get("from", "")), "to": str(conn.get("to", "")), "relation": str(conn.get("relation", ""))} for conn in connections if conn.get("from") and conn.get("to")]
        if not edge_list and len(nodes) >= 2:
            edge_list.append({"from": nodes[0]["id"], "to": nodes[1]["id"], "relation": "related"})

        return {
            "diagram_type": diagram_type,
            "objects": objects,
            "labels": labels,
            "angles": angles,
            "connections": connections,
            "graph": {"nodes": nodes, "edges": edge_list},
            "confidence": self._confidence(diagram_type, objects, angles, connections),
        }

    def _detect_diagram_type(self, text: str, base: Dict[str, Any], geometry_objects: Dict[str, Any]) -> str:
        low = str(text or "").lower()
        if any(
            k in low
            for k in (
                "charge",
                "electric field",
                "potential",
                "coulomb",
                "gauss",
                "vertex",
                "vertices",
                "square",
                "rectangle",
                "+q",
                "-q",
            )
        ):
            return "electrostatics"
        if any(k in low for k in ("resistor", "circuit", "battery", "voltage", "current", "capacitor", "inductor", "ohm")):
            return "circuit"
        if any(k in low for k in ("vector", "arrow", "i cap", "j cap", "k cap", "\\vec")):
            return "vector"
        if any(k in low for k in ("force", "friction", "tension", "normal reaction", "free body", "fbd")):
            return "force"
        if bool(base.get("is_geometry")) or bool((geometry_objects or {}).get("points")):
            return "geometry"
        return "unknown"

    def _extract_objects(
        self,
        text: str,
        base: Dict[str, Any],
        geometry_objects: Dict[str, Any],
        diagram_type: str,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        if diagram_type == "geometry":
            points = list(base.get("points") or []) + [str(x) for x in (geometry_objects.get("points") or [])]
            seen = set()
            for p in points:
                token = str(p).strip().upper()
                if not token or token in seen:
                    continue
                seen.add(token)
                out.append({"type": "point", "label": token})
        elif diagram_type == "electrostatics":
            points = list(base.get("points") or []) + [str(x) for x in (geometry_objects.get("points") or [])]
            seen_points = set()
            for p in points:
                token = str(p).strip().upper()
                if not token or token in seen_points:
                    continue
                seen_points.add(token)
                out.append({"type": "vertex", "label": token})
            charges = re.findall(r"([+\-]?\s*\d*\s*q)", text, flags=re.IGNORECASE)
            if charges:
                for idx, c in enumerate(charges[:12], start=1):
                    out.append({"type": "charge", "label": re.sub(r"\s+", "", c).upper(), "id": f"chg{idx}"})
            else:
                out.append({"type": "charge", "label": "+Q"})
        elif diagram_type == "circuit":
            for token in ("battery", "resistor", "capacitor", "inductor", "switch"):
                if token in text.lower():
                    out.append({"type": "component", "label": token})
        elif diagram_type == "vector":
            vectors = re.findall(r"(?:\\vec\{([A-Za-z])\}|vector\s+([A-Za-z]))", text, flags=re.IGNORECASE)
            for row in vectors:
                name = str(row[0] or row[1]).strip().upper()
                if name:
                    out.append({"type": "vector", "label": name})
            if not out:
                out.append({"type": "vector", "label": "unknown_vector"})
        elif diagram_type == "force":
            for token in ("tension", "normal", "friction", "weight", "gravity"):
                if token in text.lower():
                    out.append({"type": "force", "label": token})

        if not out and text.strip():
            out.append({"type": "object", "label": "diagram_entity"})
        return out[:18]

    def _extract_angles(self, base: Dict[str, Any], geometry_objects: Dict[str, Any]) -> List[Dict[str, str]]:
        out: List[Dict[str, str]] = []
        for row in base.get("angles", []) or []:
            if isinstance(row, dict):
                out.append({k: str(row.get(k, "")).strip().upper() for k in ("a", "b", "c")})
        for row in geometry_objects.get("angles", []) or []:
            if isinstance(row, dict):
                out.append({k: str(row.get(k, "")).strip().upper() for k in ("a", "b", "c")})

        dedupe = []
        seen = set()
        for row in out:
            key = (row.get("a", ""), row.get("b", ""), row.get("c", ""))
            if key in seen:
                continue
            seen.add(key)
            dedupe.append(row)
        return dedupe[:16]

    def _extract_connections(
        self,
        base: Dict[str, Any],
        geometry_objects: Dict[str, Any],
        diagram_type: str,
        objects: List[Dict[str, Any]],
    ) -> List[Dict[str, str]]:
        out: List[Dict[str, str]] = []
        segments = list(base.get("segments") or []) + list(geometry_objects.get("segments") or [])
        for row in segments:
            if not isinstance(row, dict):
                continue
            a = str(row.get("a", "")).strip().upper()
            b = str(row.get("b", "")).strip().upper()
            if a and b:
                out.append({"from": a, "to": b, "relation": "segment"})

        if diagram_type == "circuit":
            labels = [str(obj.get("label", "")).strip() for obj in objects if str(obj.get("label", "")).strip()]
            for i in range(len(labels) - 1):
                out.append({"from": labels[i], "to": labels[i + 1], "relation": "connected"})
        if diagram_type == "electrostatics":
            vertices = [str(obj.get("label", "")).strip() for obj in objects if str(obj.get("type", "")) in {"vertex", "point"}]
            charge_labels = [str(obj.get("label", "")).strip() for obj in objects if str(obj.get("type", "")) == "charge"]
            if vertices and charge_labels:
                for idx, vertex in enumerate(vertices):
                    out.append({"from": charge_labels[idx % len(charge_labels)], "to": vertex, "relation": "at_vertex"})
            if len(vertices) >= 2:
                for i in range(len(vertices) - 1):
                    out.append({"from": vertices[i], "to": vertices[i + 1], "relation": "edge"})
        if diagram_type == "force":
            labels = [str(obj.get("label", "")).strip() for obj in objects if str(obj.get("label", "")).strip()]
            for label in labels:
                out.append({"from": label, "to": "body", "relation": "acts_on"})

        dedupe = []
        seen = set()
        for row in out:
            key = (row.get("from", ""), row.get("to", ""), row.get("relation", ""))
            if key in seen:
                continue
            seen.add(key)
            dedupe.append(row)
        return dedupe[:24]

    def _confidence(
        self,
        diagram_type: str,
        objects: List[Dict[str, Any]],
        angles: List[Dict[str, str]],
        connections: List[Dict[str, str]],
    ) -> float:
        base = 0.30 if diagram_type == "unknown" else 0.55
        if diagram_type == "electrostatics":
            base = 0.62
        score = base + min(0.20, 0.02 * len(objects)) + min(0.15, 0.02 * len(connections)) + min(0.10, 0.02 * len(angles))
        return max(0.0, min(1.0, score))
