from __future__ import annotations

from typing import Dict, List, Sequence


def _norm(text: str) -> str:
    return " ".join(str(text or "").strip().lower().split())


_GENERIC_STRUCTURAL = {
    "constraint_decomposition",
    "case_partitioning",
    "symbolic_transformation",
    "dimensional_sanity_check",
    "boundary_analysis",
}


class ConceptBFSEngine:
    """
    BFS expansion engine over weighted concept graph.
    """

    def __init__(self, concept_nodes: List[Dict], concept_edges: List[Dict]):
        self.nodes = {str(node["id"]): dict(node) for node in concept_nodes}
        self.edges = list(concept_edges)
        self.name_to_ids: Dict[str, List[str]] = {}
        self.adj: Dict[str, List[Dict]] = {}
        self._build_index()

    def _build_index(self) -> None:
        self.name_to_ids.clear()
        self.adj.clear()

        for node_id, node in self.nodes.items():
            key = _norm(node.get("name", ""))
            self.name_to_ids.setdefault(key, []).append(node_id)
            self.adj.setdefault(node_id, [])

        for edge in self.edges:
            src = str(edge.get("from_concept", ""))
            dst = str(edge.get("to_concept", ""))
            if src not in self.nodes or dst not in self.nodes:
                continue
            payload = {
                "from_concept": src,
                "to_concept": dst,
                "relation_type": str(edge.get("relation_type", "")),
                "weight": float(edge.get("weight", 0.0)),
            }
            self.adj[src].append(payload)

            rev = dict(payload)
            rev["from_concept"] = dst
            rev["to_concept"] = src
            rev["reverse"] = True
            self.adj[dst].append(rev)

    def expand_concepts(
        self,
        primary_concepts: Sequence[str],
        depth: int = 2,
        trap_frequency: Dict[str, float] | None = None,
        *,
        anchor_units: Sequence[str] | None = None,
        anchor_subject: str | None = None,
        classification_entropy: float | None = None,
        anchor_core_topics: Sequence[str] | None = None,
        entropy_threshold: float = 0.62,
        max_width_per_layer: int = 8,
    ) -> Dict:
        trap_frequency = trap_frequency or {}
        max_depth = max(1, int(depth))
        max_width_per_layer = max(1, int(max_width_per_layer))
        primary_ids = self._resolve_ids(primary_concepts)
        if not primary_ids:
            return {
                "primary_concepts": [],
                "secondary_concepts": [],
                "structural_nodes": [],
                "trap_nodes": [],
            }

        visited_depth = {node_id: 0 for node_id in primary_ids}
        best_score: Dict[str, float] = {node_id: 0.0 for node_id in primary_ids}
        frontier = list(primary_ids)
        anchor_units_set = {str(unit) for unit in (anchor_units or []) if str(unit).strip()}
        normalized_anchor_topics = {_norm(topic).replace("_", " ") for topic in (anchor_core_topics or []) if _norm(topic)}
        entropy_value = 0.0 if classification_entropy is None else float(classification_entropy)

        for layer in range(1, max_depth + 1):
            candidate_scores: Dict[str, float] = {}
            for current in frontier:
                for edge in self.adj.get(current, []):
                    nxt = str(edge.get("to_concept", ""))
                    if nxt not in self.nodes or nxt in primary_ids:
                        continue
                    if nxt in visited_depth and visited_depth[nxt] <= layer:
                        continue

                    edge_weight = float(edge.get("weight", 0.0))
                    difficulty_weight = float(self.nodes[nxt].get("difficulty_weight", 1.0))
                    trap_adj = float(trap_frequency.get(nxt, self.nodes[nxt].get("trap_base_frequency", 0.0)))
                    trap_adj = max(0.0, min(1.5, trap_adj))
                    score = (edge_weight / max(1, layer)) * difficulty_weight * (1.0 + trap_adj)
                    score = self.apply_anchor_constraints(
                        score=score,
                        node=self.nodes[nxt],
                        depth=layer,
                        anchor_units=anchor_units_set,
                        anchor_subject=anchor_subject,
                        classification_entropy=entropy_value,
                        anchor_core_topics=normalized_anchor_topics,
                        entropy_threshold=float(entropy_threshold),
                    )
                    if score > candidate_scores.get(nxt, float("-inf")):
                        candidate_scores[nxt] = score

            if not candidate_scores:
                break

            ranked_full = sorted(candidate_scores.items(), key=lambda row: row[1], reverse=True)
            ranked_layer = ranked_full[:max_width_per_layer]
            if ranked_layer:
                has_secondary_tier = any(self.nodes[node_id].get("tier") not in {"structural", "trap"} for node_id, _ in ranked_layer)
                if not has_secondary_tier:
                    fallback = next(
                        ((node_id, score) for node_id, score in ranked_full if self.nodes[node_id].get("tier") not in {"structural", "trap"}),
                        None,
                    )
                    if fallback is not None and fallback[0] not in {node_id for node_id, _ in ranked_layer}:
                        ranked_layer = list(ranked_layer[:-1]) + [fallback]
            frontier = []
            for node_id, score in ranked_layer:
                visited_depth[node_id] = layer
                best_score[node_id] = max(score, best_score.get(node_id, float("-inf")))
                frontier.append(node_id)

        primary_rows = [self._row(node_id, best_score.get(node_id, 0.0), 0) for node_id in primary_ids]
        secondary_rows: List[Dict] = []
        structural_rows: List[Dict] = []
        trap_rows: List[Dict] = []

        for node_id, level in visited_depth.items():
            if node_id in primary_ids:
                continue
            node = self.nodes[node_id]
            row = self._row(node_id, best_score.get(node_id, 0.0), level)
            tier = str(node.get("tier", ""))
            if tier == "trap":
                trap_rows.append(row)
            elif tier == "structural":
                structural_rows.append(row)
            else:
                secondary_rows.append(row)

        secondary_rows.sort(key=lambda row: row["score"], reverse=True)
        structural_rows.sort(key=lambda row: row["score"], reverse=True)
        trap_rows.sort(key=lambda row: row["score"], reverse=True)

        return {
            "primary_concepts": sorted(primary_rows, key=lambda row: row["name"]),
            "secondary_concepts": secondary_rows,
            "structural_nodes": structural_rows,
            "trap_nodes": trap_rows,
            "ranked_secondary_concepts": [row["name"] for row in secondary_rows],
        }

    def apply_anchor_constraints(
        self,
        *,
        score: float,
        node: Dict,
        depth: int,
        anchor_units: Sequence[str] | set[str],
        anchor_subject: str | None,
        classification_entropy: float,
        anchor_core_topics: set[str],
        entropy_threshold: float,
    ) -> float:
        bounded_score = max(0.0, float(score))
        node_name = _norm(node.get("name", ""))
        node_id = str(node.get("id", ""))
        node_subject = str(node.get("subject", ""))
        node_unit = str(node.get("unit", ""))
        anchor_units_lookup = anchor_units if isinstance(anchor_units, set) else set(anchor_units)

        is_generic_structural = (
            "_core" in node_name
            or "fundamentals" in node_name
            or "fundamentals" in node_id
            or "::core::" in node_id
            or node_name.replace(" ", "_") in _GENERIC_STRUCTURAL
        )
        if is_generic_structural:
            bounded_score *= 0.65

        if node_unit and node_unit in anchor_units_lookup:
            bounded_score *= 1.35

        if anchor_subject and node_subject and node_subject != str(anchor_subject):
            bounded_score *= 0.7

        bounded_score *= 1.0 + 0.08 * max(0, int(depth))

        if classification_entropy > float(entropy_threshold) and anchor_core_topics:
            keywords = {_norm(k).replace("_", " ") for k in node.get("keywords", []) if _norm(k)}
            keywords.add(node_name.replace("_", " "))
            has_overlap = any(
                key in anchor_core_topics or any(key in topic or topic in key for topic in anchor_core_topics)
                for key in keywords
            )
            if not has_overlap:
                bounded_score *= 0.72

        return max(0.0, bounded_score)

    def _resolve_ids(self, primary_concepts: Sequence[str]) -> List[str]:
        out: List[str] = []
        seen = set()
        for concept in primary_concepts:
            if concept is None:
                continue
            text = str(concept).strip()
            if not text:
                continue
            if text in self.nodes and text not in seen:
                out.append(text)
                seen.add(text)
                continue

            key = _norm(text)
            for node_id in self.name_to_ids.get(key, []):
                if node_id not in seen:
                    out.append(node_id)
                    seen.add(node_id)
        return out

    def _row(self, node_id: str, score: float, depth: int) -> Dict:
        node = self.nodes[node_id]
        return {
            "id": node_id,
            "name": str(node.get("name", "")),
            "subject": str(node.get("subject", "")),
            "unit": str(node.get("unit", "")),
            "tier": str(node.get("tier", "")),
            "depth": int(depth),
            "score": float(round(score, 6)),
        }
