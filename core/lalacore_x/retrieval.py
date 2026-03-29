from __future__ import annotations

import json
import os
import re
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

from core.lalacore_x.embedding import HashEmbedding, cosine_similarity
from core.lalacore_x.schemas import RetrievedBlock


@dataclass(slots=True)
class _VaultNode:
    block_id: str
    title: str
    text: str
    tags: List[str]
    embedding: List[float]


class ConceptVault:
    """
    GraphRAG-ready local concept vault.

    Storage:
    - concepts.jsonl: nodes with title/text/tags
    - edges.jsonl: graph edges for concept expansion
    - traps.jsonl: recurring trap patterns
    """

    def __init__(self, root: str = "data/vault", embedding_dim: int = 256):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

        self.concepts_path = self.root / "concepts.jsonl"
        self.edges_path = self.root / "edges.jsonl"
        self.traps_path = self.root / "traps.jsonl"

        self.embedder = HashEmbedding(dim=embedding_dim)
        self._nodes: Dict[str, _VaultNode] = {}
        self._edges: Dict[str, List[str]] = {}
        self._tag_graph: Dict[str, set[str]] = {}
        self._load_or_bootstrap()

    def _load_or_bootstrap(self) -> None:
        if not self.concepts_path.exists() or self.concepts_path.stat().st_size == 0:
            self._bootstrap_default_concepts()

        self._load_nodes()
        self._load_edges()
        self._build_tag_graph()

        if not self.traps_path.exists():
            self.traps_path.write_text("", encoding="utf-8")

    def _bootstrap_default_concepts(self) -> None:
        defaults = [
            {
                "block_id": "math_linear_equation",
                "title": "Linear Equations",
                "text": "Solve by isolating variables and preserving equality across operations.",
                "tags": ["math", "equation", "algebra"],
            },
            {
                "block_id": "math_quadratic",
                "title": "Quadratic Equations",
                "text": "Use factoring, completing square, or formula. Verify extraneous roots.",
                "tags": ["math", "quadratic", "roots"],
            },
            {
                "block_id": "physics_kinematics",
                "title": "Kinematics",
                "text": "Check dimensions and sign conventions when applying motion equations.",
                "tags": ["physics", "kinematics", "units"],
            },
            {
                "block_id": "chem_equilibrium",
                "title": "Chemical Equilibrium",
                "text": "Apply ICE tables and activity approximations carefully under constraints.",
                "tags": ["chemistry", "equilibrium", "jee"],
            },
            {
                "block_id": "jee_traps",
                "title": "JEE Trap Memory",
                "text": "Common traps include ignoring domain restrictions and unit mismatch.",
                "tags": ["jee", "trap", "verification"],
            },
        ]

        with self.concepts_path.open("w", encoding="utf-8") as f:
            for row in defaults:
                f.write(json.dumps(row) + "\n")

        edges = [
            {"src": "math_quadratic", "dst": "jee_traps"},
            {"src": "physics_kinematics", "dst": "jee_traps"},
            {"src": "chem_equilibrium", "dst": "jee_traps"},
        ]
        with self.edges_path.open("w", encoding="utf-8") as f:
            for row in edges:
                f.write(json.dumps(row) + "\n")

    def _load_nodes(self) -> None:
        self._nodes.clear()
        for row in self._read_jsonl(self.concepts_path):
            block_id = row.get("block_id")
            if not block_id:
                continue
            text = str(row.get("text", ""))
            title = str(row.get("title", block_id))
            tags = [str(t).lower() for t in row.get("tags", [])]
            emb = self.embedder.encode(f"{title} {text} {' '.join(tags)}")
            self._nodes[block_id] = _VaultNode(
                block_id=block_id,
                title=title,
                text=text,
                tags=tags,
                embedding=emb,
            )

    def _load_edges(self) -> None:
        self._edges.clear()
        for row in self._read_jsonl(self.edges_path):
            src = str(row.get("src", "")).strip()
            dst = str(row.get("dst", "")).strip()
            if not src or not dst:
                continue
            self._edges.setdefault(src, []).append(dst)

    def _build_tag_graph(self) -> None:
        graph: Dict[str, set[str]] = defaultdict(set)

        # Intra-node tag co-occurrence.
        for node in self._nodes.values():
            tags = [str(t).lower().strip() for t in node.tags if str(t).strip()]
            for i in range(len(tags)):
                for j in range(i + 1, len(tags)):
                    a, b = tags[i], tags[j]
                    graph[a].add(b)
                    graph[b].add(a)

        # Edge-based reinforcement from prerequisite/related concepts.
        for src, dsts in self._edges.items():
            src_node = self._nodes.get(src)
            if not src_node:
                continue
            src_tags = [str(t).lower().strip() for t in src_node.tags if str(t).strip()]
            for dst in dsts:
                dst_node = self._nodes.get(dst)
                if not dst_node:
                    continue
                dst_tags = [str(t).lower().strip() for t in dst_node.tags if str(t).strip()]
                for a in src_tags:
                    for b in dst_tags:
                        if not a or not b:
                            continue
                        graph[a].add(b)
                        graph[b].add(a)

        self._tag_graph = dict(graph)

    def _read_jsonl(self, path: Path) -> Iterable[dict]:
        if not path.exists():
            return []

        rows = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return rows

    def upsert_concept(self, block_id: str, title: str, text: str, tags: Sequence[str] | None = None) -> None:
        rows = list(self._read_jsonl(self.concepts_path))
        updated = False

        for row in rows:
            if row.get("block_id") == block_id:
                row.update({
                    "title": title,
                    "text": text,
                    "tags": list(tags or []),
                })
                updated = True
                break

        if not updated:
            rows.append({
                "block_id": block_id,
                "title": title,
                "text": text,
                "tags": list(tags or []),
            })

        with self.concepts_path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")

        self._load_nodes()

    def add_trap(self, pattern: str, hint: str, weight: float = 1.0) -> None:
        row = {"pattern": pattern, "hint": hint, "weight": float(weight)}
        with self.traps_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row) + "\n")

    def retrieve(self, question: str, subject: str, top_k: int = 5) -> List[RetrievedBlock]:
        if not self._nodes:
            return []

        inferred_subject = self._resolve_subject(question, subject)
        q_emb = self.embedder.encode(question)
        scored = []

        for node in self._nodes.values():
            score = cosine_similarity(q_emb, node.embedding)
            node_subject = self._node_subject(node)

            # Subject-aware boost.
            if inferred_subject and inferred_subject in node.tags:
                score += 0.16
            if node_subject and inferred_subject and node_subject != inferred_subject:
                score -= 0.24
            if "jee" in node.tags:
                score += 0.03
            if inferred_subject == "math" and any(
                token in question.lower()
                for token in ("x^2", "y^2", "hyperbola", "parabola", "ellipse", "circle", "permutation", "combination")
            ):
                if "math" in node.tags or "algebra" in node.tags or "equation" in node.tags:
                    score += 0.08

            scored.append((score, node))

        scored.sort(key=lambda x: x[0], reverse=True)
        selected = scored[: max(1, top_k)]

        blocks: Dict[str, RetrievedBlock] = {}
        for score, node in selected:
            blocks[node.block_id] = RetrievedBlock(
                block_id=node.block_id,
                title=node.title,
                text=node.text,
                score=round(float(score), 6),
                tags=list(node.tags),
            )

            for neighbor in self._edges.get(node.block_id, []):
                nnode = self._nodes.get(neighbor)
                if not nnode or neighbor in blocks:
                    continue
                blocks[neighbor] = RetrievedBlock(
                    block_id=nnode.block_id,
                    title=nnode.title,
                    text=nnode.text,
                    score=round(float(score * 0.9), 6),
                    tags=list(nnode.tags),
                )

        traps = self._trap_notes(question)
        for idx, note in enumerate(traps):
            trap_id = f"trap_{idx}"
            blocks[trap_id] = RetrievedBlock(
                block_id=trap_id,
                title="Trap Warning",
                text=note,
                score=0.95,
                source="trap_vault",
                tags=["trap", "jee"],
            )

        ranked = sorted(blocks.values(), key=lambda b: b.score, reverse=True)
        return ranked[: top_k + len(traps)]

    def _resolve_subject(self, question: str, subject: str) -> str:
        explicit = str(subject or "").strip().lower()
        if explicit in {"mathematics", "math"}:
            return "math"
        if explicit in {"physics"}:
            return "physics"
        if explicit in {"chemistry", "chem"}:
            return "chemistry"
        lowered = str(question or "").lower()
        math_signals = (
            "x^2",
            "y^2",
            "hyperbola",
            "parabola",
            "ellipse",
            "circle",
            "asymptote",
            "permutation",
            "combination",
            "probability",
            "binomial",
            "integral",
            "derivative",
            "matrix",
        )
        physics_signals = (
            "velocity",
            "acceleration",
            "force",
            "current",
            "potential difference",
            "wavelength",
            "momentum",
            "kinematics",
        )
        chemistry_signals = (
            "mole",
            "equilibrium",
            "enthalpy",
            "ph",
            "organic",
            "electrochemistry",
            "stoichiometry",
        )
        if any(signal in lowered for signal in math_signals):
            return "math"
        if any(signal in lowered for signal in physics_signals):
            return "physics"
        if any(signal in lowered for signal in chemistry_signals):
            return "chemistry"
        return explicit

    def _node_subject(self, node: _VaultNode) -> str:
        for candidate in ("math", "physics", "chemistry"):
            if candidate in node.tags:
                return candidate
        return ""

    def _trap_notes(self, question: str) -> List[str]:
        q = question.lower()
        notes = []

        for row in self._read_jsonl(self.traps_path):
            pattern = str(row.get("pattern", "")).strip()
            hint = str(row.get("hint", "")).strip()
            if not pattern or not hint:
                continue
            if re.search(pattern, q):
                notes.append(hint)

        # Built-in safety checks.
        if "sqrt" in q or "root" in q:
            notes.append("Check domain constraints and eliminate extraneous roots.")
        if "unit" in q or any(k in q for k in ("m/s", "kg", "mol", "joule")):
            notes.append("Validate dimensional consistency before finalizing answer.")

        # De-duplicate while preserving order.
        deduped = []
        seen = set()
        for note in notes:
            if note in seen:
                continue
            deduped.append(note)
            seen.add(note)

        return deduped[:3]

    def expand_concept_clusters(self, concept_clusters: Sequence[str], depth: int = 2) -> List[str]:
        """
        Multi-hop concept reinforcement (BFS <= depth).
        Uses local tag graph only; no API calls.
        """
        seeds = [str(c).lower().strip() for c in concept_clusters if str(c).strip()]
        if not seeds:
            return []

        depth = max(0, min(int(depth), 2))
        visited = set(seeds)
        queue = deque([(seed, 0) for seed in seeds])

        while queue:
            cur, d = queue.popleft()
            if d >= depth:
                continue
            for nxt in self._tag_graph.get(cur, set()):
                if nxt in visited:
                    continue
                visited.add(nxt)
                queue.append((nxt, d + 1))

        return sorted(visited)

    def check_claims(self, claims: Sequence[str], top_k: int = 3) -> Dict[str, List[RetrievedBlock]]:
        """
        Retrieval-Augmented Verification Loop helper.
        For each claim, fetch nearest concept snippets.
        """
        out: Dict[str, List[RetrievedBlock]] = {}
        for claim in claims:
            out[claim] = self.retrieve(claim, subject="general", top_k=top_k)
        return out
