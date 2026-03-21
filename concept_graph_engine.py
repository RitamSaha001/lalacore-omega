from __future__ import annotations

import re
from collections import deque
from typing import Any, Dict, List, Set

from core.lalacore_x.retrieval import ConceptVault


class ConceptGraphEngine:
    """
    Weighted BFS traversal over concept-vault graph for top related concepts.
    """

    def __init__(self, vault: ConceptVault | None = None) -> None:
        self.vault = vault or ConceptVault()

    def traverse(self, question: str, *, subject: str = "general", top_k: int = 5) -> List[Dict[str, Any]]:
        query = str(question or "").strip()
        if not query:
            return []

        top_k = max(1, min(10, int(top_k)))
        seed_blocks = self.vault.retrieve(query, subject=subject, top_k=max(top_k, 6))
        if not seed_blocks:
            return []

        nodes = getattr(self.vault, "_nodes", {}) or {}
        edges = getattr(self.vault, "_edges", {}) or {}
        q_tokens = self._tokens(query)

        queue: deque[tuple[str, int, float]] = deque()
        seen_depth: Dict[str, int] = {}
        scores: Dict[str, float] = {}

        for block in seed_blocks:
            block_id = str(block.block_id)
            base = float(block.score)
            bonus = 0.08 if subject.lower() in set(t.lower() for t in (block.tags or [])) else 0.0
            seed_score = max(0.0, min(2.5, base + bonus))
            queue.append((block_id, 0, seed_score))
            seen_depth[block_id] = 0
            scores[block_id] = max(scores.get(block_id, 0.0), seed_score)

        max_depth = 2
        while queue:
            node_id, depth, parent_score = queue.popleft()
            if depth >= max_depth:
                continue
            for neighbor in edges.get(node_id, []):
                nid = str(neighbor).strip()
                if not nid:
                    continue
                node = nodes.get(nid)
                overlap = self._keyword_overlap(q_tokens, set(getattr(node, "tags", []) or []), str(getattr(node, "text", "")))
                next_score = max(0.0, min(2.5, parent_score * 0.82 + overlap * 0.18))
                prev = scores.get(nid, 0.0)
                if next_score > prev:
                    scores[nid] = next_score
                if nid not in seen_depth or depth + 1 < seen_depth[nid]:
                    seen_depth[nid] = depth + 1
                    queue.append((nid, depth + 1, next_score))

        ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        output: List[Dict[str, Any]] = []
        for node_id, score in ranked[:top_k]:
            node = nodes.get(node_id)
            if node is None:
                continue
            output.append(
                {
                    "id": str(node_id),
                    "title": str(getattr(node, "title", node_id)),
                    "text": str(getattr(node, "text", "")),
                    "tags": [str(t) for t in (getattr(node, "tags", []) or [])],
                    "score": round(float(score), 6),
                    "depth": int(seen_depth.get(node_id, 0)),
                    "source": "concept_vault",
                }
            )
        return output

    def _tokens(self, text: str) -> Set[str]:
        out = set(re.findall(r"[a-zA-Z][a-zA-Z0-9_]{1,}", str(text or "").lower()))
        stop = {"find", "prove", "show", "that", "from", "with", "what", "when", "where", "which", "solve", "value", "equation"}
        return {tok for tok in out if tok not in stop}

    def _keyword_overlap(self, q_tokens: Set[str], tags: Set[str], text: str) -> float:
        if not q_tokens:
            return 0.0
        tag_tokens = {str(t).lower().strip() for t in tags if str(t).strip()}
        text_tokens = self._tokens(text)
        bag = tag_tokens | text_tokens
        if not bag:
            return 0.0
        hit = len(q_tokens.intersection(bag))
        return float(hit / max(1, len(q_tokens)))
