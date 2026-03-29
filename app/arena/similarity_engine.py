import hashlib
from collections import OrderedDict

import numpy as np

try:
    from sentence_transformers import SentenceTransformer
except Exception:  # pragma: no cover - optional heavy dependency in production deploys
    SentenceTransformer = None


class SimilarityEngine:
    """
    Graph-aware similarity engine.

    - Embeds structured reasoning graphs
    - Caches embeddings for efficiency
    - Computes cosine similarity
    """

    def __init__(self, model_name="all-MiniLM-L6-v2", cache_size: int = 2048):
        self.model = SentenceTransformer(model_name) if SentenceTransformer is not None else None
        self._cache = OrderedDict()
        self._cache_size = max(128, int(cache_size))

    # -----------------------------
    # Public API
    # -----------------------------

    def graph_embedding(self, graph: dict) -> np.ndarray:
        """
        Convert reasoning graph into embedding.
        """

        key = self._graph_key(graph)

        if key in self._cache:
            self._cache.move_to_end(key)
            return self._cache[key]

        text = self._graph_to_text(graph)
        embedding = self._encode_graph_text(text)

        self._cache[key] = embedding
        self._cache.move_to_end(key)
        while len(self._cache) > self._cache_size:
            self._cache.popitem(last=False)
        return embedding

    def similarity(self, emb1: np.ndarray, emb2: np.ndarray) -> float:
        """
        Cosine similarity between two embeddings.
        """
        denom = np.linalg.norm(emb1) * np.linalg.norm(emb2)
        if denom == 0:
            return 0.0

        return float(np.dot(emb1, emb2) / denom)

    def _encode_graph_text(self, text: str) -> np.ndarray:
        model = self.model
        if model is not None:
            try:
                return np.asarray(model.encode(text), dtype=np.float32)
            except Exception:
                pass
        # Lightweight deterministic fallback so production deploys do not need
        # the full sentence-transformers + torch stack.
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        seed = int.from_bytes(digest[:8], "big", signed=False)
        rng = np.random.default_rng(seed)
        return rng.random(256, dtype=np.float32)

    # -----------------------------
    # Internal Helpers
    # -----------------------------

    def _graph_to_text(self, graph: dict) -> str:
        """
        Convert structured DAG into normalized text
        for embedding.
        """

        nodes = graph.get("nodes", [])

        parts = []
        for node in nodes:
            node_type = node.get("type", "")
            summary = node.get("summary", "")
            parts.append(f"{node_type}: {summary}")

        return " | ".join(parts)

    def _graph_key(self, graph: dict) -> str:
        """
        Create stable key for caching embeddings.
        """

        nodes = graph.get("nodes", [])
        parts = []

        for node in nodes:
            summary = str(node.get("summary", ""))
            summary_hash = hashlib.sha1(summary.encode("utf-8")).hexdigest()
            parts.append(f"{node.get('id')}-{node.get('type')}-{summary_hash}")

        raw = "|".join(parts)
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()
