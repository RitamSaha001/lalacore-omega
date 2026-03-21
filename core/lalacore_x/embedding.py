from __future__ import annotations

import hashlib
import math
import re
from typing import Iterable, List


class HashEmbedding:
    """
    Free-tier safe embedding function (no external API needed).
    Uses hashed token projection; deterministic and lightweight.
    """

    def __init__(self, dim: int = 256):
        self.dim = dim

    def encode(self, text: str) -> List[float]:
        vec = [0.0] * self.dim
        tokens = re.findall(r"[a-zA-Z0-9_\-]+", text.lower())

        if not tokens:
            return vec

        for token in tokens:
            h = int(hashlib.sha256(token.encode("utf-8")).hexdigest(), 16)
            idx = h % self.dim
            sign = 1.0 if ((h >> 8) & 1) else -1.0
            vec[idx] += sign

        norm = math.sqrt(sum(v * v for v in vec))
        if norm > 0:
            vec = [v / norm for v in vec]

        return vec



def cosine_similarity(vec_a: Iterable[float], vec_b: Iterable[float]) -> float:
    a = list(vec_a)
    b = list(vec_b)
    if len(a) != len(b):
        raise ValueError("Embedding vector dimensions do not match")

    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))

    if norm_a == 0 or norm_b == 0:
        return 0.0

    return dot / (norm_a * norm_b)
