from __future__ import annotations

import re
import os
from dataclasses import dataclass
from typing import Any

import numpy as np


@dataclass
class SimilarityRepairResult:
    matched: bool
    score: float
    replacement_text: str
    matched_question_id: str


class SimilarityRepairEngine:
    """Nearest-neighbor recovery for severely corrupted questions."""

    def __init__(self) -> None:
        self._embedder = None
        if str(os.environ.get("JEE_REPAIR_ENABLE_ST", "0")).strip() not in {"1", "true", "TRUE"}:
            return
        try:
            from sentence_transformers import SentenceTransformer

            self._embedder = SentenceTransformer("all-MiniLM-L6-v2", local_files_only=True)
        except Exception:
            self._embedder = None

    def repair_with_corpus(
        self,
        *,
        query_text: str,
        corpus: list[dict[str, Any]] | None,
        min_score: float = 0.9,
    ) -> SimilarityRepairResult:
        rows = [row for row in (corpus or []) if isinstance(row, dict)]
        if not query_text or not rows:
            return SimilarityRepairResult(False, 0.0, "", "")

        texts: list[str] = []
        ids: list[str] = []
        for row in rows:
            text = str(row.get("question_text") or row.get("repaired_question_text") or "").strip()
            if not text:
                continue
            texts.append(text)
            ids.append(str(row.get("question_id") or ""))
        if not texts:
            return SimilarityRepairResult(False, 0.0, "", "")

        if self._embedder is not None:
            return self._semantic_search(
                query_text=query_text,
                corpus_texts=texts,
                corpus_ids=ids,
                min_score=min_score,
            )
        return self._lexical_search(
            query_text=query_text,
            corpus_texts=texts,
            corpus_ids=ids,
            min_score=min_score,
        )

    def _semantic_search(
        self,
        *,
        query_text: str,
        corpus_texts: list[str],
        corpus_ids: list[str],
        min_score: float,
    ) -> SimilarityRepairResult:
        model = self._embedder
        if model is None:
            return SimilarityRepairResult(False, 0.0, "", "")
        try:
            embs = model.encode([query_text, *corpus_texts], normalize_embeddings=True)
        except Exception:
            return self._lexical_search(
                query_text=query_text,
                corpus_texts=corpus_texts,
                corpus_ids=corpus_ids,
                min_score=min_score,
            )
        query_emb = np.array(embs[0], dtype=np.float32)
        corpus_emb = np.array(embs[1:], dtype=np.float32)
        scores = corpus_emb @ query_emb
        best_idx = int(np.argmax(scores))
        best_score = float(scores[best_idx])
        if best_score < min_score:
            return SimilarityRepairResult(False, best_score, "", "")
        return SimilarityRepairResult(
            matched=True,
            score=best_score,
            replacement_text=corpus_texts[best_idx],
            matched_question_id=corpus_ids[best_idx],
        )

    def _lexical_search(
        self,
        *,
        query_text: str,
        corpus_texts: list[str],
        corpus_ids: list[str],
        min_score: float,
    ) -> SimilarityRepairResult:
        q_vec = self._char_trigram_vector(query_text)
        if q_vec.size == 0:
            return SimilarityRepairResult(False, 0.0, "", "")
        best_score = 0.0
        best_idx = -1
        for idx, text in enumerate(corpus_texts):
            c_vec = self._char_trigram_vector(text)
            score = self._cosine(q_vec, c_vec)
            if score > best_score:
                best_score = score
                best_idx = idx
        if best_idx < 0 or best_score < min_score:
            return SimilarityRepairResult(False, best_score, "", "")
        return SimilarityRepairResult(
            matched=True,
            score=best_score,
            replacement_text=corpus_texts[best_idx],
            matched_question_id=corpus_ids[best_idx],
        )

    def _char_trigram_vector(self, text: str) -> np.ndarray:
        normalized = re.sub(r"[^a-z0-9]+", "", text.lower())
        if len(normalized) < 3:
            return np.zeros((1,), dtype=np.float32)
        bins = np.zeros((2048,), dtype=np.float32)
        for i in range(len(normalized) - 2):
            tri = normalized[i : i + 3]
            idx = hash(tri) % bins.size
            bins[idx] += 1.0
        return bins

    def _cosine(self, a: np.ndarray, b: np.ndarray) -> float:
        if a.size != b.size:
            return 0.0
        denom = float(np.linalg.norm(a) * np.linalg.norm(b))
        if denom <= 0:
            return 0.0
        return float(np.dot(a, b) / denom)
