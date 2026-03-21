from __future__ import annotations

import asyncio
import math
import os
import re
from collections import OrderedDict
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Sequence
from urllib.parse import urlparse

from app.data.local_app_data_service import LocalAppDataService
from core.lalacore_x.embedding import HashEmbedding, cosine_similarity
from services.search_cache import SearchCacheStore


_PRIORITY_DOMAINS: Sequence[str] = (
    "stackexchange.com",
    "math.stackexchange.com",
    "physics.stackexchange.com",
    "chegg.com",
    "vedantu.com",
    "toppr.com",
    "byjus.com",
    "physicsforums.com",
    "jeeadv.ac.in",
)

_SOURCE_DOMAIN_MAP = {
    "stackexchange.com": "stackexchange",
    "math.stackexchange.com": "stackexchange",
    "physics.stackexchange.com": "stackexchange",
    "chegg.com": "chegg",
    "vedantu.com": "vedantu",
    "toppr.com": "toppr",
    "byjus.com": "byju",
    "physicsforums.com": "physics_forum",
    "jeeadv.ac.in": "jee_pyq_archive",
}


class QuestionSearchEngine:
    """
    Web question search layer.
    Reuses lc9_web_verify_query while adding ranking and source normalization.
    """

    _RANKER_VERSION = "hybrid_v2"

    def __init__(
        self,
        *,
        app_data_service: LocalAppDataService | None = None,
        cache_store: SearchCacheStore | None = None,
    ) -> None:
        self._app_data = app_data_service or LocalAppDataService()
        self._cache = cache_store or SearchCacheStore(ttl_days=7)
        self._embedding = _HybridEmbedder()

    async def search(
        self,
        normalized_question: Dict[str, Any],
        *,
        max_matches: int = 10,
        query_timeout_s: float = 1.2,
    ) -> Dict[str, Any]:
        query_text = str(
            normalized_question.get("search_query")
            or normalized_question.get("stem")
            or normalized_question.get("original")
            or ""
        ).strip()
        if not query_text:
            return {"query": "", "matches": [], "cache_hit": False, "providers": []}

        cached, cache_hit = await self._cache.get_cached_search(query_text)
        if cache_hit and isinstance(cached, dict):
            if str(cached.get("ranker_version") or "") == self._RANKER_VERSION:
                out = dict(cached)
                out["cache_hit"] = True
                return out

        variants = self._build_query_variants(normalized_question)
        per_variant_rows = await self._run_query_variants(
            variants=variants,
            timeout_s=query_timeout_s,
            max_rows=max(12, max_matches * 3),
        )
        matches = self._rank_matches(
            base_query=query_text,
            normalized_question=normalized_question,
            rows=per_variant_rows,
            max_matches=max_matches,
        )
        out = {
            "query": query_text,
            "query_variants": variants,
            "matches": matches,
            "cache_hit": False,
            "ranker_version": self._RANKER_VERSION,
        }
        await self._cache.put_cached_search(query=query_text, results=out)
        return out

    async def _run_query_variants(
        self,
        *,
        variants: Sequence[Dict[str, str]],
        timeout_s: float,
        max_rows: int,
    ) -> List[Dict[str, Any]]:
        if not variants:
            return []

        async def _run_single(variant: Dict[str, str]) -> List[Dict[str, Any]]:
            query = str(variant.get("query") or "").strip()
            if not query:
                return []
            payload = {
                "action": "lc9_web_verify_query",
                "query": query,
                "max_rows": int(max_rows),
            }
            try:
                response = await asyncio.wait_for(self._app_data.handle_action(payload), timeout=max(0.4, timeout_s))
            except Exception:
                return []
            rows = response.get("rows") if isinstance(response, dict) else []
            out: List[Dict[str, Any]] = []
            for row in rows or []:
                if not isinstance(row, dict):
                    continue
                out.append(
                    {
                        "query_variant": str(variant.get("kind") or "exact"),
                        "query": query,
                        "title": str(row.get("title") or ""),
                        "url": str(row.get("url") or ""),
                        "snippet": str(row.get("snippet") or ""),
                    }
                )
            return out

        tasks = [asyncio.create_task(_run_single(variant)) for variant in variants]
        try:
            chunks = await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()

        merged: List[Dict[str, Any]] = []
        for chunk in chunks:
            if isinstance(chunk, list):
                merged.extend(chunk)
        return merged

    def _rank_matches(
        self,
        *,
        base_query: str,
        normalized_question: Dict[str, Any],
        rows: Sequence[Dict[str, Any]],
        max_matches: int,
    ) -> List[Dict[str, Any]]:
        dedup: Dict[str, Dict[str, Any]] = {}
        stem = str(normalized_question.get("stem") or "")
        math_query = str(normalized_question.get("math_only_query") or "")
        query_text = stem or base_query
        query_tokens = self._tokenize(query_text)
        math_tokens = self._tokenize(math_query)

        candidates: List[Dict[str, Any]] = []
        for row in rows:
            url = str(row.get("url") or "").strip()
            if not url.startswith(("http://", "https://")):
                continue
            title = str(row.get("title") or "").strip()
            snippet = str(row.get("snippet") or "").strip()
            query_variant = str(row.get("query_variant") or "")
            source = self._source_from_url(url)
            combined = " ".join(part for part in (title, snippet) if part).strip()
            if not combined:
                continue
            candidates.append(
                {
                    "url": url,
                    "title": title,
                    "snippet": snippet,
                    "combined": combined,
                    "query_variant": query_variant,
                    "source": source,
                    "tokens": self._tokenize(combined),
                    "title_tokens": self._tokenize(title),
                }
            )

        if not candidates:
            return []

        bm25 = _BM25Ranker([row["tokens"] for row in candidates])
        bm25_scores = bm25.score(query_tokens)
        max_bm25 = max(bm25_scores) if bm25_scores else 0.0
        q_embed = self._embedding.encode(query_text)

        for idx, row in enumerate(candidates):
            url = row["url"]
            title = row["title"]
            snippet = row["snippet"]
            combined = row["combined"]
            query_variant = row["query_variant"]
            source = row["source"]

            lexical = self._token_similarity(query_text, combined)
            ratio = SequenceMatcher(a=query_text.lower(), b=combined.lower()).ratio()
            math_overlap = self._math_overlap_tokens(math_tokens, combined)
            title_overlap = self._token_similarity(query_text, title) if title else 0.0
            emb = cosine_similarity(q_embed, self._embedding.encode(combined))
            bm25_norm = (bm25_scores[idx] / max_bm25) if max_bm25 > 0 else 0.0
            domain_boost = self._source_boost(source)
            variant_boost = 0.03 if query_variant == "exact" else (0.02 if query_variant == "partial" else 0.01)

            score = (
                (0.32 * bm25_norm)
                + (0.26 * emb)
                + (0.15 * lexical)
                + (0.10 * ratio)
                + (0.07 * math_overlap)
                + (0.04 * title_overlap)
                + domain_boost
                + variant_boost
            )
            score = max(0.0, min(0.995, score))

            existing = dedup.get(url.lower())
            candidate = {
                "url": url[:600],
                "title": title[:240],
                "similarity": round(score, 6),
                "snippet": snippet[:420],
                "source": source,
                "query_variant": query_variant,
                "rank_features": {
                    "bm25": round(bm25_norm, 6),
                    "embedding": round(emb, 6),
                    "lexical": round(lexical, 6),
                    "ratio": round(ratio, 6),
                    "math_overlap": round(math_overlap, 6),
                    "title_overlap": round(title_overlap, 6),
                },
            }
            if existing is None or float(candidate["similarity"]) > float(existing.get("similarity", 0.0)):
                dedup[url.lower()] = candidate

        matches = list(dedup.values())
        matches.sort(key=lambda row: float(row.get("similarity", 0.0)), reverse=True)
        return matches[: max(1, int(max_matches))]

    def _build_query_variants(self, normalized_question: Dict[str, Any]) -> List[Dict[str, str]]:
        exact = str(normalized_question.get("search_query") or "").strip()
        partial = str(normalized_question.get("partial_query") or "").strip()
        math_only = str(normalized_question.get("math_only_query") or "").strip()

        variants: List[Dict[str, str]] = []
        if exact:
            variants.append({"kind": "exact", "query": exact})
        if partial and partial.lower() != exact.lower():
            variants.append({"kind": "partial", "query": partial})
        if math_only:
            variants.append({"kind": "math_only", "query": math_only})

        # Domain-constrained probes for educational solution surfaces.
        seed = exact or partial
        for domain in _PRIORITY_DOMAINS:
            if not seed:
                break
            variants.append({"kind": "domain", "query": f"site:{domain} {seed}"})

        dedup: List[Dict[str, str]] = []
        seen: set[str] = set()
        for row in variants:
            q = re.sub(r"\s+", " ", str(row.get("query") or "").strip())
            if not q:
                continue
            key = q.lower()
            if key in seen:
                continue
            seen.add(key)
            dedup.append({"kind": str(row.get("kind") or "exact"), "query": q})
            if len(dedup) >= 10:
                break
        return dedup

    def _source_from_url(self, url: str) -> str:
        parsed = urlparse(str(url or ""))
        host = str(parsed.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        for domain, source in _SOURCE_DOMAIN_MAP.items():
            if host == domain or host.endswith(f".{domain}"):
                return source
        return host or "web"

    def _source_boost(self, source: str) -> float:
        if source in {"stackexchange", "jee_pyq_archive"}:
            return 0.06
        if source in {"physics_forum", "vedantu", "toppr", "byju"}:
            return 0.04
        if source == "chegg":
            return 0.02
        return 0.0

    def _token_similarity(self, left: str, right: str) -> float:
        lhs = {token for token in self._tokenize(left) if len(token) >= 2}
        rhs = {token for token in self._tokenize(right) if len(token) >= 2}
        if not lhs or not rhs:
            return 0.0
        inter = len(lhs & rhs)
        union = len(lhs | rhs)
        return float(inter / max(1, union))

    def _math_overlap(self, math_query: str, text: str) -> float:
        tokens = self._tokenize(math_query)
        return self._math_overlap_tokens(tokens, text)

    def _math_overlap_tokens(self, math_tokens: Sequence[str], text: str) -> float:
        if not math_tokens:
            return 0.0
        low = str(text or "").lower()
        hits = sum(1 for tok in math_tokens if tok in low)
        return float(hits / max(1, len(math_tokens)))

    def _tokenize(self, text: str) -> List[str]:
        return [
            tok
            for tok in re.findall(r"[a-z0-9_+\-*/=^]+", str(text or "").lower())
            if tok
        ]


class _BM25Ranker:
    def __init__(self, documents: Sequence[Sequence[str]], *, k1: float = 1.6, b: float = 0.75) -> None:
        self._docs = [list(doc) for doc in documents]
        self._k1 = k1
        self._b = b
        self._doc_freq: Dict[str, int] = {}
        self._avg_len = 0.0
        self._build()

    def _build(self) -> None:
        lengths = [len(doc) for doc in self._docs]
        self._avg_len = float(sum(lengths) / max(1, len(lengths)))
        for doc in self._docs:
            seen: set[str] = set()
            for tok in doc:
                if tok in seen:
                    continue
                seen.add(tok)
                self._doc_freq[tok] = self._doc_freq.get(tok, 0) + 1

    def score(self, query_tokens: Sequence[str]) -> List[float]:
        if not self._docs:
            return []
        n_docs = len(self._docs)
        scores: List[float] = []
        for doc in self._docs:
            dl = len(doc)
            tf: Dict[str, int] = {}
            for tok in doc:
                tf[tok] = tf.get(tok, 0) + 1
            score = 0.0
            for tok in query_tokens:
                df = self._doc_freq.get(tok, 0)
                if df == 0:
                    continue
                idf = math.log(1.0 + (n_docs - df + 0.5) / (df + 0.5))
                freq = float(tf.get(tok, 0))
                if freq <= 0:
                    continue
                denom = freq + self._k1 * (1.0 - self._b + self._b * (dl / max(1.0, self._avg_len)))
                score += idf * ((freq * (self._k1 + 1.0)) / denom)
            scores.append(score)
        return scores


class _HybridEmbedder:
    def __init__(self, *, dim: int = 256, cache_size: int = 512) -> None:
        self._hash = HashEmbedding(dim=dim)
        self._model = None
        self._cache: OrderedDict[str, List[float]] = OrderedDict()
        self._cache_size = max(32, cache_size)
        self._enabled = str(os.getenv("LC9_SEMANTIC_EMBEDDINGS", "1")).strip().lower() not in {
            "0",
            "false",
            "off",
        }
        self._model_name = str(os.getenv("LC9_EMBEDDING_MODEL", "all-MiniLM-L6-v2")).strip()

    def encode(self, text: str) -> List[float]:
        key = " ".join(str(text or "").strip().lower().split())
        if not key:
            return self._hash.encode("")
        cached = self._cache.get(key)
        if cached is not None:
            self._cache.move_to_end(key)
            return cached
        vec = self._encode_fresh(key)
        self._cache[key] = vec
        if len(self._cache) > self._cache_size:
            self._cache.popitem(last=False)
        return vec

    def _encode_fresh(self, text: str) -> List[float]:
        if not self._enabled:
            return self._hash.encode(text)
        model = self._load_model()
        if model is None:
            return self._hash.encode(text)
        try:
            vec = model.encode([text], normalize_embeddings=True)[0]
            return list(vec)
        except Exception:
            return self._hash.encode(text)

    def _load_model(self):
        if self._model is not None or not self._enabled:
            return self._model
        try:
            from sentence_transformers import SentenceTransformer
        except Exception:
            self._model = None
            return None
        try:
            self._model = SentenceTransformer(self._model_name)
        except Exception:
            self._model = None
        return self._model
