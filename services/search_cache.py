from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Dict, Tuple

from core.db.connection import Database


class SearchCacheStore:
    """
    Postgres-first cache + telemetry with graceful in-memory/file fallback.
    """

    def __init__(
        self,
        *,
        ttl_days: int = 7,
        memory_limit: int = 512,
        fallback_log_path: str = "data/lc9/AI_CHAT_SEARCH_LOG.jsonl",
    ) -> None:
        self.ttl_s = int(max(60, ttl_days * 24 * 60 * 60))
        self.memory_limit = int(max(16, memory_limit))
        self._memory: Dict[str, Dict[str, Any]] = {}
        self._fallback_log_path = Path(fallback_log_path)
        self._fallback_log_path.parent.mkdir(parents=True, exist_ok=True)

    def query_hash(self, query: str) -> str:
        canonical = " ".join(str(query or "").strip().lower().split())
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    async def get_cached_search(self, query: str) -> Tuple[Dict[str, Any] | None, bool]:
        q_hash = self.query_hash(query)
        now = time.time()

        mem_row = self._memory.get(q_hash)
        if isinstance(mem_row, dict) and float(mem_row.get("expires_at", 0.0)) > now:
            return dict(mem_row.get("value") or {}), True
        if isinstance(mem_row, dict):
            self._memory.pop(q_hash, None)

        try:
            pool = await Database.get_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT results_json
                    FROM question_search_cache
                    WHERE query_hash = $1
                      AND updated_at >= (NOW() - INTERVAL '7 days')
                    LIMIT 1
                    """,
                    q_hash,
                )
            if row and isinstance(row.get("results_json"), dict):
                value = dict(row["results_json"])
                self._put_memory(q_hash, value)
                return value, True
        except Exception:
            return None, False

        return None, False

    async def put_cached_search(self, *, query: str, results: Dict[str, Any]) -> None:
        q_hash = self.query_hash(query)
        payload = dict(results or {})
        self._put_memory(q_hash, payload)
        try:
            pool = await Database.get_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO question_search_cache (query_hash, query_text, results_json, updated_at)
                    VALUES ($1, $2, $3::jsonb, NOW())
                    ON CONFLICT (query_hash)
                    DO UPDATE SET
                        query_text = EXCLUDED.query_text,
                        results_json = EXCLUDED.results_json,
                        updated_at = NOW()
                    """,
                    q_hash,
                    str(query or "")[:1200],
                    json.dumps(payload, ensure_ascii=True),
                )
                # Lazy cleanup keeps table bounded without background worker.
                await conn.execute(
                    "DELETE FROM question_search_cache WHERE updated_at < (NOW() - INTERVAL '7 days')"
                )
        except Exception:
            return

    async def log_search_event(
        self,
        *,
        question: str,
        ocr_used: bool,
        web_results_found: int,
        solution_used: bool,
        lalacore_provider: str,
        arena_triggered: bool,
        verification_passed: bool,
        mismatch_detected: bool = False,
        metadata: Dict[str, Any] | None = None,
    ) -> None:
        row = {
            "ts": int(time.time()),
            "question": str(question or "")[:4000],
            "ocr_used": bool(ocr_used),
            "web_results_found": int(max(0, web_results_found)),
            "solution_used": bool(solution_used),
            "lalacore_provider": str(lalacore_provider or ""),
            "arena_triggered": bool(arena_triggered),
            "verification_passed": bool(verification_passed),
            "mismatch_detected": bool(mismatch_detected),
            "metadata": dict(metadata or {}),
        }
        try:
            pool = await Database.get_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO ai_chat_search_log (
                        question,
                        ocr_used,
                        web_results_found,
                        solution_used,
                        lalacore_provider,
                        arena_triggered,
                        verification_passed,
                        mismatch_detected,
                        metadata_json
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
                    """,
                    row["question"],
                    row["ocr_used"],
                    row["web_results_found"],
                    row["solution_used"],
                    row["lalacore_provider"],
                    row["arena_triggered"],
                    row["verification_passed"],
                    row["mismatch_detected"],
                    json.dumps(row["metadata"], ensure_ascii=True),
                )
                # Keep a rolling retention horizon.
                await conn.execute(
                    "DELETE FROM ai_chat_search_log WHERE created_at < (NOW() - INTERVAL '45 days')"
                )
            return
        except Exception:
            pass

        # File fallback if database is unavailable in local/dev mode.
        with self._fallback_log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")

    def _put_memory(self, q_hash: str, payload: Dict[str, Any]) -> None:
        now = time.time()
        self._memory[q_hash] = {
            "value": dict(payload),
            "expires_at": now + float(self.ttl_s),
            "updated_at": now,
        }
        if len(self._memory) <= self.memory_limit:
            return
        oldest = sorted(
            self._memory.keys(),
            key=lambda key: float((self._memory.get(key) or {}).get("updated_at", 0.0)),
        )
        for stale_key in oldest[: len(self._memory) - self.memory_limit]:
            self._memory.pop(stale_key, None)
