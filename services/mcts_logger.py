from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict

from core.db.connection import Database


class MCTSLogger:
    """
    Persists MCTS telemetry to Postgres with jsonl fallback.
    """

    def __init__(self, *, fallback_log_path: str = "data/lc9/AI_MCTS_LOG.jsonl") -> None:
        self._fallback_log_path = Path(fallback_log_path)
        self._fallback_log_path.parent.mkdir(parents=True, exist_ok=True)

    async def log_event(
        self,
        *,
        question: str,
        iterations: int,
        nodes_explored: int,
        tool_calls: int,
        retrieval_calls: int,
        verification_pass: bool,
        final_confidence: float,
        metadata: Dict[str, Any] | None = None,
    ) -> None:
        row = {
            "ts": int(time.time()),
            "question": str(question or "")[:4000],
            "iterations": int(max(0, iterations)),
            "nodes_explored": int(max(0, nodes_explored)),
            "tool_calls": int(max(0, tool_calls)),
            "retrieval_calls": int(max(0, retrieval_calls)),
            "verification_pass": bool(verification_pass),
            "final_confidence": float(max(0.0, min(1.0, final_confidence))),
            "metadata": dict(metadata or {}),
        }

        try:
            pool = await Database.get_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO ai_mcts_log (
                        question,
                        iterations,
                        nodes_explored,
                        tool_calls,
                        retrieval_calls,
                        verification_pass,
                        final_confidence,
                        metadata_json
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
                    """,
                    row["question"],
                    row["iterations"],
                    row["nodes_explored"],
                    row["tool_calls"],
                    row["retrieval_calls"],
                    row["verification_pass"],
                    row["final_confidence"],
                    json.dumps(row["metadata"], ensure_ascii=True),
                )
                await conn.execute(
                    "DELETE FROM ai_mcts_log WHERE created_at < (NOW() - INTERVAL '45 days')"
                )
            return
        except Exception:
            pass

        with self._fallback_log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")
