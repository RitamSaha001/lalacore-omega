from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Dict

from core.bootstrap import initialize_keys
from core.lalacore_x.crash_replay import CrashReplayRecorder
from core.lalacore_x.engine import LalaCoreXEngine
from core.lalacore_x.runtime_telemetry import RuntimeTelemetry
from telemetry.logger import log_solve


_ENGINE: LalaCoreXEngine | None = None
_RUNTIME_TELEMETRY = RuntimeTelemetry()
_CRASH_RECORDER = CrashReplayRecorder()


def _get_engine() -> LalaCoreXEngine:
    global _ENGINE
    if _ENGINE is None:
        initialize_keys(silent=True)
        _ENGINE = LalaCoreXEngine()
    return _ENGINE


async def solve_question(question: str) -> Dict:
    try:
        result = await _get_engine().solve(question)
        log_solve(result)
        return result
    except Exception as exc:  # pragma: no cover - fatal safety net
        _RUNTIME_TELEMETRY.log_exception(
            exception_type=type(exc).__name__,
            module="core.solver",
            function="solve_question",
            input_size=len(question or ""),
            entropy=None,
            active_providers=[],
            mini_eligible=None,
            token_usage=None,
            extra={"fatal_fallback": True},
        )
        _CRASH_RECORDER.record(
            exception_type=type(exc).__name__,
            message=str(exc),
            snapshot={
                "question_hash": _get_engine()._sha1(question),
                "responses": [],
                "entropy": 0.0,
                "matches": [],
                "bt_thetas": {},
                "mini_eligible": False,
                "active_providers": [],
            },
        )
        fallback = {
            "question": question,
            "reasoning": "",
            "final_answer": "",
            "verification": {"verified": False, "risk_score": 1.0, "reason": "fatal_fallback"},
            "routing_decision": "fatal_fallback",
            "escalate": True,
            "winner_provider": "",
            "profile": {
                "subject": "general",
                "difficulty": "unknown",
                "numeric": False,
                "multiConcept": False,
                "trapProbability": 0.0,
            },
            "arena": {
                "ranked_providers": [],
                "judge_results": [],
                "bt_thetas": {},
                "posteriors": {},
                "winner_margin": 0.0,
                "arena_confidence": 0.0,
                "pairwise_confidence_margin": 0.0,
                "uncertainty_adjusted_margin": 0.0,
                "disagreement": 0.0,
                "disagreement_case_count": 0,
                "deterministic_dominance": False,
                "entropy": 0.0,
            },
            "retrieval": {"top_blocks": [], "claim_support_score": 0.0},
            "engine": {
                "name": "LALACORE_X",
                "version": "research-grade-v2",
                "backward_compatible": True,
                "mini_shadow": False,
                "degraded_mode": True,
                "degraded_reason": "fatal_fallback",
            },
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        log_solve(fallback)
        return fallback


def solve_question_sync(question: str) -> Dict:
    """
    Backward-compatible sync wrapper for scripts.
    """
    return asyncio.run(solve_question(question))
