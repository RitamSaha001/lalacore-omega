from __future__ import annotations

from pydantic import BaseModel


class ArenaResponse(BaseModel):
    provider: str
    final_answer: str
    critic_score: float
    deterministic_pass: bool
    confidence: float
    skill: float
    reasoning: str
