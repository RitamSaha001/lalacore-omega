from __future__ import annotations

from dataclasses import dataclass
from typing import Any

try:
    from .repair_engine.math_repair_engine import MathRepairEngine
except Exception:  # pragma: no cover - direct module execution fallback
    from repair_engine.math_repair_engine import MathRepairEngine


@dataclass
class QuestionRepairResult:
    question_text: str
    options: list[dict[str, str]]
    correct_answer: dict[str, Any]
    repair_actions: list[str]
    repair_issues: list[str]
    repair_confidence: float
    repair_status: str


class QuestionRepairEngine:
    """Compatibility wrapper around the modular MathRepairEngine pipeline."""

    def __init__(self) -> None:
        self._engine = MathRepairEngine()

    def repair_question(
        self,
        *,
        question_text: str,
        options: list[dict[str, str]] | None,
        correct_answer: dict[str, Any] | None,
        question_type: str = "",
        question_id: str = "",
        corpus: list[dict[str, Any]] | None = None,
    ) -> QuestionRepairResult:
        payload = {
            "question_id": question_id,
            "question_text": question_text,
            "options": list(options or []),
            "correct_answer": dict(correct_answer or {}),
            "type": question_type,
        }
        repaired = self._engine.repair_question(payload, corpus=corpus)
        return QuestionRepairResult(
            question_text=repaired.repaired_question_text,
            options=list(repaired.options),
            correct_answer=dict(repaired.correct_answer),
            repair_actions=list(repaired.repair_actions),
            repair_issues=list(repaired.validation_issues),
            repair_confidence=float(repaired.repair_confidence),
            repair_status=str(repaired.repair_status),
        )
