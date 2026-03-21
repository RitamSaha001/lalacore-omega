from __future__ import annotations

import re
from typing import Any


class QuestionTypeClassifier:
    """Context-aware detector for JEE question format variants."""

    def classify(
        self,
        *,
        question_text: str,
        options: list[dict[str, Any]],
        declared_type: str = "",
    ) -> str:
        declared = (declared_type or "").upper().strip()
        if declared in {"MCQ_SINGLE", "MCQ_MULTI", "NUMERICAL", "LIST_MATCH", "PARAGRAPH"}:
            return declared

        q = (question_text or "").lower()
        labels = [str(opt.get("label", "")).upper() for opt in options if isinstance(opt, dict)]
        opt_text = " ".join(str(opt.get("text", "")) for opt in options if isinstance(opt, dict)).lower()
        has_options = len(options) > 0

        if self._is_list_match(q, labels, opt_text):
            return "LIST_MATCH"
        if self._is_paragraph(q, options):
            return "PARAGRAPH"
        if not has_options:
            return "NUMERICAL"
        if any(token in q for token in ("one or more", "more than one", "all correct", "select all")):
            return "MCQ_MULTI"
        return "MCQ_SINGLE"

    def _is_list_match(self, q: str, labels: list[str], opt_text: str) -> bool:
        if "list-i" in q or "list ii" in q or "matrix match" in q:
            return True
        if {"P", "Q", "R", "S"}.issubset(set(labels)):
            return True
        if re.search(r"\([pqrs]\)\s*[-:>]", opt_text, flags=re.IGNORECASE):
            return True
        if re.search(r"\([1-4]\)\s*[-:>]", opt_text):
            return True
        return False

    def _is_paragraph(self, q: str, options: list[dict[str, Any]]) -> bool:
        if any(token in q for token in ("paragraph", "read the following", "statement i", "statement ii")):
            return True
        if len(q) > 460 and len(options) >= 4:
            return True
        return False
