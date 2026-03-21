from __future__ import annotations

import re
import warnings
from dataclasses import dataclass
from typing import Any


@dataclass
class ValidationResult:
    issues: list[str]
    grammar_valid: bool
    sanity_valid: bool


class MathematicalSanityValidator:
    """Syntax and semantic guards for repaired JEE math questions."""

    _DANGLING_RE = re.compile(r"[=+\-*/^(:]\s*$")
    _FUNC_DANGLING_RE = re.compile(
        r"(?i)\b(?:sin|cos|tan|cot|sec|cosec|log|ln|sqrt|lim|sgn)\s*$"
    )

    def __init__(self) -> None:
        self._parse_expr = None
        try:
            from sympy.parsing.sympy_parser import parse_expr

            self._parse_expr = parse_expr
        except Exception:
            self._parse_expr = None

    def validate(
        self,
        *,
        question_text: str,
        options: list[dict[str, Any]],
        question_type: str,
    ) -> ValidationResult:
        issues: list[str] = []
        q = (question_text or "").strip()
        if not q:
            issues.append("empty_question")
        if q and not self._balanced_brackets(q):
            issues.append("unbalanced_brackets")
        if q and self._DANGLING_RE.search(q):
            issues.append("dangling_operator")
        if q and self._FUNC_DANGLING_RE.search(q):
            issues.append("dangling_function")
        if "=" in q and re.search(r"=\s*(?:$|[,;])", q):
            issues.append("equation_rhs_missing")

        q_type = (question_type or "").upper()
        if q_type != "NUMERICAL" and not options:
            issues.append("missing_options")
        if q_type in {"LIST_MATCH"} and not self._looks_like_list_match(q, options):
            issues.append("list_match_structure_missing")
        if q_type == "PARAGRAPH" and len(q) < 120:
            issues.append("paragraph_too_short")
        if "for x" in q.lower() and q.count("for") == 1:
            issues.append("piecewise_case_incomplete")

        if self._parse_expr is not None and self._should_run_expression_parse(q):
            parse_ok = self._validate_expression_fragments(q)
            if not parse_ok:
                issues.append("expression_parse_failure")

        hard = {
            "empty_question",
            "unbalanced_brackets",
            "equation_rhs_missing",
            "missing_options",
        }
        grammar_valid = not any(token in issues for token in hard)
        sanity_valid = not any(
            token in issues
            for token in (
                "empty_question",
                "piecewise_case_incomplete",
                "list_match_structure_missing",
            )
        )
        return ValidationResult(issues=issues, grammar_valid=grammar_valid, sanity_valid=sanity_valid)

    def _validate_expression_fragments(self, text: str) -> bool:
        candidates = re.findall(r"[A-Za-z0-9_()+\-*/^.=]{7,}", text)
        if not candidates:
            return True
        attempted = 0
        parse_fail = 0
        for fragment in candidates[:4]:
            if not any(op in fragment for op in ("+", "-", "*", "/", "^", "=")):
                continue
            attempted += 1
            compact = fragment.replace("^", "**").replace("ln(", "log(")
            if compact.count("=") == 1:
                left, right = compact.split("=", 1)
                compact = f"({left})-({right})"
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    self._parse_expr(compact, evaluate=False)
            except Exception:
                parse_fail += 1
        if attempted <= 1:
            return True
        return parse_fail <= 1

    def _should_run_expression_parse(self, text: str) -> bool:
        if not text:
            return False
        operators = sum(text.count(op) for op in ("+", "-", "*", "/", "^", "="))
        if operators < 2:
            return False
        words = re.findall(r"[A-Za-z]{3,}", text)
        # Skip long natural-language statements; parse only expression-heavy prompts.
        return len(words) <= 45

    def _balanced_brackets(self, text: str) -> bool:
        stack: list[str] = []
        pairs = {")": "(", "]": "[", "}": "{"}
        for ch in text:
            if ch in "([{":
                stack.append(ch)
            elif ch in ")]}":
                if not stack or stack[-1] != pairs[ch]:
                    return False
                stack.pop()
        return not stack

    def _looks_like_list_match(self, question_text: str, options: list[dict[str, Any]]) -> bool:
        blob = (question_text or "") + " " + " ".join(
            str(opt.get("text", "")) for opt in options if isinstance(opt, dict)
        )
        if re.search(r"(?i)list[-\s]*i", blob) and re.search(r"(?i)list[-\s]*ii", blob):
            return True
        if re.search(r"\([pqrs]\)", blob, flags=re.IGNORECASE) and re.search(r"\([1-4]\)", blob):
            return True
        return False
