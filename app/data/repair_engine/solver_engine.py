from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


@dataclass
class SolverResult:
    attempted: bool
    verified: bool
    answer_mismatch: bool
    notes: list[str]
    computed_answer: str | None
    computed_label: str | None = None
    suggested_correct_answer: dict[str, Any] | None = None


class DeterministicSolverEngine:
    """Low-risk symbolic checks for limit/integral style expressions."""

    def __init__(self) -> None:
        self._sympy_ok = False
        self._sympy = None
        try:
            import sympy as sp

            self._sympy_ok = True
            self._sympy = sp
        except Exception:
            self._sympy_ok = False
            self._sympy = None

    def verify(
        self,
        *,
        question_text: str,
        options: list[dict[str, Any]],
        correct_answer: dict[str, Any],
        question_type: str,
    ) -> SolverResult:
        if not self._sympy_ok or self._sympy is None:
            return SolverResult(
                attempted=False,
                verified=False,
                answer_mismatch=False,
                notes=["sympy_unavailable"],
                computed_answer=None,
                computed_label=None,
                suggested_correct_answer=None,
            )

        low = (question_text or "").lower()
        if "lim" in low:
            result = self._verify_limit(
                question_text=question_text,
                options=options,
                correct_answer=correct_answer,
                question_type=question_type,
            )
            if result.attempted:
                return result
        if "∫" in question_text or "\\int" in question_text or "integral" in low:
            result = self._verify_definite_integral(
                question_text=question_text,
                options=options,
                correct_answer=correct_answer,
                question_type=question_type,
            )
            if result.attempted:
                return result
        return SolverResult(
            attempted=False,
            verified=False,
            answer_mismatch=False,
            notes=["solver_rule_not_applicable"],
            computed_answer=None,
            computed_label=None,
            suggested_correct_answer=None,
        )

    def _verify_limit(
        self,
        *,
        question_text: str,
        options: list[dict[str, Any]],
        correct_answer: dict[str, Any],
        question_type: str,
    ) -> SolverResult:
        sp = self._sympy
        if sp is None:
            return SolverResult(False, False, False, ["sympy_unavailable"], None, None, None)
        pattern = re.compile(
            r"(?i)lim(?:_\{)?\s*([A-Za-z])\s*(?:->|→)\s*([+\-]?\d+(?:\.\d+)?|0\+|0\-|[A-Za-z]+)(?:\})?\s*(.+)"
        )
        match = pattern.search(question_text)
        if not match:
            return SolverResult(False, False, False, ["limit_pattern_not_found"], None, None, None)
        var = match.group(1)
        point = match.group(2).replace(" ", "")
        expr_src = match.group(3).strip(" .;,:")
        if not expr_src:
            return SolverResult(False, False, False, ["limit_expression_missing"], None, None, None)
        try:
            x = sp.Symbol(var)
            expr = sp.sympify(expr_src.replace("^", "**"))
            point_expr = sp.sympify(point.replace("+", "") if point in {"0+", "0-"} else point)
            direction = "+" if point == "0+" else "-" if point == "0-" else "+-"
            value = sp.limit(expr, x, point_expr, dir=direction)
            computed = str(sp.simplify(value))
        except Exception:
            return SolverResult(True, False, False, ["limit_solver_failed"], None, None, None)
        return self._compare_answer(
            computed=computed,
            options=options,
            correct_answer=correct_answer,
            question_type=question_type,
            context_note="limit",
        )

    def _verify_definite_integral(
        self,
        *,
        question_text: str,
        options: list[dict[str, Any]],
        correct_answer: dict[str, Any],
        question_type: str,
    ) -> SolverResult:
        sp = self._sympy
        if sp is None:
            return SolverResult(False, False, False, ["sympy_unavailable"], None, None, None)
        pattern = re.compile(
            r"(?is)(?:int|∫)\s*[_\{(]?\s*([+\-]?\d+(?:\.\d+)?)\s*[,)]?\s*[\^]?\s*([+\-]?\d+(?:\.\d+)?)\s*[\})]?\s*(.+?)\s*(?:dx|d\s*x)\b"
        )
        match = pattern.search(question_text)
        if not match:
            return SolverResult(False, False, False, ["integral_pattern_not_found"], None, None, None)
        lower = sp.sympify(match.group(1))
        upper = sp.sympify(match.group(2))
        expr_src = match.group(3).strip(" .;,:")
        try:
            x = sp.Symbol("x")
            expr = sp.sympify(expr_src.replace("^", "**"))
            value = sp.integrate(expr, (x, lower, upper))
            computed = str(sp.simplify(value))
        except Exception:
            return SolverResult(True, False, False, ["integral_solver_failed"], None, None, None)
        return self._compare_answer(
            computed=computed,
            options=options,
            correct_answer=correct_answer,
            question_type=question_type,
            context_note="integral",
        )

    def _compare_answer(
        self,
        *,
        computed: str,
        options: list[dict[str, Any]],
        correct_answer: dict[str, Any],
        question_type: str,
        context_note: str,
    ) -> SolverResult:
        sp = self._sympy
        if sp is None:
            return SolverResult(False, False, False, ["sympy_unavailable"], None, None, None)

        q_type = (question_type or "").upper()
        notes = [f"{context_note}_computed"]
        mismatch = False
        verified = False
        computed_label = ""
        suggested_correct_answer: dict[str, Any] | None = None

        if q_type == "NUMERICAL":
            expected = str(correct_answer.get("numerical") or "").strip()
            if expected:
                try:
                    verified = bool(sp.simplify(sp.sympify(expected) - sp.sympify(computed)) == 0)
                except Exception:
                    verified = expected == computed
                mismatch = not verified
                if mismatch:
                    suggested_correct_answer = {
                        "single": None,
                        "multiple": [],
                        "numerical": computed,
                        "tolerance": correct_answer.get("tolerance"),
                    }
        else:
            labels = [str(opt.get("label", "")).upper() for opt in options if isinstance(opt, dict)]
            by_label = {
                str(opt.get("label", "")).upper(): str(opt.get("text", "")).strip()
                for opt in options
                if isinstance(opt, dict)
            }
            computed_label = ""
            for label in labels:
                text = by_label.get(label, "")
                if not text:
                    continue
                try:
                    same = bool(sp.simplify(sp.sympify(text.replace("^", "**")) - sp.sympify(computed)) == 0)
                except Exception:
                    same = text == computed
                if same:
                    computed_label = label
                    break
            expected_label = str(correct_answer.get("single") or "").upper()
            if computed_label and expected_label:
                verified = computed_label == expected_label
                mismatch = not verified
                if mismatch:
                    suggested_correct_answer = {
                        "single": computed_label,
                        "multiple": [computed_label],
                        "numerical": None,
                        "tolerance": correct_answer.get("tolerance"),
                    }
            elif computed_label and not expected_label:
                verified = True
                suggested_correct_answer = {
                    "single": computed_label,
                    "multiple": [computed_label],
                    "numerical": None,
                    "tolerance": correct_answer.get("tolerance"),
                }
            else:
                notes.append("option_match_not_found")

        return SolverResult(
            attempted=True,
            verified=verified,
            answer_mismatch=mismatch,
            notes=notes,
            computed_answer=computed,
            computed_label=computed_label or None,
            suggested_correct_answer=suggested_correct_answer,
        )
