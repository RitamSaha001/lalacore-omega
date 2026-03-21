from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class GraphRepairResult:
    text: str
    actions: list[str]
    issues: list[str]


class GraphRepairEngine:
    """Rule-driven repair over token adjacency / expression graph patterns."""

    _FUNC_NO_PAREN_RE = re.compile(
        r"(?i)\b(sin|cos|tan|cot|sec|cosec|log|ln|sqrt|sgn)\s+([A-Za-z](?:\^\d+)?|\d+)\b"
    )

    def repair(self, text: str) -> GraphRepairResult:
        if not text:
            return GraphRepairResult(text="", actions=[], issues=["empty_expression"])
        out = text
        actions: list[str] = []
        issues: list[str] = []

        out2 = self._repair_adjacent_variables(out)
        if out2 != out:
            out = out2
            actions.append("graph_adjacent_variable_fix")

        out2 = self._repair_missing_exponent(out)
        if out2 != out:
            out = out2
            actions.append("graph_missing_exponent_fix")

        out2 = self._repair_missing_parentheses(out)
        if out2 != out:
            out = out2
            actions.append("graph_missing_parentheses_fix")

        out2 = self._repair_missing_multiplication(out)
        if out2 != out:
            out = out2
            actions.append("graph_missing_multiplication_fix")

        out2 = self._repair_limit_form(out)
        if out2 != out:
            out = out2
            actions.append("graph_limit_fix")

        if re.search(r"[=+\-*/^(:]\s*$", out):
            issues.append("dangling_operator")
        if out.count("(") != out.count(")"):
            issues.append("unbalanced_parenthesis")

        return GraphRepairResult(text=re.sub(r"\s+", " ", out).strip(), actions=actions, issues=issues)

    def _repair_adjacent_variables(self, text: str) -> str:
        out = text
        out = re.sub(r"\b([A-Za-z])\s+\1\b", r"\1^2", out)
        out = re.sub(r"\b([A-Za-z])\s+\1\s+\1\b", r"\1^3", out)
        return out

    def _repair_missing_exponent(self, text: str) -> str:
        out = text
        out = re.sub(r"\b([A-Za-z])([2-9])\b", r"\1^\2", out)
        out = re.sub(r"\bsgn\(\s*([A-Za-z])([2-9])\s*\)", r"sgn(\1^\2)", out, flags=re.IGNORECASE)
        return out

    def _repair_missing_parentheses(self, text: str) -> str:
        return self._FUNC_NO_PAREN_RE.sub(lambda m: f"{m.group(1).lower()}({m.group(2)})", text)

    def _repair_missing_multiplication(self, text: str) -> str:
        out = text
        # Conservative fixes only; avoid corrupting function notation like f(x).
        out = re.sub(r"(\))([A-Za-z0-9])", r"\1*\2", out)
        out = re.sub(r"(?<![A-Za-z0-9_])(\d+(?:\.\d+)?)([A-Za-z])", r"\1*\2", out)
        out = re.sub(r"(?<![A-Za-z0-9_])(\d+(?:\.\d+)?)\(", r"\1*(", out)
        return out

    def _repair_limit_form(self, text: str) -> str:
        out = text
        out = re.sub(
            r"(?i)\blim\s+([A-Za-z])\s*->\s*([+\-]?\d+(?:\.\d+)?|0\+|0\-|[A-Za-z]+)",
            lambda m: f"lim_{{{m.group(1)}->{m.group(2)}}}",
            out,
        )
        out = re.sub(r"(?i)\blim\s+([A-Za-z])\s*->\s*(?=[^\w]|$)", r"lim_{\1->?}", out)
        return out
