from __future__ import annotations

import re
from typing import Any, Dict

import sympy as sp
from sympy.parsing.sympy_parser import (
    convert_xor,
    implicit_multiplication_application,
    parse_expr,
    standard_transformations,
)

_X = sp.Symbol("x", real=True)
_TRANSFORMS = standard_transformations + (implicit_multiplication_application, convert_xor)
_LOCAL_DICT = {
    "x": _X,
    "pi": sp.pi,
    "e": sp.E,
    "oo": sp.oo,
    "sin": sp.sin,
    "cos": sp.cos,
    "tan": sp.tan,
    "asin": sp.asin,
    "acos": sp.acos,
    "atan": sp.atan,
    "sqrt": sp.sqrt,
}


def _normalize_text(text: str) -> str:
    value = str(text or "").strip()
    value = value.replace("−", "-").replace("–", "-").replace("—", "-")
    value = value.replace("π", "pi").replace("∞", "inf").replace("√", "sqrt")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _convert_inverse_notation(expr: str) -> str:
    out = str(expr or "")
    patterns = (
        (r"sin\s*\^\s*\(\s*-\s*1\s*\)\s*\(", "asin("),
        (r"cos\s*\^\s*\(\s*-\s*1\s*\)\s*\(", "acos("),
        (r"tan\s*\^\s*\(\s*-\s*1\s*\)\s*\(", "atan("),
        (r"sin\s*\^\s*\{\s*-\s*1\s*\}\s*\(", "asin("),
        (r"cos\s*\^\s*\{\s*-\s*1\s*\}\s*\(", "acos("),
        (r"tan\s*\^\s*\{\s*-\s*1\s*\}\s*\(", "atan("),
        (r"sin\s*\^\s*-1\s*\(", "asin("),
        (r"cos\s*\^\s*-1\s*\(", "acos("),
        (r"tan\s*\^\s*-1\s*\(", "atan("),
    )
    for pattern, repl in patterns:
        out = re.sub(pattern, repl, out, flags=re.IGNORECASE)
    return out


def _clean_expr_text(expr: str) -> str:
    out = _normalize_text(expr)
    out = _convert_inverse_notation(out)
    out = re.sub(r"\binf\b", "oo", out, flags=re.IGNORECASE)
    out = out.strip().rstrip(".").rstrip(",").strip()
    return out


def _parse_expr(expr: str):
    cleaned = _clean_expr_text(expr)
    return parse_expr(cleaned, transformations=_TRANSFORMS, local_dict=_LOCAL_DICT, evaluate=True)


def _format_value(expr) -> str:
    value = sp.simplify(expr)
    text = sp.sstr(value)

    if value.is_number and not value.has(sp.pi, sp.sqrt):
        try:
            numeric = float(sp.N(value, 16))
            formatted = f"{numeric:.6f}".rstrip("0").rstrip(".")
            if len(re.sub(r"[\s+\-]", "", formatted)) < 4:
                formatted = f"{numeric:.4f}"
            return formatted
        except Exception:
            return text

    return text


def _parse_condition(condition_text: str):
    text = _normalize_text(condition_text).rstrip(".")
    m = re.match(r"^\s*x\s*(>=|<=|>|<)\s*(.+)$", text, flags=re.IGNORECASE)
    if not m:
        return None
    op = m.group(1)
    rhs = _parse_expr(m.group(2))
    return op, rhs


def _condition_to_set(cond) -> sp.Set | None:
    if not cond:
        return None
    op, rhs = cond
    if op == ">":
        return sp.Interval.open(rhs, sp.oo)
    if op == ">=":
        return sp.Interval(rhs, sp.oo)
    if op == "<":
        return sp.Interval.open(-sp.oo, rhs)
    if op == "<=":
        return sp.Interval(-sp.oo, rhs)
    return None


def _is_asin_acos_identity(lhs, rhs) -> bool:
    return sp.simplify(lhs - (sp.asin(_X) + sp.acos(_X))) == 0 and sp.simplify(rhs - sp.pi / 2) == 0


def _is_asin_acos_contradiction(lhs, rhs) -> bool:
    return sp.simplify(lhs - (sp.asin(_X) + sp.acos(_X))) == 0 and sp.simplify(rhs - sp.pi) == 0


def _solve_inverse_equation(lhs, rhs, cond_set: sp.Set | None):
    # Known identity shortcuts that SymPy often leaves as ConditionSet.
    if _is_asin_acos_identity(lhs, rhs):
        sol = sp.Interval(-1, 1)
    elif _is_asin_acos_contradiction(lhs, rhs):
        sol = sp.EmptySet
    elif (
        sp.simplify(lhs - sp.asin(_X)) == 0
        and sp.simplify(rhs - sp.acos(_X)) == 0
    ) or (
        sp.simplify(lhs - sp.acos(_X)) == 0
        and sp.simplify(rhs - sp.asin(_X)) == 0
    ):
        sol = sp.FiniteSet(sp.sqrt(2) / 2)
    elif sp.simplify(lhs - (sp.atan(_X) + sp.atan(1 / _X))) == 0 and sp.simplify(rhs - sp.pi / 2) == 0:
        sol = sp.Complement(sp.S.Reals, sp.FiniteSet(0))
    else:
        sol = sp.solveset(sp.Eq(lhs, rhs), _X, domain=sp.S.Reals)

    if cond_set is not None:
        sol = sp.Intersection(sol, cond_set)

    return sp.simplify(sol)


def _solution_to_payload(solution) -> Dict[str, Any]:
    if solution == sp.EmptySet:
        return {
            "answer": "no real solution",
            "expected_expr": None,
            "expected_solution_text": "no real solution",
        }

    if isinstance(solution, sp.FiniteSet):
        elements = list(solution)
        if len(elements) == 1:
            element = sp.simplify(elements[0])
            return {
                "answer": _format_value(element),
                "expected_expr": sp.sstr(element),
                "expected_solution_text": None,
            }
        rendered = ", ".join(_format_value(e) for e in sorted(elements, key=lambda v: float(sp.N(v, 12))))
        answer = "{" + rendered + "}"
        return {
            "answer": answer,
            "expected_expr": None,
            "expected_solution_text": answer,
        }

    if solution == sp.Interval(-1, 1):
        text = "x in [-1, 1]"
        return {
            "answer": text,
            "expected_expr": None,
            "expected_solution_text": text,
        }

    if solution == sp.Interval.open(0, sp.oo):
        text = "all real x > 0"
        return {
            "answer": text,
            "expected_expr": None,
            "expected_solution_text": text,
        }

    if solution == sp.Complement(sp.S.Reals, sp.FiniteSet(0)):
        text = "all real x except 0"
        return {
            "answer": text,
            "expected_expr": None,
            "expected_solution_text": text,
        }

    text = f"x in {sp.sstr(solution)}"
    return {
        "answer": text,
        "expected_expr": None,
        "expected_solution_text": text,
    }


def _extract_evaluate_body(question: str) -> tuple[str, str | None] | None:
    m = re.match(r"^\s*evaluate\s+(.+?)\s*$", question, flags=re.IGNORECASE)
    if not m:
        return None
    body = m.group(1).strip()
    body = re.sub(r"\(\s*limit\s*\)\s*\.?\s*$", "", body, flags=re.IGNORECASE).strip()
    body = re.sub(r"\s+in\s+radians\.?\s*$", "", body, flags=re.IGNORECASE).strip()
    at = re.search(r"\s+at\s+x\s*=\s*(.+)$", body, flags=re.IGNORECASE)
    if at:
        expr_text = body[: at.start()].strip()
        x_value = at.group(1).strip().rstrip(".")
        return expr_text, x_value
    return body.rstrip("."), None


def _extract_solve_body(question: str) -> tuple[str, str | None] | None:
    q = question.strip()
    m_solve = re.match(r"^\s*solve\s+(.+?)\s*(?:find\s+x\.?)?\s*$", q, flags=re.IGNORECASE)
    if m_solve:
        body = m_solve.group(1).strip().rstrip(".")
    else:
        m_find = re.match(r"^\s*find\s+x\s+if\s+(.+?)\s*$", q, flags=re.IGNORECASE)
        if not m_find:
            return None
        body = m_find.group(1).strip().rstrip(".")

    cond_text = None
    m_cond = re.search(r"\s+and\s+(x\s*(?:>=|<=|>|<)\s*.+)$", body, flags=re.IGNORECASE)
    if m_cond:
        cond_text = m_cond.group(1).strip().rstrip(".")
        body = body[: m_cond.start()].strip()

    return body, cond_text


def _contains_inverse_trig(question: str) -> bool:
    q = _normalize_text(question).lower()
    triggers = (
        "sin^(-1)",
        "cos^(-1)",
        "tan^(-1)",
        "asin(",
        "acos(",
        "atan(",
    )
    return any(t in q for t in triggers)


def solve_inverse_trig_question(question: str) -> Dict[str, Any] | None:
    if not _contains_inverse_trig(question):
        return None

    normalized_q = _normalize_text(question)

    eval_payload = _extract_evaluate_body(normalized_q)
    if eval_payload:
        expr_text, x_value = eval_payload
        try:
            expr = _parse_expr(expr_text)
            if x_value is not None:
                expr = sp.simplify(expr.subs({_X: _parse_expr(x_value)}))
            value = sp.simplify(expr)
            answer = _format_value(value)
            return {
                "handled": True,
                "kind": "evaluate",
                "answer": answer,
                "expected_expr": sp.sstr(value),
                "expected_solution_text": None,
                "reasoning": "Deterministic inverse-trig evaluation.",
            }
        except Exception:
            return None

    solve_payload = _extract_solve_body(normalized_q)
    if solve_payload:
        body, cond_text = solve_payload
        if "=" not in body:
            return None
        lhs_text, rhs_text = body.split("=", 1)
        try:
            lhs = _parse_expr(lhs_text)
            rhs = _parse_expr(rhs_text)
            cond = _parse_condition(cond_text) if cond_text else None
            cond_set = _condition_to_set(cond)
            solution = _solve_inverse_equation(lhs, rhs, cond_set)
            packed = _solution_to_payload(solution)
            return {
                "handled": True,
                "kind": "solve",
                "answer": packed["answer"],
                "expected_expr": packed["expected_expr"],
                "expected_solution_text": packed["expected_solution_text"],
                "reasoning": "Deterministic inverse-trig equation solving.",
            }
        except Exception:
            return None

    return None


def _norm_solution_text(text: str) -> str:
    value = _normalize_text(text).lower()
    value = value.replace(" ", "")
    aliases = {
        "x∈[-1,1]": "xin[-1,1]",
        "xin[-1,1]": "xin[-1,1]",
        "allx>0": "allrealx>0",
        "x>0": "allrealx>0",
        "allrealx>0": "allrealx>0",
        "norealsolution": "norealsolution",
        "nosolution": "norealsolution",
        "emptyset": "norealsolution",
        "allrealxexcept0": "allrealxexcept0",
        "x!=0": "allrealxexcept0",
    }
    return aliases.get(value, value)


def solution_text_equivalent(predicted: str, expected: str) -> bool:
    p = _norm_solution_text(predicted)
    e = _norm_solution_text(expected)
    if p == e:
        return True
    # Keep a tiny compatibility layer for equivalent interval wording.
    eq_pairs = {
        ("xin[-1,1]", "xininterval(-1,1)"),
        ("allrealx>0", "xininterval.open(0,oo)"),
        ("allrealxexcept0", "xincomplement(reals,{0})"),
    }
    return (p, e) in eq_pairs or (e, p) in eq_pairs
