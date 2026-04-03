from __future__ import annotations

import concurrent.futures
import math
import random
import re
import time
from typing import Any, Dict, Tuple

import sympy as sp
from cachetools import LRUCache
from pint import UnitRegistry
from sympy.parsing.sympy_parser import (
    convert_xor,
    factorial_notation,
    implicit_multiplication_application,
    parse_expr,
    standard_transformations,
)

from core.math.contextual_math_solver import solve_contextual_math_question
from core.math.inverse_trig_solver import solve_inverse_trig_question, solution_text_equivalent

try:
    from z3 import Real, Solver, sat

    _HAS_Z3 = True
except Exception:  # pragma: no cover - fallback if z3 unavailable
    _HAS_Z3 = False


ureg = UnitRegistry()

NUMERIC_SAMPLES = 10
NUMERIC_TOL = 1e-6
STAGE_TIMEOUT = 1.2
MAX_WORKERS = 3

expr_cache = LRUCache(maxsize=4000)
_LIST_PREFIX_RE = re.compile(r"^\s*(?:\d+|[ivxlcdm]+|[a-zA-Z])[\)\.]\s+", flags=re.IGNORECASE)
_ANSWER_PREFIX_RE = re.compile(
    r"^\s*(?:final\s+answer|answer|ans|therefore|thus|hence)\s*(?:is|=|:|-)?\s*",
    flags=re.IGNORECASE,
)
_INLINE_NCR_RE = re.compile(r"(?<![A-Za-z0-9_])(\d+)\s*[cC]\s*(\d+)(?![A-Za-z0-9_])")
_FUNC_NCR_RE = re.compile(
    r"(?<![A-Za-z0-9_])(?:nCr|NCR|ncr|C)\s*\(\s*([^(),]+?)\s*,\s*([^(),]+?)\s*\)"
)
_PARSE_TRANSFORMS = standard_transformations + (
    implicit_multiplication_application,
    convert_xor,
    factorial_notation,
)
_PARSE_LOCALS = {
    "x": sp.Symbol("x", real=True),
    "y": sp.Symbol("y", real=True),
    "m": sp.Symbol("m", real=True),
    "a": sp.Symbol("a", positive=True, real=True),
    "b": sp.Symbol("b", positive=True, real=True),
    "C": sp.binomial,
    "nCr": sp.binomial,
    "NCR": sp.binomial,
    "binomial": sp.binomial,
    "factorial": sp.factorial,
}


def _sanitize_expression_candidate(text: str) -> str:
    candidate = str(text or "").strip()
    if not candidate:
        return ""
    candidate = candidate.replace("−", "-").replace("×", "*").replace("÷", "/")
    candidate = candidate.replace("`", "").strip()
    if candidate.startswith("```") and candidate.endswith("```"):
        candidate = candidate.strip("`").strip()
    boxed = re.search(r"\\boxed\{([^{}]+)\}", candidate)
    if boxed:
        candidate = boxed.group(1).strip()
    candidate = _LIST_PREFIX_RE.sub("", candidate)
    candidate = _ANSWER_PREFIX_RE.sub("", candidate)
    return candidate.strip().rstrip(".,;")


def _expression_candidates(expr: str) -> list[str]:
    base = _sanitize_expression_candidate(expr)
    if not base:
        return []

    candidates: list[str] = []
    seen: set[str] = set()

    def _add(value: str) -> None:
        cleaned = _sanitize_expression_candidate(value)
        if not cleaned or cleaned in seen:
            return
        seen.add(cleaned)
        candidates.append(cleaned)

    _add(base)
    for line in re.split(r"[\r\n]+", base):
        for segment in line.split(";"):
            _add(segment)

    for item in list(candidates):
        if ":" in item:
            prefix, suffix = item.split(":", 1)
            if suffix.strip() and len(prefix.split()) <= 5:
                _add(suffix)
        if item.count("=") == 1 and "==" not in item:
            left, right = [part.strip() for part in item.split("=", 1)]
            if right:
                _add(right)
            if left and len(left.split()) <= 2:
                _add(left)

    return candidates


def _normalize_sympy_notation(expr: str) -> str:
    out = str(expr or "").strip()
    if not out:
        return ""
    out = out.replace("^", "**")
    out = _INLINE_NCR_RE.sub(r"binomial(\1,\2)", out)
    out = _FUNC_NCR_RE.sub(r"binomial(\1,\2)", out)
    return out


def safe_parse(expr: str):
    text = str(expr or "").strip()
    if text in expr_cache:
        return expr_cache[text]
    parse_error: Exception | None = None
    for candidate in _expression_candidates(text):
        normalized = _normalize_sympy_notation(candidate)
        attempts = [normalized]
        if normalized != candidate:
            attempts.append(candidate)
        else:
            attempts.append(_sanitize_expression_candidate(candidate))
        seen_attempts: set[str] = set()
        ordered_attempts: list[str] = []
        for item in attempts:
            value = str(item or "").strip()
            if not value or value in seen_attempts:
                continue
            seen_attempts.add(value)
            ordered_attempts.append(value)
        for attempt in ordered_attempts:
            try:
                try:
                    parsed = parse_expr(
                        attempt,
                        transformations=_PARSE_TRANSFORMS,
                        local_dict=_PARSE_LOCALS,
                        evaluate=True,
                    )
                except Exception:
                    parsed = sp.sympify(attempt, evaluate=True)
                expr_cache[text] = parsed
                expr_cache[candidate] = parsed
                expr_cache[attempt] = parsed
                return parsed
            except Exception as exc:  # pragma: no cover - fallback candidate loop
                parse_error = exc
                continue
    if parse_error is not None:
        raise parse_error
    raise ValueError("Empty expression")


def timed_call(func, *args):
    start = time.time()
    try:
        result = func(*args)
        elapsed = time.time() - start
        return result, elapsed, None
    except Exception as exc:
        elapsed = time.time() - start
        return False, elapsed, str(exc)


def numeric_equivalence(e1, e2, samples=NUMERIC_SAMPLES):
    variables = list(e1.free_symbols.union(e2.free_symbols))
    if not variables:
        try:
            return abs(float(sp.N(e1 - e2, 20))) < NUMERIC_TOL
        except Exception:
            return False

    checked = 0
    attempts = 0
    target = max(2, int(samples))
    while checked < target and attempts < (target * 6):
        attempts += 1
        subs = {var: random.uniform(0.8, 9.2) for var in variables}
        try:
            v1 = complex(sp.N(e1.subs(subs), 20))
            v2 = complex(sp.N(e2.subs(subs), 20))
        except Exception:
            continue
        if abs(v1.imag) > 1e-8 or abs(v2.imag) > 1e-8:
            continue
        if math.isnan(v1.real) or math.isnan(v2.real):
            return False
        if abs(v1.real - v2.real) > NUMERIC_TOL:
            return False
        checked += 1

    return checked >= 2


def symbolic_equivalence(e1, e2):
    return sp.simplify(e1 - e2) == 0


def z3_equivalence(e1, e2):
    if not _HAS_Z3:
        return False

    vars_ = sorted({str(v) for v in e1.free_symbols.union(e2.free_symbols)})
    if not vars_:
        try:
            return bool(sp.simplify(e1 - e2) == 0)
        except Exception:
            return False

    z3_vars = {name: Real(name) for name in vars_}
    diff = sp.simplify(e1 - e2)
    if diff == 0:
        return True

    s = Solver()
    for _, z3_var in z3_vars.items():
        s.add(z3_var > -100)
        s.add(z3_var < 100)

    if s.check() == sat:
        model = s.model()
        subs = {}
        for name in vars_:
            val = model[z3_vars[name]]
            if val is None:
                subs[sp.Symbol(name)] = 0.0
            else:
                try:
                    subs[sp.Symbol(name)] = float(val.as_decimal(20).replace("?", ""))
                except Exception:
                    subs[sp.Symbol(name)] = 0.0
        try:
            witness = float(diff.subs(subs))
            return abs(witness) < NUMERIC_TOL
        except Exception:
            return False

    return True


def unit_check(predicted: str, expected: str):
    try:
        q1 = ureg(predicted)
        q2 = ureg(expected)
    except Exception:
        return False
    return q1.to_base_units().units == q2.to_base_units().units


def boundary_detection(expr):
    try:
        denom = sp.denom(sp.together(expr))
        if denom == 1:
            return True
        roots = sp.solve(sp.Eq(denom, 0))
        return len(roots) == 0
    except Exception:
        return False


def extraneous_root_check(question: str, predicted_expr) -> bool:
    text = question.lower()
    if "sqrt" not in text and "root" not in text:
        return True

    symbols = list(predicted_expr.free_symbols)
    if not symbols:
        try:
            value = complex(predicted_expr.evalf())
            return abs(value.imag) < 1e-8
        except Exception:
            return False

    for _ in range(5):
        subs = {sym: random.uniform(1, 10) for sym in symbols}
        try:
            value = complex(predicted_expr.subs(subs).evalf())
            if abs(value.imag) > 1e-8:
                return False
        except Exception:
            return False

    return True


def graph_monotonicity_check(question: str, expr) -> bool:
    q = question.lower()
    if "increasing" not in q and "decreasing" not in q and "monotonic" not in q:
        return True

    symbols = list(expr.free_symbols)
    if len(symbols) != 1:
        return False

    x = symbols[0]
    try:
        d = sp.diff(expr, x)
    except Exception:
        return False

    vals = []
    for sample in (1, 2, 3, 4):
        try:
            vals.append(float(d.subs({x: sample})))
        except Exception:
            return False

    all_pos = all(v >= -NUMERIC_TOL for v in vals)
    all_neg = all(v <= NUMERIC_TOL for v in vals)
    return all_pos or all_neg


def optimization_sanity(question: str, expr) -> bool:
    q = question.lower()
    if "maximize" not in q and "minimize" not in q:
        return True

    symbols = list(expr.free_symbols)
    if len(symbols) != 1:
        return True

    x = symbols[0]
    try:
        d1 = sp.diff(expr, x)
        critical = sp.solve(sp.Eq(d1, 0))
        return len(critical) > 0
    except Exception:
        return False


def numeric_substitution_hook(expr_pred, expr_expected, hooks) -> bool:
    if not hooks:
        return True

    symbols = {str(s): s for s in expr_pred.free_symbols.union(expr_expected.free_symbols)}
    for hook in hooks:
        subs = {}
        for key, value in hook.items():
            sym = symbols.get(str(key))
            if sym is not None:
                try:
                    subs[sym] = float(value)
                except Exception:
                    return False
        if not subs:
            continue

        try:
            v1 = float(expr_pred.subs(subs))
            v2 = float(expr_expected.subs(subs))
        except Exception:
            return False
        if abs(v1 - v2) > NUMERIC_TOL:
            return False

    return True


def _looks_open_query(question: str) -> bool:
    q = str(question or "").strip().lower()
    if q.endswith("?"):
        return True
    if re.match(r"^\s*(evaluate|solve|find|differentiate|integrate|compute|determine)\b", q):
        return True
    return False


def _explicit_answer_marker(question: str) -> str | None:
    q = str(question or "")
    patterns = (
        r"(?:expected|correct|final)\s*answer\s*(?:is|=|:)\s*([^\n;]+)",
        r"(?:ans|answer)\s*(?:=|:)\s*([^\n;]+)",
    )
    for pattern in patterns:
        m = re.search(pattern, q, flags=re.IGNORECASE)
        if m:
            candidate = str(m.group(1)).strip().rstrip(".")
            if candidate:
                return candidate
    return None


def _extract_equation_fact_expected(question: str) -> str | None:
    text = str(question or "").strip()
    if "=" not in text:
        return None
    if _looks_open_query(text):
        return None
    if re.search(r"\bat\s*[a-z]\s*=", text, flags=re.IGNORECASE):
        return None
    if re.search(r"\bfrom\b.+\bto\b", text, flags=re.IGNORECASE):
        return None

    parts = text.split("=")
    if len(parts) != 2:
        return None
    lhs = parts[0].strip()
    rhs = parts[1].strip().rstrip(".")
    if not lhs or not rhs:
        return None
    if re.search(r"[A-Za-z]{3,}", lhs):
        return None
    return rhs


def _extract_expected(question: str) -> str | None:
    deterministic = solve_inverse_trig_question(question)
    if deterministic and bool(deterministic.get("handled")):
        expected_expr = deterministic.get("expected_expr")
        expected_solution_text = deterministic.get("expected_solution_text")
        if isinstance(expected_expr, str) and expected_expr.strip():
            return expected_expr.strip()
        if isinstance(expected_solution_text, str) and expected_solution_text.strip():
            return "__solution__:" + expected_solution_text.strip()

    contextual = solve_contextual_math_question(question)
    if contextual and bool(contextual.get("handled")):
        expected_expr = contextual.get("expected_expr")
        expected_solution_text = contextual.get("expected_solution_text")
        if isinstance(expected_expr, str) and expected_expr.strip():
            return expected_expr.strip()
        if isinstance(expected_solution_text, str) and expected_solution_text.strip():
            return "__solution__:" + expected_solution_text.strip()

    marker = _explicit_answer_marker(question)
    if marker is not None:
        return marker

    equation_expected = _extract_equation_fact_expected(question)
    if equation_expected is not None:
        return equation_expected

    options = re.findall(r"\([A-D]\)\s*([^\n]+)", question, flags=re.IGNORECASE)
    if options:
        return options[0].strip()
    return None


def _verify_deterministic_inverse_trig(question: str, predicted_answer: str, difficulty: str | None = None) -> Dict[str, Any] | None:
    deterministic = solve_inverse_trig_question(question)
    if not deterministic or not bool(deterministic.get("handled")):
        return None

    report = {
        "verified": False,
        "confidence_score": 0.0,
        "stage_results": {},
        "stage_timings": {},
        "risk_score": 1.0,
        "escalate": True,
        "reason": None,
        "failure_reason": None,
    }

    expected_expr = deterministic.get("expected_expr")
    expected_solution_text = deterministic.get("expected_solution_text")

    if isinstance(expected_expr, str) and expected_expr.strip():
        try:
            e1 = safe_parse(predicted_answer)
            e2 = safe_parse(expected_expr)
            symbolic = bool(symbolic_equivalence(e1, e2))
            numeric = bool(numeric_equivalence(e1, e2, samples=NUMERIC_SAMPLES))
            boundary = bool(boundary_detection(e1))
            extraneous = bool(extraneous_root_check(question, e1))
            report["stage_results"] = {
                "inverse_trig_expected_match_symbolic": symbolic,
                "inverse_trig_expected_match_numeric": numeric,
                "boundary": boundary,
                "extraneous_root": extraneous,
            }
            report["stage_timings"] = {
                "inverse_trig_expected_match_symbolic": 0.0,
                "inverse_trig_expected_match_numeric": 0.0,
                "boundary": 0.0,
                "extraneous_root": 0.0,
            }
            matched = symbolic or numeric
            if matched and boundary and extraneous:
                report["verified"] = True
                report["confidence_score"] = 0.985
                report["risk_score"] = 0.015
                report["escalate"] = False
                return report
            report["verified"] = False
            report["confidence_score"] = 0.15 if matched else 0.05
            report["risk_score"] = 1.0 - report["confidence_score"]
            report["failure_reason"] = "inverse_trig_expected_mismatch"
            return report
        except Exception as exc:
            report["reason"] = str(exc)
            report["failure_reason"] = "inverse_trig_exception"
            return report

    if isinstance(expected_solution_text, str) and expected_solution_text.strip():
        ok = solution_text_equivalent(str(predicted_answer or ""), expected_solution_text)
        report["stage_results"] = {"inverse_trig_solution_set_match": bool(ok)}
        report["stage_timings"] = {"inverse_trig_solution_set_match": 0.0}
        if ok:
            report["verified"] = True
            report["confidence_score"] = 0.97
            report["risk_score"] = 0.03
            report["escalate"] = False
        else:
            report["verified"] = False
            report["confidence_score"] = 0.05
            report["risk_score"] = 0.95
            report["escalate"] = True
            report["failure_reason"] = "inverse_trig_solution_set_mismatch"
        return report
    return None


def _normalize_text_answer(text: str) -> str:
    value = str(text or "").strip().lower()
    value = value.replace("−", "-").replace("±", "+/-")
    value = re.sub(r"\s+", " ", value)
    return value


def _expand_plus_minus(text: str) -> list[str]:
    raw = str(text or "").strip()
    if not raw:
        return []
    out = [raw]
    for token in ("±", "+/-"):
        next_out: list[str] = []
        for item in out:
            if token in item:
                next_out.append(item.replace(token, "+"))
                next_out.append(item.replace(token, "-"))
            else:
                next_out.append(item)
        out = next_out
    dedup: list[str] = []
    for item in out:
        cleaned = str(item).strip()
        if cleaned and cleaned not in dedup:
            dedup.append(cleaned)
    return dedup


def _extract_equation_candidates(text: str) -> list[str]:
    raw = _normalize_text_answer(text)
    raw = raw.replace(" and ", "; ").replace("\n", "; ")
    pieces = [chunk.strip(" .") for chunk in re.split(r"[;]", raw) if chunk.strip(" .")]
    candidates: list[str] = []
    for piece in pieces:
        fragment = piece.split(":", 1)[-1].strip()
        if "=" not in fragment:
            continue
        if not any(var in fragment for var in ("x", "y", "m")):
            continue
        for expanded in _expand_plus_minus(fragment):
            if expanded not in candidates:
                candidates.append(expanded)
    return candidates


def _parse_equation_expr(candidate: str):
    text = str(candidate or "").strip()
    if "=" not in text:
        return None
    lhs_text, rhs_text = text.split("=", 1)
    try:
        lhs = safe_parse(lhs_text)
        rhs = safe_parse(rhs_text)
    except Exception:
        return None
    return sp.expand(sp.together(lhs - rhs).as_numer_denom()[0])


def _equation_equivalent(left: str, right: str) -> bool:
    expr_left = _parse_equation_expr(left)
    expr_right = _parse_equation_expr(right)
    if expr_left is None or expr_right is None:
        return False
    try:
        ratio = sp.simplify(expr_left / expr_right)
        if ratio != 0 and not getattr(ratio, "free_symbols", set()):
            return True
    except Exception:
        pass
    try:
        diff = sp.expand(expr_left - expr_right)
        if diff == 0:
            return True
    except Exception:
        pass
    vars_ = list(expr_left.free_symbols.union(expr_right.free_symbols))
    if not vars_:
        return False
    reference_ratio: complex | None = None
    informative_samples = 0
    for sample in (
        {"x": 2, "y": -3, "m": 1, "a": 2, "b": 3},
        {"x": -5, "y": 7, "m": 2, "a": 5, "b": 4},
        {"x": 3, "y": 2, "m": -1, "a": 7, "b": 2},
    ):
        subs = {var: sample.get(str(var), 1) for var in vars_}
        try:
            lv = complex(sp.N(expr_left.subs(subs), 20))
            rv = complex(sp.N(expr_right.subs(subs), 20))
        except Exception:
            return False
        if abs(lv) < NUMERIC_TOL and abs(rv) < NUMERIC_TOL:
            continue
        if abs(lv) < NUMERIC_TOL or abs(rv) < NUMERIC_TOL:
            return False
        try:
            ratio = lv / rv
        except Exception:
            return False
        if abs(ratio.imag) > 1e-8 or abs(ratio.real) < NUMERIC_TOL:
            return False
        if reference_ratio is None:
            reference_ratio = ratio
        elif abs(ratio - reference_ratio) > max(1e-6, 1e-6 * abs(reference_ratio)):
            return False
        informative_samples += 1
    if informative_samples == 0:
        return False
    return True


def _extract_expression_candidates(text: str) -> list[str]:
    raw = _normalize_text_answer(text)
    raw = raw.replace(" or ", "; ").replace(" and ", "; ").replace("\n", "; ")
    pieces = [chunk.strip(" .") for chunk in re.split(r"[;,]", raw) if chunk.strip(" .")]
    out: list[str] = []
    for piece in pieces:
        fragment = piece.split(":", 1)[-1].strip()
        if "=" in fragment:
            fragment = fragment.rsplit("=", 1)[-1].strip()
        for expanded in _expand_plus_minus(fragment):
            candidate = _sanitize_expression_candidate(expanded)
            if candidate and candidate not in out:
                out.append(candidate)
    return out


def _expression_equivalent(left: str, right: str) -> bool:
    try:
        return bool(symbolic_equivalence(safe_parse(left), safe_parse(right))) or bool(
            numeric_equivalence(safe_parse(left), safe_parse(right), samples=max(NUMERIC_SAMPLES, 14))
        )
    except Exception:
        return False


def _keyword_text_match(predicted_answer: str, expected_keywords: list[str]) -> bool:
    low = _normalize_text_answer(predicted_answer)
    return any(str(keyword or "").strip().lower() in low for keyword in expected_keywords if str(keyword or "").strip())


def _verify_contextual_structured_answer(
    contextual: Dict[str, Any],
    predicted_answer: str,
) -> Dict[str, Any] | None:
    kind = str(contextual.get("verification_kind") or "").strip().lower()
    if not kind:
        return None

    report = {
        "verified": False,
        "confidence_score": 0.0,
        "stage_results": {"contextual_task_detected": True, "structured_kind": kind},
        "stage_timings": {"contextual_task_detected": 0.0, "structured_kind": 0.0},
        "risk_score": 1.0,
        "escalate": True,
        "reason": None,
        "failure_reason": None,
    }

    if kind == "text":
        expected_keywords = [str(x) for x in (contextual.get("expected_keywords") or []) if str(x).strip()]
        matched = _keyword_text_match(predicted_answer, expected_keywords)
        report["stage_results"]["contextual_text_match"] = bool(matched)
        report["stage_timings"]["contextual_text_match"] = 0.0
        report["verified"] = bool(matched)
        report["confidence_score"] = 0.96 if matched else 0.05
        report["risk_score"] = 1.0 - report["confidence_score"]
        report["escalate"] = not bool(matched)
        report["failure_reason"] = None if matched else "contextual_text_mismatch"
        return report

    if kind == "equation":
        expected_equations = [str(x) for x in (contextual.get("expected_equations") or []) if str(x).strip()]
        observed = _extract_equation_candidates(predicted_answer)
        matched = bool(expected_equations) and any(
            _equation_equivalent(obs, expected_equations[0]) for obs in observed
        )
        report["stage_results"]["contextual_equation_match"] = bool(matched)
        report["stage_results"]["observed_equations"] = observed[:6]
        report["stage_timings"]["contextual_equation_match"] = 0.0
        report["verified"] = bool(matched)
        report["confidence_score"] = 0.975 if matched else 0.06
        report["risk_score"] = 1.0 - report["confidence_score"]
        report["escalate"] = not bool(matched)
        report["failure_reason"] = None if matched else "contextual_equation_mismatch"
        return report

    if kind == "equation_set":
        expected_equations = [str(x) for x in (contextual.get("expected_equations") or []) if str(x).strip()]
        observed = _extract_equation_candidates(predicted_answer)
        matched_all = bool(expected_equations) and all(
            any(_equation_equivalent(obs, exp) for obs in observed) for exp in expected_equations
        )
        report["stage_results"]["contextual_equation_set_match"] = bool(matched_all)
        report["stage_results"]["observed_equations"] = observed[:8]
        report["stage_timings"]["contextual_equation_set_match"] = 0.0
        report["verified"] = bool(matched_all)
        report["confidence_score"] = 0.975 if matched_all else 0.06
        report["risk_score"] = 1.0 - report["confidence_score"]
        report["escalate"] = not bool(matched_all)
        report["failure_reason"] = None if matched_all else "contextual_equation_set_mismatch"
        return report

    if kind == "expression_set":
        expected_expressions = [str(x) for x in (contextual.get("expected_expressions") or []) if str(x).strip()]
        observed = _extract_expression_candidates(predicted_answer)
        matched_all = bool(expected_expressions) and all(
            any(_expression_equivalent(obs, exp) for obs in observed) for exp in expected_expressions
        )
        report["stage_results"]["contextual_expression_set_match"] = bool(matched_all)
        report["stage_results"]["observed_expressions"] = observed[:8]
        report["stage_timings"]["contextual_expression_set_match"] = 0.0
        report["verified"] = bool(matched_all)
        report["confidence_score"] = 0.97 if matched_all else 0.06
        report["risk_score"] = 1.0 - report["confidence_score"]
        report["escalate"] = not bool(matched_all)
        report["failure_reason"] = None if matched_all else "contextual_expression_set_mismatch"
        return report

    if kind == "composite":
        expected_numbers = [str(x) for x in (contextual.get("expected_numbers") or []) if str(x).strip()]
        expected_equations = [str(x) for x in (contextual.get("expected_equations") or []) if str(x).strip()]
        required_keywords = [str(x) for x in (contextual.get("required_keywords") or []) if str(x).strip()]
        observed_numbers = _extract_expression_candidates(predicted_answer)
        observed_equations = _extract_equation_candidates(predicted_answer)
        number_ok = bool(expected_numbers) and all(
            any(_expression_equivalent(obs, exp) for obs in observed_numbers) for exp in expected_numbers
        )
        equation_ok = bool(expected_equations) and all(
            any(_equation_equivalent(obs, exp) for obs in observed_equations) for exp in expected_equations
        )
        keyword_ok = True if not required_keywords else _keyword_text_match(predicted_answer, required_keywords)
        matched = bool(number_ok and equation_ok and (keyword_ok or (number_ok and equation_ok)))
        report["stage_results"]["contextual_composite_numbers"] = bool(number_ok)
        report["stage_results"]["contextual_composite_equations"] = bool(equation_ok)
        report["stage_results"]["contextual_composite_keywords"] = bool(keyword_ok)
        report["stage_results"]["observed_equations"] = observed_equations[:8]
        report["stage_results"]["observed_expressions"] = observed_numbers[:8]
        report["stage_timings"]["contextual_composite_numbers"] = 0.0
        report["stage_timings"]["contextual_composite_equations"] = 0.0
        report["stage_timings"]["contextual_composite_keywords"] = 0.0
        report["verified"] = matched
        report["confidence_score"] = 0.98 if matched else 0.06
        report["risk_score"] = 1.0 - report["confidence_score"]
        report["escalate"] = not bool(matched)
        report["failure_reason"] = None if matched else "contextual_composite_mismatch"
        return report

    return None


def _verify_contextual_math(
    question: str,
    predicted_answer: str,
    *,
    substitution_hooks: list[dict] | None = None,
) -> Dict[str, Any] | None:
    contextual = solve_contextual_math_question(question)
    if not contextual or not bool(contextual.get("handled")):
        return None

    structured = _verify_contextual_structured_answer(contextual, predicted_answer)
    if structured is not None:
        return structured

    report = {
        "verified": False,
        "confidence_score": 0.0,
        "stage_results": {"contextual_task_detected": True},
        "stage_timings": {"contextual_task_detected": 0.0},
        "risk_score": 1.0,
        "escalate": True,
        "reason": None,
        "failure_reason": None,
    }

    expected_expr = contextual.get("expected_expr")
    expected_solution_text = contextual.get("expected_solution_text")

    if isinstance(expected_expr, str) and expected_expr.strip():
        try:
            pred = safe_parse(predicted_answer)
            exp = safe_parse(expected_expr)
        except Exception as exc:
            report["reason"] = str(exc)
            report["failure_reason"] = "contextual_parse_failure"
            return report

        report["stage_results"]["contextual_symbolic"] = bool(symbolic_equivalence(pred, exp))
        report["stage_results"]["contextual_numeric"] = bool(numeric_equivalence(pred, exp, samples=max(NUMERIC_SAMPLES, 14)))
        report["stage_results"]["contextual_hook"] = bool(numeric_substitution_hook(pred, exp, substitution_hooks or []))
        report["stage_results"]["boundary"] = bool(boundary_detection(pred))
        report["stage_results"]["extraneous_root"] = bool(extraneous_root_check(question, pred))
        report["stage_timings"].update(
            {
                "contextual_symbolic": 0.0,
                "contextual_numeric": 0.0,
                "contextual_hook": 0.0,
                "boundary": 0.0,
                "extraneous_root": 0.0,
            }
        )

        eq = (
            bool(report["stage_results"]["contextual_symbolic"])
            or bool(report["stage_results"]["contextual_numeric"])
        )
        sanity = bool(report["stage_results"]["boundary"]) and bool(report["stage_results"]["extraneous_root"])
        hook_ok = bool(report["stage_results"]["contextual_hook"])
        if eq and sanity and hook_ok:
            report["verified"] = True
            report["confidence_score"] = 0.975
            report["risk_score"] = 0.025
            report["escalate"] = False
            return report

        report["verified"] = False
        report["confidence_score"] = 0.10 if eq else 0.04
        report["risk_score"] = 1.0 - report["confidence_score"]
        report["failure_reason"] = "contextual_expected_mismatch"
        return report

    if isinstance(expected_solution_text, str) and expected_solution_text.strip():
        ok = solution_text_equivalent(str(predicted_answer or ""), expected_solution_text)
        report["stage_results"]["contextual_solution_set"] = bool(ok)
        report["stage_timings"]["contextual_solution_set"] = 0.0
        if ok:
            report["verified"] = True
            report["confidence_score"] = 0.96
            report["risk_score"] = 0.04
            report["escalate"] = False
            return report
        report["verified"] = False
        report["confidence_score"] = 0.05
        report["risk_score"] = 0.95
        report["failure_reason"] = "contextual_solution_set_mismatch"
        return report

    report["failure_reason"] = "contextual_missing_expected"
    report["reason"] = "Contextual task detected without deterministic expected answer."
    return report


def _weighted_stage_score(question: str, stage_results: Dict[str, bool]) -> Tuple[float, list[str]]:
    q = str(question or "").lower()
    weights = {
        "symbolic": 0.24,
        "numeric": 0.22,
        "z3": 0.08,
        "numeric_hook": 0.12,
        "boundary": 0.10,
        "extraneous_root": 0.10,
        "graph_monotonicity": 0.06,
        "optimization_sanity": 0.04,
        "unit": 0.04,
    }

    if "integral" in q or "d/dx" in q or "differentiate" in q or "from " in q:
        weights["numeric_hook"] += 0.05
        weights["boundary"] += 0.03
        weights["symbolic"] += 0.02
    if "unit" in q:
        weights["unit"] += 0.08
    if "monotonic" in q or "increasing" in q or "decreasing" in q:
        weights["graph_monotonicity"] += 0.06
    if "maximize" in q or "minimize" in q:
        weights["optimization_sanity"] += 0.06

    relevant = [k for k in weights if k in stage_results]
    if not relevant:
        return 0.0, []

    total_weight = sum(weights[k] for k in relevant)
    passed_weight = sum(weights[k] for k in relevant if bool(stage_results.get(k)))
    failed = [k for k in relevant if not bool(stage_results.get(k))]
    return passed_weight / max(total_weight, 1e-9), failed


def verify_solution(
    question: str,
    predicted_answer: str,
    difficulty: str | None = None,
    substitution_hooks: list[dict] | None = None,
) -> Dict[str, Any]:
    report = {
        "verified": False,
        "confidence_score": 0.0,
        "stage_results": {},
        "stage_timings": {},
        "risk_score": 1.0,
        "escalate": False,
        "reason": None,
        "failure_reason": None,
    }

    try:
        deterministic = _verify_deterministic_inverse_trig(question, predicted_answer, difficulty=difficulty)
        if deterministic is not None:
            return deterministic

        contextual = _verify_contextual_math(
            question=question,
            predicted_answer=predicted_answer,
            substitution_hooks=substitution_hooks,
        )
        if contextual is not None:
            return contextual

        expected = _extract_expected(question)
        if expected is None:
            report["reason"] = "No expected answer found"
            report["escalate"] = True
            report["failure_reason"] = "missing_ground_truth"
            return report

        if expected.startswith("__solution__:"):
            expected_solution = expected[len("__solution__:") :].strip()
            ok = solution_text_equivalent(str(predicted_answer or ""), expected_solution)
            report["stage_results"]["solution_set_match"] = bool(ok)
            report["stage_timings"]["solution_set_match"] = 0.0
            if ok:
                report["verified"] = True
                report["confidence_score"] = 0.965
                report["risk_score"] = 0.035
                report["escalate"] = False
            else:
                report["verified"] = False
                report["confidence_score"] = 0.05
                report["risk_score"] = 0.95
                report["escalate"] = True
                report["failure_reason"] = "solution_set_mismatch"
            return report

        e1 = safe_parse(predicted_answer)
        e2 = safe_parse(expected)

        sample_budget = NUMERIC_SAMPLES
        if difficulty:
            d = difficulty.lower().strip()
            if d == "medium":
                sample_budget = max(sample_budget, 14)
            elif d == "hard":
                sample_budget = max(sample_budget, 24)

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                "symbolic": executor.submit(symbolic_equivalence, e1, e2),
                "numeric": executor.submit(numeric_equivalence, e1, e2, sample_budget),
                "z3": executor.submit(z3_equivalence, e1, e2),
                "boundary": executor.submit(boundary_detection, e1),
                "extraneous_root": executor.submit(extraneous_root_check, question, e1),
                "graph_monotonicity": executor.submit(graph_monotonicity_check, question, e1),
                "optimization_sanity": executor.submit(optimization_sanity, question, e1),
                "numeric_hook": executor.submit(numeric_substitution_hook, e1, e2, substitution_hooks or []),
            }

            for stage, future in futures.items():
                result, elapsed, err = timed_call(lambda f=future: f.result(timeout=STAGE_TIMEOUT))
                report["stage_results"][stage] = bool(result)
                report["stage_timings"][stage] = elapsed
                if err and not report["reason"]:
                    report["reason"] = err

        unit_result, unit_time, _ = timed_call(unit_check, predicted_answer, expected)
        report["stage_results"]["unit"] = bool(unit_result)
        report["stage_timings"]["unit"] = unit_time

        weighted_ratio, failed = _weighted_stage_score(question, report["stage_results"])
        strong_pass = (
            bool(report["stage_results"].get("symbolic"))
            or bool(report["stage_results"].get("numeric"))
            or bool(report["stage_results"].get("z3"))
        )
        sanity_pass = bool(report["stage_results"].get("boundary")) and bool(report["stage_results"].get("extraneous_root"))
        hooks_ok = bool(report["stage_results"].get("numeric_hook"))

        if strong_pass and sanity_pass and hooks_ok and weighted_ratio >= 0.58:
            report["verified"] = True
            report["confidence_score"] = min(0.99, 0.62 + 0.36 * weighted_ratio)
            report["risk_score"] = max(0.01, 1.0 - report["confidence_score"])
            report["escalate"] = False
            return report

        if weighted_ratio >= 0.78 and sanity_pass:
            report["verified"] = True
            report["confidence_score"] = 0.74
            report["risk_score"] = 0.26
            report["escalate"] = False
            return report

        report["verified"] = False
        report["confidence_score"] = max(0.05, 0.55 * weighted_ratio)
        report["risk_score"] = 1.0 - report["confidence_score"]
        report["escalate"] = True
        report["failure_reason"] = ",".join(failed[:4]) if failed else "verification_failed"
    except Exception as exc:
        report["reason"] = str(exc)
        report["failure_reason"] = "exception"
        report["escalate"] = True

    return report
