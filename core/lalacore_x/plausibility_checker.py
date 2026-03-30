from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any, Dict, List


def _norm(text: str) -> str:
    return " ".join(str(text or "").strip().lower().split())


def _alnum_count(text: str) -> int:
    return len(re.findall(r"[A-Za-z0-9]", str(text or "")))


def _is_counting_numeric_prompt(question_text: str) -> bool:
    q = _norm(question_text)
    if re.search(r"\b(how many|number of|count|probability)\b", q):
        return True
    if re.search(r"\b(arrangements?|permutations?|combinations?|selections?|subsets?|ways)\b", q):
        return True
    if re.search(r"\b(onto|surjective|injective|one[\s\-]?to[\s\-]?one)\s+functions?\b", q):
        return True
    if re.search(r"\b(?:positive|non[\s\-]?negative)\s+integer\s+solutions?\b", q) and re.search(r"\b(each|where|with)\b", q):
        return True
    return False


def _is_equation_or_locus_prompt(question_text: str) -> bool:
    q = _norm(question_text)
    return bool(
        re.search(
            r"\b("
            r"equation of|locus|tangent|tangents|normal|chord|chord of contact|"
            r"asymptote|envelope|pair of perpendicular lines|condition"
            r")\b",
            q,
        )
    )


def _detect_expected_type(question_text: str, metadata: Dict[str, Any]) -> str:
    q = _norm(question_text)
    if re.search(r"\b(option|mcq|correct option|which option)\b", q) or re.search(r"\([a-d]\)", q):
        return "option"
    if _is_counting_numeric_prompt(q):
        return "numeric"
    if re.search(
        r"\b("
        r"equation of|locus|tangent|tangents|normal|chord|chord of contact|"
        r"circle|parabola|ellipse|hyperbola|asymptote|envelope|"
        r"pair of perpendicular lines|condition"
        r")\b",
        q,
    ):
        return "solution"
    if re.search(
        r"\b(list|roots?|ordered pair|ordered pairs|vector|vectors|solution set|set of solutions|set of values|possible values|all values|all roots)\b",
        q,
    ):
        return "list"
    if re.search(r"\b(solve|find|determine|evaluate|compute|calculate|simplify)\b", q):
        return "solution"
    if bool(metadata.get("numeric_expected")):
        return "numeric"
    if re.search(r"[\d\+\-\*/\^=]", q):
        return "numeric"
    return "text"


def _looks_numeric(answer: str) -> bool:
    s = str(answer or "").strip()
    if not s:
        return False
    if re.fullmatch(r"[-+]?\d+(\.\d*)?([eE][-+]?\d+)?", s):
        return True
    if re.fullmatch(r"[-+]?\d+(\.\d*)?\s*/\s*[-+]?\d+(\.\d*)?", s):
        return True
    if re.fullmatch(r"[-+]?(pi|π)(\s*/\s*[-+]?\d+)?", s, flags=re.IGNORECASE):
        return True
    if re.fullmatch(r"[-+]?[a-zA-Z]?\s*=\s*[-+]?\d+(\.\d+)?", s):
        return True
    if re.fullmatch(r"[-+]?(pi|π|e|sqrt\([^)]+\)|\d+(\.\d*)?)(\s*[-+*/]\s*[-+]?(pi|π|e|sqrt\([^)]+\)|\d+(\.\d*)?))+", s, flags=re.IGNORECASE):
        return True
    if re.fullmatch(r"[-+]?\s*(sqrt\(\d+(\.\d+)?\)|\d+(\.\d+)?\s*\^\s*[-+]?\d+)", s, flags=re.IGNORECASE):
        return True
    if re.fullmatch(r"[-+]?\s*sqrt\(\d+(\.\d+)?\)\s*/\s*[-+]?\d+(\.\d+)?", s, flags=re.IGNORECASE):
        return True
    return False


def _looks_list(answer: str) -> bool:
    s = str(answer or "").strip()
    if not s:
        return False
    if "," in s:
        return True
    if ("{" in s and "}" in s) or ("[" in s and "]" in s) or ("(" in s and ")" in s):
        return True
    return False


def _looks_option(answer: str) -> bool:
    s = str(answer or "").strip()
    if bool(re.fullmatch(r"\(?[A-Da-d]\)?", s)):
        return True
    normalized = s.lower()
    normalized = normalized.replace("&", ",").replace("/", ",")
    normalized = re.sub(r"\b(and|or)\b", ",", normalized)
    parts = [part.strip("()[]{} .") for part in normalized.split(",") if part.strip()]
    if not parts:
        return False
    return all(bool(re.fullmatch(r"[a-d]", part)) for part in parts)


def _looks_solution(answer: str) -> bool:
    original = str(answer or "").strip().lower()
    if not original:
        return False
    candidates = [original]
    labeled = re.sub(r"^[a-z][a-z\s_-]{0,32}:\s*", "", original)
    if labeled != original:
        candidates.append(labeled)
    for s in candidates:
        if _looks_numeric(s) or _looks_list(s):
            return True
        if re.search(r"\\frac|\\sqrt|\\boxed", s):
            return True
        if "=" in s and re.search(r"\b[xymab]\b|[xyabm]\s*[\^*/+\-]", s):
            return True
        if re.search(r"[a-z]\s*[\^*/+\-=]\s*[-+a-z0-9./()]+", s):
            return True
        if re.search(r"[a-z]\s*/\s*[a-z0-9()]+", s):
            return True
        if re.search(r"\bx\s*(>=|<=|>|<|!=)\s*[-+a-z0-9\./()]+", s):
            return True
        if re.search(r"\b(no real solution|no solution|no real tangents?|no tangents?|empty set|all real)\b", s):
            return True
        if re.search(r"\bx\s*in\s*[\[\(\{].+[\]\)\}]", s):
            return True
    return False


def _is_formatting_only(answer: str) -> bool:
    s = str(answer or "").strip()
    if not s:
        return False
    # Single latex wrapper with minimal semantic payload.
    if re.fullmatch(r"\$?\\[A-Za-z]+\{[^{}]*\}\$?", s):
        inner = re.sub(r"^\$?\\[A-Za-z]+\{", "", s).rstrip("}$")
        return _alnum_count(inner) <= 1
    if re.fullmatch(r"[\$\s\\\{\}\[\]\(\)_^]+", s):
        return True
    return False


def _echo_fragment(question_text: str, answer: str) -> float:
    q = _norm(question_text)
    a = _norm(answer)
    if not q or not a:
        return 0.0
    if a in q and len(a) >= 8:
        return 1.0
    return float(SequenceMatcher(a=q, b=a).ratio())


def check_answer_plausibility(question_text: str, final_answer: str, metadata: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """
    Deterministic plausibility gate for final answers.
    """
    metadata = metadata or {}
    answer = str(final_answer or "").strip()
    expected_type = _detect_expected_type(question_text, metadata)
    issues: List[str] = []
    score = 1.0

    compact_len = len(re.sub(r"\s+", "", answer))
    valid_short = False
    numeric_short_ok = False
    if expected_type == "numeric":
        valid_short = bool(re.fullmatch(r"[-+]?(pi|π|e)", answer.strip(), flags=re.IGNORECASE))
        numeric_short_ok = _looks_numeric(answer)
    elif expected_type == "option":
        valid_short = _looks_option(answer) or _looks_numeric(answer) or _looks_solution(answer)
    elif expected_type == "solution":
        valid_short = _looks_solution(answer)

    complexity_markers = (
        "integral",
        "∫",
        "differentiate",
        "derivative",
        "expression",
        "from ",
        " to ",
        "^",
        "sqrt",
        "/",
    )
    question_complex = any(marker in str(question_text or "").lower() for marker in complexity_markers) or len(str(question_text or "")) >= 42
    short_numeric_discrete_ok = False
    numeric_solution_short = bool(_looks_numeric(answer)) if expected_type == "solution" else False
    if expected_type in {"numeric", "solution"} and (numeric_short_ok or numeric_solution_short) and compact_len <= 1:
        q_lower = str(question_text or "").lower()
        discrete_markers = (
            "coefficient",
            "constant term",
            "term independent of x",
            "how many",
            "number of",
            "count",
            "divisible",
            "probability",
            "subsets",
            "permutations",
            "arrangements",
            "digits",
        )
        optimization_markers = (
            "minimum value",
            "maximum value",
            "minimum",
            "maximum",
            "least value",
            "greatest value",
            "minimize",
            "maximize",
        )
        short_numeric_discrete_ok = any(marker in q_lower for marker in discrete_markers) or any(
            marker in q_lower for marker in optimization_markers
        )

    if compact_len < 4:
        if valid_short:
            if expected_type == "solution" and numeric_solution_short:
                if compact_len <= 1 and question_complex and not short_numeric_discrete_ok:
                    issues.append("too_short")
                    score -= 0.35
                else:
                    score -= 0.05
            else:
                score -= 0.05
        elif expected_type == "numeric" and numeric_short_ok:
            if compact_len <= 1 and question_complex and not short_numeric_discrete_ok:
                issues.append("too_short")
                score -= 0.35
            else:
                score -= 0.03
        else:
            issues.append("too_short")
            score -= 0.35

    echo_ratio = _echo_fragment(question_text, answer)
    if expected_type != "option" and echo_ratio >= 0.78:
        issues.append("echo_fragment")
        score -= 0.35

    if _is_formatting_only(answer):
        issues.append("formatting_only")
        score -= 0.30

    if _alnum_count(answer) == 0:
        issues.append("empty_semantics")
        score -= 0.40

    # Type checks
    if expected_type == "numeric":
        if not _looks_numeric(answer):
            issues.append("expected_numeric_type")
            score -= 0.30
    elif expected_type == "solution":
        if not _looks_solution(answer):
            issues.append("expected_solution_type")
            score -= 0.25
        elif _looks_numeric(answer) and _is_equation_or_locus_prompt(question_text):
            issues.append("expected_solution_type")
            score -= 0.55
    elif expected_type == "list":
        if not _looks_list(answer):
            issues.append("expected_list_type")
            score -= 0.20
    elif expected_type == "option":
        if not (_looks_option(answer) or _looks_numeric(answer) or _looks_solution(answer)):
            issues.append("expected_option_type")
            score -= 0.20

    # Explicit mismatch hint from upstream type estimation.
    observed_type = str(metadata.get("observed_type", "")).strip().lower()
    if observed_type and observed_type != expected_type:
        compatible = False
        if expected_type == "numeric":
            compatible = _looks_numeric(answer)
        elif expected_type == "solution":
            compatible = _looks_solution(answer)
        elif expected_type == "list":
            compatible = _looks_list(answer)
        elif expected_type == "option":
            compatible = _looks_option(answer) or _looks_numeric(answer) or _looks_solution(answer)
        if not compatible:
            issues.append("type_mismatch")
            score -= 0.25

    score = max(0.0, min(1.0, score))
    hard_fail = {"too_short", "echo_fragment", "formatting_only", "empty_semantics"}
    plausible = score >= 0.55 and len(hard_fail.intersection(issues)) == 0

    return {
        "plausible": bool(plausible),
        "issues": issues,
        "score": round(float(score), 6),
        "expected_type": expected_type,
        "echo_ratio": round(float(echo_ratio), 6),
    }
