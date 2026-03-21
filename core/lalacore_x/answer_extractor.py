from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any, Dict, List, Tuple


def _norm(text: str) -> str:
    return " ".join(str(text or "").strip().lower().split())


def _strip_markdown(text: str) -> str:
    cleaned = str(text or "")
    cleaned = re.sub(r"```(?:[a-zA-Z0-9_+-]+)?", "", cleaned)
    cleaned = cleaned.replace("```", "")
    return cleaned.strip()


def _clean_candidate(value: str) -> str:
    text = str(value or "").strip()
    text = re.sub(r"^\s*(final\s*answer|answer)\s*[:\-]\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^\s*(is|=)\s*", "", text, flags=re.IGNORECASE)
    text = text.strip("`*$ ")
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > 280:
        text = text[:280].strip()
    return text


def _echo_ratio(question_text: str, candidate: str) -> float:
    q = _norm(question_text)
    c = _norm(candidate)
    if not q or not c:
        return 0.0
    if c in q:
        return 1.0
    return float(SequenceMatcher(a=q, b=c).ratio())


def _looks_numeric(text: str) -> bool:
    token = _clean_candidate(text)
    if not token:
        return False
    if re.fullmatch(r"[-+]?\d+(\.\d+)?([eE][-+]?\d+)?", token):
        return True
    if re.fullmatch(r"[-+]?\d+\s*/\s*[-+]?\d+", token):
        return True
    if re.fullmatch(r"[-+]?(pi|π)(\s*/\s*[-+]?\d+)?", token, flags=re.IGNORECASE):
        return True
    if re.fullmatch(r"[-+]?(sqrt\(\d+(\.\d+)?\)|\d+(\.\d+)?\s*\^\s*\d+)", token, flags=re.IGNORECASE):
        return True
    if re.fullmatch(r"[-+]?\s*sqrt\(\d+(\.\d+)?\)\s*/\s*[-+]?\d+(\.\d+)?", token, flags=re.IGNORECASE):
        return True
    if re.fullmatch(r"[-+]?[a-zA-Z]?\s*=\s*[-+]?\d+(\.\d+)?", token):
        return True
    return False


def _looks_option_label(text: str) -> bool:
    token = _clean_candidate(text)
    if bool(re.fullmatch(r"\(?[A-Da-d]\)?", token)):
        return True
    normalized = token.lower().replace("&", ",").replace("/", ",")
    normalized = re.sub(r"\b(and|or)\b", ",", normalized)
    parts = [part.strip("()[]{} .") for part in normalized.split(",") if part.strip()]
    if not parts:
        return False
    return all(bool(re.fullmatch(r"[a-d]", part)) for part in parts)


def _expected_answer_type(question_text: str, metadata: Dict[str, Any]) -> str:
    q = _norm(question_text)
    if re.search(r"\b(solve|find\s+[a-z])\b", q):
        return "solution"
    if re.search(r"\b(option|mcq|correct option|which option)\b", q) or re.search(r"\([a-d]\)", q):
        return "option"
    if re.search(r"\b(list|set of|roots|values|ordered pair)\b", q):
        return "list"
    if bool(metadata.get("numeric_expected")):
        return "numeric"
    if re.search(r"[\d\+\-\*/\^=]", q):
        return "numeric"
    return "text"


def extract_answer(
    question_text: str,
    raw_output: str,
    metadata: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """
    Robust answer extraction from provider text output.
    """
    metadata = metadata or {}
    cleaned = _strip_markdown(raw_output)
    expected_type = _expected_answer_type(question_text, metadata)

    if not cleaned:
        return {
            "reasoning": "",
            "final_answer": "",
            "matched": False,
            "pattern": "empty_output",
            "candidates": [],
            "expected_type": expected_type,
        }

    # Priority patterns: explicit answer tags, boxed latex, option labels.
    patterns: List[Tuple[str, str]] = [
        ("final_answer_tag", r"final\s*answer\s*[:\-]\s*(.+?)(?:\n|$)"),
        ("answer_tag", r"(?:^|\n)\s*answer\s*[:\-]\s*(.+?)(?:\n|$)"),
        ("option_statement", r"(?:correct\s+option(?:s)?|option(?:s)?\s*(?:is|are)?)\s*[:\-]?\s*([A-D](?:\s*(?:,|and|or|&)\s*[A-D])*)"),
        ("boxed_latex", r"\\boxed\{([^{}]{1,220})\}"),
        ("answer_is", r"answer\s+is\s+(.+?)(?:\n|$)"),
        ("hence_value", r"(?:hence|therefore|thus)\s*(?:,|:)?\s*([^\n]{1,220})"),
    ]

    found: List[Tuple[str, str, Tuple[int, int]]] = []
    for label, pattern in patterns:
        for match in re.finditer(pattern, cleaned, flags=re.IGNORECASE | re.DOTALL):
            candidate = _clean_candidate(match.group(1))
            if candidate:
                found.append((label, candidate, (match.start(), match.end())))

    if not found:
        # Fallback: prefer short lines near the end that look like answers.
        lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
        tail = lines[-4:] if lines else []
        for line in tail:
            candidate = _clean_candidate(line)
            if not candidate:
                continue
            if _looks_option_label(candidate) or _looks_numeric(candidate) or len(candidate.split()) <= 10:
                found.append(("tail_line", candidate, (-1, -1)))
        if not found and lines:
            found.append(("last_line_fallback", _clean_candidate(lines[-1]), (-1, -1)))

    best = None
    best_score = float("-inf")
    for label, candidate, span in found:
        score = 0.0
        score += 1.2 if label in {"final_answer_tag", "answer_tag", "boxed_latex"} else 0.5
        score += 0.5 if len(candidate) <= 80 else -0.2
        score += 0.5 if _echo_ratio(question_text, candidate) < 0.70 else -1.2
        if expected_type == "numeric":
            score += 0.8 if _looks_numeric(candidate) else -0.7
        elif expected_type == "solution":
            score += 0.8 if (
                _looks_numeric(candidate)
                or "," in candidate
                or re.search(r"\bx\s*(>=|<=|>|<|!=|in)\b", candidate, flags=re.IGNORECASE)
                or re.search(r"\b(no real solution|all real)\b", candidate, flags=re.IGNORECASE)
            ) else -0.5
        elif expected_type == "option":
            score += 0.8 if _looks_option_label(candidate) else -0.5
        elif expected_type == "list":
            score += 0.5 if ("," in candidate or "{" in candidate or "[" in candidate) else -0.2

        if score > best_score:
            best_score = score
            best = (label, candidate, span)

    if best is None:
        return {
            "reasoning": cleaned,
            "final_answer": _clean_candidate(cleaned),
            "matched": False,
            "pattern": "no_pattern",
            "candidates": [],
            "expected_type": expected_type,
        }

    label, final_answer, span = best
    if span[0] >= 0:
        reasoning = cleaned[: span[0]].strip()
    else:
        lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
        if lines and _norm(lines[-1]) == _norm(final_answer):
            reasoning = "\n".join(lines[:-1]).strip()
        else:
            reasoning = cleaned

    if not reasoning:
        reasoning = cleaned[:400]

    return {
        "reasoning": reasoning,
        "final_answer": _clean_candidate(final_answer),
        "matched": True,
        "pattern": label,
        "candidates": [candidate for _, candidate, _ in found[:8]],
        "expected_type": expected_type,
    }
