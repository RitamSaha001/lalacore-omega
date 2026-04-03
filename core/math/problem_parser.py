from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(slots=True)
class StructuredProblem:
    type: str
    payload: Dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> Dict[str, Any]:
        return {"type": self.type, **dict(self.payload)}


def _normalize_text(text: str) -> str:
    value = str(text or "").strip()
    value = value.replace("−", "-").replace("–", "-").replace("—", "-")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _extract_ints(text: str) -> List[int]:
    return [int(v) for v in re.findall(r"\d+", str(text or ""))]


def _unique_digits_from_clause(clause: str) -> List[int]:
    seen = set()
    digits: List[int] = []
    text = str(clause or "")
    token_pattern = re.compile(r"\d+\s*-\s*\d+|\d+")
    for match in token_pattern.finditer(text):
        token = match.group(0)
        if "-" in token:
            m_range = re.match(r"(\d+)\s*-\s*(\d+)", token)
            if not m_range:
                continue
            lo = int(m_range.group(1))
            hi = int(m_range.group(2))
            if lo > hi:
                lo, hi = hi, lo
            for iv in range(lo, hi + 1):
                if not (0 <= iv <= 9):
                    continue
                if iv in seen:
                    continue
                seen.add(iv)
                digits.append(iv)
            continue
        iv = int(token)
        if not (0 <= iv <= 9):
            continue
        if iv in seen:
            continue
        seen.add(iv)
        digits.append(iv)
    return digits


def _parse_digit_permutation(text: str) -> Optional[StructuredProblem]:
    q = _normalize_text(text).lower()
    if "digit" not in q and "numeral" not in q:
        return None
    if any(marker in q for marker in ("probability", "random", "conditioned", "given that")):
        return None
    if "without repetition" not in q and "no repetition" not in q and "with repetition" not in q:
        return None

    m_len = re.search(r"(?:how many\s+|a\s+)?(\d+)\s*-?\s*digit\s+number(?:s)?", q)
    if not m_len:
        return None
    length = int(m_len.group(1))

    digits: List[int] = []
    m_using = re.search(
        r"using(?:\s+the)?\s+(?:numerals|digits)\s+(.+?)(?:without repetition|with repetition|no repetition|$)",
        q,
        flags=re.IGNORECASE,
    )
    if m_using:
        digits = _unique_digits_from_clause(m_using.group(1))

    if not digits:
        m_range = re.search(r"(?:from|of)\s*(\d+)\s*-\s*(\d+)", q)
        if m_range:
            lo = int(m_range.group(1))
            hi = int(m_range.group(2))
            if lo > hi:
                lo, hi = hi, lo
            digits = [x for x in range(lo, hi + 1) if 0 <= x <= 9]

    if not digits:
        return None

    repetition = True
    if "without repetition" in q or "no repetition" in q:
        repetition = False
    elif "with repetition" in q:
        repetition = True

    constraint: Dict[str, Any] = {}
    m_div = re.search(r"divisible by\s+(\d+)", q)
    if m_div:
        constraint["divisible_by"] = int(m_div.group(1))

    m_gt = re.search(r"greater than\s+(\d+)", q)
    if m_gt:
        constraint["greater_than"] = int(m_gt.group(1))

    if "sum odd" in q or re.search(r"\bodd\s+digit\s+sum\b", q) or re.search(
        r"\bdigit\s+sum\s+(?:is\s+)?odd\b", q
    ):
        constraint["sum_parity"] = "odd"
    elif "sum even" in q or re.search(r"\beven\s+digit\s+sum\b", q) or re.search(
        r"\bdigit\s+sum\s+(?:is\s+)?even\b", q
    ):
        constraint["sum_parity"] = "even"

    if "exactly one even digit" in q:
        constraint["exact_even_count"] = 1

    if (
        "first digit > last digit" in q
        or "first digit greater than last digit" in q
    ):
        constraint["first_digit_gt_last"] = True

    return StructuredProblem(
        type="digit_permutation",
        payload={
            "digits": digits,
            "length": length,
            "repetition": repetition,
            "constraint": constraint,
        },
    )


def _parse_word_arrangement(text: str) -> Optional[StructuredProblem]:
    q_raw = _normalize_text(text)
    q = q_raw.lower()
    if "arrangement" not in q and "permutation" not in q:
        return None

    m_word = re.search(r"(?:of|word)\s+([A-Za-z]{4,})", q_raw)
    if not m_word:
        return None
    word = m_word.group(1).upper()

    m_no_two = re.search(r"no two\s+([A-Za-z])", q)
    if not m_no_two:
        m_no_two = re.search(r"no two\s+([A-Za-z])s\b", q)
    if not m_no_two:
        return None
    letter = m_no_two.group(1).upper()

    return StructuredProblem(
        type="word_arrangement_no_adjacent_letter",
        payload={
            "word": word,
            "target_letter": letter,
        },
    )


def parse_structured_problem(question: str) -> Optional[StructuredProblem]:
    text = str(question or "").strip()
    if not text:
        return None

    parsers = (
        _parse_digit_permutation,
        _parse_word_arrangement,
    )
    for parser in parsers:
        parsed = parser(text)
        if parsed is not None:
            return parsed
    return None
