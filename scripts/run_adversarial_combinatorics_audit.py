#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import math
import re
import statistics
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from fractions import Fraction
from itertools import combinations, permutations, product
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.api.entrypoint import lalacore_entry


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))


def _mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _variance(values: Sequence[float]) -> float:
    if len(values) <= 1:
        return 0.0
    return float(statistics.pvariance([float(v) for v in values]))


def _fraction_to_str(value: int | Fraction) -> str:
    if isinstance(value, Fraction):
        if value.denominator == 1:
            return str(value.numerator)
        return f"{value.numerator}/{value.denominator}"
    return str(int(value))


def _subset_iter(universe: Sequence[int]) -> Iterable[tuple[int, ...]]:
    n = len(universe)
    for mask in range(1 << n):
        cur: List[int] = []
        for i in range(n):
            if (mask >> i) & 1:
                cur.append(int(universe[i]))
        yield tuple(cur)


def _count_subsets(universe: Sequence[int], predicate: Callable[[tuple[int, ...]], bool]) -> int:
    return sum(1 for subset in _subset_iter(universe) if predicate(subset))


def _count_permutations(n: int, predicate: Callable[[tuple[int, ...]], bool]) -> int:
    base = tuple(range(1, n + 1))
    return sum(1 for perm in permutations(base) if predicate(tuple(perm)))


def _iter_numbers(
    *,
    digits: Sequence[int],
    length: int,
    distinct: bool,
    allow_repetition: bool,
    leading_zero_allowed: bool,
) -> Iterable[tuple[int, ...]]:
    if distinct and allow_repetition:
        raise ValueError("distinct and allow_repetition cannot both be true")

    if distinct:
        gen = permutations(digits, length)
    elif allow_repetition:
        gen = product(digits, repeat=length)
    else:
        gen = combinations(digits, length)

    for tup in gen:
        if (not leading_zero_allowed) and int(tup[0]) == 0:
            continue
        yield tuple(int(x) for x in tup)


def _is_prime_digit(d: int) -> bool:
    return int(d) in {2, 3, 5, 7}


def _cyclic_permutations_count(n: int) -> int:
    if n <= 0:
        return 0
    return math.factorial(n - 1)


def _question_bank() -> List[Dict[str, Any]]:
    universe_8 = tuple(range(1, 9))

    # Category A: subset edge cases (8)
    a1 = _count_subsets(universe_8, lambda s: sum(1 for x in s if x in {1, 2, 3}) == 2)
    a2 = _count_subsets(universe_8, lambda s: sum(1 for x in s if x in {1, 2, 3}) >= 2)
    a3 = _count_subsets(universe_8, lambda s: 1 not in s and 8 not in s)
    a4 = _count_subsets(universe_8, lambda s: (1 in s) ^ (8 in s))
    a5 = _count_subsets(universe_8, lambda s: {1, 3, 5, 7}.issubset(set(s)))
    a6 = _count_subsets(universe_8, lambda s: any(x in {2, 3, 5, 7} for x in s))
    a7 = _count_subsets(universe_8, lambda s: all((x + 1) not in s for x in s))
    a8 = _count_subsets(universe_8, lambda s: (sum(s) % 3) == 0)

    # Category B: permutation ordering traps (8)
    b1 = _count_permutations(6, lambda p: p.index(1) < p.index(2) and p.index(1) < p.index(3))
    b2 = _count_permutations(
        6,
        lambda p: (p.index(2) < p.index(1) < p.index(3)) or (p.index(3) < p.index(1) < p.index(2)),
    )
    b3 = _count_permutations(6, lambda p: abs(p.index(2) - p.index(3)) != 1)
    b4 = _count_permutations(6, lambda p: abs(p.index(1) - p.index(2)) == 1)
    b5 = _count_permutations(6, lambda p: p.index(3) < p.index(1) < p.index(2))
    b6 = _count_permutations(6, lambda p: sum(1 for i, val in enumerate(p, start=1) if i == val) == 2)
    b7 = _count_permutations(5, lambda p: all(i != val for i, val in enumerate(p, start=1)))
    b8 = _cyclic_permutations_count(6)

    # Category C: digit construction traps (10)
    c_domain_distinct_4 = list(
        _iter_numbers(digits=tuple(range(10)), length=4, distinct=True, allow_repetition=False, leading_zero_allowed=False)
    )
    c_domain_distinct_4_1to9 = list(
        _iter_numbers(digits=tuple(range(1, 10)), length=4, distinct=True, allow_repetition=False, leading_zero_allowed=True)
    )
    c_domain_rep_4_1to9 = list(
        _iter_numbers(digits=tuple(range(1, 10)), length=4, distinct=False, allow_repetition=True, leading_zero_allowed=True)
    )
    c_domain_5_from_1to7 = list(
        _iter_numbers(digits=tuple(range(1, 8)), length=5, distinct=True, allow_repetition=False, leading_zero_allowed=True)
    )

    c1 = sum(1 for d in c_domain_distinct_4 if (sum(d) % 2) == 1)
    c2 = sum(1 for d in c_domain_distinct_4 if (sum(d) % 2) == 0)
    c3 = sum(1 for d in c_domain_distinct_4 if int("".join(str(x) for x in d)) % 3 == 0)
    c4 = sum(1 for d in c_domain_distinct_4 if d[0] > d[-1])
    c5 = sum(1 for d in c_domain_distinct_4_1to9 if list(d) == sorted(d))
    c6 = sum(1 for d in c_domain_distinct_4_1to9 if sum(1 for x in d if x % 2 == 0) == 2)
    c7 = sum(1 for a in range(1, 10) for b in range(1, 10) if True)  # abba, digits 1..9
    c8 = sum(1 for d in c_domain_rep_4_1to9 if all((d[i] + d[i + 1]) % 2 == 1 for i in range(3)))
    c9 = sum(1 for d in c_domain_5_from_1to7 if int("".join(str(x) for x in d)) > 50000)
    c10 = sum(1 for d in c_domain_distinct_4_1to9 if int("".join(str(x) for x in d)) % 9 == 0)

    # Category D: arrangement + grouping traps (8)
    books_6 = tuple("ABCDEF")
    people_7 = tuple("ABCDEFG")
    people_8 = tuple("ABCDEFGH")
    men = tuple("M1 M2 M3".split())
    women = tuple("W1 W2 W3".split())

    d1 = sum(
        1
        for p in permutations(books_6)
        if max(p.index("A"), p.index("B"), p.index("C")) - min(p.index("A"), p.index("B"), p.index("C")) == 2
    )
    d2 = sum(
        1
        for p in permutations(books_6)
        if abs(p.index("A") - p.index("B")) > 1 and abs(p.index("A") - p.index("C")) > 1 and abs(p.index("B") - p.index("C")) > 1
    )
    d3 = sum(
        1
        for p in permutations(people_8)
        if abs(p.index("A") - p.index("B")) == 1 and abs(p.index("C") - p.index("D")) == 1
    )
    d4 = 2 * math.factorial(5)  # AB block in circular 7-person arrangement
    d5 = 2 * math.factorial(3) * math.factorial(3)
    d6 = sum(1 for p in permutations(people_7) if abs(p.index("A") - p.index("B")) != 1)
    d7 = sum(
        1
        for p in permutations(books_6)
        if (abs(p.index("A") - p.index("B")) == 1) ^ (abs(p.index("C") - p.index("D")) == 1)
    )
    d8 = sum(
        1
        for p in permutations(people_7)
        if (abs(p.index("A") - p.index("B")) == 1) or (abs(p.index("C") - p.index("D")) == 1)
    )

    # Category E: inclusion-exclusion traps (8)
    e1 = sum(1 for x in range(1, 121) if (x % 2 == 0) or (x % 3 == 0))
    e2 = sum(1 for x in range(1, 121) if (x % 2 == 0) and (x % 3 != 0))
    e3 = sum(1 for x in range(1, 201) if (x % 2 == 0) or (x % 3 == 0) or (x % 5 == 0))
    e4 = sum(
        1
        for x in range(1, 201)
        if sum(1 for cond in (x % 2 == 0, x % 3 == 0, x % 5 == 0) if cond) == 2
    )
    e5 = sum(1 for x in range(1, 201) if (x % 2 != 0) and (x % 3 != 0) and (x % 5 != 0))
    e6 = sum(1 for x in range(1000, 10000) if len(set(str(x))) < 4)
    e7 = sum(1 for x in range(1000, 10000) if sum(1 for ch in str(x) if _is_prime_digit(int(ch))) == 1)
    e8 = sum(1 for x in range(10000, 100000) if len(set(str(x))) < 5)

    # Category F: probability / conditioning traps (8)
    perms_6 = list(permutations(tuple(range(1, 7))))
    perms_7 = list(permutations(tuple(range(1, 8))))
    total_4_1to9_distinct = len(c_domain_distinct_4_1to9)
    even_sum_4_1to9_distinct = sum(1 for d in c_domain_distinct_4_1to9 if (sum(d) % 2) == 0)

    f1 = Fraction(sum(1 for p in perms_6 if p.index(1) < p.index(2)), len(perms_6))
    f2 = Fraction(even_sum_4_1to9_distinct, total_4_1to9_distinct)
    cond_first_odd = [d for d in c_domain_distinct_4_1to9 if d[0] % 2 == 1]
    f3 = Fraction(sum(1 for d in cond_first_odd if (sum(d) % 2) == 0), len(cond_first_odd))
    cond_1_before_2 = [p for p in perms_6 if p.index(1) < p.index(2)]
    f4 = Fraction(sum(1 for p in cond_1_before_2 if p.index(3) < p.index(4)), len(cond_1_before_2))
    subsets_with_1 = [s for s in _subset_iter(universe_8) if 1 in s]
    f5 = Fraction(sum(1 for s in subsets_with_1 if 8 in s), len(subsets_with_1))
    cond_1_before_2_in_7 = [p for p in perms_7 if p.index(1) < p.index(2)]
    f6 = Fraction(
        sum(1 for p in cond_1_before_2_in_7 if p.index(1) < p.index(3)),
        len(cond_1_before_2_in_7),
    )
    cond_gt_50000 = [d for d in c_domain_5_from_1to7 if int("".join(str(x) for x in d)) > 50000]
    f7 = Fraction(sum(1 for d in cond_gt_50000 if d[0] % 2 == 1), len(cond_gt_50000))
    cond_div_3 = [d for d in c_domain_distinct_4 if int("".join(str(x) for x in d)) % 3 == 0]
    f8 = Fraction(
        sum(1 for d in cond_div_3 if int("".join(str(x) for x in d)) % 9 == 0),
        len(cond_div_3),
    )

    rows: List[Dict[str, Any]] = [
        # A
        _q("A1", "A", "subset_edge", "How many subsets of {1-8} contain exactly 2 elements from {1,2,3}?", a1, True, ["exactly_vs_at_least", "subset_edge"]),
        _q("A2", "A", "subset_edge", "How many subsets of {1-8} contain at least 2 elements from {1,2,3}?", a2, True, ["exactly_vs_at_least", "complement"]),
        _q("A3", "A", "subset_edge", "How many subsets of {1-8} contain neither 1 nor 8?", a3, False, ["complement_counting"]),
        _q("A4", "A", "subset_edge", "How many subsets of {1-8} contain exactly one of {1,8}?", a4, True, ["xor_trap"]),
        _q("A5", "A", "subset_edge", "How many subsets of {1-8} contain all odd elements?", a5, True, ["mandatory_elements"]),
        _q("A6", "A", "subset_edge", "How many subsets of {1-8} contain at least one prime element?", a6, True, ["complement_counting"]),
        _q("A7", "A", "subset_edge", "How many subsets of {1-8} contain no consecutive integers?", a7, True, ["adjacency"]),
        _q("A8", "A", "subset_edge", "How many subsets of {1-8} have sum divisible by 3?", a8, True, ["modulo_parity"]),
        # B
        _q("B1", "B", "ordering", "How many permutations of 1-6 have 1 appearing before both 2 and 3?", b1, True, ["symmetry"]),
        _q("B2", "B", "ordering", "How many permutations of 1-6 have 1 between 2 and 3?", b2, True, ["ordering_trap"]),
        _q("B3", "B", "ordering", "How many permutations of 1-6 have 2 and 3 not adjacent?", b3, True, ["adjacency_complement"]),
        _q("B4", "B", "ordering", "How many permutations of 1-6 have 1 and 2 adjacent?", b4, True, ["block_method"]),
        _q("B5", "B", "ordering", "How many permutations of 1-6 have 1 before 2 but after 3?", b5, True, ["conditional_ordering"]),
        _q("B6", "B", "fixed_points", "How many permutations of 1-6 have exactly two fixed points?", b6, True, ["fixed_point_derangement"]),
        _q("B7", "B", "fixed_points", "How many derangements are there of 1-5?", b7, True, ["derangement"]),
        _q("B8", "B", "ordering", "How many cyclic permutations of 1-6 are there?", b8, True, ["circular_symmetry"]),
        # C
        _q("C1", "C", "digit_parity", "How many 4-digit numbers using digits 0-9 without repetition have odd digit sum?", c1, True, ["parity"]),
        _q("C2", "C", "digit_parity", "How many 4-digit numbers using digits 0-9 without repetition have even digit sum?", c2, True, ["parity"]),
        _q("C3", "C", "digit_divisibility", "How many 4-digit numbers using digits 0-9 without repetition are divisible by 3?", c3, True, ["digit_dp", "divisibility"]),
        _q("C4", "C", "digit_boundary", "How many 4-digit numbers using digits 0-9 without repetition satisfy first digit > last digit?", c4, True, ["symmetry_boundary"]),
        _q("C5", "C", "digit_monotone", "How many 4-digit numbers from digits 1-9 without repetition have strictly increasing digits?", c5, True, ["combination_vs_permutation"]),
        _q("C6", "C", "digit_parity", "How many 4-digit numbers from digits 1-9 without repetition have exactly two even digits?", c6, True, ["parity_exactly"]),
        _q("C7", "C", "digit_pattern", "How many 4-digit palindromes can be formed from digits 1-9?", c7, True, ["palindrome_structure"]),
        _q("C8", "C", "digit_parity", "How many 4-digit numbers from digits 1-9 with repetition allowed have no two adjacent digits of the same parity?", c8, True, ["adjacent_parity"]),
        _q("C9", "C", "digit_boundary", "How many 5-digit numbers from digits 1-7 without repetition are greater than 50000?", c9, True, ["threshold_boundary"]),
        _q("C10", "C", "digit_divisibility", "How many 4-digit numbers from digits 1-9 without repetition are divisible by 9?", c10, True, ["digit_dp", "divisibility"]),
        # D
        _q("D1", "D", "grouping", "How many arrangements of 6 distinct books A-F in a row keep A,B,C together?", d1, True, ["together_vs_separate"]),
        _q("D2", "D", "grouping", "How many arrangements of 6 distinct books A-F in a row keep A,B,C pairwise non-adjacent?", d2, True, ["separation_trap"]),
        _q("D3", "D", "grouping", "How many arrangements of 8 people A-H in a row have A,B together and C,D together?", d3, True, ["multiple_blocks"]),
        _q("D4", "D", "grouping", "How many circular arrangements of 7 people A-G have A and B adjacent?", d4, True, ["circular_block"]),
        _q("D5", "D", "grouping", "How many row arrangements of M1,M2,M3,W1,W2,W3 alternate men and women?", d5, True, ["alternation"]),
        _q("D6", "D", "grouping", "How many arrangements of 7 people A-G in a row have A and B not adjacent?", d6, True, ["complement_counting"]),
        _q("D7", "D", "grouping", "How many arrangements of 6 people A-F in a row have exactly one of pairs (A,B) and (C,D) adjacent?", d7, True, ["inclusion_exclusion"]),
        _q("D8", "D", "grouping", "How many arrangements of 7 people A-G in a row have at least one of pairs (A,B) or (C,D) adjacent?", d8, True, ["inclusion_exclusion"]),
        # E
        _q("E1", "E", "inclusion_exclusion", "How many integers from 1-120 are divisible by 2 or 3?", e1, False, ["union_count"]),
        _q("E2", "E", "inclusion_exclusion", "How many integers from 1-120 are divisible by 2 and not by 3?", e2, False, ["intersection_minus"]),
        _q("E3", "E", "inclusion_exclusion", "How many integers from 1-200 are divisible by at least one of 2,3,5?", e3, True, ["triple_union"]),
        _q("E4", "E", "inclusion_exclusion", "How many integers from 1-200 are divisible by exactly two of 2,3,5?", e4, True, ["exactly_k_properties"]),
        _q("E5", "E", "inclusion_exclusion", "How many integers from 1-200 are divisible by none of 2,3,5?", e5, True, ["complement"]),
        _q("E6", "E", "inclusion_exclusion", "How many 4-digit numbers (1000-9999) have at least one repeated digit?", e6, True, ["repetition_complement"]),
        _q("E7", "E", "inclusion_exclusion", "How many 4-digit numbers (1000-9999) have exactly one prime digit (2,3,5,7)?", e7, True, ["exactly_k_properties", "digit_prime"]),
        _q("E8", "E", "inclusion_exclusion", "How many 5-digit numbers (10000-99999) have at least one repeated digit?", e8, True, ["repetition_complement"]),
        # F
        _q("F1", "F", "conditioning_probability", "In a random permutation of 1-6, what is the probability that 1 appears before 2?", f1, True, ["symmetry_probability"]),
        _q("F2", "F", "conditioning_probability", "For a random 4-digit number from digits 1-9 without repetition, what is the probability the digit sum is even?", f2, True, ["parity_probability"]),
        _q("F3", "F", "conditioning_probability", "Conditioned on first digit odd, for a random 4-digit number from digits 1-9 without repetition, what is the probability the digit sum is even?", f3, True, ["conditional_parity"]),
        _q("F4", "F", "conditioning_probability", "Given 1 appears before 2 in a random permutation of 1-6, what is the probability that 3 appears before 4?", f4, True, ["conditional_independence"]),
        _q("F5", "F", "conditioning_probability", "For a random subset of {1-8} conditioned to contain 1, what is the probability it also contains 8?", f5, True, ["subset_conditioning"]),
        _q("F6", "F", "conditioning_probability", "Given 1 appears before 2 in a random permutation of 1-7, what is the probability that 1 appears before both 2 and 3?", f6, True, ["conditional_ordering"]),
        _q("F7", "F", "conditioning_probability", "For a random 5-digit number from digits 1-7 without repetition conditioned to be greater than 50000, what is the probability the first digit is odd?", f7, True, ["boundary_conditioning"]),
        _q("F8", "F", "conditioning_probability", "For a random 4-digit number using digits 0-9 without repetition conditioned to be divisible by 3, what is the probability it is divisible by 9?", f8, True, ["divisibility_conditioning"]),
    ]
    if len(rows) != 50:
        raise RuntimeError(f"Expected 50 questions, found {len(rows)}")
    return rows


def _q(
    qid: str,
    category: str,
    concept_cluster: str,
    question: str,
    expected: int | Fraction,
    hard_case: bool,
    trap_tags: Sequence[str],
) -> Dict[str, Any]:
    return {
        "id": str(qid),
        "category": str(category),
        "concept_cluster": str(concept_cluster),
        "question": str(question),
        "expected": expected,
        "expected_str": _fraction_to_str(expected),
        "hard_case": bool(hard_case),
        "trap_tags": [str(tag) for tag in trap_tags],
    }


_FRACTION_RE = re.compile(r"(-?\d+)\s*/\s*(\d+)")
_PERCENT_RE = re.compile(r"(-?\d+(?:\.\d+)?)\s*%")
_NUMBER_RE = re.compile(r"(-?\d+(?:\.\d+)?)")


def _parse_numeric(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace(",", "")
    frac = _FRACTION_RE.search(text)
    if frac:
        num = int(frac.group(1))
        den = int(frac.group(2))
        if den != 0:
            return float(Fraction(num, den))
    pct = _PERCENT_RE.search(text)
    if pct:
        return float(pct.group(1)) / 100.0
    num = _NUMBER_RE.search(text)
    if num:
        return float(num.group(1))
    return None


def _compare_answer(parsed_value: float | None, expected: int | Fraction) -> bool:
    if parsed_value is None:
        return False
    if isinstance(expected, Fraction):
        return abs(float(parsed_value) - float(expected)) <= 5e-3
    expected_f = float(expected)
    if abs(parsed_value - expected_f) <= 1e-9:
        return True
    return abs(parsed_value - expected_f) <= 5e-3


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes"}:
        return True
    if text in {"0", "false", "no"}:
        return False
    return bool(value)


def _provider_count(provider_availability: Any) -> int:
    if not isinstance(provider_availability, dict):
        return 0
    return sum(1 for _, status in provider_availability.items() if bool(status))


async def _call_entry(
    question: str,
    *,
    enable_persona: bool,
    retries: int,
    retry_backoff_seconds: float,
) -> Dict[str, Any]:
    last_err: str = ""
    for attempt in range(max(1, retries)):
        try:
            out = await lalacore_entry(
                input_data=question,
                input_type="text",
                options={"enable_persona": bool(enable_persona), "enable_meta_verification": True},
            )
            if isinstance(out, dict):
                status = str(out.get("status", "ok")).lower()
                if status != "error":
                    return out
                last_err = str(out.get("error", "unknown_error"))
            else:
                last_err = "non_dict_response"
        except Exception as exc:  # noqa: BLE001
            last_err = str(exc)
        await asyncio.sleep(float(retry_backoff_seconds) * float(2**attempt))
    return {"status": "error", "error": f"audit_call_failed:{last_err}", "final_answer": ""}


async def _stability_probe(
    question: str,
    *,
    runs: int,
    retries: int,
    retry_backoff_seconds: float,
    sleep_seconds: float,
) -> Dict[str, Any]:
    sampled_answers: List[str] = []
    sampled_conf: List[float] = []
    sampled_entropy: List[float] = []
    for _ in range(max(1, runs)):
        out = await _call_entry(
            question,
            enable_persona=False,
            retries=retries,
            retry_backoff_seconds=retry_backoff_seconds,
        )
        ans = str(out.get("final_answer", "")).strip()
        cal = dict(out.get("calibration_metrics", {}) or {})
        conf = _clamp(float(cal.get("confidence_score", cal.get("confidence", 0.0))))
        ent = _clamp(float(cal.get("entropy", out.get("entropy", 0.0))))
        sampled_answers.append(ans)
        sampled_conf.append(conf)
        sampled_entropy.append(ent)
        await asyncio.sleep(float(sleep_seconds))

    answer_counts: Dict[str, int] = {}
    for ans in sampled_answers:
        answer_counts[ans] = answer_counts.get(ans, 0) + 1
    dominant = max(answer_counts.values()) if answer_counts else 0
    self_disagreement_rate = 1.0 - (dominant / len(sampled_answers)) if sampled_answers else 0.0
    answer_variance = (len(answer_counts) / len(sampled_answers)) if sampled_answers else 0.0
    return {
        "runs": int(len(sampled_answers)),
        "unique_answers": int(len(answer_counts)),
        "answer_variance": float(answer_variance),
        "confidence_variance": float(_variance(sampled_conf)),
        "self_disagreement_rate": float(self_disagreement_rate),
        "mean_entropy": float(_mean(sampled_entropy)),
        "answers": sampled_answers,
    }


@dataclass(slots=True)
class AuditConfig:
    sleep_seconds: float = 0.25
    chunk_size: int = 10
    chunk_cooldown_seconds: float = 1.5
    retries: int = 3
    retry_backoff_seconds: float = 1.0
    overconfidence_threshold: float = 0.85
    low_entropy_threshold: float = 0.30
    run_stability_probe: bool = True
    stability_runs: int = 5
    max_stability_probes: int = 12


def _rare_clusters(questions: Sequence[Dict[str, Any]]) -> set[str]:
    freq: Dict[str, int] = {}
    for q in questions:
        cluster = str(q.get("concept_cluster", "")).strip().lower()
        if not cluster:
            continue
        freq[cluster] = freq.get(cluster, 0) + 1
    if not freq:
        return set()
    values = sorted(freq.values())
    cutoff_idx = max(0, int(math.ceil(0.25 * len(values))) - 1)
    cutoff = values[cutoff_idx]
    return {cluster for cluster, count in freq.items() if count <= cutoff}


def _build_summary(
    *,
    rows: Sequence[Dict[str, Any]],
    questions: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    total = len(rows)
    correct = sum(1 for row in rows if bool(row.get("correct", False)))
    accuracy = (correct / total) if total else 0.0

    category_stats: Dict[str, Dict[str, float]] = {}
    for row in rows:
        cat = str(row.get("category", "unknown"))
        slot = category_stats.setdefault(cat, {"total": 0.0, "correct": 0.0})
        slot["total"] += 1.0
        slot["correct"] += 1.0 if bool(row.get("correct", False)) else 0.0
    accuracy_by_category = {
        cat: {"correct": int(val["correct"]), "total": int(val["total"]), "accuracy": float(val["correct"] / val["total"]) if val["total"] else 0.0}
        for cat, val in sorted(category_stats.items())
    }

    hard_total = sum(1 for row in rows if bool(row.get("hard_case", False)))
    hard_correct = sum(1 for row in rows if bool(row.get("hard_case", False)) and bool(row.get("correct", False)))

    rare = _rare_clusters(questions)
    rare_total = sum(1 for row in rows if str(row.get("concept_cluster", "")).lower() in rare)
    rare_correct = sum(
        1 for row in rows if str(row.get("concept_cluster", "")).lower() in rare and bool(row.get("correct", False))
    )

    overconfidence = sum(1 for row in rows if bool(row.get("overconfidence", False)))
    low_entropy_wrong = sum(1 for row in rows if bool(row.get("low_entropy_wrong", False)))
    low_entropy_overconfidence = sum(1 for row in rows if bool(row.get("low_entropy_overconfidence", False)))
    single_provider = sum(1 for row in rows if bool(row.get("single_provider_mode", False)))
    persona_integrity = sum(1 for row in rows if bool(row.get("persona_integrity", False)))
    verification_correctness = sum(1 for row in rows if bool(row.get("verification_correctness", False)))

    risk_vals = [float(row.get("risk_score", 1.0)) for row in rows]
    entropy_vals = [float(row.get("entropy", 1.0)) for row in rows]
    confidence_vals = [float(row.get("confidence", 0.0)) for row in rows]

    stability_rows = [row for row in rows if isinstance(row.get("stability_probe"), dict)]
    unstable = 0
    for row in stability_rows:
        probe = dict(row.get("stability_probe", {}))
        if float(probe.get("answer_variance", 0.0)) > 0.20 or float(probe.get("confidence_variance", 0.0)) > 0.02:
            unstable += 1

    return {
        "generated_at": _utc_now(),
        "total_questions": int(total),
        "correct": int(correct),
        "accuracy": float(accuracy),
        "accuracy_by_category": accuracy_by_category,
        "hard_case_accuracy": float((hard_correct / hard_total) if hard_total else 0.0),
        "rare_cluster_accuracy": float((rare_correct / rare_total) if rare_total else 0.0),
        "overconfidence_count": int(overconfidence),
        "low_entropy_wrong_count": int(low_entropy_wrong),
        "low_entropy_overconfidence_count": int(low_entropy_overconfidence),
        "single_provider_count": int(single_provider),
        "single_provider_frequency": float((single_provider / total) if total else 0.0),
        "persona_integrity_rate": float((persona_integrity / total) if total else 0.0),
        "verification_correctness_rate": float((verification_correctness / total) if total else 0.0),
        "risk_mean": float(_mean(risk_vals)),
        "entropy_mean": float(_mean(entropy_vals)),
        "confidence_mean": float(_mean(confidence_vals)),
        "wrong_questions": [row["id"] for row in rows if not bool(row.get("correct", False))],
        "stability_probe_count": int(len(stability_rows)),
        "stability_unstable_count": int(unstable),
    }


async def run_stress_audit(
    questions: Sequence[Dict[str, Any]],
    *,
    config: AuditConfig,
) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    stability_budget = int(max(0, config.max_stability_probes))

    for idx, q in enumerate(questions, start=1):
        question = str(q["question"])
        expected = q["expected"]
        expected_str = q["expected_str"]

        raw = await _call_entry(
            question,
            enable_persona=False,
            retries=config.retries,
            retry_backoff_seconds=config.retry_backoff_seconds,
        )
        await asyncio.sleep(float(config.sleep_seconds))
        persona = await _call_entry(
            question,
            enable_persona=True,
            retries=config.retries,
            retry_backoff_seconds=config.retry_backoff_seconds,
        )

        raw_final = str(raw.get("final_answer", "")).strip()
        persona_final = str(persona.get("final_answer", "")).strip()
        persona_display = str(persona.get("display_answer", "")).strip()

        parsed = _parse_numeric(raw_final)
        if parsed is None:
            parsed = _parse_numeric(raw.get("display_answer", ""))
        correct = _compare_answer(parsed, expected)

        verification = dict(raw.get("verification", {}) or {})
        calibration = dict(raw.get("calibration_metrics", {}) or {})
        diagnostics = dict(raw.get("provider_diagnostics", {}) or {})
        provider_availability = diagnostics.get("provider_availability", {})

        verified = _safe_bool(verification.get("verified", False))
        risk = _clamp(float(verification.get("risk_score", calibration.get("risk_score", 1.0))))
        entropy = _clamp(float(calibration.get("entropy", raw.get("entropy", 1.0))))
        confidence = _clamp(float(calibration.get("confidence_score", calibration.get("confidence", 0.0))))
        single_provider_mode = bool(calibration.get("single_provider_mode", False))
        if not single_provider_mode:
            active_providers = _provider_count(provider_availability)
            if active_providers <= 1:
                single_provider_mode = True

        overconfidence = (confidence > float(config.overconfidence_threshold)) and (not correct)
        low_entropy_wrong = (entropy < float(config.low_entropy_threshold)) and (not correct)
        low_entropy_overconfidence = overconfidence and low_entropy_wrong
        persona_integrity = raw_final == persona_final
        verification_correctness = (verified == bool(correct))

        row: Dict[str, Any] = {
            "id": q["id"],
            "category": q["category"],
            "concept_cluster": q["concept_cluster"],
            "hard_case": bool(q["hard_case"]),
            "trap_tags": list(q["trap_tags"]),
            "question": question,
            "expected": expected_str,
            "raw_parsed_numeric": parsed,
            "correct": bool(correct),
            "without_persona": {
                "status": raw.get("status"),
                "final_answer": raw.get("final_answer"),
                "display_answer": raw.get("display_answer"),
                "winner_provider": raw.get("winner_provider"),
                "verification": raw.get("verification"),
                "calibration_metrics": raw.get("calibration_metrics"),
                "provider_diagnostics": raw.get("provider_diagnostics"),
                "meta_verification": raw.get("meta_verification"),
                "reasoning": raw.get("reasoning"),
            },
            "with_persona": {
                "status": persona.get("status"),
                "final_answer": persona.get("final_answer"),
                "display_answer": persona.get("display_answer"),
                "winner_provider": persona.get("winner_provider"),
                "verification": persona.get("verification"),
                "calibration_metrics": persona.get("calibration_metrics"),
                "provider_diagnostics": persona.get("provider_diagnostics"),
                "meta_verification": persona.get("meta_verification"),
                "reasoning": persona.get("reasoning"),
            },
            "verified": bool(verified),
            "risk_score": float(risk),
            "entropy": float(entropy),
            "confidence": float(confidence),
            "winner_provider": raw.get("winner_provider"),
            "single_provider_mode": bool(single_provider_mode),
            "overconfidence": bool(overconfidence),
            "low_entropy_wrong": bool(low_entropy_wrong),
            "low_entropy_overconfidence": bool(low_entropy_overconfidence),
            "persona_integrity": bool(persona_integrity),
            "persona_display_contains_raw_answer": bool(raw_final and raw_final in persona_display),
            "verification_correctness": bool(verification_correctness),
            "flags": {
                "wrong_answer": bool(not correct),
                "verified_false": bool(not verified),
                "overconfidence": bool(overconfidence),
                "single_provider_mode": bool(single_provider_mode),
                "persona_integrity_failure": bool(not persona_integrity),
                "verification_mismatch": bool(not verification_correctness),
            },
        }

        requires_stability = (
            bool(config.run_stability_probe)
            and stability_budget > 0
            and (
                (not correct)
                or overconfidence
                or (not persona_integrity)
                or (not verification_correctness)
            )
        )
        if requires_stability:
            row["stability_probe"] = await _stability_probe(
                question,
                runs=config.stability_runs,
                retries=config.retries,
                retry_backoff_seconds=config.retry_backoff_seconds,
                sleep_seconds=config.sleep_seconds,
            )
            stability_budget -= 1

        rows.append(row)
        print(
            f"[{idx:02d}/{len(questions)}] {q['id']} correct={bool(correct)} verified={bool(verified)} "
            f"risk={risk:.3f} entropy={entropy:.3f} overconf={bool(overconfidence)}",
            flush=True,
        )

        if idx % max(1, int(config.chunk_size)) == 0:
            await asyncio.sleep(float(config.chunk_cooldown_seconds))
        else:
            await asyncio.sleep(float(config.sleep_seconds))

    return {"rows": rows}


def _sanitize_rows_for_json(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(json.loads(json.dumps(row, ensure_ascii=True, default=str)))
    return out


async def _main_async(args: argparse.Namespace) -> Dict[str, Any]:
    full_questions = _question_bank()
    if args.max_questions is not None:
        full_questions = full_questions[: max(1, int(args.max_questions))]

    config = AuditConfig(
        sleep_seconds=float(args.sleep_seconds),
        chunk_size=int(args.chunk_size),
        chunk_cooldown_seconds=float(args.chunk_cooldown_seconds),
        retries=int(args.retries),
        retry_backoff_seconds=float(args.retry_backoff_seconds),
        overconfidence_threshold=float(args.overconfidence_threshold),
        low_entropy_threshold=float(args.low_entropy_threshold),
        run_stability_probe=bool(args.run_stability_probe),
        stability_runs=int(args.stability_runs),
        max_stability_probes=int(args.max_stability_probes),
    )
    run_out = await run_stress_audit(full_questions, config=config)
    rows = _sanitize_rows_for_json(run_out["rows"])

    summary = _build_summary(rows=rows, questions=full_questions)
    payload = {
        "generated_at": _utc_now(),
        "config": {
            "sleep_seconds": config.sleep_seconds,
            "chunk_size": config.chunk_size,
            "chunk_cooldown_seconds": config.chunk_cooldown_seconds,
            "retries": config.retries,
            "retry_backoff_seconds": config.retry_backoff_seconds,
            "overconfidence_threshold": config.overconfidence_threshold,
            "low_entropy_threshold": config.low_entropy_threshold,
            "run_stability_probe": config.run_stability_probe,
            "stability_runs": config.stability_runs,
            "max_stability_probes": config.max_stability_probes,
        },
        "questions": [
            {
                "id": q["id"],
                "category": q["category"],
                "concept_cluster": q["concept_cluster"],
                "question": q["question"],
                "expected": q["expected_str"],
                "hard_case": q["hard_case"],
                "trap_tags": q["trap_tags"],
            }
            for q in full_questions
        ],
        "summary": summary,
        "results": rows,
    }
    return payload


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run adversarial combinatorics audit (50 questions) with persona on/off.")
    parser.add_argument("--output", default="data/audit/adversarial_combinatorics_50_audit.json")
    parser.add_argument("--summary-output", default="data/audit/adversarial_combinatorics_50_summary.json")
    parser.add_argument("--max-questions", type=int, default=50)
    parser.add_argument("--sleep-seconds", type=float, default=0.25)
    parser.add_argument("--chunk-size", type=int, default=10)
    parser.add_argument("--chunk-cooldown-seconds", type=float, default=1.5)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-backoff-seconds", type=float, default=1.0)
    parser.add_argument("--overconfidence-threshold", type=float, default=0.85)
    parser.add_argument("--low-entropy-threshold", type=float, default=0.30)
    parser.add_argument("--run-stability-probe", action="store_true", default=True)
    parser.add_argument("--no-stability-probe", dest="run_stability_probe", action="store_false")
    parser.add_argument("--stability-runs", type=int, default=5)
    parser.add_argument("--max-stability-probes", type=int, default=12)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    payload = asyncio.run(_main_async(args))

    output_path = Path(args.output)
    summary_path = Path(args.summary_output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    summary_path.write_text(json.dumps(payload.get("summary", {}), indent=2, ensure_ascii=True), encoding="utf-8")

    print(str(output_path))
    print(str(summary_path))
    print(json.dumps(payload.get("summary", {}), indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
