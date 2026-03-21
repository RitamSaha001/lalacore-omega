from __future__ import annotations

import math
import re
from collections import Counter
from itertools import combinations
from typing import List, Optional


def _normalize(text: str) -> str:
    out = str(text or "").strip().lower()
    out = out.replace("–", "-").replace("−", "-").replace("—", "-")
    out = re.sub(r"\s+", " ", out)
    return out


def _extract_ints(text: str) -> List[int]:
    return [int(x) for x in re.findall(r"-?\d+", str(text or ""))]


class InclusionExclusionSolver:
    """
    Generic inclusion-exclusion counter for divisible-by style prompts.
    """

    def solve(self, question: str) -> Optional[int]:
        q = _normalize(question)
        m = re.search(r"integers?\s+from\s+1\s*-\s*(\d+)", q)
        if not m:
            m = re.search(r"integers?\s+from\s+1\s+to\s+(\d+)", q)
        if not m:
            return None
        upper = int(m.group(1))

        if "divisible by" not in q:
            return None

        div_tail = q.split("divisible by", 1)[1] if "divisible by" in q else ""
        divisors = sorted({abs(v) for v in _extract_ints(div_tail) if abs(v) > 1})
        if not divisors:
            return None

        def lcm(a: int, b: int) -> int:
            return abs(a * b) // math.gcd(a, b)

        def count_divisible_by(subset: List[int]) -> int:
            if not subset:
                return 0
            value = subset[0]
            for d in subset[1:]:
                value = lcm(value, d)
            if value <= 0:
                return 0
            return upper // value

        if "at least one" in q:
            total = 0
            for r in range(1, len(divisors) + 1):
                term = sum(count_divisible_by(list(combo)) for combo in combinations(divisors, r))
                total += term if r % 2 == 1 else -term
            return int(total)

        if "none of" in q:
            atleast_one = 0
            for r in range(1, len(divisors) + 1):
                term = sum(count_divisible_by(list(combo)) for combo in combinations(divisors, r))
                atleast_one += term if r % 2 == 1 else -term
            return int(max(0, upper - atleast_one))

        if "exactly two" in q:
            total = 0
            for value in range(1, upper + 1):
                hits = sum(1 for d in divisors if value % d == 0)
                if hits == 2:
                    total += 1
            return int(total)

        return None


class DerangementSolver:
    """
    Derangement module for prompts like:
    - derangements of 1-5
    - derangements of n objects
    """

    def solve(self, question: str) -> Optional[int]:
        q = _normalize(question)
        if "derangement" not in q and "derangements" not in q:
            return None

        n = None
        m_range = re.search(r"(\d+)\s*-\s*(\d+)", q)
        if m_range:
            lo = int(m_range.group(1))
            hi = int(m_range.group(2))
            if lo == 1 and hi >= lo:
                n = hi
        if n is None:
            m_n = re.search(r"derangements?\s+of\s+(\d+)", q)
            if m_n:
                n = int(m_n.group(1))
        if n is None:
            m_obj = re.search(r"(\d+)\s+objects", q)
            if m_obj:
                n = int(m_obj.group(1))
        if n is None or n < 0 or n > 15:
            return None

        return int(self._derangement_count(n))

    def _derangement_count(self, n: int) -> int:
        if n == 0:
            return 1
        if n == 1:
            return 0
        d_prev2, d_prev1 = 1, 0
        for k in range(2, n + 1):
            d_cur = (k - 1) * (d_prev1 + d_prev2)
            d_prev2, d_prev1 = d_prev1, d_cur
        return d_prev1


class DistributionSolver:
    """
    Stars-bars and constrained arrangement module:
    - distribute r identical balls into n distinct boxes
    - arrangement of a word with no two vowels together
    """

    _VOWELS = set("AEIOU")

    def solve(self, question: str) -> Optional[int]:
        q = _normalize(question)
        out = self._solve_balls_boxes(q)
        if out is not None:
            return out
        return self._solve_no_two_vowels(question)

    def _solve_balls_boxes(self, q: str) -> Optional[int]:
        m = re.search(
            r"distribut(?:e|ing)\s+(\d+)\s+identical\s+(?:balls|objects)\s+into\s+(\d+)\s+distinct\s+(?:boxes|bins)",
            q,
        )
        if not m:
            return None
        balls = int(m.group(1))
        boxes = int(m.group(2))
        if boxes <= 0:
            return None

        if "at least one" in q:
            if balls < boxes:
                return 0
            return math.comb(balls - 1, boxes - 1)

        if "at most one" in q:
            if balls > boxes:
                return 0
            return math.comb(boxes, balls)

        m_cap = re.search(r"at most\s+(\d+)\s+in each", q)
        if m_cap:
            cap = int(m_cap.group(1))
            total = 0
            for j in range(0, boxes + 1):
                rem = balls - j * (cap + 1)
                if rem < 0:
                    continue
                term = math.comb(boxes, j) * math.comb(rem + boxes - 1, boxes - 1)
                total += term if j % 2 == 0 else -term
            return int(total)

        return math.comb(balls + boxes - 1, boxes - 1)

    def _solve_no_two_vowels(self, question: str) -> Optional[int]:
        q = _normalize(question)
        if "no two vowels together" not in q:
            return None
        m_word = re.search(
            r"(?:word\s+|arrangements?\s+of\s+(?:word\s+)?)"
            r"([a-zA-Z]{4,})",
            str(question or ""),
            flags=re.IGNORECASE,
        )
        if not m_word:
            return None
        word = m_word.group(1).upper()
        counts = Counter(word)
        vowel_counts = {k: v for k, v in counts.items() if k in self._VOWELS}
        cons_counts = {k: v for k, v in counts.items() if k not in self._VOWELS}
        total_vowels = sum(vowel_counts.values())
        total_cons = sum(cons_counts.values())
        if total_vowels == 0:
            return self._multiset_count(counts)
        if total_vowels > total_cons + 1:
            return 0

        cons_arrangements = self._multiset_count(Counter(cons_counts))
        vowel_arrangements = self._multiset_count(Counter(vowel_counts))
        gap_choices = math.comb(total_cons + 1, total_vowels)
        return int(cons_arrangements * vowel_arrangements * gap_choices)

    def _multiset_count(self, counts: Counter) -> int:
        total = sum(counts.values())
        value = math.factorial(total)
        for count in counts.values():
            value //= math.factorial(count)
        return int(value)
