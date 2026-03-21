import re
import unittest
from fractions import Fraction

from core.math.contextual_math_solver import solve_contextual_math_question


def _parse_value(text: str) -> float:
    s = str(text or "").strip().replace(",", "")
    m = re.fullmatch(r"\s*(-?\d+)\s*/\s*(\d+)\s*", s)
    if m:
        return float(Fraction(int(m.group(1)), int(m.group(2))))
    n = re.search(r"-?\d+(?:\.\d+)?", s)
    if not n:
        raise ValueError(f"No numeric value in {text!r}")
    return float(n.group(0))


class ContextualAdversarialCombinatoricsTests(unittest.TestCase):
    def test_selected_adversarial_questions(self):
        cases = [
            ("How many subsets of {1-8} contain no consecutive integers?", 55.0),
            ("How many permutations of 1-6 have 1 appearing before both 2 and 3?", 240.0),
            ("How many derangements are there of 1-5?", 44.0),
            ("How many 4-digit numbers using digits 0-9 without repetition are divisible by 3?", 1548.0),
            ("How many 4-digit palindromes can be formed from digits 1-9?", 81.0),
            ("How many circular arrangements of 7 people A-G have A and B adjacent?", 240.0),
            ("How many integers from 1-200 are divisible by at least one of 2,3,5?", 146.0),
            ("In a random permutation of 1-6, what is the probability that 1 appears before 2?", 0.5),
            (
                "Given 1 appears before 2 in a random permutation of 1-7, what is the probability that 1 appears before both 2 and 3?",
                float(Fraction(2, 3)),
            ),
            (
                "For a random 4-digit number using digits 0-9 without repetition conditioned to be divisible by 3, what is the probability it is divisible by 9?",
                float(Fraction(1, 3)),
            ),
        ]
        for question, expected in cases:
            out = solve_contextual_math_question(question)
            self.assertIsNotNone(out, msg=question)
            self.assertTrue(bool(out.get("handled", False)), msg=question)
            got = _parse_value(out.get("answer", ""))
            self.assertAlmostEqual(got, expected, places=6, msg=question)


if __name__ == "__main__":
    unittest.main()
