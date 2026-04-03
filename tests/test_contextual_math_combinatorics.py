import re
import unittest

from core.math.contextual_math_solver import solve_contextual_math_question
from models.mini_loader import run_mini


def _as_int(text: str) -> int:
    m = re.search(r"-?\d+", str(text or "").replace(",", ""))
    if not m:
        raise ValueError(f"no integer found in {text!r}")
    return int(m.group(0))


class ContextualCombinatoricsTests(unittest.TestCase):
    def test_target_combinatorics_questions(self):
        cases = [
            ("How many subsets of {1–7} contain both 1 and 7?", 32),
            ("How many subsets of {1–7} contain neither 1 nor 7?", 32),
            ("How many permutations of 1–6 where 1 appears before 2?", 360),
            ("How many permutations of 1–6 where 1 appears before 2 and 3?", 240),
            ("How many 4-digit numbers from 1–9 with no repetition and sum odd?", 1440),
            ("How many 4-digit numbers from 1–9 with no repetition and sum even?", 1584),
            ("How many arrangements of 6 books if 3 specific books stay together?", 144),
            ("How many arrangements of 6 books if 3 specific books are separated?", 144),
            ("How many 5-digit numbers from 1–7 without repetition greater than 50000?", 1080),
            ("How many 3-digit numbers from 1–9 with exactly one even digit?", 240),
            ("How many 4-digit numbers using digits 0-9 without repetition have odd digit sum?", 2160),
            ("How many 4-digit numbers using digits 0-9 without repetition have even digit sum?", 2376),
            ("How many 4-digit numbers using digits 0-9 without repetition satisfy first digit > last digit?", 2520),
        ]

        for question, expected in cases:
            out = solve_contextual_math_question(question)
            self.assertIsNotNone(out, msg=question)
            self.assertTrue(bool(out.get("handled", False)), msg=question)
            self.assertEqual(_as_int(out.get("answer", "")), expected, msg=question)

    def test_mini_uses_contextual_solver_for_counting_case(self):
        out = run_mini("How many subsets of {1-7} contain both 1 and 7?", "")
        self.assertEqual(out.get("mode"), "deterministic_contextual_math")
        self.assertEqual(_as_int(out.get("final_answer", "")), 32)

    def test_sum_of_all_numbers_from_repeated_digits(self):
        question = (
            "Sum of all the numbers that can be formed using all the digits "
            "2, 3, 3, 4, 4, 4 is - (A) 22222200 (B) 11111100 (C) 55555500 (D) 20333280"
        )
        out = solve_contextual_math_question(question)
        self.assertIsNotNone(out)
        self.assertTrue(bool(out.get("handled", False)))
        self.assertEqual(_as_int(out.get("answer", "")), 22222200)

        mini = run_mini(question, "")
        self.assertEqual(mini.get("mode"), "deterministic_contextual_math")
        self.assertEqual(_as_int(mini.get("final_answer", "")), 22222200)

    def test_digit_divisibility_without_repetition_template(self):
        question = (
            "A 5 digit number divisible by 3 is to be formed using the numerals "
            "0,1,2,3,4 & 5 without repetition. The total number of ways this can be done is - "
            "(A) 3125 (B) 600 (C) 240 (D) 216"
        )
        out = solve_contextual_math_question(question)
        self.assertIsNotNone(out)
        self.assertTrue(bool(out.get("handled", False)))
        self.assertEqual(_as_int(out.get("answer", "")), 216)

        mini = run_mini(question, "")
        self.assertEqual(mini.get("mode"), "deterministic_contextual_math")
        self.assertEqual(_as_int(mini.get("final_answer", "")), 216)

    def test_baraakobama_option_check(self):
        question = (
            "The linear permutation of the word BARAAKOBAMA can be done (using all letters) - "
            "(A) in 11!/(5!2!) ways if there is no constraint "
            "(B) in 6C2*5! ways if A's are together and B's are separated "
            "(C) in 6!*2! ways if all consonants and all vowels are together "
            "(D) in 6!/3! ways if letters of BARAAK are arranged keeping OBAMA fixed at extreme right position"
        )
        out = solve_contextual_math_question(question)
        self.assertIsNotNone(out)
        self.assertTrue(bool(out.get("handled", False)))
        self.assertEqual(str(out.get("answer", "")).strip(), "A, B and D")

        mini = run_mini(question, "")
        self.assertEqual(mini.get("mode"), "deterministic_contextual_math")
        self.assertEqual(str(mini.get("final_answer", "")).strip(), "A, B and D")

    def test_mississippi_no_two_s_together(self):
        question = "How many arrangements of MISSISSIPPI have no two S together?"
        out = solve_contextual_math_question(question)
        self.assertIsNotNone(out)
        self.assertTrue(bool(out.get("handled", False)))
        self.assertEqual(_as_int(out.get("answer", "")), 7350)

        mini = run_mini(question, "")
        self.assertEqual(mini.get("mode"), "deterministic_contextual_math")
        self.assertEqual(_as_int(mini.get("final_answer", "")), 7350)


if __name__ == "__main__":
    unittest.main()
