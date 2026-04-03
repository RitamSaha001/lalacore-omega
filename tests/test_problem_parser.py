import unittest

from core.math.problem_parser import parse_structured_problem


class ProblemParserTests(unittest.TestCase):
    def test_digit_range_inside_using_clause_is_expanded(self):
        question = "How many 4-digit numbers using digits 0-9 without repetition are divisible by 3?"
        parsed = parse_structured_problem(question)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.type, "digit_permutation")
        payload = dict(parsed.payload or {})
        self.assertEqual(payload.get("length"), 4)
        self.assertEqual(payload.get("digits"), list(range(10)))
        self.assertFalse(bool(payload.get("repetition")))
        self.assertEqual(dict(payload.get("constraint") or {}).get("divisible_by"), 3)

    def test_probability_style_question_is_not_misparsed_as_counting(self):
        question = (
            "For a random 4-digit number using digits 0-9 without repetition "
            "conditioned to be divisible by 3, what is the probability it is divisible by 9?"
        )
        parsed = parse_structured_problem(question)
        self.assertIsNone(parsed)

    def test_digit_permutation_parser_captures_digit_sum_parity(self):
        question = "How many 4-digit numbers using digits 0-9 without repetition have odd digit sum?"
        parsed = parse_structured_problem(question)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.type, "digit_permutation")
        payload = dict(parsed.payload or {})
        self.assertEqual(
            dict(payload.get("constraint") or {}).get("sum_parity"),
            "odd",
        )

    def test_digit_permutation_parser_captures_first_digit_relation(self):
        question = "How many 4-digit numbers using digits 0-9 without repetition satisfy first digit > last digit?"
        parsed = parse_structured_problem(question)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.type, "digit_permutation")
        payload = dict(parsed.payload or {})
        self.assertTrue(
            bool(dict(payload.get("constraint") or {}).get("first_digit_gt_last"))
        )


if __name__ == "__main__":
    unittest.main()
