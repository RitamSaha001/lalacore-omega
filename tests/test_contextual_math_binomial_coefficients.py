import re
import unittest

from core.math.contextual_math_solver import solve_contextual_math_question
from models.mini_loader import run_mini


def _as_int(text: str) -> int:
    m = re.search(r"-?\d+", str(text or "").replace(",", ""))
    if not m:
        raise ValueError(f"no integer found in {text!r}")
    return int(m.group(0))


class ContextualBinomialCoefficientTests(unittest.TestCase):
    def test_target_coefficient_questions(self):
        cases = [
            ("Find the coefficient of x^10 in (1 + x)^20.", 184756),
            ("Find the coefficient of x^9 in (1 + x)^18.", 48620),
            ("Find the coefficient of x^7 in (1 + x)^14 + (1 - x)^14.", 0),
            ("Find the coefficient of x^8 in (1 + x)^16 - (1 - x)^16.", 0),
            ("Find the coefficient of x^12 in (1 + x)^24.", 2704156),
            ("Find the coefficient of x^6 in (1 + x)^12 + (1 - x)^12.", 1848),
            ("Find the coefficient of x^5 in (1 + x)^11 - (1 - x)^11.", 924),
            ("Find the coefficient of x^4 in (1 + x)^8 (1 - x)^8.", 28),
            ("Find the coefficient of x^15 in (1 + x)^30.", 155117520),
            ("Find the coefficient of x^3 in (1 + x)^9 + (1 - x)^9.", 0),
        ]

        for question, expected in cases:
            out = solve_contextual_math_question(question)
            self.assertIsNotNone(out, msg=question)
            self.assertTrue(bool(out.get("handled", False)), msg=question)
            self.assertEqual(_as_int(out.get("answer", "")), expected, msg=question)

    def test_mini_uses_contextual_solver_for_coefficient_case(self):
        out = run_mini("Find the coefficient of x^10 in (1 + x)^20.", "")
        self.assertEqual(out.get("mode"), "deterministic_contextual_math")
        self.assertEqual(_as_int(out.get("final_answer", "")), 184756)

    def test_target_constant_term_questions(self):
        cases = [
            ("Find the constant term in (2x^2 - 3/x)^6.", 4860),
            ("Find the constant term in (x^3 + 2/x)^9.", 0),
            ("Find the constant term in (3x - 1/x^2)^8.", 0),
            ("Find the constant term in (x^4 - 2/x)^7.", 0),
            ("Find the constant term in (2x^3 + 1/x^2)^10.", 3360),
            ("Find the constant term in (x^2 - 3/x)^9.", 61236),
            ("Find the constant term in (4x^2 + 1/x)^8.", 0),
            ("Find the constant term in (x^5 - 1/x^2)^7.", -21),
            ("Find the constant term in (3x^3 + 2/x)^8.", 16128),
            ("Find the constant term in (x^2 + 4/x)^10.", 0),
        ]

        for question, expected in cases:
            out = solve_contextual_math_question(question)
            self.assertIsNotNone(out, msg=question)
            self.assertTrue(bool(out.get("handled", False)), msg=question)
            self.assertEqual(_as_int(out.get("answer", "")), expected, msg=question)

    def test_mini_uses_contextual_solver_for_constant_term_case(self):
        out = run_mini("Find the constant term in (2x^2 - 3/x)^6.", "")
        self.assertEqual(out.get("mode"), "deterministic_contextual_math")
        self.assertEqual(_as_int(out.get("final_answer", "")), 4860)


if __name__ == "__main__":
    unittest.main()
