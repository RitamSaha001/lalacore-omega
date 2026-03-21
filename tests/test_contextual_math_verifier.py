import unittest

from models.mini_loader import run_mini
from verification.verifier import safe_parse, verify_solution


class ContextualMathVerifierTests(unittest.TestCase):
    def test_derivative_point_does_not_leak_ground_truth_from_prompt(self):
        report = verify_solution(
            question="Differentiate sin^(-1)(2x) at x=1/4.",
            predicted_answer="1/4",
            difficulty="medium",
        )
        self.assertFalse(report["verified"])
        self.assertIn("contextual", str(report.get("failure_reason", "")))

    def test_derivative_point_contextual_verification(self):
        report = verify_solution(
            question="Differentiate sin^(-1)(2x) at x=1/4.",
            predicted_answer="4/sqrt(3)",
            difficulty="medium",
        )
        self.assertTrue(report["verified"])
        self.assertLess(report["risk_score"], 0.1)

    def test_definite_integral_contextual_verification(self):
        report = verify_solution(
            question="Evaluate ∫ dx/(1+x^2) from 0 to 1.",
            predicted_answer="pi/4",
            difficulty="medium",
        )
        self.assertTrue(report["verified"])
        self.assertLess(report["risk_score"], 0.1)

    def test_mini_uses_contextual_math_solver(self):
        out = run_mini("Differentiate sin^(-1)(2x) at x=1/4.", "")
        self.assertEqual(out.get("mode"), "deterministic_contextual_math")
        self.assertNotEqual(str(out.get("final_answer", "")).strip(), "1/4")

    def test_safe_parse_strips_numbering_and_answer_prefix(self):
        parsed = safe_parse("1) Final Answer: x^2 + 2*x + 1")
        self.assertEqual(str(parsed), "x**2 + 2*x + 1")

    def test_safe_parse_handles_combination_notation(self):
        parsed = safe_parse("6C2*5!")
        self.assertEqual(int(parsed), 1800)

    def test_safe_parse_handles_combination_function_notation(self):
        parsed = safe_parse("C(6,2) * 5!")
        self.assertEqual(int(parsed), 1800)


if __name__ == "__main__":
    unittest.main()
