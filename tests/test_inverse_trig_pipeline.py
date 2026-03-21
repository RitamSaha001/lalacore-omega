import unittest

from core.math.inverse_trig_solver import solve_inverse_trig_question, solution_text_equivalent
from models.mini_loader import run_mini
from verification.verifier import verify_solution


class InverseTrigPipelineTests(unittest.TestCase):
    def test_deterministic_evaluate(self):
        out = solve_inverse_trig_question("Evaluate sin^(-1)(1/2) in radians.")
        self.assertIsNotNone(out)
        self.assertTrue(out["handled"])
        self.assertEqual(out["answer"], "pi/6")
        self.assertEqual(out["expected_expr"], "pi/6")

    def test_deterministic_composition(self):
        out = solve_inverse_trig_question("Evaluate tan^(-1)(tan(5*pi/4)).")
        self.assertIsNotNone(out)
        self.assertTrue(out["handled"])
        self.assertEqual(out["answer"], "pi/4")

    def test_deterministic_limit_suffix_period(self):
        out = solve_inverse_trig_question("Evaluate tan^(-1)(inf) (limit).")
        self.assertIsNotNone(out)
        self.assertTrue(out["handled"])
        self.assertEqual(out["answer"], "pi/2")

    def test_deterministic_solve_scalar(self):
        out = solve_inverse_trig_question("Solve sin^(-1)(x)=pi/6. Find x.")
        self.assertIsNotNone(out)
        self.assertTrue(out["handled"])
        # numeric formatting avoids short-answer false negatives
        self.assertEqual(out["answer"], "0.5000")
        self.assertEqual(out["expected_expr"], "1/2")

    def test_deterministic_solve_interval(self):
        out = solve_inverse_trig_question("Find x if sin^(-1)(x)+cos^(-1)(x)=pi/2.")
        self.assertIsNotNone(out)
        self.assertTrue(out["handled"])
        self.assertEqual(out["answer"], "x in [-1, 1]")
        self.assertIsNone(out["expected_expr"])
        self.assertEqual(out["expected_solution_text"], "x in [-1, 1]")

    def test_solution_text_equivalence(self):
        self.assertTrue(solution_text_equivalent("x > 0", "all real x > 0"))
        self.assertTrue(solution_text_equivalent("no solution", "no real solution"))
        self.assertFalse(solution_text_equivalent("x in [-1, 1]", "all real x > 0"))

    def test_verifier_inverse_trig_correct(self):
        report = verify_solution(
            question="Evaluate sin^(-1)(1/2) in radians.",
            predicted_answer="pi/6",
            difficulty="easy",
        )
        self.assertTrue(report["verified"])
        self.assertLess(report["risk_score"], 0.1)

    def test_verifier_inverse_trig_wrong(self):
        report = verify_solution(
            question="Evaluate sin^(-1)(1/2) in radians.",
            predicted_answer="0",
            difficulty="easy",
        )
        self.assertFalse(report["verified"])
        self.assertGreater(report["risk_score"], 0.8)

    def test_verifier_inverse_trig_solution_text(self):
        report = verify_solution(
            question="Find x if sin^(-1)(x)+cos^(-1)(x)=pi.",
            predicted_answer="no real solution",
            difficulty="easy",
        )
        self.assertTrue(report["verified"])
        self.assertLess(report["risk_score"], 0.1)

    def test_mini_uses_deterministic_inverse_trig(self):
        out = run_mini("Evaluate cos^(-1)(-1/2).", "")
        self.assertEqual(out.get("mode"), "deterministic_inverse_trig")
        self.assertEqual(out.get("final_answer"), "2*pi/3")


if __name__ == "__main__":
    unittest.main()
