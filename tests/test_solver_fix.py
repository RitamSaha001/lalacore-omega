import unittest

from app.arena.entropy import compute_entropy
from core.lalacore_x.answer_extractor import extract_answer
from core.lalacore_x.plausibility_checker import check_answer_plausibility
from core.lalacore_x.solve_pipeline import SolvePipelinePolicy


class SolverFixTests(unittest.TestCase):
    def test_echo_detection_rejects_question_tail(self):
        question = "If x + 2 = 5, find x."
        answer = "If x + 2 = 5"
        report = check_answer_plausibility(question, answer, {"numeric_expected": True})
        self.assertFalse(report["plausible"])
        self.assertIn("echo_fragment", report["issues"])

    def test_short_answer_rejected(self):
        question = "Evaluate a long expression and provide final numeric value."
        answer = "7"
        report = check_answer_plausibility(question, answer, {"numeric_expected": True})
        self.assertFalse(report["plausible"])
        self.assertIn("too_short", report["issues"])

    def test_short_numeric_allowed_for_minimum_value_questions(self):
        question = "Find the minimum value of 1/x + 1/y + 1/z for x+y+z=1."
        answer = "9"
        report = check_answer_plausibility(question, answer, {"numeric_expected": True})
        self.assertTrue(report["plausible"])
        self.assertNotIn("too_short", report["issues"])

    def test_mcq_multi_option_answer_is_plausible(self):
        question = (
            "The linear permutation of BARAAKOBAMA can be done: "
            "(A) ... (B) ... (C) ... (D) ..."
        )
        answer = "A, B and D"
        report = check_answer_plausibility(question, answer, {"observed_type": "option"})
        self.assertTrue(report["plausible"])
        self.assertEqual(report["expected_type"], "option")

    def test_mcq_numeric_answer_is_allowed_when_options_present(self):
        question = "Sum of all numbers formed is - (A) 1 (B) 2 (C) 3 (D) 4"
        answer = "22222200"
        report = check_answer_plausibility(question, answer, {"observed_type": "numeric"})
        self.assertTrue(report["plausible"])
        self.assertNotIn("expected_option_type", report["issues"])

    def test_mcq_short_numeric_answer_is_allowed(self):
        question = "Number of ways is - (A) 10 (B) 108 (C) 216 (D) 600"
        answer = "216"
        report = check_answer_plausibility(question, answer, {"observed_type": "numeric"})
        self.assertTrue(report["plausible"])
        self.assertNotIn("too_short", report["issues"])

    def test_solution_prompt_not_forced_numeric(self):
        question = "Find x if sin^(-1)(x)+cos^(-1)(x)=pi."
        answer = "no real solution"
        report = check_answer_plausibility(question, answer, {"numeric_expected": True, "observed_type": "text"})
        self.assertTrue(report["plausible"])
        self.assertEqual(report["expected_type"], "solution")

    def test_extraction_prefers_final_answer_tag(self):
        question = "Solve and provide final answer."
        raw = "Reasoning: expand equation and simplify.\nFinal Answer: 13"
        out = extract_answer(question, raw, {"numeric_expected": True})
        self.assertTrue(out["matched"])
        self.assertEqual(out["final_answer"], "13")

    def test_extraction_detects_multi_option_statement(self):
        question = "Which options are correct? (A) ... (B) ... (C) ... (D) ..."
        raw = "Reasoning: checked each statement.\nCorrect options: A, B and D"
        out = extract_answer(question, raw, {"numeric_expected": False})
        self.assertTrue(out["matched"])
        self.assertEqual(out["final_answer"], "A, B and D")

    def test_escalation_on_unverified_high_risk(self):
        policy = SolvePipelinePolicy()
        gate = policy.evaluate(
            verified=False,
            risk=0.95,
            plausibility={"plausible": True, "score": 0.9, "issues": []},
            disagreement=0.0,
            arena_winner_found=True,
            entropy=0.1,
        )
        self.assertTrue(gate["force_escalate"])
        self.assertEqual(gate["final_status"], "Failed")
        self.assertIn("verification_failed_high_risk", gate["reasons"])

    def test_no_ground_truth_can_complete_with_plausible_answer(self):
        policy = SolvePipelinePolicy()
        gate = policy.evaluate(
            verified=False,
            risk=1.0,
            plausibility={"plausible": True, "score": 0.92, "issues": []},
            disagreement=0.4,
            arena_winner_found=True,
            entropy=0.6,
            verification_supported=False,
        )
        self.assertFalse(gate["force_escalate"])
        self.assertEqual(gate["final_status"], "Completed")
        self.assertIn("verification_unavailable", gate["reasons"])
        self.assertNotIn("verification_failed_high_risk", gate["reasons"])

    def test_cross_provider_structural_disagreement_increases_entropy(self):
        responses = [
            {
                "provider": "a",
                "final_answer": "42",
                "reasoning": "By algebraic substitution after eliminating variables, the result is 42.",
            },
            {
                "provider": "b",
                "final_answer": "42",
                "reasoning": "Used geometric argument and limit approximation; final stabilized value becomes forty two.",
            },
        ]
        entropy = compute_entropy(responses)
        self.assertGreater(entropy, 0.0)


if __name__ == "__main__":
    unittest.main()
