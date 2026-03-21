import unittest

from app.arena.entropy import compute_entropy
from core.lalacore_x.calibration import ConfidenceCalibrator
from core.lalacore_x.provider_orchestrator import ProviderOrchestrator
from verification.verifier import _extract_equation_fact_expected


class ResearchRegressionGuardTests(unittest.TestCase):
    def test_routing_guard_single_provider_forces_escalation(self):
        orchestrator = ProviderOrchestrator(min_provider_count=2)
        self.assertTrue(
            orchestrator.should_force_escalation(
                entropy=0.0,
                disagreement=0.0,
                plausibility_failed=False,
                verification_failed=False,
                provider_count=1,
            )
        )

    def test_arena_entropy_nonzero_for_reasoning_divergence(self):
        responses = [
            {"provider": "a", "final_answer": "42", "reasoning": "Algebraic substitution gives 42."},
            {"provider": "b", "final_answer": "42", "reasoning": "Geometric argument and limit give forty two."},
        ]
        self.assertGreater(compute_entropy(responses), 0.0)

    def test_verifier_expected_extraction_blocks_prompt_leak(self):
        q = "Differentiate f(x)=x^2 at x=1/4 and report slope"
        self.assertIsNone(_extract_equation_fact_expected(q))
        self.assertEqual(_extract_equation_fact_expected("x=4"), "4")

    def test_calibration_predict_risk_bounds(self):
        calibrator = ConfidenceCalibrator()
        risk = calibrator.predict_risk(
            {
                "verification_fail": 0.0,
                "disagreement": 0.1,
                "retrieval_strength": 0.8,
                "critic_score": 0.7,
                "provider_reliability": 0.8,
            }
        )
        self.assertGreaterEqual(risk, 0.0)
        self.assertLessEqual(risk, 1.0)


if __name__ == "__main__":
    unittest.main()
