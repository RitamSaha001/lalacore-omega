import unittest

from app.arena.bayesian_aggregator import BayesianAggregator
from app.arena.bradley_terry import BradleyTerryEngine
from app.arena.pairwise_engine import PairwiseEngine
from verification.verifier import verify_solution


class BtAndVerifierTests(unittest.TestCase):
    def test_bayesian_aggregator_handles_zero_total(self):
        agg = BayesianAggregator()
        responses = [
            {"provider": "a", "skill": 0.0, "critic_score": 0.0, "deterministic_pass": False},
            {"provider": "b", "skill": 0.0, "critic_score": 0.0, "deterministic_pass": False},
        ]

        post = agg.compute(responses, {"a": 0.0, "b": 0.0})
        self.assertAlmostEqual(post["a"] + post["b"], 1.0, places=6)

    def test_verifier_accepts_equivalent_answer(self):
        report = verify_solution("2+2=4", "4")
        self.assertTrue(report["verified"])

    def test_log_space_aggregator_details(self):
        agg = BayesianAggregator()
        responses = [
            {"provider": "a", "skill": 0.9, "critic_score": 0.8, "deterministic_pass": True},
            {"provider": "b", "skill": 0.7, "critic_score": 0.6, "deterministic_pass": False},
        ]
        details = agg.compute(
            responses=responses,
            thetas={"a": 1.2, "b": 0.2},
            uncertainties={"a": 0.2, "b": 0.9},
            entropy=0.4,
            return_details=True,
        )
        self.assertIn("winner_margin", details)
        self.assertIn("confidence", details)
        self.assertAlmostEqual(sum(details["posteriors"].values()), 1.0, places=6)

    def test_pairwise_engine_returns_diagnostics(self):
        bt = BradleyTerryEngine()
        engine = PairwiseEngine(bt_engine=bt, similarity_engine=None)
        responses = [
            {
                "provider": "a",
                "critic_score": 0.9,
                "deterministic_pass": True,
                "skill": 0.7,
                "confidence": 0.8,
                "final_answer": "4",
            },
            {
                "provider": "b",
                "critic_score": 0.6,
                "deterministic_pass": False,
                "skill": 0.6,
                "confidence": 0.5,
                "final_answer": "5",
            },
        ]
        thetas, matches, details = engine.run(responses, entropy=0.2, return_details=True)
        self.assertEqual(set(thetas.keys()), {"a", "b"})
        self.assertEqual(len(matches), 1)
        self.assertIn("confidence_margin", details)
        self.assertIn("disagreement_cases", details)


if __name__ == "__main__":
    unittest.main()
