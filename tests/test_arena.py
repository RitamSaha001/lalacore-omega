import unittest

from core.lalacore_x.arena import ArenaJudge, MCTSReasoner
from core.lalacore_x.reasoning import DAGReasoner
from core.lalacore_x.schemas import ProviderAnswer


class ArenaTests(unittest.TestCase):
    def test_judge_scores_candidates(self):
        judge = ArenaJudge()
        candidates = [
            ProviderAnswer(
                provider="mini",
                reasoning="We check constraints and verify result.",
                final_answer="4",
                confidence=0.7,
                self_critique="Checked domain.",
            ),
            ProviderAnswer(
                provider="openrouter",
                reasoning="Maybe answer is 5",
                final_answer="5",
                confidence=0.4,
                self_critique="not sure",
            ),
        ]

        verif = {
            "mini": {"verified": True, "trap_probability": 0.1},
            "openrouter": {"verified": False, "trap_probability": 0.1},
        }
        reliability = {"mini": 0.7, "openrouter": 0.4}

        results = judge.evaluate(candidates, verif, reliability, retrieval_strength=0.8)
        self.assertEqual(results[0].provider, "mini")

    def test_mcts_selects_best_provider(self):
        mcts = MCTSReasoner(simulations=20)
        winner, trace = mcts.select({"a": 0.9, "b": 0.2})
        self.assertEqual(winner, "a")
        self.assertTrue(len(trace) > 0)

    def test_reasoning_graph_metrics_present(self):
        reasoner = DAGReasoner()
        graph = reasoner.build_graph(
            [
                ProviderAnswer(
                    provider="mini",
                    reasoning="Let x=2\nSubstitute x into x+3\nTherefore answer is 5",
                    final_answer="5",
                    confidence=0.8,
                )
            ]
        )
        metrics = graph["structure_metrics"]["mini"]
        self.assertIn("graph_depth", metrics)
        self.assertIn("structural_coherence_score", metrics)
        self.assertIn("process_reward_score", metrics)
        self.assertGreaterEqual(metrics["structural_coherence_score"], 0.0)


if __name__ == "__main__":
    unittest.main()
