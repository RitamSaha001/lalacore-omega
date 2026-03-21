import unittest

from engine.mcts_reasoner import MCTSSearch


class MCTSReasonerTests(unittest.IsolatedAsyncioTestCase):
    async def test_search_builds_tree_and_best_path(self):
        engine = MCTSSearch(max_iterations=12, max_depth=6, max_nodes=120)
        out = await engine.search(
            question="Solve x^2 - 5x + 6 = 0",
            profile={"subject": "math", "difficulty": "easy", "numeric": True},
            web_retrieval={"matches": [], "solution": {}},
            allow_provider_reasoning=False,
            timeout_s=1.8,
        )
        self.assertIn(out.get("status"), {"ok", "unverified"})
        self.assertIn("telemetry", out)
        self.assertIn("tree", out)
        self.assertGreaterEqual(int((out.get("telemetry") or {}).get("iterations", 0)), 1)
        self.assertGreaterEqual(len((out.get("tree") or {}).get("nodes", [])), 1)

    async def test_search_respects_failsafe_on_empty_question(self):
        engine = MCTSSearch()
        out = await engine.search(question="", allow_provider_reasoning=False)
        self.assertEqual(out.get("status"), "failed")
        self.assertEqual((out.get("telemetry") or {}).get("verification_pass"), False)


if __name__ == "__main__":
    unittest.main()
