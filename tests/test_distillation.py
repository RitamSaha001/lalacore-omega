import tempfile
import unittest
from pathlib import Path

from core.lalacore_x.mini_distillation import LC9DistillationHub


class DistillationTests(unittest.TestCase):
    def test_graph_compression_and_dedup(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            hub = LC9DistillationHub(root=str(root / "lc9"), export_dir=str(root / "zaggle"))

            graph = {
                "nodes": [
                    {"id": 1, "type": "assumption", "summary": "Let x = 2"},
                    {"id": 2, "type": "numeric_evaluation", "summary": "Compute x+3 = 5"},
                    {"id": 3, "type": "numeric_evaluation", "summary": "Compute x+3 = 5"},
                ],
                "edges": [
                    {"from": 1, "to": 2},
                    {"from": 2, "to": 3},
                ],
            }

            steps = hub.compress_graph(graph)
            self.assertGreaterEqual(len(steps), 2)

            added = hub.try_add_training_entry(
                {
                    "question": "If x=2, x+3?",
                    "subject": "math",
                    "difficulty": "easy",
                    "concept_cluster": ["algebra"],
                    "verified_answer": "5",
                    "best_provider": "openrouter",
                    "best_reasoning_graph": graph,
                    "judge_score": 0.9,
                    "deterministic_pass": True,
                    "winner_margin": 0.2,
                    "uncertainty": 0.1,
                }
            )
            self.assertTrue(added)

            # Duplicate graph should be skipped.
            added_again = hub.try_add_training_entry(
                {
                    "question": "If x=2, x+3?",
                    "subject": "math",
                    "difficulty": "easy",
                    "concept_cluster": ["algebra"],
                    "verified_answer": "5",
                    "best_provider": "openrouter",
                    "best_reasoning_graph": graph,
                    "judge_score": 0.9,
                    "deterministic_pass": True,
                    "winner_margin": 0.2,
                    "uncertainty": 0.1,
                }
            )
            self.assertFalse(added_again)

    def test_prompt_effectiveness_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            hub = LC9DistillationHub(root=str(root / "lc9"), export_dir=str(root / "zaggle"))

            hub.log_prompt_record(
                {
                    "subject": "math",
                    "difficulty": "hard",
                    "provider": "groq",
                    "model_name": "llama3-8b-8192",
                    "template_version": "v1",
                    "system_instructions": "inst",
                    "prompt_hash": "abc",
                    "prompt": "q",
                    "is_winner": True,
                    "winner_verified": True,
                    "winner_margin": 0.2,
                    "bt_theta": 1.4,
                }
            )

            summary = hub.analyze_prompt_effectiveness()
            self.assertIn("rows", summary)
            self.assertGreaterEqual(len(summary["rows"]), 1)


if __name__ == "__main__":
    unittest.main()
