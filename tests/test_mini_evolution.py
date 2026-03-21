import tempfile
import unittest
from pathlib import Path

from core.lalacore_x.mini_evolution import MiniEvolutionEngine


class MiniEvolutionTests(unittest.TestCase):
    def test_promotion_requires_cluster_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = MiniEvolutionEngine(
                state_path=str(root / "state.json"),
                disagreement_path=str(root / "disagree.jsonl"),
                replay_queue_path=str(root / "queue.jsonl"),
            )

            # Build reliability history.
            for _ in range(60):
                engine.record_shadow_outcome(
                    subject="math",
                    difficulty="hard",
                    predicted_confidence=0.92,
                    verified=True,
                    calibration_risk=0.08,
                    disagreement_size=0,
                    concept_clusters=["algebra", "equation"],
                )

            # No replay coverage yet, should fail gate.
            self.assertFalse(engine.can_promote("math", "hard", concept_clusters=["algebra", "equation"]))

            # Add failure replay coverage.
            for _ in range(4):
                engine.enqueue_failure(
                    {
                        "question": "q",
                        "subject": "math",
                        "difficulty": "hard",
                        "provider": "mini",
                        "risk": 0.8,
                        "calibration_risk": 0.7,
                        "deterministic_fail": True,
                        "entropy": 0.6,
                        "mini_disagreement": 0.9,
                        "concept_clusters": ["algebra", "equation"],
                    }
                )

            self.assertTrue(engine.can_promote("math", "hard", concept_clusters=["algebra", "equation"]))

    def test_replay_priority_decay_and_sampling(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = MiniEvolutionEngine(
                state_path=str(root / "state.json"),
                disagreement_path=str(root / "disagree.jsonl"),
                replay_queue_path=str(root / "queue.jsonl"),
            )

            engine.enqueue_failure(
                {
                    "question": "hard_case",
                    "subject": "physics",
                    "difficulty": "hard",
                    "provider": "mini",
                    "risk": 0.95,
                    "calibration_risk": 0.9,
                    "deterministic_fail": True,
                    "entropy": 0.8,
                    "mini_disagreement": 0.7,
                    "concept_clusters": ["mechanics"],
                }
            )

            batch = engine.sample_replay_batch(max_items=10, subject="physics", difficulty="hard")
            self.assertGreaterEqual(len(batch), 1)
            self.assertEqual(batch[0]["difficulty"], "hard")

    def test_calibration_pressure_updates_multiplier(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = MiniEvolutionEngine(
                state_path=str(root / "state.json"),
                disagreement_path=str(root / "disagree.jsonl"),
                replay_queue_path=str(root / "queue.jsonl"),
            )

            for _ in range(30):
                engine.record_shadow_outcome(
                    subject="math",
                    difficulty="medium",
                    predicted_confidence=0.95,
                    verified=False,
                    calibration_risk=0.85,
                    disagreement_size=2,
                    concept_clusters=["algebra"],
                )

            multiplier = float(engine.state["global"].get("confidence_multiplier", 1.0))
            self.assertLess(multiplier, 1.0)


if __name__ == "__main__":
    unittest.main()
