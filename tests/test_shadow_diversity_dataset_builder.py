import json
import tempfile
import unittest
from pathlib import Path

from core.mini_training.dataset_builder import DatasetBuildConfig, MiniTrainingDatasetBuilder


class ShadowDiversityDatasetBuilderTests(unittest.TestCase):
    def _write_jsonl(self, path: Path, rows):
        with path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")

    def test_builder_ingests_provider_gap_and_shadow_logs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            solver = root / "solver.jsonl"
            hooks = root / "hooks.jsonl"
            shadow = root / "mini_shadow.jsonl"
            arena_shadow = root / "arena_shadow.jsonl"
            provider_gap = root / "provider_gap.jsonl"
            divergence = root / "divergence.jsonl"
            rare_cross = root / "rare_cross.jsonl"

            self._write_jsonl(solver, [])
            self._write_jsonl(hooks, [])
            self._write_jsonl(shadow, [])
            self._write_jsonl(
                arena_shadow,
                [
                    {
                        "question": "q1",
                        "winner_answer": "42",
                        "winner_provider": "mini",
                        "winner_verified": True,
                        "subject": "math",
                        "difficulty": "medium",
                        "concept_cluster": ["algebra"],
                        "entropy": 0.5,
                        "disagreement": 0.4,
                        "provider_count": 3,
                    }
                ],
            )
            self._write_jsonl(
                provider_gap,
                [
                    {
                        "question": "q1",
                        "winner_answer": "42",
                        "winner_provider": "mini",
                        "winner_verified": True,
                        "provider": "openrouter",
                        "provider_answer": "41",
                        "answer_mismatch": True,
                        "subject": "math",
                        "difficulty": "medium",
                        "concept_cluster": ["algebra"],
                        "entropy": 0.5,
                        "disagreement": 0.8,
                        "provider_risk": 0.95,
                    }
                ],
            )
            self._write_jsonl(
                divergence,
                [
                    {
                        "question": "q1",
                        "winner_answer": "42",
                        "winner_provider": "mini",
                        "subject": "math",
                        "difficulty": "medium",
                        "concept_cluster": ["algebra"],
                        "normalized_answer": "42",
                        "count": 2,
                        "verified_count": 1,
                        "entropy": 0.5,
                        "disagreement": 0.7,
                    }
                ],
            )
            self._write_jsonl(
                rare_cross,
                [
                    {
                        "question": "q2",
                        "winner_answer": "84",
                        "winner_provider": "mini",
                        "winner_verified": True,
                        "subject": "math",
                        "difficulty": "hard",
                        "rare_clusters": ["rare_cluster"],
                        "entropy": 0.6,
                        "disagreement": 0.6,
                    }
                ],
            )

            builder = MiniTrainingDatasetBuilder(
                solver_debug_path=str(solver),
                automation_hooks_path=str(hooks),
                mini_shadow_logs_path=str(shadow),
                arena_shadow_disagreements_path=str(arena_shadow),
                deterministic_vs_provider_gap_path=str(provider_gap),
                reasoning_divergence_clusters_path=str(divergence),
                rare_cluster_cross_provider_path=str(rare_cross),
                output_dir=str(root / "out"),
            )
            result = builder.build_dataset(
                DatasetBuildConfig(
                    require_verified=False,
                    risk_threshold=1.0,
                    entropy_threshold=1.0,
                    include_synthetic_disagreement_rows=False,
                )
            )
            sources = {str(row.get("source", "")) for row in result.rows}
            self.assertIn("deterministic_vs_provider_gap", sources)
            self.assertIn("arena_shadow_disagreement", sources)
            self.assertIn("reasoning_divergence_cluster", sources)
            self.assertIn("rare_cluster_cross_provider", sources)
            self.assertEqual(int(result.stats["sources"]["provider_gap_rows"]), 1)
            self.assertEqual(int(result.stats["sources"]["arena_shadow_rows"]), 1)


if __name__ == "__main__":
    unittest.main()
