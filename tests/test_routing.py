import tempfile
import unittest
from pathlib import Path

from core.lalacore_x.routing import ProviderStatsMemory
from core.lalacore_x.schemas import ProblemProfile


class RoutingTokenTests(unittest.TestCase):
    def test_token_efficiency_penalty(self):
        with tempfile.TemporaryDirectory() as tmp:
            stats = ProviderStatsMemory(path=str(Path(tmp) / "provider_stats.json"))
            profile = ProblemProfile(
                subject="math",
                difficulty="medium",
                numeric=True,
                multi_concept=False,
                trap_probability=0.1,
            )

            for _ in range(12):
                stats.record_outcome(
                    provider="verbose",
                    subject="math",
                    difficulty="medium",
                    predicted_confidence=0.7,
                    verified=True,
                    token_usage={"total_tokens": 680},
                    question_tokens=8,
                )
                stats.record_outcome(
                    provider="lean",
                    subject="math",
                    difficulty="medium",
                    predicted_confidence=0.7,
                    verified=True,
                    token_usage={"total_tokens": 140},
                    question_tokens=8,
                )

            verbose_score = stats.provider_score("verbose", profile, question_tokens=8, entropy=0.1)
            lean_score = stats.provider_score("lean", profile, question_tokens=8, entropy=0.1)
            self.assertGreaterEqual(lean_score, verbose_score)


if __name__ == "__main__":
    unittest.main()
