import unittest

from core.lalacore_x.model_mix import ProviderModelMixLayer
from core.lalacore_x.providers import ProviderFabric
from core.lalacore_x.schemas import ProblemProfile


def _profile(difficulty: str) -> ProblemProfile:
    return ProblemProfile(
        subject="math",
        difficulty=difficulty,
        numeric=True,
        multi_concept=False,
        trap_probability=0.0,
    )


class ProviderCandidateModelTests(unittest.TestCase):
    def test_easy_gemini_keeps_flash_lite_first(self) -> None:
        fabric = ProviderFabric()
        models = fabric.candidate_models("gemini", _profile("easy"))

        self.assertGreaterEqual(len(models), 1)
        self.assertEqual(models[0], "gemini-2.5-flash-lite")

    def test_hard_gemini_prepends_stronger_models(self) -> None:
        fabric = ProviderFabric()
        models = fabric.candidate_models("gemini", _profile("hard"))

        self.assertGreaterEqual(len(models), 3)
        self.assertEqual(models[0], "gemini-2.5-pro")
        self.assertIn("gemini-2.5-flash-lite", models)

    def test_hard_openrouter_keeps_old_model_but_adds_stronger_candidates(self) -> None:
        fabric = ProviderFabric()
        models = fabric.candidate_models("openrouter", _profile("hard"))

        self.assertIn("deepseek/deepseek-r1:free", models)
        self.assertIn("meta-llama/llama-3.1-8b-instruct", models)
        self.assertLess(
            models.index("deepseek/deepseek-r1:free"),
            models.index("meta-llama/llama-3.1-8b-instruct"),
        )


class ProviderModelMixLayerTests(unittest.TestCase):
    def test_hard_mix_ranks_stronger_models_above_light_defaults(self) -> None:
        mix = ProviderModelMixLayer()
        ranked = mix.rank(
            provider_ranked=[
                ("openrouter", 1.0),
                ("gemini", 0.98),
            ],
            profile=_profile("hard"),
            candidate_models_by_provider={
                "openrouter": [
                    "meta-llama/llama-3.1-8b-instruct",
                    "deepseek/deepseek-r1:free",
                ],
                "gemini": [
                    "gemini-2.5-flash-lite",
                    "gemini-2.5-pro",
                ],
            },
            request_policy={},
        )

        pairs = [(row.provider, row.model) for row in ranked[:3]]
        self.assertIn(("gemini", "gemini-2.5-pro"), pairs)
        self.assertLess(
            next(
                index
                for index, row in enumerate(ranked)
                if row.provider == "gemini" and row.model == "gemini-2.5-pro"
            ),
            next(
                index
                for index, row in enumerate(ranked)
                if row.provider == "gemini" and row.model == "gemini-2.5-flash-lite"
            ),
        )

    def test_easy_mix_penalizes_premium_models(self) -> None:
        mix = ProviderModelMixLayer()
        ranked = mix.rank(
            provider_ranked=[("openrouter", 1.0)],
            profile=_profile("easy"),
            candidate_models_by_provider={
                "openrouter": [
                    "anthropic/claude-3.7-sonnet",
                    "meta-llama/llama-3.1-8b-instruct",
                ]
            },
            request_policy={},
        )

        self.assertEqual(ranked[0].model, "meta-llama/llama-3.1-8b-instruct")

    def test_policy_bonus_can_force_preferred_provider_model_to_top(self) -> None:
        mix = ProviderModelMixLayer()
        ranked = mix.rank(
            provider_ranked=[
                ("openrouter", 1.0),
                ("gemini", 0.99),
            ],
            profile=_profile("hard"),
            candidate_models_by_provider={
                "openrouter": [
                    "deepseek/deepseek-r1:free",
                    "openai/gpt-4o-mini",
                ],
                "gemini": [
                    "gemini-2.5-pro",
                    "gemini-2.5-flash",
                ],
            },
            request_policy={
                "preferred_provider": "gemini",
                "preferred_model": "gemini-2.5-pro",
                "quality_retry_force_max": True,
            },
        )

        self.assertEqual(ranked[0].provider, "gemini")
        self.assertEqual(ranked[0].model, "gemini-2.5-pro")


if __name__ == "__main__":
    unittest.main()
