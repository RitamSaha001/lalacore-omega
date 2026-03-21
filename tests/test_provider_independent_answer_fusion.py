import unittest

from core.lalacore_x.answer_fusion import ProviderIndependentAnswerResolver, normalize_answer
from core.lalacore_x.schemas import ProviderAnswer


class ProviderIndependentAnswerFusionTests(unittest.TestCase):
    def test_switches_to_verified_cluster_when_current_unverified(self):
        resolver = ProviderIndependentAnswerResolver()
        candidates = [
            ProviderAnswer(provider="p1", reasoning="r1", final_answer="41", confidence=0.8),
            ProviderAnswer(provider="p2", reasoning="r2", final_answer="42", confidence=0.6),
        ]
        out = resolver.resolve(
            candidates=candidates,
            current_provider="p1",
            posteriors={"p1": 0.7, "p2": 0.3},
            verification_by_provider={"p1": {"verified": False}, "p2": {"verified": True}},
            plausibility_by_provider={"p1": {"plausible": True}, "p2": {"plausible": True}},
            judge_by_provider={},
        )
        self.assertTrue(bool(out.get("switched", False)))
        self.assertEqual(str(out.get("provider", "")), "p2")

    def test_equivalent_numeric_answers_are_grouped(self):
        resolver = ProviderIndependentAnswerResolver()
        candidates = [
            ProviderAnswer(provider="p1", reasoning="r1", final_answer="0.0000", confidence=0.8),
            ProviderAnswer(provider="p2", reasoning="r2", final_answer="0", confidence=0.6),
        ]
        out = resolver.resolve(
            candidates=candidates,
            current_provider="p1",
            posteriors={"p1": 0.6, "p2": 0.4},
            verification_by_provider={"p1": {"verified": True}, "p2": {"verified": True}},
            plausibility_by_provider={"p1": {"plausible": True}, "p2": {"plausible": True}},
            judge_by_provider={},
        )
        self.assertFalse(bool(out.get("switched", False)))
        self.assertEqual(str(out.get("provider", "")), "p1")
        groups = list(out.get("groups", []))
        self.assertEqual(len(groups), 1)
        self.assertEqual(normalize_answer("0.0000"), normalize_answer("0"))

    def test_does_not_switch_to_implausible_posterior_dominant_cluster(self):
        resolver = ProviderIndependentAnswerResolver()
        candidates = [
            ProviderAnswer(provider="openrouter", reasoning="r1", final_answer="2016a^5b^4", confidence=0.62),
            ProviderAnswer(provider="mini", reasoning="r2", final_answer="2", confidence=0.08),
        ]
        out = resolver.resolve(
            candidates=candidates,
            current_provider="openrouter",
            posteriors={"openrouter": 0.00002, "mini": 0.9999},
            verification_by_provider={"openrouter": {"verified": False}, "mini": {"verified": False}},
            plausibility_by_provider={"openrouter": {"plausible": True}, "mini": {"plausible": False}},
            judge_by_provider={},
        )
        self.assertFalse(bool(out.get("switched", False)))
        self.assertEqual(str(out.get("provider", "")), "openrouter")
        self.assertIn(str(out.get("reason", "")), {"keep_current", "current_more_plausible"})


if __name__ == "__main__":
    unittest.main()
