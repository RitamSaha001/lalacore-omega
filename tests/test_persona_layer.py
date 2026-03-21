import unittest
from unittest.mock import AsyncMock, patch

from core.api.entrypoint import lalacore_entry
from core.api.persona_layer import apply_persona


class PersonaLayerTests(unittest.TestCase):
    def test_persona_structure_and_exact_answer_line(self):
        final_answer = r"\\frac{\\pi}{6}"
        out = apply_persona(final_answer)
        lines = out.splitlines()

        self.assertGreaterEqual(len(lines), 4)
        self.assertEqual(lines[2], final_answer)

    def test_persona_word_cap(self):
        final_answer = "42"
        out = apply_persona(final_answer)
        lines = [line for line in out.splitlines() if line.strip()]
        added_words = sum(len(line.split()) for line in lines if line.strip() != final_answer)
        self.assertLessEqual(added_words, 120)


class PersonaEntrypointBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_persona_only_affects_display_answer(self):
        fake_result = {
            "question": "What is 2+2?",
            "reasoning": "2+2 equals 4.",
            "final_answer": "4",
            "verification": {"verified": True, "risk_score": 0.01},
            "routing_decision": "test",
            "escalate": False,
            "winner_provider": "mini",
            "profile": {"subject": "math", "difficulty": "easy", "numeric": True, "multiConcept": False, "trapProbability": 0.0},
            "arena": {"entropy": 0.0, "disagreement": 0.0, "winner_margin": 1.0, "ranked_providers": [{"provider": "mini", "score": 1.0}]},
            "retrieval": {"top_blocks": [], "claim_support_score": 0.0},
            "engine": {"name": "LALACORE_X", "version": "research-grade-v2", "backward_compatible": True, "provider_availability": {"mini": {"eligible": True}}},
        }

        with patch("core.api.entrypoint.solve_question", new=AsyncMock(return_value=fake_result)):
            out = await lalacore_entry(
                input_data="What is 2+2?",
                input_type="text",
                options={
                    "enable_meta_verification": False,
                    "enable_persona": True,
                    "response_style": "companion_chat",
                },
            )

        self.assertEqual(out["final_answer"], "4")
        self.assertIn("display_answer", out)
        self.assertNotEqual(out["display_answer"], out["final_answer"])
        self.assertIn("4", out["display_answer"])

    async def test_persona_disabled_returns_raw_display_answer(self):
        fake_result = {
            "question": "What is 2+2?",
            "reasoning": "2+2 equals 4.",
            "final_answer": "4",
            "verification": {"verified": True, "risk_score": 0.01},
            "routing_decision": "test",
            "escalate": False,
            "winner_provider": "mini",
            "profile": {"subject": "math", "difficulty": "easy", "numeric": True, "multiConcept": False, "trapProbability": 0.0},
            "arena": {"entropy": 0.0, "disagreement": 0.0, "winner_margin": 1.0, "ranked_providers": [{"provider": "mini", "score": 1.0}]},
            "retrieval": {"top_blocks": [], "claim_support_score": 0.0},
            "engine": {"name": "LALACORE_X", "version": "research-grade-v2", "backward_compatible": True, "provider_availability": {"mini": {"eligible": True}}},
        }

        with patch("core.api.entrypoint.solve_question", new=AsyncMock(return_value=fake_result)):
            out = await lalacore_entry(
                input_data="What is 2+2?",
                input_type="text",
                options={"enable_meta_verification": False, "enable_persona": False},
            )

        self.assertEqual(out["final_answer"], "4")
        self.assertEqual(out["display_answer"], "4")


if __name__ == "__main__":
    unittest.main()
