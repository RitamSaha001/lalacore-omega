import json
import unittest
from unittest.mock import AsyncMock, patch

from core.analytics_insight_engine import (
    AnalyticsInsightEngine,
    analyze_exam_entry,
)
from core.lalacore_x.schemas import ProviderAnswer


class AnalyticsInsightEngineTests(unittest.IsolatedAsyncioTestCase):
    async def test_analyze_exam_prefers_valid_structured_provider_output(self) -> None:
        engine = AnalyticsInsightEngine()
        with patch.object(engine.providers, "ensure_startup_warmup", new=AsyncMock()), patch.object(
            engine.providers,
            "available_providers",
            return_value=["gemini", "mini"],
        ), patch.object(
            engine.providers,
            "generate_many",
            new=AsyncMock(
                return_value=[
                    ProviderAnswer(
                        provider="gemini",
                        reasoning="Built grounded exam review.",
                        final_answer=json.dumps(
                            {
                                "summary": "Momentum is good but section repair is still needed.",
                                "strengths": ["Mechanics accuracy stayed stable."],
                                "weaknesses": ["Thermodynamics accuracy dipped."],
                                "strategy": ["Repair Thermodynamics before the next test."],
                                "next_steps": ["Reattempt the wrong Thermodynamics questions."],
                            }
                        ),
                        confidence=0.82,
                    ),
                    ProviderAnswer(
                        provider="mini",
                        reasoning="Bad output.",
                        final_answer='{"summary":"Only summary"}',
                        confidence=0.40,
                    ),
                ]
            ),
        ):
            out = await engine.analyze_exam(
                result={
                    "quiz_title": "Thermodynamics Test|Physics",
                    "score": 62,
                    "max_score": 100,
                    "correct": 16,
                    "wrong": 6,
                    "skipped": 3,
                    "section_accuracy": {"Mechanics": 74, "Thermodynamics": 41},
                }
            )

        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("winner_provider"), "gemini")
        analytics = out.get("analytics") or {}
        self.assertIn("Momentum is good", str(analytics.get("summary", "")))
        self.assertTrue(list(analytics.get("strategy") or []))

    async def test_analyze_exam_entry_fails_on_invalid_provider_payload(self) -> None:
        engine = AnalyticsInsightEngine()
        with patch.object(engine.providers, "ensure_startup_warmup", new=AsyncMock()), patch.object(
            engine.providers,
            "available_providers",
            return_value=["gemini"],
        ), patch.object(
            engine.providers,
            "generate_many",
            new=AsyncMock(
                return_value=[
                    ProviderAnswer(
                        provider="gemini",
                        reasoning="Returned placeholder.",
                        final_answer="[UNRESOLVED]",
                        confidence=0.1,
                    )
                ]
            ),
        ):
            out = await engine.analyze_exam(
                result={"quiz_title": "Math Test", "score": 40, "max_score": 100}
            )

        self.assertFalse(out.get("ok"))
        self.assertEqual(out.get("status"), "ANALYTICS_ENGINE_EMPTY_OUTPUT")


class AnalyticsInsightEntrySmokeTests(unittest.IsolatedAsyncioTestCase):
    async def test_top_level_entry_returns_engine_payload(self) -> None:
        with patch(
            "core.analytics_insight_engine._ANALYTICS_ENGINE.analyze_exam",
            new=AsyncMock(return_value={"ok": True, "analytics": {"summary": "ok"}}),
        ):
            out = await analyze_exam_entry(result={"quiz_title": "Test"})
        self.assertTrue(out.get("ok"))
        self.assertIn("analytics", out)


if __name__ == "__main__":
    unittest.main()
