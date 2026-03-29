import asyncio
import os
import tempfile
import unittest
from typing import Any
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from app.main import app
from app.live_classes_api import (
    _live_context_prompt,
    _maybe_generate_live_support_actions,
    _run_live_class_pipeline,
)


class LiveClassesAiApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    def test_health_live_endpoint_available(self) -> None:
        response = self.client.get("/health/live")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json().get("status"), "live")

    def test_worker_transcribe_reads_recording_and_returns_transcript(self) -> None:
        with tempfile.NamedTemporaryFile(delete=False) as handle:
            handle.write(b"fake-audio-bytes")
            recording_path = handle.name
        self.addCleanup(lambda: os.path.exists(recording_path) and os.unlink(recording_path))

        with patch(
            "app.live_classes_api._STT.transcribe_bytes",
            return_value={"text": "Teacher explains Gauss law.", "confidence": 0.93},
        ):
            response = self.client.post(
                "/transcribe",
                json={"recording_path": recording_path},
            )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("text"), "Teacher explains Gauss law.")
        self.assertTrue(body.get("transcript"))
        self.assertEqual(
            body["transcript"][0].get("message"),
            "Teacher explains Gauss law.",
        )

    def test_worker_notes_endpoint_returns_structured_payload(self) -> None:
        mocked_result = {
            "final_answer": (
                '{"key_concepts":["Gauss law"],'
                '"formulas":["Phi=q/eps0"],'
                '"shortcuts":["Use symmetry"],'
                '"common_mistakes":["Ignoring enclosed charge"]}'
            )
        }
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/notes",
                json={
                    "transcript": [
                        {"speaker": "Teacher", "message": "Today we study Gauss law."}
                    ]
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("key_concepts"), ["Gauss law"])
        self.assertEqual(body.get("formulas"), ["Phi=q/eps0"])

    def test_worker_flashcards_endpoint_returns_structured_payload(self) -> None:
        mocked_result = {
            "final_answer": (
                '{"flashcards":[{"front":"What is Gauss law?","back":"Flux equals enclosed charge over eps0."}]}'
            )
        }
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/flashcards",
                json={
                    "transcript": [
                        {"speaker": "Teacher", "message": "Today we study Gauss law."}
                    ]
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("flashcards")[0].get("front"), "What is Gauss law?")

    def test_worker_summary_endpoint_returns_structured_payload(self) -> None:
        mocked_result = {
            "final_answer": (
                '{"summary":"Gauss law recap.",'
                '"highlights":["Choose a symmetric surface"],'
                '"action_items":["Practice 3 flux questions"]}'
            )
        }
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/summary",
                json={
                    "transcript": [
                        {"speaker": "Teacher", "message": "Choose a symmetric Gaussian surface."}
                    ]
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("summary"), "Gauss law recap.")
        self.assertEqual(body.get("highlights"), ["Choose a symmetric surface"])
        self.assertEqual(body.get("action_items"), ["Practice 3 flux questions"])

    def test_class_explain_returns_rich_pipeline_fields(self) -> None:
        mocked_result = {
            "final_answer": "Eccentricity is 5/4 and asymptotes are y = ±3x/4.",
            "reasoning": "Using e = sqrt(1 + b^2/a^2) and y = ±(b/a)x.",
            "visualization": {
                "type": "desmos",
                "expressions": [{"latex": r"x^2/16-y^2/9=1"}],
            },
            "web_retrieval": {
                "enabled": True,
                "context_injected": True,
                "matches": [
                    {
                        "title": "Hyperbola asymptotes reference",
                        "url": "https://math.stackexchange.com/q/example",
                    }
                ],
            },
            "citations": [
                {
                    "title": "Hyperbola asymptotes reference",
                    "url": "https://math.stackexchange.com/q/example",
                }
            ],
            "sources_consulted": ["math.stackexchange.com"],
            "calibration_metrics": {"confidence_score": 0.91},
            "profile": {"subject": "Conic Sections"},
            "student_profile": {"weak_concepts": ["Hyperbola tangents"]},
            "atlas_actions": {"triggered": False, "recommended_actions": []},
            "retrieval_score": 0.84,
        }
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/ai/class/explain",
                json={
                    "prompt": "Find eccentricity and asymptotes.",
                    "context": {"lecture_concepts": ["Hyperbola"]},
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(
            body.get("answer"),
            "Eccentricity is 5/4 and asymptotes are y = ±3x/4.",
        )
        self.assertEqual(body.get("concept"), "Conic Sections")
        self.assertEqual(body.get("sources_consulted"), ["math.stackexchange.com"])
        self.assertIsInstance(body.get("web_retrieval"), dict)
        self.assertIsInstance(body.get("visualization"), dict)
        self.assertIsInstance(body.get("student_profile"), dict)
        self.assertEqual(body.get("retrieval_score"), 0.84)

    def test_run_live_class_pipeline_applies_live_only_time_budgets(self) -> None:
        captured: dict[str, object] = {}

        async def _fake_entry(*args, **kwargs):
            captured["options"] = kwargs.get("options")
            return {"status": "ok", "final_answer": "42"}

        async def _run() -> dict[str, Any]:
            with patch(
                "core.api.entrypoint.lalacore_entry",
                new=AsyncMock(side_effect=_fake_entry),
            ):
                return await _run_live_class_pipeline(
                    task_prompt="Explain the hyperbola asymptotes.",
                    context={"lecture_concepts": ["Hyperbola"]},
                    enable_web_retrieval=True,
                )

        result = asyncio.run(_run())
        self.assertEqual(result.get("final_answer"), "42")
        options = captured.get("options")
        self.assertIsInstance(options, dict)
        self.assertEqual(options.get("pipeline_timeout_s"), 46.0)
        self.assertEqual(options.get("solve_stage_timeout_s"), 28.0)
        self.assertEqual(options.get("solve_reevaluation_timeout_s"), 14.0)
        self.assertIsInstance(options.get("provider_timeout_overrides"), dict)
        self.assertEqual(options["provider_timeout_overrides"].get("openrouter"), 14.0)
        self.assertEqual(options["provider_timeout_overrides"].get("gemini"), 16.0)

    def test_class_explain_defers_support_actions_when_atlas_triggers(self) -> None:
        mocked_result = {
            "final_answer": "Use the tangent condition.",
            "reasoning": "Base explanation.",
            "atlas_actions": {
                "triggered": True,
                "recommended_actions": [{"action": "mini_quiz"}],
            },
        }
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ), patch(
            "app.live_classes_api._maybe_generate_live_support_actions",
            new=AsyncMock(
                return_value={
                    "simplified_explanation": {"answer": "Simpler answer"},
                    "mini_quiz": {"question": "Quick check?"},
                }
            ),
        ):
            response = self.client.post(
                "/ai/class/explain",
                json={
                    "prompt": "Explain hyperbola tangent condition.",
                    "context": {"lecture_concepts": ["Hyperbola"]},
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("support_actions_pending"))
        self.assertTrue(body.get("evidence_pending"))
        self.assertNotIn("support_actions", body)

    def test_class_explain_defers_support_actions_when_generation_is_slow(self) -> None:
        mocked_result = {
            "final_answer": "Use the tangent condition.",
            "reasoning": "Base explanation.",
            "atlas_actions": {
                "triggered": True,
                "recommended_actions": [{"action": "mini_quiz"}],
            },
        }

        async def _slow_support(*args, **kwargs):
            await asyncio.sleep(0.8)
            return {"mini_quiz": {"question": "Late quiz"}}

        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ), patch(
            "app.live_classes_api._maybe_generate_live_support_actions",
            new=AsyncMock(side_effect=_slow_support),
        ):
            response = self.client.post(
                "/ai/class/explain",
                json={
                    "prompt": "Explain hyperbola tangent condition.",
                    "context": {"lecture_concepts": ["Hyperbola"]},
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("support_actions_pending"))
        self.assertTrue(body.get("evidence_pending"))
        self.assertNotIn("support_actions", body)

    def test_support_actions_timeout_isolated(self) -> None:
        async def run_case() -> dict[str, object]:
            result = {
                "atlas_actions": {
                    "triggered": True,
                    "recommended_actions": [{"action": "mini_quiz"}],
                }
            }

            async def slow_pipeline(*args, **kwargs):
                await asyncio.sleep(0.05)
                return {
                    "final_answer": "Worked answer",
                    "reasoning": "Detailed steps",
                }

            async def slow_quiz(*args, **kwargs):
                await asyncio.sleep(6.0)
                return {"question": "Too late"}

            with patch(
                "app.live_classes_api._run_live_class_pipeline",
                new=AsyncMock(side_effect=slow_pipeline),
            ), patch(
                "app.live_classes_api._generate_quiz_via_app_backend",
                new=AsyncMock(side_effect=slow_quiz),
            ):
                return await _maybe_generate_live_support_actions(
                    result=result,
                    prompt="Explain hyperbola tangent condition.",
                    context={"lecture_concepts": ["Hyperbola"]},
                )

        payload = asyncio.run(run_case())
        self.assertIn("simplified_explanation", payload)
        self.assertNotIn("mini_quiz", payload)

    def test_class_explain_support_endpoint_returns_followup_bundle(self) -> None:
        with patch(
            "app.live_classes_api._deferred_live_support_actions",
            new=AsyncMock(
                return_value={
                    "simplified_explanation": {"answer": "Simpler answer"},
                    "mini_quiz": {"question": "Quick check?"},
                }
            ),
        ):
            response = self.client.post(
                "/ai/class/explain/support",
                json={
                    "prompt": "Explain hyperbola tangent condition.",
                    "context": {"lecture_concepts": ["Hyperbola"]},
                    "atlas_actions": {"triggered": True},
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("ok"))
        self.assertIn("support_actions", body)
        self.assertIn("mini_quiz", body["support_actions"])
        self.assertIn("citations", body)

    def test_class_notes_parses_structured_json(self) -> None:
        mocked_result = {
            "final_answer": (
                '{"key_concepts":["Rectangular hyperbola"],'
                '"formulas":["y=\\u00b1(b/a)x"],'
                '"shortcuts":["Use symmetry"],'
                '"common_mistakes":["Wrong asymptote slope"]}'
            )
        }
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/ai/class/notes",
                json={"context": {"lecture_concepts": ["Hyperbola"]}},
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("key_concepts"), ["Rectangular hyperbola"])
        self.assertEqual(body.get("formulas"), ["y=±(b/a)x"])
        self.assertEqual(body.get("shortcuts"), ["Use symmetry"])
        self.assertEqual(body.get("common_mistakes"), ["Wrong asymptote slope"])

    def test_class_quiz_returns_live_poll_shape(self) -> None:
        with patch(
            "app.live_classes_api._generate_quiz_via_app_backend",
            new=AsyncMock(
                return_value={
                    "question": "Which line is an asymptote?",
                    "options": ["y=x", "y=2x", "x=2", "y=0"],
                    "correct_index": 1,
                    "timer_seconds": 25,
                }
            ),
        ):
            response = self.client.post(
                "/ai/class/quiz",
                json={
                    "topic": "Hyperbola",
                    "difficulty": "hard",
                    "live_mode": True,
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("question"), "Which line is an asymptote?")
        self.assertEqual(body.get("correct_option"), 1)
        self.assertEqual(body.get("timer_seconds"), 25)
        self.assertEqual(body.get("topic"), "Hyperbola")

    def test_class_quiz_degraded_generation_fails_closed(self) -> None:
        mocked_result = {
            "final_answer": "Uncertain answer: verification failed under high risk. Please retry with a stronger model.",
            "reasoning": "Provider error: curl: (6) Could not resolve host: openrouter.ai",
            "verification": {"verified": False, "reason": "all_provider_answers_empty"},
        }
        with patch(
            "app.live_classes_api._generate_quiz_via_app_backend",
            new=AsyncMock(return_value=None),
        ), patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/ai/class/quiz",
                json={
                    "topic": "Hyperbola",
                    "difficulty": "hard",
                    "live_mode": True,
                },
            )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json().get("detail"), "Quiz generation unavailable")

    def test_class_notes_degraded_generation_uses_context_fallback(self) -> None:
        mocked_result = {
            "final_answer": "Uncertain answer: verification failed under high risk. Please retry with a stronger model.",
            "reasoning": "Provider error: curl: (6) Could not resolve host: openrouter.ai",
            "verification": {"verified": False, "reason": "all_provider_answers_empty"},
            "steps": ["Provider error: curl: (6) Could not resolve host: openrouter.ai"],
        }
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/ai/class/notes",
                json={
                    "context": {
                        "lecture_concepts": ["Hyperbola", "Asymptotes"],
                        "recent_doubts": [
                            {
                                "student_name": "Ritam",
                                "question": "Why is the asymptote slope 3/4?",
                                "status": "queued",
                            }
                        ],
                    }
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("key_concepts"), ["Hyperbola", "Asymptotes"])
        self.assertEqual(body.get("formulas"), [])
        self.assertIn("Ritam [queued]: Why is the asymptote slope 3/4?", body.get("common_mistakes"))

    def test_class_flashcards_degraded_generation_uses_context_fallback(self) -> None:
        mocked_result = {
            "final_answer": "Uncertain answer: providers returned no usable output. Please retry with stronger settings.",
            "reasoning": "Provider error: curl: (6) Could not resolve host: openrouter.ai",
            "verification": {"verified": False, "reason": "all_provider_answers_empty"},
        }
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/ai/class/flashcards",
                json={"context": {"lecture_concepts": ["Hyperbola", "Asymptotes"]}},
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("flashcards")[0]["front"], "Key idea: Hyperbola")
        self.assertEqual(
            body.get("flashcards")[1]["front"], "Key idea: Asymptotes"
        )

    def test_class_analysis_degraded_generation_uses_context_fallback(self) -> None:
        mocked_result = {
            "final_answer": "Uncertain answer: verification failed under high risk. Please retry with a stronger model.",
            "reasoning": "Provider error: curl: (6) Could not resolve host: openrouter.ai",
            "verification": {"verified": False, "reason": "all_provider_answers_empty"},
        }
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/ai/class/analysis",
                json={
                    "context": {
                        "mastery_snapshot": {"weakest_concepts": ["Hyperbola tangents"]},
                        "recent_doubts": [
                            {
                                "student_name": "Ritam",
                                "question": "Why is the asymptote slope 3/4?",
                                "status": "queued",
                            }
                        ],
                    }
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("Weakest concepts: Hyperbola tangents", body.get("insights"))
        self.assertIn(
            "Ritam [queued]: Why is the asymptote slope 3/4?",
            body.get("doubt_clusters"),
        )

    def test_live_context_prompt_includes_classroom_awareness_sections(self) -> None:
        prompt = _live_context_prompt(
            {
                "class_metadata": {
                    "class_title": "Conic Sections Marathon",
                    "subject": "Mathematics",
                    "topic": "Hyperbola",
                },
                "lecture_concepts": ["Hyperbola", "Tangents"],
                "recent_doubts": [
                    {
                        "student_name": "Ritam",
                        "question": "Why is the tangent not real here?",
                        "status": "queued",
                    }
                ],
                "mastery_snapshot": {
                    "weakest_concepts": ["Hyperbola tangents (0.42)"],
                },
                "active_poll": {
                    "question": "Which line is an asymptote?",
                    "options": ["y=x", "y=3x/4", "x=4", "y=0"],
                },
            }
        )
        self.assertIn("Class identity:", prompt)
        self.assertIn("Recent doubts:", prompt)
        self.assertIn("Weakest concepts:", prompt)
        self.assertIn("Which line is an asymptote?", prompt)

    def test_run_live_class_pipeline_uses_clean_retrieval_prompt(self) -> None:
        captured: dict[str, object] = {}

        async def _fake_entrypoint(*, input_data, input_type, user_context, options):
            captured["input_data"] = input_data
            captured["input_type"] = input_type
            captured["user_context"] = user_context
            captured["options"] = options
            return {"status": "ok", "final_answer": "42"}

        with patch("core.api.entrypoint.lalacore_entry", new=AsyncMock(side_effect=_fake_entrypoint)):
            result = asyncio.run(
                _run_live_class_pipeline(
                    task_prompt=(
                        "Answer as a live-class teaching copilot.\n\n"
                        "Find the eccentricity and asymptotes of the hyperbola "
                        "x^2/16 - y^2/9 = 1."
                    ),
                    retrieval_prompt=(
                        "Find the eccentricity and asymptotes of the hyperbola "
                        "x^2/16 - y^2/9 = 1."
                    ),
                    context={
                        "class_metadata": {
                            "subject": "Mathematics",
                            "topic": "Hyperbola",
                            "class_title": "Conic Sections",
                        },
                        "lecture_concepts": ["Hyperbola", "Asymptotes"],
                    },
                    enable_web_retrieval=True,
                    compact_context=True,
                    function_hint="ai_chat",
                    app_surface="ai_chat",
                )
            )

        self.assertEqual(result.get("status"), "ok")
        self.assertEqual(
            captured.get("input_data"),
            "Find the eccentricity and asymptotes of the hyperbola x^2/16 - y^2/9 = 1.",
        )
        self.assertEqual(captured.get("input_type"), "text")
        options = captured.get("options")
        self.assertIsInstance(options, dict)
        self.assertEqual(options.get("function"), "ai_chat")
        self.assertEqual(options.get("app_surface"), "ai_chat")
        self.assertGreaterEqual(options.get("search_max_matches"), 16)
        user_context = captured.get("user_context")
        self.assertIsInstance(user_context, dict)
        self.assertIsInstance(user_context.get("student_profile"), dict)
        self.assertIn(
            "preferred_style",
            user_context.get("student_profile"),
        )
        aux_blocks = options.get("auxiliary_reasoning_blocks")
        self.assertIsInstance(aux_blocks, list)
        self.assertTrue(any("Live class context:" in str(item) for item in aux_blocks))
        self.assertTrue(
            any("CLASSROOM TASK DIRECTIVE:" in str(item) for item in aux_blocks)
        )

    def test_class_explain_defers_evidence_hydration_to_followup_route(self) -> None:
        mocked_result = {
            "final_answer": "Eccentricity is 5/4.",
            "reasoning": "Use e = sqrt(1 + b^2/a^2).",
            "web_retrieval": {"enabled": True, "context_injected": False, "matches": []},
            "citations": [],
            "sources_consulted": [],
        }
        with patch(
            "app.live_classes_api._run_live_class_pipeline",
            new=AsyncMock(return_value=mocked_result),
        ):
            response = self.client.post(
                "/ai/class/explain",
                json={
                    "prompt": "Find eccentricity and asymptotes.",
                    "context": {"lecture_concepts": ["Hyperbola"]},
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(len(body.get("citations") or []), 0)
        self.assertEqual(body.get("sources_consulted"), [])
        self.assertTrue(body.get("evidence_pending"))
