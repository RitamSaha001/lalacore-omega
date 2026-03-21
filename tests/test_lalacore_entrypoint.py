import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from core.api.entrypoint import lalacore_entry
from core.multimodal.intake import IntakePayload


class LalaCoreEntrypointTests(unittest.IsolatedAsyncioTestCase):
    def _fake_solve_result(self) -> dict:
        return {
            "question": "placeholder",
            "reasoning": "reasoning",
            "final_answer": "42",
            "verification": {"verified": True, "risk_score": 0.05},
            "routing_decision": "test",
            "escalate": False,
            "winner_provider": "mini",
            "profile": {
                "subject": "math",
                "difficulty": "medium",
                "numeric": True,
                "multiConcept": False,
                "trapProbability": 0.0,
            },
            "arena": {
                "entropy": 0.1,
                "disagreement": 0.0,
                "winner_margin": 0.8,
                "ranked_providers": [{"provider": "mini", "score": 1.0}],
            },
            "retrieval": {"top_blocks": [], "claim_support_score": 0.0},
            "engine": {
                "name": "LALACORE_X",
                "version": "research-grade-v2",
                "backward_compatible": True,
                "provider_availability": {"mini": {"eligible": True}},
            },
        }

    async def test_single_entrypoint_text_flow(self):
        fake_result = {
            "question": "What is 2+2?",
            "reasoning": "2+2 equals 4.",
            "final_answer": "4",
            "verification": {"verified": True, "risk_score": 0.02},
            "routing_decision": "test",
            "escalate": False,
            "winner_provider": "mini",
            "profile": {
                "subject": "math",
                "difficulty": "easy",
                "numeric": True,
                "multiConcept": False,
                "trapProbability": 0.0,
            },
            "arena": {
                "entropy": 0.05,
                "disagreement": 0.0,
                "winner_margin": 0.85,
                "ranked_providers": [{"provider": "mini", "score": 1.0}],
            },
            "retrieval": {"top_blocks": [], "claim_support_score": 0.0},
            "engine": {
                "name": "LALACORE_X",
                "version": "research-grade-v2",
                "backward_compatible": True,
                "provider_availability": {"mini": {"eligible": True}},
            },
        }

        with patch("core.api.entrypoint.solve_question", new=AsyncMock(return_value=fake_result)):
            out = await lalacore_entry(
                input_data="What is 2+2?",
                input_type="text",
                options={"enable_meta_verification": False},
            )

        self.assertEqual(out["status"], "ok")
        self.assertIn("provider_diagnostics", out)
        self.assertIn("calibration_metrics", out)
        self.assertIn("research_verification", out)
        self.assertEqual(out["winner_provider"], "mini")

    async def test_meta_verification_correction_is_applied_when_unverified(self):
        fake_result = {
            "question": "Find minimum value.",
            "reasoning": "Initial result.",
            "final_answer": "9/2",
            "verification": {"verified": False, "risk_score": 0.95},
            "routing_decision": "test",
            "escalate": True,
            "winner_provider": "hf",
            "profile": {
                "subject": "math",
                "difficulty": "medium",
                "numeric": True,
                "multiConcept": False,
                "trapProbability": 0.0,
            },
            "arena": {
                "entropy": 0.1,
                "disagreement": 0.2,
                "winner_margin": 0.5,
                "ranked_providers": [{"provider": "hf", "score": 1.0}],
            },
            "retrieval": {"top_blocks": [], "claim_support_score": 0.0},
            "engine": {
                "name": "LALACORE_X",
                "version": "research-grade-v2",
                "backward_compatible": True,
                "provider_availability": {"hf": {"eligible": True}},
            },
        }
        fake_meta = {
            "attempted": True,
            "override_allowed": True,
            "timed_out": False,
            "flags": [],
            "suggested_correction": "\\boxed{9}",
            "review_final_answer": "\\boxed{9}",
            "consistent": False,
        }

        with patch("core.api.entrypoint.solve_question", new=AsyncMock(return_value=fake_result)), patch(
            "core.api.entrypoint._run_meta_verification",
            new=AsyncMock(return_value=fake_meta),
        ):
            out = await lalacore_entry(
                input_data="Find minimum value.",
                input_type="text",
                options={
                    "enable_meta_verification": True,
                    "enable_persona": False,
                    "meta_override_min_confidence": 0.0,
                    "meta_override_max_risk": 1.0,
                    "meta_override_max_disagreement": 1.0,
                },
            )

        self.assertEqual(out["final_answer"], "9")
        self.assertTrue(bool((out.get("meta_verification") or {}).get("applied_correction")))

    async def test_meta_verification_rejects_implausible_correction(self):
        fake_result = {
            "question": "Solve for x: 2^(x+1)+2^(1-x)=5",
            "reasoning": "Initial result.",
            "final_answer": "1",
            "verification": {"verified": False, "risk_score": 0.95},
            "routing_decision": "test",
            "escalate": True,
            "winner_provider": "hf",
            "profile": {
                "subject": "math",
                "difficulty": "medium",
                "numeric": True,
                "multiConcept": False,
                "trapProbability": 0.0,
            },
            "arena": {
                "entropy": 0.1,
                "disagreement": 0.2,
                "winner_margin": 0.5,
                "ranked_providers": [{"provider": "hf", "score": 1.0}],
            },
            "retrieval": {"top_blocks": [], "claim_support_score": 0.0},
            "engine": {
                "name": "LALACORE_X",
                "version": "research-grade-v2",
                "backward_compatible": True,
                "provider_availability": {"hf": {"eligible": True}},
            },
        }
        fake_meta = {
            "attempted": True,
            "override_allowed": True,
            "timed_out": False,
            "flags": [],
            "suggested_correction": "This appears to be a numerical value but not clearly justified.",
            "review_final_answer": "This appears to be a numerical value but not clearly justified.",
            "consistent": False,
        }

        with patch("core.api.entrypoint.solve_question", new=AsyncMock(return_value=fake_result)), patch(
            "core.api.entrypoint._run_meta_verification",
            new=AsyncMock(return_value=fake_meta),
        ):
            out = await lalacore_entry(
                input_data="Solve for x: 2^(x+1)+2^(1-x)=5",
                input_type="text",
                options={
                    "enable_meta_verification": True,
                    "enable_persona": False,
                    "meta_override_min_confidence": 0.0,
                    "meta_override_max_risk": 1.0,
                    "meta_override_max_disagreement": 1.0,
                },
            )

        self.assertEqual(out["final_answer"], "1")
        self.assertFalse(bool((out.get("meta_verification") or {}).get("applied_correction")))
        self.assertEqual(
            (out.get("meta_verification") or {}).get("correction_rejected"),
            "implausible_suggested_correction",
        )

    async def test_meta_verification_guard_blocks_high_risk_override(self):
        fake_result = {
            "question": "Electrostatics cavity question",
            "reasoning": "Initial result.",
            "final_answer": "E_P = 0",
            "verification": {"verified": False, "risk_score": 0.99},
            "routing_decision": "test",
            "escalate": True,
            "winner_provider": "openrouter",
            "profile": {
                "subject": "physics",
                "difficulty": "hard",
                "numeric": True,
                "multiConcept": True,
                "trapProbability": 0.0,
            },
            "arena": {
                "entropy": 1.7,
                "disagreement": 0.75,
                "winner_margin": 0.02,
                "ranked_providers": [{"provider": "openrouter", "score": 0.4}],
            },
            "quality_gate": {
                "completion_ok": False,
                "final_status": "Failed",
                "force_escalate": True,
                "reasons": ["verification_failed_high_risk", "cross_provider_disagreement", "high_entropy"],
            },
            "retrieval": {"top_blocks": [], "claim_support_score": 0.0},
            "engine": {
                "name": "LALACORE_X",
                "version": "research-grade-v2",
                "backward_compatible": True,
                "provider_availability": {"openrouter": {"eligible": True}},
            },
        }
        fake_meta = {
            "attempted": True,
            "override_allowed": True,
            "timed_out": False,
            "flags": [],
            "suggested_correction": "E_P = 0",
            "review_final_answer": "E_P = 0",
            "consistent": False,
        }

        with patch("core.api.entrypoint.solve_question", new=AsyncMock(return_value=fake_result)), patch(
            "core.api.entrypoint._run_meta_verification",
            new=AsyncMock(return_value=fake_meta),
        ):
            out = await lalacore_entry(
                input_data="Electrostatics cavity question",
                input_type="text",
                options={"enable_meta_verification": True, "enable_persona": False},
            )

        self.assertEqual(out["status"], "uncertain")
        self.assertIn("Uncertain answer", str(out.get("final_answer", "")))
        self.assertFalse(bool((out.get("meta_verification") or {}).get("applied_correction")))
        self.assertIn(
            "quality_gate_failed",
            str((out.get("meta_verification") or {}).get("override_block_reason", "")),
        )

    async def test_empty_final_answer_becomes_uncertain_payload(self):
        fake_result = {
            "question": "Hard unsupported prompt",
            "reasoning": "No provider returned parseable output.",
            "final_answer": "",
            "verification": {"verified": False, "risk_score": 1.0, "reason": "all_provider_answers_empty"},
            "routing_decision": "degraded_mode",
            "escalate": True,
            "winner_provider": "mini",
            "profile": {
                "subject": "math",
                "difficulty": "hard",
                "numeric": True,
                "multiConcept": True,
                "trapProbability": 0.0,
            },
            "arena": {
                "entropy": 0.0,
                "disagreement": 0.0,
                "winner_margin": 0.0,
                "ranked_providers": [],
            },
            "retrieval": {"top_blocks": [], "claim_support_score": 0.0},
            "engine": {
                "name": "LALACORE_X",
                "version": "research-grade-v2",
                "backward_compatible": True,
                "provider_availability": {"mini": {"eligible": True}},
            },
        }

        with patch("core.api.entrypoint.solve_question", new=AsyncMock(return_value=fake_result)):
            out = await lalacore_entry(
                input_data="Hard unsupported prompt",
                input_type="text",
                options={"enable_meta_verification": True, "enable_persona": False},
            )

        self.assertEqual(out["status"], "uncertain")
        self.assertIn("Uncertain answer", str(out.get("final_answer", "")))
        self.assertIn(
            "empty_final_answer",
            str(((out.get("quality_gate") or {}).get("reasons") or [])),
        )

    async def test_multimodal_default_limit_accepts_larger_ocr_preprocess(self):
        intake_payload = IntakePayload(
            input_type="mixed",
            text="Solve the OCR problem.",
            image_bytes=b"\x89PNG\r\n\x1a\n",
            files=[{"type": "image", "size": 8}],
        )
        with patch("core.api.entrypoint._INTAKE.normalize", return_value=intake_payload), patch(
            "core.api.entrypoint._OCR.extract_async",
            new=AsyncMock(return_value={"clean_text": "A" * 50_000}),
        ), patch(
            "core.api.entrypoint._VISION.analyze",
            new=AsyncMock(return_value={"status": "ok"}),
        ), patch(
            "core.api.entrypoint.solve_question",
            new=AsyncMock(return_value=self._fake_solve_result()),
        ) as mocked_solve:
            out = await lalacore_entry(
                input_data={"text": "Solve", "image": "fake"},
                input_type="mixed",
                options={"enable_meta_verification": False},
            )

        self.assertEqual(out.get("status"), "ok")
        self.assertGreaterEqual(mocked_solve.await_count, 1)

    async def test_pipeline_timeout_returns_safe_error_payload(self):
        async def _slow_execute(*args, **kwargs):
            await asyncio.sleep(0.05)
            return {"status": "ok"}

        with patch("core.api.entrypoint._PIPELINE_TIMEOUT_S", 0.01), patch(
            "core.api.entrypoint._PIPELINE_CONTROLLER.execute",
            new=AsyncMock(side_effect=_slow_execute),
        ):
            out = await lalacore_entry(
                input_data="What is 2+2?",
                input_type="text",
                options={"enable_meta_verification": False},
            )

        self.assertEqual(out.get("status"), "error")
        self.assertEqual(out.get("error"), "pipeline_timeout")

    async def test_pipeline_runtime_failure_returns_safe_error_payload(self):
        with patch(
            "core.api.entrypoint._PIPELINE_CONTROLLER.execute",
            new=AsyncMock(side_effect=RuntimeError("boom")),
        ):
            out = await lalacore_entry(
                input_data="What is 2+2?",
                input_type="text",
                options={"enable_meta_verification": False},
            )

        self.assertEqual(out.get("status"), "error")
        self.assertEqual(out.get("error"), "pipeline_runtime_failure")

    async def test_multimodal_explicit_limit_is_still_enforced(self):
        intake_payload = IntakePayload(
            input_type="mixed",
            text="Solve the OCR problem.",
            image_bytes=b"\x89PNG\r\n\x1a\n",
            files=[{"type": "image", "size": 8}],
        )
        with patch("core.api.entrypoint._INTAKE.normalize", return_value=intake_payload), patch(
            "core.api.entrypoint._OCR.extract_async",
            new=AsyncMock(return_value={"clean_text": "A" * 50_000}),
        ), patch(
            "core.api.entrypoint.solve_question",
            new=AsyncMock(return_value=self._fake_solve_result()),
        ) as mocked_solve:
            out = await lalacore_entry(
                input_data={"text": "Solve", "image": "fake"},
                input_type="mixed",
                options={"max_input_chars": 18_000, "enable_meta_verification": False},
            )

        self.assertEqual(out.get("status"), "error")
        self.assertEqual(out.get("error"), "preprocessed_input_too_long")
        self.assertEqual(mocked_solve.await_count, 0)

    async def test_graph_of_thought_injects_context_when_enabled(self):
        fake_graph = {
            "status": "ok",
            "context_block": "GRAPH-OF-THOUGHT CONTEXT\nHypotheses:\n- Use algebraic reduction.",
            "nodes": [{"id": "n1", "type": "hypothesis", "content": "Use algebraic reduction.", "confidence": 0.74}],
            "edges": [],
            "telemetry": {
                "node_count": 1,
                "tool_calls": 0,
                "retrieval_nodes": 0,
                "verification_pass": False,
                "final_confidence": 0.74,
                "stop_reason": "graph_expanded",
            },
            "diagram": {},
            "concepts": [],
            "early_verified": False,
        }
        fake_result = self._fake_solve_result()

        with patch("core.api.entrypoint._GOT_ENGINE.run", new=AsyncMock(return_value=fake_graph)), patch(
            "core.api.entrypoint.solve_question",
            new=AsyncMock(return_value=fake_result),
        ) as mocked_solve:
            out = await lalacore_entry(
                input_data="Solve x^2 - 5x + 6 = 0",
                input_type="text",
                options={
                    "enable_graph_of_thought": True,
                    "enable_meta_verification": False,
                    "enable_persona": False,
                },
            )

        self.assertEqual(out.get("status"), "ok")
        self.assertEqual(out.get("reasoning_graph", {}).get("status"), "ok")
        self.assertGreaterEqual(mocked_solve.await_count, 1)
        injected_prompt = mocked_solve.await_args.args[0]
        self.assertIn("GRAPH-OF-THOUGHT CONTEXT", injected_prompt)

    async def test_graph_of_thought_failure_falls_back_to_solver(self):
        fake_result = self._fake_solve_result()

        with patch(
            "core.api.entrypoint._GOT_ENGINE.run",
            new=AsyncMock(side_effect=RuntimeError("got_failure")),
        ), patch(
            "core.api.entrypoint.solve_question",
            new=AsyncMock(return_value=fake_result),
        ) as mocked_solve:
            out = await lalacore_entry(
                input_data="Find value of integral",
                input_type="text",
                options={
                    "enable_graph_of_thought": True,
                    "enable_meta_verification": False,
                    "enable_persona": False,
                },
            )

        self.assertEqual(out.get("status"), "ok")
        self.assertGreaterEqual(mocked_solve.await_count, 1)
        self.assertEqual(out.get("final_answer"), "42")
        self.assertEqual(out.get("reasoning_graph", {}).get("status"), "failed")

    async def test_mcts_context_injected_before_solver_when_verified(self):
        fake_result = self._fake_solve_result()
        fake_mcts = {
            "status": "ok",
            "context_block": "MCTS SEARCH CONTEXT\nChosen reasoning path:\n1. [expand_reasoning_step] isolate variable",
            "best_path": [
                {
                    "node_id": 2,
                    "action": {
                        "type": "expand_reasoning_step",
                        "content": "isolate variable",
                        "prior": 0.8,
                        "provider": "mini",
                        "payload": {},
                    },
                }
            ],
            "tree": {"nodes": [], "edges": []},
            "developer_mode": False,
            "telemetry": {
                "iterations": 6,
                "nodes_explored": 12,
                "tool_calls": 2,
                "retrieval_calls": 3,
                "verification_pass": True,
                "final_confidence": 0.82,
                "stop_reason": "verified_solution_found",
            },
        }

        with patch("core.api.entrypoint._MCTS_ENGINE.search", new=AsyncMock(return_value=fake_mcts)), patch(
            "core.api.entrypoint.solve_question",
            new=AsyncMock(return_value=fake_result),
        ) as mocked_solve:
            out = await lalacore_entry(
                input_data="Solve x+2=5",
                input_type="text",
                options={
                    "enable_mcts_reasoning": True,
                    "enable_graph_of_thought": False,
                    "enable_meta_verification": False,
                    "enable_persona": False,
                },
            )

        self.assertEqual(out.get("status"), "ok")
        self.assertEqual(out.get("mcts_search", {}).get("status"), "ok")
        injected_prompt = mocked_solve.await_args.args[0]
        self.assertIn("MCTS SEARCH CONTEXT", injected_prompt)


if __name__ == "__main__":
    unittest.main()
