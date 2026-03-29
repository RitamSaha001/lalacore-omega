import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from core.api.entrypoint import lalacore_entry
from core.lalacore_x.research_verifier import ResearchMetaVerifier
from core.math.contextual_math_solver import solve_contextual_math_question
from core.multimodal.telemetry import MultimodalTelemetry
from core.multimodal.vision_router import VisionRouter


class CrossModalConsistencyTests(unittest.TestCase):
    def test_cross_modal_consistency_scoring(self):
        verifier = ResearchMetaVerifier()
        high = verifier.cross_modal_consistency(
            ocr_text="triangle ABC angle ABC equals 60 find x",
            vision_analysis={"detected_text": "triangle ABC angle ABC 60", "figure_interpretation": "Geometry diagram detected", "structured_math_expressions": ["x+2=5"]},
            reasoning_summary="Using triangle ABC and angle ABC=60, solve x+2=5.",
        )
        low = verifier.cross_modal_consistency(
            ocr_text="triangle ABC angle ABC equals 60 find x",
            vision_analysis={"detected_text": "organic chemistry reaction mechanism", "figure_interpretation": "No geometry", "structured_math_expressions": []},
            reasoning_summary="Use capacitor formula in electrostatics.",
        )
        self.assertGreater(high["score"], low["score"])


class VisionNormalizationTests(unittest.TestCase):
    def test_vision_confidence_normalization_softmax(self):
        router = VisionRouter()
        analyses = [
            {
                "provider": "heuristic_vision",
                "confidence": 0.99,
                "detected_text": "triangle abc",
                "structured_math_expressions": ["x+2=5"],
                "detected_diagrams": {"geometry": True, "points": 3, "segments": 3, "angles": 3},
            },
            {
                "provider": "gemini_vision",
                "confidence": 0.78,
                "detected_text": "triangle abc",
                "structured_math_expressions": ["x+2=5"],
                "detected_diagrams": {"geometry": True, "points": 3, "segments": 3, "angles": 3},
            },
        ]

        out = router._arena_compare(analyses)
        probs = [row["_prob"] for row in out]
        self.assertAlmostEqual(sum(probs), 1.0, places=5)
        for row in out:
            self.assertGreaterEqual(row["_normalized_confidence"], 0.0)
            self.assertLessEqual(row["_normalized_confidence"], 1.0)


class SingleProviderEntropyGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_single_provider_entropy_guard(self):
        fake_result = {
            "question": "What is 2+2?",
            "reasoning": "2+2 equals 4.",
            "final_answer": "4",
            "verification": {"verified": True, "risk_score": 0.02},
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

        self.assertGreaterEqual(float(out["entropy"]), 0.38)
        self.assertTrue(out["calibration_metrics"]["single_provider_mode"])


class MetaContradictionTests(unittest.TestCase):
    def test_meta_verification_contradiction_detection(self):
        verifier = ResearchMetaVerifier()
        contradiction = verifier.detect_self_contradiction(
            "The function is increasing and positive, therefore the claim is true.",
            "The function is decreasing and negative, therefore this is false.",
        )
        self.assertTrue(contradiction["contradiction"])
        self.assertGreaterEqual(len(contradiction["signals"]), 1)


class LatencyTelemetryTests(unittest.TestCase):
    def test_latency_timing_and_slow_warning_logged(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            telemetry = MultimodalTelemetry(
                debug_path=str(root / "mm_debug.jsonl"),
                drift_path=str(root / "drift.json"),
                failure_cluster_path=str(root / "cluster.json"),
            )
            telemetry.log_timing(stage="solver", duration_s=2.2, slow_threshold_s=1.5)

            rows = [json.loads(line) for line in (root / "mm_debug.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
            event_types = {row.get("event_type") for row in rows}
            self.assertIn("latency_timing", event_types)
            self.assertIn("slow_path_warning", event_types)


class DeterministicGuardingTests(unittest.TestCase):
    def test_malformed_or_unsafe_expression_not_handled(self):
        out1 = solve_contextual_math_question("Evaluate __import__('os').system('ls') at x=1")
        out2 = solve_contextual_math_question("Differentiate (((x+1) at x=2")
        self.assertIsNone(out1)
        self.assertIsNone(out2)

    def test_onto_function_count_is_deterministically_solved(self):
        out = solve_contextual_math_question(
            "How many onto functions are there from a 5-element set to a 3-element set?"
        )
        self.assertIsNotNone(out)
        self.assertTrue(bool(out.get("handled")))
        self.assertEqual(out.get("answer"), "150")

    def test_bounded_integer_solution_count_is_deterministically_solved(self):
        out = solve_contextual_math_question(
            "Positive integer solutions of x1+x2+x3+x4=20 with each >=2 and x1<=5"
        )
        self.assertIsNotNone(out)
        self.assertTrue(bool(out.get("handled")))
        self.assertEqual(out.get("answer"), "290")


if __name__ == "__main__":
    unittest.main()
