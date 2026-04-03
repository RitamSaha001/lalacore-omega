import os
import unittest
from dataclasses import dataclass
from unittest.mock import patch

from core.lalacore_x.answer_quality_verifier import run_answer_quality_verifier
from core.lalacore_x.schemas import ProblemProfile


@dataclass
class _FakeReview:
    final_answer: str = ""
    raw: dict | None = None
    reasoning: str = ""


class _FakeFabric:
    def __init__(self, outputs):
        self._outputs = list(outputs)
        self.calls = 0

    def available_providers(self):
        return ["gemini"]

    async def generate(self, provider, prompt, profile, attachments):
        self.calls += 1
        item = self._outputs.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class AnswerQualityVerifierTests(unittest.IsolatedAsyncioTestCase):
    def _profile(self) -> ProblemProfile:
        return ProblemProfile(
            subject="math",
            difficulty="medium",
            numeric=True,
            multi_concept=False,
            trap_probability=0.0,
        )

    async def test_deterministic_roots_mismatch_blocks_and_proposes_exact_fix(self):
        fabric = _FakeFabric([])
        review = await run_answer_quality_verifier(
            fabric=fabric,
            question="what are the roots of x^2+7x+9=0",
            candidate_answer="-9/2 and -3",
            candidate_reasoning="The roots are -9/2 and -3.",
            profile=self._profile(),
            base_verification={
                "verified": False,
                "risk_score": 0.94,
                "failure_reason": "contextual_expression_set_mismatch",
            },
            research_verification={"answer_type": {"match": True}, "score": 0.7},
            enabled=False,
        )
        self.assertTrue(review["should_block_response"])
        self.assertIn("sqrt(13)", str(review.get("review_final_answer", "")))
        self.assertEqual(review.get("provider"), "heuristic_guard")

    async def test_fast_verifier_can_clear_missing_ground_truth_answer(self):
        fabric = _FakeFabric(
            [
                _FakeReview(
                    final_answer=(
                        'Reasoning: Good conceptual answer.\n'
                        'Final Answer: {"consistent": true, "confidence_score": 0.74, '
                        '"risk_score": 0.24, "answer_quality_score": 0.78, '
                        '"should_block_response": false, "verdict": "safe", '
                        '"suggested_correction": "", "review_final_answer": "Use conservation of energy.", '
                        '"review_reasoning": "The answer is plausible and useful.", '
                        '"issues": []}'
                    ),
                    raw={},
                    reasoning="The answer is plausible and useful.",
                )
            ]
        )
        review = await run_answer_quality_verifier(
            fabric=fabric,
            question="Explain why total mechanical energy stays constant in the ideal case.",
            candidate_answer="Use conservation of energy.",
            candidate_reasoning="In the ideal case, total mechanical energy remains constant.",
            profile=self._profile(),
            base_verification={
                "verified": False,
                "risk_score": 0.52,
                "failure_reason": "missing_ground_truth",
            },
            research_verification={"answer_type": {"match": True}, "score": 0.6},
            enabled=True,
        )
        self.assertFalse(review["should_block_response"])
        self.assertTrue(review["consistent"])
        self.assertGreaterEqual(float(review["answer_quality_score"]), 0.52)
        self.assertEqual(fabric.calls, 1)

    async def test_fast_verifier_retries_once_on_transient_failure(self):
        fabric = _FakeFabric(
            [
                RuntimeError("temporary"),
                _FakeReview(
                    final_answer=(
                        'Reasoning: Corrected after retry.\n'
                        'Final Answer: {"consistent": true, "confidence_score": 0.61, '
                        '"risk_score": 0.31, "answer_quality_score": 0.63, '
                        '"should_block_response": false, "verdict": "safe", '
                        '"suggested_correction": "", "review_final_answer": "x = 3", '
                        '"review_reasoning": "Retry succeeded and answer is usable.", '
                        '"issues": []}'
                    ),
                    raw={},
                    reasoning="Retry succeeded.",
                ),
            ]
        )
        with patch.dict(os.environ, {"LC9_FAST_VERIFIER_RETRY_COUNT": "1"}):
            review = await run_answer_quality_verifier(
                fabric=fabric,
                question="Solve 2x+1=7",
                candidate_answer="x = 3",
                candidate_reasoning="Subtract 1 and divide by 2.",
                profile=self._profile(),
                base_verification={"verified": False, "risk_score": 0.61},
                research_verification={"answer_type": {"match": True}, "score": 0.5},
                enabled=True,
            )
        self.assertFalse(review["should_block_response"])
        self.assertEqual(review.get("review_final_answer"), "x = 3")
        self.assertEqual(fabric.calls, 2)


if __name__ == "__main__":
    unittest.main()
