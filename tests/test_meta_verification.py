import tempfile
import unittest
from pathlib import Path

from core.lalacore_x.meta_verification import MetaVerificationLayer


class MetaVerificationTests(unittest.TestCase):
    def test_error_typing_and_weighting(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "lc9"
            layer = MetaVerificationLayer(root=str(root))

            record = layer.classify(
                question="If x has units of m/s, simplify expression and give unit",
                subject="physics",
                difficulty="hard",
                concept_clusters=["physics", "units"],
                predicted_answer="5 kg",
                predicted_confidence=0.91,
                verification={
                    "verified": False,
                    "failure_reason": "unit,boundary",
                    "stage_results": {
                        "unit": False,
                        "symbolic": False,
                        "numeric": True,
                        "boundary": False,
                    },
                },
                structure={
                    "structural_coherence_score": 0.28,
                    "missing_inference_rate": 0.5,
                    "circular_reasoning": 0.0,
                    "step_redundancy_rate": 0.2,
                },
            )

            self.assertEqual(record["error_type"], "unit_mismatch")
            layer.log({"question": "q", "provider": "mini", **record})

            weight = layer.error_weight("unit_mismatch", concept_clusters=["units"])
            self.assertGreaterEqual(weight, 1.0)
            summary = layer.summarize()
            self.assertEqual(summary["total"], 1)


if __name__ == "__main__":
    unittest.main()
