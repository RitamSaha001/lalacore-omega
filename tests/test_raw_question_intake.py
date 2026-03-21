import tempfile
import unittest
from pathlib import Path

from core.automation.adaptive_question_classifier import AdaptiveQuestionClassifier
from core.automation.feeder_engine import FeederEngine
from core.automation.raw_question_intake import RawQuestionIntakeSystem


class RawQuestionIntakeTests(unittest.TestCase):
    def test_adaptive_classifier_on_raw_questions(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            classifier = AdaptiveQuestionClassifier(
                feeder_cases_path=str(root / "cases.jsonl"),
                replay_cases_path=str(root / "replay.jsonl"),
                queue_path=str(root / "queue.jsonl"),
                state_path=str(root / "state.json"),
            )

            q1 = "If the coefficients of x^7 and x^8 in the expansion of (2 + x/3)^n are equal, then the value of n is"
            q2 = "The number of four-digit numbers strictly greater than 4321 that can be formed using digits 0,1,2,3,4,5 is"

            out1 = classifier.classify(q1)
            out2 = classifier.classify(q2)

            self.assertEqual(out1.subject, "math")
            self.assertEqual(out2.subject, "math")
            self.assertIn("algebra", out1.concept_cluster)
            self.assertIn("combinatorics", out2.concept_cluster)
            self.assertIn(out1.difficulty, {"easy", "medium", "hard"})
            self.assertIn(out2.difficulty, {"easy", "medium", "hard"})

    def test_raw_intake_enqueues_feeder_compatible_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            feeder = FeederEngine(
                queue_path=str(root / "queue.jsonl"),
                training_cases_path=str(root / "cases.jsonl"),
                replay_cases_path=str(root / "replay.jsonl"),
            )
            classifier = AdaptiveQuestionClassifier(
                feeder_cases_path=str(root / "cases.jsonl"),
                replay_cases_path=str(root / "replay.jsonl"),
                queue_path=str(root / "queue.jsonl"),
                state_path=str(root / "state.json"),
            )
            system = RawQuestionIntakeSystem(feeder=feeder, classifier=classifier)

            raw_questions = [
                "If the coefficients of x^7 and x^8 in the expansion of (2 + x/3)^n are equal, then the value of n is",
                "The number of four-digit numbers strictly greater than 4321 that can be formed using digits 0,1,2,3,4,5 is",
            ]

            classified = system.classify_raw_questions(raw_questions, default_source_tag="unit_test")
            enqueue = system.enqueue_classified(classified)
            status = system.status(limit=10)

            self.assertEqual(enqueue["requested"], 2)
            self.assertEqual(enqueue["added"], 2)
            self.assertEqual(status["counts"]["Pending"], 2)
            self.assertEqual(status["counts"]["Completed"], 0)
            self.assertEqual(status["counts"]["Failed"], 0)


if __name__ == "__main__":
    unittest.main()
