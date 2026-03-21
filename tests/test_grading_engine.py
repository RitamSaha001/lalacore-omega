import unittest

from grading_engine import evaluate_attempt


class GradingEngineTests(unittest.TestCase):
    def test_mcq_single_correct(self) -> None:
        question = {
            "question_id": "q1",
            "question_type": "MCQ_SINGLE",
            "options": ["2", "3", "4", "5"],
            "_correct_option": "C",
            "_correct_answers": ["C"],
            "_numerical_answer": "",
            "_solution_explanation": "2+2=4",
        }
        answer = {"question_id": "q1", "answer": "c"}
        out = evaluate_attempt(question, answer)
        self.assertTrue(out["is_correct"])
        self.assertEqual(out["score_awarded"], 4.0)
        self.assertEqual(out["penalty_applied"], 0.0)
        self.assertEqual(out["confidence"], 1.0)

    def test_mcq_single_wrong(self) -> None:
        question = {
            "question_id": "q2",
            "question_type": "MCQ_SINGLE",
            "options": ["A", "B", "C", "D"],
            "_correct_option": "A",
            "_correct_answers": ["A"],
            "_numerical_answer": "",
            "_solution_explanation": "A",
            "marks_correct": 4,
            "marks_incorrect": -1,
        }
        answer = {"question_id": "q2", "answer": "D"}
        out = evaluate_attempt(question, answer)
        self.assertFalse(out["is_correct"])
        self.assertEqual(out["score_awarded"], -1.0)
        self.assertEqual(out["confidence"], 0.0)

    def test_mcq_multi_exact(self) -> None:
        question = {
            "question_id": "q3",
            "question_type": "MCQ_MULTI",
            "options": ["2", "3", "4", "5"],
            "_correct_option": "",
            "_correct_answers": ["A", "B"],
            "_numerical_answer": "",
            "_solution_explanation": "2 and 3 are prime",
            "partial_marking": True,
        }
        answer = {"question_id": "q3", "answers": ["A", "B"]}
        out = evaluate_attempt(question, answer)
        self.assertTrue(out["is_correct"])
        self.assertEqual(out["score_awarded"], 4.0)
        self.assertEqual(out["confidence"], 1.0)
        self.assertEqual(out["grading_metadata"]["correct_count"], 2)
        self.assertEqual(out["grading_metadata"]["incorrect_count"], 0)
        self.assertEqual(out["grading_metadata"]["missing_count"], 0)

    def test_mcq_multi_partial_subset(self) -> None:
        question = {
            "question_id": "q4",
            "question_type": "MCQ_MULTI",
            "options": ["2", "3", "4", "5"],
            "_correct_option": "",
            "_correct_answers": ["A", "B"],
            "_numerical_answer": "",
            "_solution_explanation": "2 and 3 are prime",
            "partial_marking": True,
        }
        answer = {"question_id": "q4", "answers": ["A"]}
        out = evaluate_attempt(question, answer)
        self.assertFalse(out["is_correct"])
        self.assertEqual(out["score_awarded"], 2.0)
        self.assertEqual(out["confidence"], 0.5)
        self.assertEqual(out["grading_metadata"]["correct_count"], 1)
        self.assertEqual(out["grading_metadata"]["missing_count"], 1)

    def test_mcq_multi_wrong_selection_with_penalty(self) -> None:
        question = {
            "question_id": "q5",
            "question_type": "MCQ_MULTI",
            "options": ["2", "3", "4", "5"],
            "_correct_option": "",
            "_correct_answers": ["A", "B"],
            "_numerical_answer": "",
            "_solution_explanation": "2 and 3 are prime",
            "partial_marking": True,
            "strict_multi_mode": False,
        }
        answer = {"question_id": "q5", "answers": ["A", "D"]}
        out = evaluate_attempt(question, answer)
        self.assertFalse(out["is_correct"])
        self.assertLess(out["score_awarded"], 2.0)
        self.assertGreaterEqual(out["score_awarded"], -1.0)
        self.assertGreater(out["penalty_applied"], 0.0)
        self.assertEqual(out["grading_metadata"]["incorrect_count"], 1)

    def test_numerical_exact(self) -> None:
        question = {
            "question_id": "q6",
            "question_type": "NUMERICAL",
            "options": [],
            "_correct_option": "",
            "_correct_answers": [],
            "_numerical_answer": "3.141",
            "_solution_explanation": "pi approximation",
            "numerical_tolerance": 0.0,
        }
        answer = {"question_id": "q6", "answer": "3.141"}
        out = evaluate_attempt(question, answer)
        self.assertTrue(out["is_correct"])
        self.assertEqual(out["score_awarded"], 4.0)
        self.assertEqual(out["confidence"], 1.0)

    def test_numerical_within_tolerance(self) -> None:
        question = {
            "question_id": "q7",
            "question_type": "NUMERICAL",
            "options": [],
            "_correct_option": "",
            "_correct_answers": [],
            "_numerical_answer": "10",
            "_solution_explanation": "ten",
            "numerical_tolerance": 0.01,
        }
        answer = {"question_id": "q7", "answer": "10.008 m"}
        out = evaluate_attempt(question, answer)
        self.assertTrue(out["is_correct"])
        self.assertEqual(out["score_awarded"], 4.0)

    def test_numerical_outside_tolerance(self) -> None:
        question = {
            "question_id": "q8",
            "question_type": "NUMERICAL",
            "options": [],
            "_correct_option": "",
            "_correct_answers": [],
            "_numerical_answer": "10",
            "_solution_explanation": "ten",
            "numerical_tolerance": 0.001,
        }
        answer = {"question_id": "q8", "answer": "10.1"}
        out = evaluate_attempt(question, answer)
        self.assertFalse(out["is_correct"])
        self.assertEqual(out["score_awarded"], -1.0)
        self.assertEqual(out["confidence"], 0.0)


if __name__ == "__main__":
    unittest.main()
