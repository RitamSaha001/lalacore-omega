import unittest

from core.lalacore_x.plausibility_checker import check_answer_plausibility


class PlausibilityNumericZeroTests(unittest.TestCase):
    def test_zero_is_plausible_for_discrete_coefficient_question(self):
        report = check_answer_plausibility(
            question_text="Find the coefficient of x^7 in (1 + x)^14 + (1 - x)^14.",
            final_answer="0",
            metadata={"numeric_expected": True, "observed_type": "numeric"},
        )
        self.assertTrue(bool(report.get("plausible", False)))
        self.assertNotIn("too_short", list(report.get("issues", [])))

    def test_zero_is_plausible_for_constant_term_question(self):
        report = check_answer_plausibility(
            question_text="Find the constant term in (x^4 - 2/x)^7.",
            final_answer="0",
            metadata={"numeric_expected": True, "observed_type": "numeric"},
        )
        self.assertTrue(bool(report.get("plausible", False)))
        self.assertNotIn("too_short", list(report.get("issues", [])))


if __name__ == "__main__":
    unittest.main()
