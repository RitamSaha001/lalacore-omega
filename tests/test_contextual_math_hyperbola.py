import unittest

from core.math.contextual_math_solver import solve_contextual_math_question
from verification.verifier import verify_solution


class ContextualHyperbolaTests(unittest.TestCase):
    def test_hyperbola_eccentricity_and_asymptotes_contextual(self) -> None:
        out = solve_contextual_math_question(
            "For the hyperbola x^2/16 - y^2/9 = 1, find its eccentricity and equations of asymptotes."
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.get("verification_kind"), "composite")
        self.assertIn("5/4", str(out.get("answer")))
        self.assertIn("3/4", str(out.get("answer")))
        self.assertIn(r"\frac{5}{4}", str(out.get("expected_solution_text")))
        self.assertIn(r"c^2 = a^2 + b^2", str(out.get("expected_solution_text")))

    def test_hyperbola_chord_of_contact_contextual(self) -> None:
        out = solve_contextual_math_question(
            "Find the equation of the chord of contact of tangents drawn from the point (8, 4) to the hyperbola x^2/16 - y^2/9 = 1."
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.get("verification_kind"), "equation")
        self.assertIn("9*x", str(out.get("answer")))

    def test_hyperbola_touching_line_contextual(self) -> None:
        out = solve_contextual_math_question(
            "If the line y = m x + 2 touches the hyperbola x^2/25 - y^2/9 = 1, find the possible values of m."
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.get("verification_kind"), "expression_set")
        self.assertIn("sqrt(13)/5", str(out.get("answer")))

    def test_hyperbola_tangent_at_nested_sqrt_point_contextual(self) -> None:
        out = solve_contextual_math_question(
            "Find the equation of the tangent to the hyperbola x^2/9 - y^2/16 = 1 at the point (3*sqrt(5), 8)."
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.get("verification_kind"), "equation")
        self.assertIn("sqrt(5)", str(out.get("answer")))

    def test_hyperbola_tangent_intercept_combo_contextual(self) -> None:
        out = solve_contextual_math_question(
            "A tangent perpendicular to the line 2x + 3y = 6 is drawn to the hyperbola x^2/16 - y^2/9 = 1 in the first quadrant. If its intercepts on the x-axis and y-axis are a and b respectively, find |6a| + |5b|."
        )
        self.assertIsNotNone(out)
        self.assertIn("27*sqrt(3)", str(out.get("answer")))

    def test_hyperbola_equation_from_point_and_eccentricity(self) -> None:
        out = solve_contextual_math_question(
            "If the hyperbola x^2/a^2 - y^2/b^2 = 1 passes through (5, 4) and its eccentricity is 3/2, find its equation."
        )
        self.assertIsNotNone(out)
        self.assertIn("5*x**2 - 4*y**2 - 61 = 0", str(out.get("answer")))

    def test_hyperbola_normal_at_nested_sqrt_point_contextual(self) -> None:
        out = solve_contextual_math_question(
            "For the hyperbola x^2/25 - y^2/9 = 1, find the equation of the normal at the point (5*sqrt(13)/2, 9/2)."
        )
        self.assertIsNotNone(out)
        self.assertEqual(out.get("verification_kind"), "equation")
        self.assertIn("sqrt(13)", str(out.get("answer")))

    def test_verify_hyperbola_composite_answer(self) -> None:
        report = verify_solution(
            question="For the hyperbola x^2/16 - y^2/9 = 1, find its eccentricity and equations of asymptotes.",
            predicted_answer="e = 5/4; asymptotes: y = 3*x/4 and y = -3*x/4",
            difficulty="hard",
        )
        self.assertTrue(report["verified"])
        self.assertLess(report["risk_score"], 0.1)

    def test_verify_hyperbola_equivalent_equation(self) -> None:
        report = verify_solution(
            question="Find the equation of the chord of contact of tangents drawn from the point (8, 4) to the hyperbola x^2/16 - y^2/9 = 1.",
            predicted_answer="x/2 - 4*y/9 = 1",
            difficulty="hard",
        )
        self.assertTrue(report["verified"])
        self.assertLess(report["risk_score"], 0.1)

    def test_verify_hyperbola_no_real_tangent_text(self) -> None:
        report = verify_solution(
            question="Find the equations of the tangents to the hyperbola x^2/16 - y^2/9 = 1 that are parallel to the line 3x - 4y + 12 = 0.",
            predicted_answer="No real tangent exists because the given direction is asymptotic.",
            difficulty="hard",
        )
        self.assertTrue(report["verified"])
        self.assertLess(report["risk_score"], 0.1)

    def test_verify_hyperbola_equation_does_not_accept_unrelated_equations(self) -> None:
        report = verify_solution(
            question="If the hyperbola x^2/a^2 - y^2/b^2 = 1 passes through (5, 4) and its eccentricity is 3/2, find its equation.",
            predicted_answer="e = sqrt(a^2 + b^2)/a; asymptotes: y = b/a*x and y = -b/a*x.",
            difficulty="hard",
        )
        self.assertFalse(report["verified"])
        self.assertEqual(report.get("failure_reason"), "contextual_equation_mismatch")


if __name__ == "__main__":
    unittest.main()
