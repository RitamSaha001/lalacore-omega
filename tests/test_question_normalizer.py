import unittest

from services.question_normalizer import QuestionNormalizer


class QuestionNormalizerTests(unittest.TestCase):
    def test_normalize_removes_options_and_builds_query(self) -> None:
        normalizer = QuestionNormalizer()
        out = normalizer.normalize(
            "Find value of ∫₀¹ x dx\n(A) 0\n(B) 1/2\n(C) 1\n(D) 2"
        )
        self.assertTrue(out.get("options_removed"))
        self.assertIn("integral", str(out.get("search_query")))
        self.assertNotIn("(A)", str(out.get("stem")))

    def test_empty_input(self) -> None:
        normalizer = QuestionNormalizer()
        out = normalizer.normalize("   ")
        self.assertEqual(out.get("search_query"), "")
        self.assertEqual(out.get("stem"), "")

    def test_hyperbola_prompt_keeps_semantic_and_equation_queries(self) -> None:
        normalizer = QuestionNormalizer()
        out = normalizer.normalize(
            "JEE Advanced level Hyperbola question: For the hyperbola x^2/16 - y^2/9 = 1, find its eccentricity and asymptotes. Give full step-by-step solution. Include cited sources if available."
        )
        self.assertIn("hyperbola", str(out.get("semantic_query")))
        self.assertIn("eccentricity", str(out.get("semantic_query")))
        self.assertIn("asymptotes", str(out.get("semantic_query")))
        self.assertEqual(
            out.get("equation_query"),
            "x^2/16 - y^2/9 = 1",
        )
        self.assertNotIn("include cited sources", str(out.get("search_query")))
        self.assertNotIn("jee advanced", str(out.get("search_query")))
        self.assertNotIn("step-by-step", str(out.get("search_query")))


if __name__ == "__main__":
    unittest.main()
