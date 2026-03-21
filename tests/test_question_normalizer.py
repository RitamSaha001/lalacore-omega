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


if __name__ == "__main__":
    unittest.main()
