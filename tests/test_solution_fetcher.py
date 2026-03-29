import unittest

from services.solution_fetcher import SolutionFetcher


class SolutionFetcherTests(unittest.TestCase):
    def test_pick_best_prefers_more_topical_row(self) -> None:
        fetcher = SolutionFetcher()
        rows = [
            {
                "ok": True,
                "source_url": "https://example.com/1",
                "source": "web",
                "title": "Generic conic sections note",
                "snippet": "eccentricity and asymptotes",
                "query_variant": "semantic",
                "similarity": 0.92,
                "answer": "",
                "hint": "General note",
                "solution_text": "Hyperbola overview without the target equation.",
                "formulas": [],
                "confidence": 0.62,
            },
            {
                "ok": True,
                "source_url": "https://example.com/2",
                "source": "web",
                "title": "How to find the eccentricity of the given hyperbola?",
                "snippet": "x^2/16 - y^2/9 = 1 with asymptotes",
                "query_variant": "semantic_equation",
                "similarity": 0.81,
                "answer": "5/4",
                "hint": "Use e^2 = 1 + b^2/a^2",
                "solution_text": "For x^2/16 - y^2/9 = 1, a^2 = 16 and b^2 = 9, so e = 5/4 and asymptotes are y = ±3x/4.",
                "formulas": ["x^2/16 - y^2/9 = 1"],
                "confidence": 0.55,
            },
        ]
        best = fetcher._pick_best(
            rows,
            search_payload={
                "query_signals": {
                    "stem": "For the hyperbola x^2/16 - y^2/9 = 1, find its eccentricity and equations of asymptotes.",
                    "semantic_query": "hyperbola eccentricity asymptote asymptotes",
                    "equation_query": "x^2/16 - y^2/9 = 1",
                }
            },
        )
        self.assertIsNotNone(best)
        self.assertEqual(best.get("source_url"), "https://example.com/2")


if __name__ == "__main__":
    unittest.main()
