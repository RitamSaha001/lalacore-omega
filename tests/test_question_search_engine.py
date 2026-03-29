import unittest
from unittest.mock import AsyncMock, patch

from services.question_search_engine import QuestionSearchEngine


class QuestionSearchEngineTests(unittest.IsolatedAsyncioTestCase):
    def test_build_query_variants_adds_formula_and_topic_equation_probes(self) -> None:
        engine = QuestionSearchEngine()
        variants = engine._build_query_variants(
            {
                "search_query": "the eccentricity and asymptotes of the hyperbola x squared /16 - y squared /9 = 1",
                "partial_query": "the eccentricity and asymptotes of the hyperbola x squared /16 - y squared /9 = 1",
                "math_only_query": "x^2/16 - y^2/9 = 1",
                "semantic_query": "hyperbola eccentricity asymptote asymptotes",
                "equation_query": "x^2/16 - y^2/9 = 1",
                "stem": "Find the eccentricity and asymptotes of the hyperbola x^2/16 - y^2/9 = 1.",
            }
        )
        queries = [str(item.get("query") or "") for item in variants]
        self.assertIn("hyperbola x^2/16 - y^2/9 = 1", queries)
        self.assertIn("hyperbola eccentricity asymptote asymptotes formula", queries)
        self.assertIn(
            "hyperbola eccentricity asymptote asymptotes hyperbola eccentricity asymptote asymptotes formula",
            queries,
        )

    async def test_search_caps_upstream_rows_for_heavier_requests(self) -> None:
        engine = QuestionSearchEngine()
        normalized = {
            "search_query": "hyperbola eccentricity asymptotes",
            "partial_query": "hyperbola eccentricity asymptotes",
            "math_only_query": "x^2/16 - y^2/9 = 1",
            "semantic_query": "hyperbola eccentricity asymptotes",
            "equation_query": "x^2/16 - y^2/9 = 1",
            "stem": "For the hyperbola x^2/16 - y^2/9 = 1, find its eccentricity and asymptotes.",
        }
        seen: dict[str, int] = {}

        async def fake_run_query_variants(
            *,
            variants,
            timeout_s,
            max_rows,
            search_scope,
        ):
            seen["max_rows"] = int(max_rows)
            return []

        with patch.object(
            engine._cache,
            "get_cached_search",
            AsyncMock(return_value=(None, False)),
        ), patch.object(
            engine._cache,
            "put_cached_search",
            AsyncMock(return_value=None),
        ), patch.object(
            engine,
            "_run_query_variants",
            side_effect=fake_run_query_variants,
        ):
            out = await engine.search(
                normalized,
                max_matches=18,
                query_timeout_s=4.8,
                search_scope="general_ai",
            )

        self.assertEqual(out.get("matches"), [])
        self.assertEqual(seen.get("max_rows"), 12)

    async def test_search_does_not_cache_empty_results(self) -> None:
        engine = QuestionSearchEngine()
        normalized = {
            "search_query": "hyperbola eccentricity asymptotes",
            "semantic_query": "hyperbola eccentricity asymptotes",
            "equation_query": "x^2/16 - y^2/9 = 1",
            "stem": "For the hyperbola x^2/16 - y^2/9 = 1, find its eccentricity and asymptotes.",
        }
        put_cached = AsyncMock(return_value=None)

        with patch.object(
            engine._cache,
            "get_cached_search",
            AsyncMock(return_value=(None, False)),
        ), patch.object(
            engine._cache,
            "put_cached_search",
            put_cached,
        ), patch.object(
            engine,
            "_run_query_variants",
            AsyncMock(return_value=[]),
        ):
            out = await engine.search(
                normalized,
                max_matches=5,
                query_timeout_s=2.0,
                search_scope="general_ai",
            )

        self.assertEqual(out.get("matches"), [])
        put_cached.assert_not_called()

    async def test_search_retries_evidence_queries_when_first_pass_is_empty(self) -> None:
        engine = QuestionSearchEngine()
        normalized = {
            "search_query": "hyperbola eccentricity asymptotes",
            "semantic_query": "hyperbola eccentricity asymptotes",
            "equation_query": "x^2/16 - y^2/9 = 1",
            "stem": "For the hyperbola x^2/16 - y^2/9 = 1, find its eccentricity and asymptotes.",
        }
        calls: list[dict[str, object]] = []

        async def fake_run_query_variants(
            *,
            variants,
            timeout_s,
            max_rows,
            search_scope,
        ):
            calls.append(
                {
                    "timeout_s": float(timeout_s),
                    "search_scope": str(search_scope),
                    "kinds": [str(item.get("kind") or "") for item in variants],
                }
            )
            if len(calls) == 1:
                return []
            return [
                {
                    "query_variant": "semantic",
                    "query": "hyperbola eccentricity asymptotes",
                    "title": "Eccentricity of a hyperbola",
                    "url": "https://math.stackexchange.com/questions/1/example",
                    "fetch_url": "",
                    "snippet": "conic sections answer",
                }
            ]

        with patch.object(
            engine._cache,
            "get_cached_search",
            AsyncMock(return_value=(None, False)),
        ), patch.object(
            engine._cache,
            "put_cached_search",
            AsyncMock(return_value=None),
        ), patch.object(
            engine,
            "_run_query_variants",
            side_effect=fake_run_query_variants,
        ):
            out = await engine.search(
                normalized,
                max_matches=5,
                query_timeout_s=2.0,
                search_scope="general_ai",
            )

        self.assertGreaterEqual(len(calls), 2)
        self.assertEqual(calls[1]["search_scope"], "general_ai")
        self.assertTrue(
            any(
                kind in {"topic_equation", "equation", "formula", "semantic_formula"}
                for kind in calls[1]["kinds"]
            )
        )
        self.assertEqual(len(out.get("matches") or []), 1)


if __name__ == "__main__":
    unittest.main()
