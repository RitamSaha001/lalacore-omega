import unittest

from concept_graph_engine import ConceptGraphEngine
from engine.got_engine import GraphOfThoughtEngine
from tools.tool_router import ToolRouter


class ToolRouterTests(unittest.TestCase):
    def test_symbolic_solver_quadratic(self):
        router = ToolRouter()
        out = router.solve_equation("x^2 - 5*x + 6 = 0")
        self.assertTrue(out.get("ok"))
        joined = ",".join(out.get("output", []))
        self.assertIn("2", joined)
        self.assertIn("3", joined)

    def test_integral_solver_definite(self):
        router = ToolRouter()
        out = router.integral_solver("integral from 0 to 1 of x dx")
        self.assertTrue(out.get("ok"))
        self.assertIn("1/2", str(out.get("output")))


class ConceptGraphEngineTests(unittest.TestCase):
    def test_traverse_returns_ranked_concepts(self):
        engine = ConceptGraphEngine()
        rows = engine.traverse("Solve quadratic roots with domain constraints", subject="math", top_k=5)
        self.assertGreaterEqual(len(rows), 1)
        self.assertIn("title", rows[0])
        self.assertIn("score", rows[0])


class GraphOfThoughtEngineTests(unittest.IsolatedAsyncioTestCase):
    async def test_got_engine_builds_graph_with_tool_and_synthesis(self):
        engine = GraphOfThoughtEngine(max_nodes=20)
        out = await engine.run(
            question="Solve x^2 - 5x + 6 = 0",
            profile={"subject": "math", "difficulty": "easy", "numeric": True},
            web_retrieval={"matches": [], "solution": {}},
            allow_provider_reasoning=False,
            timeout_s=0.9,
        )
        self.assertEqual(out.get("status"), "ok")
        self.assertGreaterEqual(len(out.get("nodes", [])), 1)
        self.assertIn("telemetry", out)
        self.assertIn("context_block", out)
        self.assertIn("synthesis", ",".join(str(row.get("type")) for row in out.get("nodes", [])))

    async def test_got_engine_wires_ocr_into_diagram_parsing(self):
        engine = GraphOfThoughtEngine(max_nodes=20)
        out = await engine.run(
            question="Find electric field at center.",
            profile={"subject": "physics", "difficulty": "hard", "numeric": True},
            web_retrieval={"matches": [], "solution": {}},
            ocr_data={
                "clean_text": "Charges +q and -q at vertices A B C D of a square.",
                "layout_blocks": [
                    {"text": "A +q", "bbox": [0, 0, 20, 20]},
                    {"text": "B -q", "bbox": [20, 0, 40, 20]},
                    {"text": "C +q", "bbox": [20, 20, 40, 40]},
                    {"text": "D -q", "bbox": [0, 20, 20, 40]},
                ],
            },
            allow_provider_reasoning=False,
            timeout_s=1.0,
        )
        self.assertEqual(out.get("status"), "ok")
        diagram = dict(out.get("diagram") or {})
        self.assertIn(str(diagram.get("diagram_type", "")), {"electrostatics", "geometry"})
        self.assertGreaterEqual(len(diagram.get("objects", []) or []), 1)


if __name__ == "__main__":
    unittest.main()
