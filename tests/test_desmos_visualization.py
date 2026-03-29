import unittest

from core.visualization import DesmosGraphBuilder


class DesmosVisualizationTests(unittest.TestCase):
    def setUp(self):
        self.builder = DesmosGraphBuilder()

    def test_graph_keyword_generates_visualization_payload(self):
        payload = self.builder.build(
            question="Sketch the graph of y = x^2 - 4 and mark intersections.",
            profile={"subject": "math", "graph_like": True},
        )
        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload.get("type"), "desmos")
        self.assertTrue(payload.get("expressions"))
        self.assertIn("viewport", payload)
        self.assertIn("options", payload)

    def test_pure_algebra_does_not_trigger_graph_payload(self):
        payload = self.builder.build(
            question="Solve 2x + 3 = 7 for x.",
            profile={"subject": "math", "graph_like": False},
        )
        self.assertIsNone(payload)

    def test_vertical_line_and_circle_are_supported(self):
        payload = self.builder.build(
            question="Plot x = 3 and x^2 + y^2 = 25 on same graph.",
            profile={"subject": "coordinate geometry", "graph_like": True},
        )
        self.assertIsNotNone(payload)
        assert payload is not None
        exprs = payload.get("expressions", [])
        latex = [str(e.get("latex", "")) for e in exprs]
        self.assertTrue(any(x.startswith("x=3") for x in latex))
        self.assertTrue(any("x^2+y^2=25" in x.replace(" ", "") for x in latex))

    def test_viewport_is_clamped_for_large_growth(self):
        payload = self.builder.build(
            question="Plot the graph of y = e^(10x).",
            profile={"subject": "calculus", "graph_like": True},
        )
        self.assertIsNotNone(payload)
        assert payload is not None
        viewport = payload["viewport"]
        self.assertLessEqual(abs(float(viewport["xmin"])), 1000.0)
        self.assertLessEqual(abs(float(viewport["xmax"])), 1000.0)
        self.assertLessEqual(abs(float(viewport["ymin"])), 1000.0)
        self.assertLessEqual(abs(float(viewport["ymax"])), 1000.0)

    def test_prefixed_hyperbola_prompt_still_generates_visualization(self):
        payload = self.builder.build(
            question=(
                "JEE Advanced level Hyperbola question: For the hyperbola "
                "x^2/16 - y^2/9 = 1, find its eccentricity and equations of "
                "asymptotes. Give full step-by-step solution."
            ),
            profile={"subject": "math", "graph_like": False},
        )
        self.assertIsNotNone(payload)
        assert payload is not None
        latex = [
            str(item.get("latex", "")).replace(" ", "")
            for item in payload.get("expressions", [])
        ]
        self.assertTrue(any("x^2/16-y^2/9=1" in expr for expr in latex))


if __name__ == "__main__":
    unittest.main()
