import asyncio
import unittest

from core.multimodal.diagram_parser import DiagramParser
from core.multimodal.vision_router import VisionRouter
from vision.diagram_parser import DiagramParser as ReasoningDiagramParser


class DiagramParserTests(unittest.TestCase):
    def test_geometry_extraction(self):
        parser = DiagramParser()
        text = "In triangle ABC, line AB is perpendicular to AC and angle ABC = 60. Circle O passes through A and B."
        out = parser.parse(text)

        self.assertTrue(out["is_geometry"])
        self.assertIn("A", out["points"])
        self.assertGreaterEqual(len(out["segments"]), 1)
        self.assertGreaterEqual(len(out["angles"]), 1)

    def test_reasoning_parser_uses_ocr_payload_for_electrostatics(self):
        parser = ReasoningDiagramParser()
        vision_payload = {
            "ocr": {
                "clean_text": "Charges +q, -q, +q, -q are placed at vertices A B C D of a square.",
                "layout_blocks": [
                    {"text": "A +q", "bbox": [0, 0, 20, 20]},
                    {"text": "B -q", "bbox": [20, 0, 40, 20]},
                    {"text": "C +q", "bbox": [20, 20, 40, 40]},
                    {"text": "D -q", "bbox": [0, 20, 20, 40]},
                ],
            }
        }
        out = parser.parse("Find field at center.", vision_payload)
        self.assertEqual(out.get("diagram_type"), "electrostatics")
        self.assertGreaterEqual(len(out.get("objects", [])), 4)
        self.assertGreaterEqual(len(out.get("connections", [])), 1)
        self.assertGreaterEqual(float(out.get("confidence", 0.0)), 0.5)


class VisionRouterTests(unittest.TestCase):
    def test_image_math_question_analysis(self):
        router = VisionRouter()
        image_like_bytes = (
            b"Triangle ABC with angle ABC = 60 and line AB perpendicular AC. "
            b"Find x if x+2=5"
        )
        out = asyncio.run(router.analyze(image_like_bytes, {"subject": "math"}))

        self.assertTrue(out["winner_provider"])
        self.assertIn("provider_comparison", out)
        self.assertIn("geometry_objects", out)
        self.assertTrue(0.0 <= float(out["confidence"]) <= 1.0)


if __name__ == "__main__":
    unittest.main()
