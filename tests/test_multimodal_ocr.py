import unittest
from unittest.mock import patch

from core.multimodal.ocr_engine import OCREngine, OCRLayoutBlock


class MultimodalOCRTests(unittest.TestCase):
    def test_ocr_math_normalization_with_fallback(self):
        engine = OCREngine(provider_preference=("paddle",))
        payload = b"Integral: \\u222b sin^-1(x) dx = \\u221a(1-x^2)"
        out = engine.extract(payload, page_number=1, math_aware=True)

        self.assertIn("integral", out["math_normalized_text"].lower())
        self.assertIn("asin", out["math_normalized_text"].lower())
        self.assertTrue(len(out["layout_blocks"]) >= 1)
        self.assertTrue(0.0 <= float(out["confidence"]) <= 1.0)
        self.assertIn("clean_text", out)
        self.assertIn("lc_iie_questions", out)
        self.assertIn("lc_iie_metadata", out)
        self.assertTrue(isinstance(out["lc_iie_questions"], list))
        self.assertTrue(isinstance(out["lc_iie_metadata"], dict))

    def test_merge_text_candidates_removes_duplicates(self):
        engine = OCREngine(provider_preference=("heuristic",))
        merged = engine._merge_text_candidates(
            "Q1 Solve x+y=2\nOption A",
            [
                "Q1   Solve   x+y=2",
                "Option B",
                "Option A",
            ],
        )
        lines = [line.strip() for line in merged.splitlines() if line.strip()]
        self.assertIn("Q1 Solve x+y=2", lines)
        self.assertIn("Option A", lines)
        self.assertIn("Option B", lines)
        self.assertEqual(len(lines), 3)

    def test_map_blocks_to_page_scales_and_offsets(self):
        engine = OCREngine(provider_preference=("heuristic",))
        blocks = [
            OCRLayoutBlock(
                text="x^2",
                bbox=[20, 10, 60, 30],
                confidence=0.8,
                block_id=1,
                page_number=1,
            )
        ]
        mapped = engine._map_blocks_to_page(
            blocks,
            offset_x=100,
            offset_y=50,
            scale=2.0,
            page_number=1,
        )
        self.assertEqual(len(mapped), 1)
        self.assertEqual(mapped[0].bbox, [110, 55, 130, 65])
        self.assertEqual(mapped[0].page_number, 1)

    def test_should_try_handwriting_refine_on_low_quality(self):
        engine = OCREngine(provider_preference=("heuristic",))
        should = engine._should_try_handwriting_refine(
            b"random-bytes",
            "",
            [],
        )
        self.assertTrue(should)

    def test_handwriting_provider_preference_defaults(self):
        engine = OCREngine(provider_preference=("paddle",))
        pref = engine._handwriting_provider_preference()
        self.assertEqual(pref, ("tesseract_best", "tesseract", "heuristic"))

    def test_select_best_provider_uses_handwriting_hint(self):
        engine = OCREngine(provider_preference=("tesseract_best",))
        with patch.object(
            engine,
            "_run_tesseract_best",
            return_value=("sample text", [], "tesseract_hw_psm4"),
        ) as mocked_native, patch.object(
            engine,
            "_run_tesseract_best_cli",
            return_value=("sample text", [], "tesseract_cli_hw_psm4"),
        ) as mocked_cli, patch("core.multimodal.ocr_engine.shutil.which", return_value="/usr/bin/tesseract"):
            text, blocks, provider, model = engine._select_best_provider(
                b"image",
                b"image",
                page_number=1,
                handwritten_hint=True,
            )
        self.assertEqual(text, "sample text")
        self.assertEqual(blocks, [])
        self.assertIn(provider, ("tesseract", "tesseract_cli"))
        self.assertIn(model, ("tesseract_hw_psm4", "tesseract_cli_hw_psm4"))
        self.assertTrue(mocked_native.called or mocked_cli.called)

    def test_heuristic_ocr_rejects_binary_payload(self):
        engine = OCREngine(provider_preference=("heuristic",))
        text, blocks = engine._heuristic_ocr(b"\x89PNG\r\n\x1a\nbinary", page_number=1)
        self.assertEqual(text, "")
        self.assertEqual(blocks, [])

    def test_quality_score_penalizes_gibberish(self):
        engine = OCREngine(provider_preference=("heuristic",))
        score = engine._quality_score("IHDR IDAT xmpmeta pdfcpu %%&&**", [])
        self.assertLessEqual(score, 0.02)


if __name__ == "__main__":
    unittest.main()
