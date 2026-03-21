import base64
import asyncio
import unittest
from unittest.mock import patch

from core.multimodal.ocr_engine import OCREngine
from core.multimodal.pdf_processor import PDFProcessor


class MultimodalPDFTests(unittest.TestCase):
    def test_pdf_ingestion_fallback_pipeline(self):
        processor = PDFProcessor(ocr_engine=OCREngine(provider_preference=("paddle",)))
        fake_pdf = b"%PDF-1.7\nQ1: Solve x+y=2\nA | B | C\n1 | 2 | 3\n"

        out = asyncio.run(processor.process(fake_pdf))

        self.assertGreaterEqual(out["page_count"], 1)
        self.assertIn("merged_text", out)
        self.assertTrue(isinstance(out["pages"], list))
        self.assertTrue(0.0 <= float(out["overall_confidence"]) <= 1.0)
        self.assertGreaterEqual(len(out["equations"]), 1)
        self.assertGreaterEqual(len(out["tables"]), 1)
        self.assertIn("lc_iie_questions", out)
        self.assertTrue(isinstance(out["lc_iie_questions"], list))
        self.assertIn("retry_report", out)

    def test_sips_fallback_not_used_outside_macos(self):
        processor = PDFProcessor(ocr_engine=OCREngine(provider_preference=("paddle",)))
        with patch("core.multimodal.pdf_processor.sys.platform", "linux"), patch(
            "core.multimodal.pdf_processor.subprocess.run"
        ) as run_mock:
            pages = processor._convert_pdf_to_images_with_sips(b"%PDF-1.4\n")
        self.assertEqual(pages, [])
        run_mock.assert_not_called()

    def test_sips_fallback_skips_when_binary_missing(self):
        processor = PDFProcessor(ocr_engine=OCREngine(provider_preference=("paddle",)))
        with patch("core.multimodal.pdf_processor.sys.platform", "darwin"), patch(
            "core.multimodal.pdf_processor.shutil.which",
            return_value=None,
        ), patch("core.multimodal.pdf_processor.subprocess.run") as run_mock:
            pages = processor._convert_pdf_to_images_with_sips(b"%PDF-1.4\n")
        self.assertEqual(pages, [])
        run_mock.assert_not_called()

    def test_sips_fallback_prefers_split_pages_when_available(self):
        processor = PDFProcessor(ocr_engine=OCREngine(provider_preference=("paddle",)))
        with patch("core.multimodal.pdf_processor.sys.platform", "darwin"), patch(
            "core.multimodal.pdf_processor.shutil.which",
            return_value="/usr/bin/sips",
        ), patch.object(
            processor,
            "_convert_pdf_to_images_with_sips_split",
            return_value=[b"p1", b"p2"],
        ) as split_mock, patch("core.multimodal.pdf_processor.subprocess.run") as run_mock:
            pages = processor._convert_pdf_to_images_with_sips(b"%PDF-1.4\n")
        self.assertEqual(pages, [b"p1", b"p2"])
        split_mock.assert_called_once()
        run_mock.assert_not_called()

    def test_image_bytes_are_accepted_as_single_page_input(self):
        processor = PDFProcessor(ocr_engine=OCREngine(provider_preference=("heuristic",)))
        tiny_png = base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+X6O0AAAAASUVORK5CYII="
        )
        out = asyncio.run(processor.process(tiny_png))
        self.assertGreaterEqual(out.get("page_count", 0), 1)
        self.assertIn("pages", out)

    def test_pdftoppm_fallback_is_used_when_available(self):
        processor = PDFProcessor(ocr_engine=OCREngine(provider_preference=("heuristic",)))
        with patch("core.multimodal.pdf_processor.convert_from_bytes", None), patch.object(
            processor,
            "_convert_pdf_to_images_with_pdftoppm",
            return_value=[b"p1", b"p2"],
        ) as pdftoppm_mock, patch.object(
            processor,
            "_convert_pdf_to_images_with_mutool",
            return_value=[],
        ) as mutool_mock, patch.object(
            processor,
            "_convert_pdf_to_images_with_sips",
            return_value=[],
        ) as sips_mock:
            pages = processor._convert_pdf_to_images(b"%PDF-1.7\nmock")
        self.assertEqual(pages, [b"p1", b"p2"])
        pdftoppm_mock.assert_called_once()
        mutool_mock.assert_not_called()
        sips_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
