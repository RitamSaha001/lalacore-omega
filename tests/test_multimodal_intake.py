import base64
import tempfile
import unittest
from pathlib import Path

from core.multimodal.intake import MultimodalIntake


class MultimodalIntakeTests(unittest.TestCase):
    def test_auto_detection_and_mixed_normalization(self):
        intake = MultimodalIntake(max_input_bytes=1024 * 1024)

        self.assertEqual(intake.detect_type("What is 2+2?", "auto"), "text")
        self.assertEqual(intake.detect_type(b"%PDF-1.4 sample", "auto"), "pdf")

        with tempfile.TemporaryDirectory() as tmp:
            image_path = Path(tmp) / "sample.png"
            image_path.write_bytes(b"\x89PNG\r\n\x1a\n")
            self.assertEqual(intake.detect_type(str(image_path), "auto"), "image")

        mixed = intake.normalize({"text": "Solve x+1=2", "image": b"fake image bytes"}, "auto")
        self.assertEqual(mixed.input_type, "mixed")
        self.assertEqual(mixed.text, "Solve x+1=2")
        self.assertIsNotNone(mixed.image_bytes)

    def test_base64_and_data_url_image_payloads(self):
        intake = MultimodalIntake(max_input_bytes=1024 * 1024)
        png_blob = b"\x89PNG\r\n\x1a\n" + b"\x00" * 40
        raw_b64 = base64.b64encode(png_blob).decode("ascii")
        data_url = f"data:image/png;base64,{raw_b64}"

        mixed_raw = intake.normalize({"text": "Solve", "image": raw_b64}, "auto")
        self.assertEqual(mixed_raw.input_type, "mixed")
        self.assertEqual(mixed_raw.image_bytes, png_blob)

        mixed_data_url = intake.normalize({"text": "Solve", "image": data_url}, "auto")
        self.assertEqual(mixed_data_url.input_type, "mixed")
        self.assertEqual(mixed_data_url.image_bytes, png_blob)


if __name__ == "__main__":
    unittest.main()
