import io
import unittest

from fastapi.testclient import TestClient

from import_worker.main import create_app


class _FakeService:
    def __init__(self) -> None:
        self.calls = []

    async def _lc9_parse_import_questions(self, payload):
        self.calls.append(payload)
        return {
            "ok": True,
            "status": "SUCCESS",
            "questions": [{"question_id": "imp_q_1"}],
            "count": 1,
        }


class _FakeOcrEngine:
    def __init__(self) -> None:
        self.calls = []

    async def extract_async(self, blob, *, page_number=1, math_aware=True, optional_web_snippets=None):
        self.calls.append(
            {
                "blob": blob,
                "page_number": page_number,
                "math_aware": math_aware,
                "optional_web_snippets": list(optional_web_snippets or ()),
            }
        )
        return {
            "raw_text": "x^2 + 2x + 1 = 0",
            "clean_text": "x^2 + 2x + 1 = 0",
            "math_normalized_text": "x^2 + 2x + 1 = 0",
            "confidence": 0.91,
            "layout_blocks": [],
            "bounding_boxes": [],
            "provider": "worker-test",
            "ocr_model": "worker-test-model",
            "lc_iie_questions": [],
            "lc_iie_metadata": {},
        }


class _FakePdfProcessor:
    def __init__(self) -> None:
        self.calls = []

    async def process(self, blob, *, optional_web_snippets=None):
        self.calls.append(
            {
                "blob": blob,
                "optional_web_snippets": list(optional_web_snippets or ()),
            }
        )
        return {
            "page_count": 2,
            "pages": [{"page_number": 1, "confidence": 0.82}],
            "merged_text": "Q1. Evaluate the integral.",
            "tables": [],
            "equations": [],
            "overall_confidence": 0.82,
            "retry_report": {},
            "lc_iie_questions": [],
        }


class ImportWorkerTests(unittest.TestCase):
    def test_parse_pdf_reuses_existing_import_parser(self) -> None:
        service = _FakeService()
        client = TestClient(create_app(service=service))

        response = client.post(
            "/import/parse-pdf",
            data={
                "teacher_id": "teacher_1",
                "subject": "Mathematics",
                "chapter": "Definite Integration",
                "difficulty": "JEE Main",
            },
            files={
                "file": ("paper.pdf", io.BytesIO(b"%PDF-1.4 test"), "application/pdf")
            },
        )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("status"), "SUCCESS")
        self.assertEqual(len(service.calls), 1)
        payload = service.calls[0]
        self.assertEqual(payload["meta"]["teacher_id"], "teacher_1")
        self.assertEqual(payload["meta"]["subject"], "Mathematics")
        self.assertEqual(payload["meta"]["chapter"], "Definite Integration")
        self.assertEqual(payload["meta"]["difficulty"], "JEE Main")
        self.assertEqual(payload["mime_type"], "application/pdf")
        self.assertTrue(payload["file_path"].endswith(".pdf"))

    def test_parse_pdf_enforces_bearer_token_when_configured(self) -> None:
        client = TestClient(create_app(service=_FakeService(), worker_token="secret"))

        response = client.post(
            "/import/parse-pdf",
            files={
                "file": ("paper.pdf", io.BytesIO(b"%PDF-1.4 test"), "application/pdf")
            },
        )

        self.assertEqual(response.status_code, 401)

    def test_ocr_frame_returns_ocr_payload_and_text(self) -> None:
        ocr_engine = _FakeOcrEngine()
        client = TestClient(create_app(service=_FakeService(), ocr_engine=ocr_engine))

        response = client.post(
            "/ocr/frame",
            json={"image_base64": "aGVsbG8=", "page_number": 2, "math_aware": True},
        )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("status"), "SUCCESS")
        self.assertEqual(body.get("text"), "x^2 + 2x + 1 = 0")
        self.assertEqual(len(ocr_engine.calls), 1)
        self.assertEqual(ocr_engine.calls[0]["page_number"], 2)

    def test_ocr_pdf_returns_pdf_payload(self) -> None:
        pdf_processor = _FakePdfProcessor()
        client = TestClient(
            create_app(service=_FakeService(), pdf_processor=pdf_processor)
        )

        response = client.post(
            "/ocr/pdf",
            data={
                "optional_web_snippets_json": '[{"title":"Snippet","url":"https://example.com"}]'
            },
            files={
                "file": ("paper.pdf", io.BytesIO(b"%PDF-1.4 test"), "application/pdf")
            },
        )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("status"), "SUCCESS")
        self.assertEqual(body.get("text"), "Q1. Evaluate the integral.")
        self.assertEqual(len(pdf_processor.calls), 1)
        self.assertEqual(
            pdf_processor.calls[0]["optional_web_snippets"][0]["title"],
            "Snippet",
        )


if __name__ == "__main__":
    unittest.main()
