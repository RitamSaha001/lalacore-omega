from __future__ import annotations

import base64
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, File, Form, Header, HTTPException, UploadFile

from app.data.local_app_data_service import LocalAppDataService
from core.multimodal.ocr_engine import OCREngine
from core.multimodal.pdf_processor import PDFProcessor


def create_app(
    *,
    service: LocalAppDataService | None = None,
    worker_token: str | None = None,
    ocr_engine: OCREngine | None = None,
    pdf_processor: PDFProcessor | None = None,
) -> FastAPI:
    app = FastAPI(title="LalaCore Import Worker")
    svc = service or LocalAppDataService()
    image_ocr = ocr_engine or OCREngine(enable_remote_worker=False)
    pdf_ocr = pdf_processor or PDFProcessor(
        ocr_engine=image_ocr,
        enable_remote_worker=False,
    )
    expected_token = (
        worker_token
        if worker_token is not None
        else os.getenv("IMPORT_WORKER_TOKEN", "").strip()
    )

    def _authorize(authorization: str | None) -> None:
        if expected_token and authorization != f"Bearer {expected_token}":
            raise HTTPException(status_code=401, detail="Unauthorized")

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {"ok": True, "status": "healthy"}

    @app.post("/ocr/frame")
    async def ocr_frame(
        payload: dict[str, Any] = Body(default_factory=dict),
        authorization: str | None = Header(default=None),
    ) -> dict[str, Any]:
        _authorize(authorization)
        raw_image = str(
            payload.get("image_base64")
            or payload.get("base64_image")
            or payload.get("image")
            or ""
        ).strip()
        if not raw_image:
            raise HTTPException(status_code=400, detail="image_base64 is required")
        if raw_image.startswith("data:") and "," in raw_image:
            raw_image = raw_image.split(",", 1)[1]
        try:
            image_bytes = base64.b64decode(raw_image, validate=False)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"invalid_image_base64:{exc}") from exc
        ocr_data = await image_ocr.extract_async(
            image_bytes,
            page_number=max(1, int(payload.get("page_number") or 1)),
            math_aware=bool(payload.get("math_aware", True)),
            optional_web_snippets=[
                dict(x)
                for x in (payload.get("optional_web_snippets") or [])
                if isinstance(x, dict)
            ],
        )
        text = (
            str(ocr_data.get("clean_text") or "").strip()
            or str(ocr_data.get("math_normalized_text") or "").strip()
            or str(ocr_data.get("raw_text") or "").strip()
        )
        return {
            "ok": True,
            "status": "SUCCESS",
            "text": text,
            "ocr_data": ocr_data,
        }

    @app.post("/ocr/pdf")
    async def ocr_pdf(
        file: UploadFile = File(...),
        optional_web_snippets_json: str = Form(""),
        authorization: str | None = Header(default=None),
    ) -> dict[str, Any]:
        _authorize(authorization)
        blob = await file.read()
        snippets: list[dict[str, Any]] = []
        if optional_web_snippets_json.strip():
            try:
                decoded = json.loads(optional_web_snippets_json)
                if isinstance(decoded, list):
                    snippets = [dict(x) for x in decoded if isinstance(x, dict)]
            except Exception:
                snippets = []
        pdf_data = await pdf_ocr.process(
            blob,
            optional_web_snippets=snippets,
        )
        return {
            "ok": True,
            "status": "SUCCESS",
            "text": str(pdf_data.get("merged_text") or "").strip(),
            "pdf_data": pdf_data,
        }

    @app.post("/import/parse-pdf")
    async def parse_pdf(
        file: UploadFile = File(...),
        teacher_id: str = Form("teacher_import"),
        subject: str = Form("Mathematics"),
        chapter: str = Form(""),
        difficulty: str = Form("JEE Main"),
        authorization: str | None = Header(default=None),
    ) -> dict[str, Any]:
        _authorize(authorization)

        suffix = Path(file.filename or "paper.pdf").suffix or ".pdf"
        temp_path = ""
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp:
                shutil.copyfileobj(file.file, temp)
                temp_path = temp.name
            return await svc._lc9_parse_import_questions(
                {
                    "file_path": temp_path,
                    "mime_type": file.content_type or "application/pdf",
                    "meta": {
                        "teacher_id": teacher_id,
                        "subject": subject,
                        "chapter": chapter,
                        "difficulty": difficulty,
                    },
                }
            )
        finally:
            if temp_path:
                Path(temp_path).unlink(missing_ok=True)

    return app


app = create_app()
