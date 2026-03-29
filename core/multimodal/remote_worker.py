from __future__ import annotations

import base64
import json
import os
from typing import Any, Dict, Sequence
from urllib.parse import urlparse, urlunparse

import requests


def _env_first(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return ""


def _is_truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def remote_worker_enabled(
    *,
    explicit: bool | None = None,
    worker_url: str | None = None,
) -> bool:
    if _is_truthy(os.getenv("LC_DISABLE_REMOTE_OCR_WORKER", "")):
        return False
    if explicit is not None:
        return bool(explicit)
    effective = (worker_url or _default_worker_url()).strip()
    return effective.startswith("http://") or effective.startswith("https://")


def _default_worker_url() -> str:
    return _env_first(
        "OCR_WORKER_URL",
        "IMPORT_WORKER_URL",
        "IMPORT_QUESTION_WORKER_URL",
    )


def _default_worker_token() -> str:
    return _env_first(
        "OCR_WORKER_TOKEN",
        "IMPORT_WORKER_TOKEN",
        "IMPORT_QUESTION_WORKER_TOKEN",
    )


def resolve_worker_endpoint(raw: str, default_path: str) -> str:
    parsed = urlparse(raw.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc.strip():
        return ""
    current_path = parsed.path.rstrip("/")
    if current_path.endswith(default_path):
        return urlunparse(parsed._replace(params="", query="", fragment=""))
    if (
        default_path.startswith("/ocr/")
        and current_path.endswith("/import/parse-pdf")
    ):
        current_path = "/ocr/frame"
    elif (
        default_path.startswith("/import/")
        and current_path.endswith("/ocr/frame")
    ):
        current_path = "/import/parse-pdf"
    elif (
        default_path.startswith("/ocr/")
        and current_path.endswith("/ocr")
    ):
        current_path = f"{current_path}{default_path[len('/ocr'):]}"
    elif (
        default_path.startswith("/import/")
        and current_path.endswith("/import")
    ):
        current_path = f"{current_path}{default_path[len('/import'):]}"
    else:
        current_path = f"{current_path}{default_path}" if current_path else default_path
    return urlunparse(
        parsed._replace(path=current_path, params="", query="", fragment="")
    )


def _auth_headers(worker_token: str) -> dict[str, str]:
    token = worker_token.strip()
    return {"Authorization": f"Bearer {token}"} if token else {}


def remote_image_ocr(
    image_bytes: bytes,
    *,
    page_number: int = 1,
    math_aware: bool = True,
    optional_web_snippets: Sequence[Dict[str, Any]] | None = None,
    worker_url: str | None = None,
    worker_token: str | None = None,
    timeout_s: float = 45.0,
) -> Dict[str, Any] | None:
    effective_url = resolve_worker_endpoint(
        (worker_url or _default_worker_url()).strip(),
        "/ocr/frame",
    )
    if not effective_url:
        return None
    try:
        response = requests.post(
            effective_url,
            json={
                "image_base64": base64.b64encode(image_bytes).decode("ascii"),
                "page_number": int(max(1, page_number)),
                "math_aware": bool(math_aware),
                "optional_web_snippets": list(optional_web_snippets or ()),
            },
            headers=_auth_headers(worker_token or _default_worker_token()),
            timeout=timeout_s,
        )
        if response.status_code < 200 or response.status_code >= 300:
            return None
        payload = response.json()
        if isinstance(payload, dict):
            ocr_data = payload.get("ocr_data")
            if isinstance(ocr_data, dict):
                return dict(ocr_data)
            if "raw_text" in payload or "clean_text" in payload:
                return dict(payload)
    except Exception:
        return None
    return None


def remote_pdf_ocr(
    pdf_bytes: bytes,
    *,
    optional_web_snippets: Sequence[Dict[str, Any]] | None = None,
    worker_url: str | None = None,
    worker_token: str | None = None,
    timeout_s: float = 240.0,
) -> Dict[str, Any] | None:
    effective_url = resolve_worker_endpoint(
        (worker_url or _default_worker_url()).strip(),
        "/ocr/pdf",
    )
    if not effective_url:
        return None
    data: dict[str, str] = {}
    if optional_web_snippets:
        data["optional_web_snippets_json"] = json.dumps(
            list(optional_web_snippets),
            ensure_ascii=False,
        )
    try:
        response = requests.post(
            effective_url,
            data=data,
            files={"file": ("document.pdf", pdf_bytes, "application/pdf")},
            headers=_auth_headers(worker_token or _default_worker_token()),
            timeout=timeout_s,
        )
        if response.status_code < 200 or response.status_code >= 300:
            return None
        payload = response.json()
        if isinstance(payload, dict):
            pdf_data = payload.get("pdf_data")
            if isinstance(pdf_data, dict):
                return dict(pdf_data)
            if "page_count" in payload or "merged_text" in payload:
                return dict(payload)
    except Exception:
        return None
    return None
