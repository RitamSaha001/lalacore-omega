from __future__ import annotations

import asyncio
import io
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Sequence

from core.multimodal.ocr_engine import OCREngine
from core.multimodal.remote_worker import remote_pdf_ocr, remote_worker_enabled
from core.multimodal.telemetry import DEFAULT_MULTIMODAL_TELEMETRY, MultimodalTelemetry


try:  # pragma: no cover - optional dependency
    from pdf2image import convert_from_bytes
except Exception:  # pragma: no cover - optional dependency
    convert_from_bytes = None

try:  # pragma: no cover - optional dependency
    from PIL import Image, ImageEnhance, ImageFilter, ImageOps
except Exception:  # pragma: no cover - optional dependency
    Image = None
    ImageEnhance = None
    ImageFilter = None
    ImageOps = None

try:  # pragma: no cover - optional dependency
    from pypdf import PdfReader, PdfWriter
except Exception:  # pragma: no cover - optional dependency
    PdfReader = None
    PdfWriter = None


class PDFProcessor:
    """
    PDF -> page-images -> OCR pipeline with layout/page preservation.
    """

    def __init__(
        self,
        *,
        ocr_engine: OCREngine | None = None,
        dpi: int = 220,
        max_pages: int = 20,
        ocr_parallelism: int = 4,
        max_pdf_bytes: int = 24_000_000,
        enable_remote_worker: bool | None = None,
        remote_worker_url: str | None = None,
        remote_worker_token: str | None = None,
        telemetry: MultimodalTelemetry | None = None,
    ) -> None:
        self.ocr_engine = ocr_engine or OCREngine()
        self.dpi = int(max(100, dpi))
        self.max_pages = int(max(1, max_pages))
        self.ocr_parallelism = int(max(1, ocr_parallelism))
        self.max_pdf_bytes = int(max(1024, max_pdf_bytes))
        self.enable_remote_worker = enable_remote_worker
        self.remote_worker_url = (remote_worker_url or "").strip()
        self.remote_worker_token = (remote_worker_token or "").strip()
        self.telemetry = telemetry or DEFAULT_MULTIMODAL_TELEMETRY

    async def process(
        self,
        pdf_bytes: bytes,
        *,
        optional_web_snippets: Sequence[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        if not pdf_bytes:
            return {
                "page_count": 0,
                "pages": [],
                "merged_text": "",
                "tables": [],
                "equations": [],
                "overall_confidence": 0.0,
                "reason": "pdf_empty",
            }
        if len(pdf_bytes) > self.max_pdf_bytes:
            return {
                "page_count": 0,
                "pages": [],
                "merged_text": "",
                "tables": [],
                "equations": [],
                "overall_confidence": 0.0,
                "reason": "pdf_size_limit_exceeded",
                "max_pdf_bytes": int(self.max_pdf_bytes),
            }
        if remote_worker_enabled(
            explicit=self.enable_remote_worker,
            worker_url=self.remote_worker_url,
        ):
            remote_payload = await asyncio.to_thread(
                remote_pdf_ocr,
                pdf_bytes,
                optional_web_snippets=optional_web_snippets,
                worker_url=self.remote_worker_url,
                worker_token=self.remote_worker_token,
            )
            if isinstance(remote_payload, dict) and remote_payload:
                return remote_payload

        pages = await asyncio.to_thread(self._convert_pdf_to_images, pdf_bytes)
        if not pages:
            return {
                "page_count": 0,
                "pages": [],
                "merged_text": "",
                "tables": [],
                "equations": [],
                "overall_confidence": 0.0,
                "reason": "pdf_to_image_failed",
            }

        if len(pages) > self.max_pages:
            pages = pages[: self.max_pages]

        semaphore = asyncio.Semaphore(self.ocr_parallelism)

        async def _ocr_page(page_no: int, image_bytes: bytes) -> Dict[str, Any]:
            async with semaphore:
                return await self._ocr_page_bytes(
                    page_no=page_no,
                    image_bytes=image_bytes,
                    optional_web_snippets=optional_web_snippets,
                )

        tasks = [asyncio.create_task(_ocr_page(idx + 1, page_blob)) for idx, page_blob in enumerate(pages)]
        try:
            page_rows = await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()

        page_rows.sort(key=lambda row: int(row.get("page_number", 0)))
        page_rows, retry_report = await self._retry_low_confidence_pages(
            page_rows=page_rows,
            page_images=pages,
            optional_web_snippets=optional_web_snippets,
        )
        effective_rows = [row for row in page_rows if self._row_has_usable_text(row)]
        if not effective_rows:
            effective_rows = list(page_rows)
        merged_text = "\n\n".join(
            f"[Page {row['page_number']}]\n{row.get('clean_text') or row.get('math_normalized_text') or row.get('raw_text', '')}"
            for row in effective_rows
        ).strip()

        tables = self._detect_tables(effective_rows)
        equations = self._detect_equations(effective_rows)
        structured_questions: List[Dict[str, Any]] = []
        for row in effective_rows:
            for question in row.get("lc_iie_questions", []) or []:
                if isinstance(question, dict):
                    structured_questions.append(dict(question))
        overall_conf = self._average([float(row.get("confidence", 0.0)) for row in effective_rows])

        payload = {
            "page_count": len(page_rows),
            "effective_page_count": len(effective_rows),
            "pages": page_rows,
            "merged_text": merged_text,
            "tables": tables,
            "equations": equations,
            "lc_iie_questions": structured_questions,
            "overall_confidence": float(overall_conf),
            "retry_report": retry_report,
        }
        self.telemetry.log_event(
            "pdf_ingestion",
            {
                "page_count": len(page_rows),
                "effective_page_count": len(effective_rows),
                "overall_confidence": float(overall_conf),
                "table_count": len(tables),
                "equation_count": len(equations),
                "lc_iie_question_count": len(structured_questions),
                "retry_pages": int(retry_report.get("retried_pages", 0)),
                "retry_improved_pages": int(retry_report.get("improved_pages", 0)),
            },
        )
        return payload

    async def _ocr_page_bytes(
        self,
        *,
        page_no: int,
        image_bytes: bytes,
        optional_web_snippets: Sequence[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        ocr = await self.ocr_engine.extract_async(
            image_bytes,
            page_number=page_no,
            math_aware=True,
            optional_web_snippets=optional_web_snippets,
        )
        return {
            "page_number": int(page_no),
            "raw_text": str(ocr.get("raw_text", "")),
            "math_normalized_text": str(ocr.get("math_normalized_text", "")),
            "clean_text": str(ocr.get("clean_text", "")),
            "layout_blocks": list(ocr.get("layout_blocks", [])),
            "bounding_boxes": list(ocr.get("bounding_boxes", [])),
            "confidence": float(ocr.get("confidence", 0.0)),
            "provider": str(ocr.get("provider", "unknown")),
            "ocr_model": str(ocr.get("ocr_model", "")),
            "lc_iie_questions": [
                dict(x)
                for x in (ocr.get("lc_iie_questions") or [])
                if isinstance(x, dict)
            ],
            "lc_iie_metadata": dict(ocr.get("lc_iie_metadata", {})),
        }

    async def _retry_low_confidence_pages(
        self,
        *,
        page_rows: List[Dict[str, Any]],
        page_images: Sequence[bytes],
        optional_web_snippets: Sequence[Dict[str, Any]] | None = None,
    ) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
        report: Dict[str, Any] = {
            "retried_pages": 0,
            "improved_pages": 0,
            "retry_candidates": [],
        }
        if not page_rows or not page_images:
            return page_rows, report
        candidates: List[int] = []
        for idx, row in enumerate(page_rows):
            confidence = float(row.get("confidence", 0.0))
            if confidence < 0.58 or not self._row_has_usable_text(row):
                candidates.append(idx)
        if not candidates:
            return page_rows, report
        max_retry = min(len(candidates), max(1, self.max_pages // 2))
        for idx in candidates[:max_retry]:
            page_no = int(page_rows[idx].get("page_number", idx + 1))
            if page_no <= 0 or page_no > len(page_images):
                continue
            original_blob = page_images[page_no - 1]
            enhanced = self._enhance_page_for_retry(original_blob)
            if not enhanced:
                continue
            report["retried_pages"] = int(report.get("retried_pages", 0)) + 1
            try:
                retried = await self._ocr_page_bytes(
                    page_no=page_no,
                    image_bytes=enhanced,
                    optional_web_snippets=optional_web_snippets,
                )
            except Exception as exc:
                self.telemetry.log_event(
                    "ocr_retry_error",
                    {
                        "page_number": page_no,
                        "error_type": type(exc).__name__,
                        "reason": str(exc)[:220],
                    },
                )
                continue
            prev_quality = self._row_quality_value(page_rows[idx])
            new_quality = self._row_quality_value(retried)
            if new_quality > (prev_quality + 0.04):
                report["improved_pages"] = int(report.get("improved_pages", 0)) + 1
                report["retry_candidates"].append(
                    {
                        "page_number": page_no,
                        "before_confidence": float(page_rows[idx].get("confidence", 0.0)),
                        "after_confidence": float(retried.get("confidence", 0.0)),
                        "before_quality": round(prev_quality, 6),
                        "after_quality": round(new_quality, 6),
                    }
                )
                page_rows[idx] = retried
        return page_rows, report

    def _row_quality_value(self, row: Dict[str, Any]) -> float:
        confidence = float(row.get("confidence", 0.0))
        text = str(row.get("clean_text") or row.get("math_normalized_text") or row.get("raw_text") or "")
        compact = re.sub(r"\s+", " ", text).strip()
        readability = 0.0
        if compact:
            length_bonus = min(1.0, len(compact) / 420.0)
            readable = sum(1 for ch in compact if ch.isalnum() or ch.isspace())
            readability = (readable / max(1, len(compact))) * length_bonus
        usable_bonus = 0.18 if self._row_has_usable_text(row) else 0.0
        return max(0.0, min(1.0, (confidence * 0.62) + (readability * 0.38) + usable_bonus))

    def _enhance_page_for_retry(self, image_bytes: bytes) -> bytes:
        if not image_bytes:
            return b""
        if Image is None:
            return b""
        try:
            image = Image.open(io.BytesIO(image_bytes))
            image.load()
            gray = image.convert("L")
            if ImageOps is not None:
                gray = ImageOps.autocontrast(gray, cutoff=2)
            if ImageFilter is not None:
                gray = gray.filter(ImageFilter.MedianFilter(size=3))
            if ImageEnhance is not None:
                gray = ImageEnhance.Contrast(gray).enhance(1.42)
                gray = ImageEnhance.Sharpness(gray).enhance(1.3)
            width, height = gray.size
            upscale = 1.0
            if width * height < 2_200_000:
                upscale = 1.55
            if upscale > 1.0:
                resampling = getattr(getattr(Image, "Resampling", Image), "LANCZOS", None)
                if resampling is None:
                    resampling = getattr(Image, "BICUBIC", 3)
                gray = gray.resize(
                    (
                        max(140, int(round(width * upscale))),
                        max(140, int(round(height * upscale))),
                    ),
                    resample=resampling,
                )
            out = io.BytesIO()
            gray.save(out, format="PNG")
            return out.getvalue()
        except Exception:
            return b""

    def _row_has_usable_text(self, row: Dict[str, Any]) -> bool:
        text = str(row.get("clean_text") or row.get("math_normalized_text") or row.get("raw_text") or "")
        trimmed = text.strip()
        if not trimmed:
            return False
        compact = re.sub(r"\s+", " ", trimmed)
        if len(compact) < 40 and float(row.get("confidence", 0.0)) < 0.36:
            return False
        total = max(1, len(compact))
        readable = sum(1 for ch in compact if ch.isalnum() or ch.isspace())
        readable_ratio = readable / total
        if readable_ratio < 0.55 and float(row.get("confidence", 0.0)) < 0.44:
            return False
        low = compact.lower()
        if ("ihdr" in low or "idat" in low or "xmpmeta" in low) and float(row.get("confidence", 0.0)) < 0.5:
            return False
        return True

    def _convert_pdf_to_images(self, pdf_bytes: bytes) -> List[bytes]:
        if not pdf_bytes:
            return []
        if self._looks_like_image_bytes(pdf_bytes):
            image_blob = self._normalize_image_bytes(pdf_bytes)
            return [image_blob] if image_blob else [pdf_bytes]

        if convert_from_bytes is not None:
            try:
                pil_images = convert_from_bytes(pdf_bytes, dpi=self.dpi)
                pages: List[bytes] = []
                for image in pil_images:
                    try:
                        buff = io.BytesIO()
                        image.save(buff, format="PNG")
                        pages.append(buff.getvalue())
                    finally:
                        try:
                            image.close()
                        except Exception:
                            pass
                if pages:
                    return pages
            except Exception as exc:
                self.telemetry.log_event(
                    "pdf_conversion_error",
                    {"error_type": type(exc).__name__, "reason": str(exc)[:300]},
                )

        pages_via_pdftoppm = self._convert_pdf_to_images_with_pdftoppm(pdf_bytes)
        if pages_via_pdftoppm:
            return pages_via_pdftoppm

        pages_via_mutool = self._convert_pdf_to_images_with_mutool(pdf_bytes)
        if pages_via_mutool:
            return pages_via_mutool

        pages_via_sips = self._convert_pdf_to_images_with_sips(pdf_bytes)
        if pages_via_sips:
            return pages_via_sips

        # Fallback: treat textual PDF bytes as one pseudo-page for OCR heuristic.
        text = pdf_bytes.decode("utf-8", errors="ignore")
        text = re.sub(r"\s+", " ", text).strip()
        if text:
            return [text.encode("utf-8")]
        return [pdf_bytes]

    def _looks_like_image_bytes(self, blob: bytes) -> bool:
        sig = bytes(blob[:16])
        if sig.startswith(b"\x89PNG\r\n\x1a\n"):
            return True
        if sig.startswith(b"\xff\xd8\xff"):
            return True
        if sig.startswith((b"GIF87a", b"GIF89a")):
            return True
        if sig.startswith(b"BM"):
            return True
        if sig.startswith((b"II*\x00", b"MM\x00*")):
            return True
        if sig.startswith(b"RIFF") and b"WEBP" in bytes(blob[8:16]):
            return True
        return False

    def _normalize_image_bytes(self, image_bytes: bytes) -> bytes:
        if Image is None:
            return image_bytes
        try:
            image = Image.open(io.BytesIO(image_bytes))
            image.load()
            if image.mode not in ("L", "RGB"):
                image = image.convert("RGB")
            out = io.BytesIO()
            image.save(out, format="PNG")
            return out.getvalue()
        except Exception:
            return image_bytes

    def _convert_pdf_to_images_with_pdftoppm(self, pdf_bytes: bytes) -> List[bytes]:
        pdftoppm_bin = shutil.which("pdftoppm")
        if not pdftoppm_bin:
            return []
        try:
            with tempfile.TemporaryDirectory(prefix="lc_pdf_ppm_") as tmp_dir:
                in_path = Path(tmp_dir) / "in.pdf"
                out_prefix = Path(tmp_dir) / "page"
                in_path.write_bytes(pdf_bytes)
                res = subprocess.run(
                    [
                        pdftoppm_bin,
                        "-png",
                        "-r",
                        str(self.dpi),
                        str(in_path),
                        str(out_prefix),
                    ],
                    capture_output=True,
                    check=False,
                    timeout=42.0,
                )
                if res.returncode != 0:
                    return []
                pages: List[bytes] = []
                for page_path in sorted(Path(tmp_dir).glob("page-*.png"))[: self.max_pages]:
                    blob = page_path.read_bytes()
                    if blob:
                        pages.append(blob)
                return pages
        except Exception as exc:
            self.telemetry.log_event(
                "pdf_conversion_error",
                {
                    "error_type": type(exc).__name__,
                    "reason": str(exc)[:300],
                    "transport": "pdftoppm",
                },
            )
            return []

    def _convert_pdf_to_images_with_mutool(self, pdf_bytes: bytes) -> List[bytes]:
        mutool_bin = shutil.which("mutool")
        if not mutool_bin:
            return []
        try:
            with tempfile.TemporaryDirectory(prefix="lc_pdf_mutool_") as tmp_dir:
                in_path = Path(tmp_dir) / "in.pdf"
                out_pattern = Path(tmp_dir) / "page-%03d.png"
                in_path.write_bytes(pdf_bytes)
                res = subprocess.run(
                    [
                        mutool_bin,
                        "draw",
                        "-F",
                        "png",
                        "-r",
                        str(self.dpi),
                        "-o",
                        str(out_pattern),
                        str(in_path),
                    ],
                    capture_output=True,
                    check=False,
                    timeout=42.0,
                )
                if res.returncode != 0:
                    return []
                pages: List[bytes] = []
                for page_path in sorted(Path(tmp_dir).glob("page-*.png"))[: self.max_pages]:
                    blob = page_path.read_bytes()
                    if blob:
                        pages.append(blob)
                return pages
        except Exception as exc:
            self.telemetry.log_event(
                "pdf_conversion_error",
                {
                    "error_type": type(exc).__name__,
                    "reason": str(exc)[:300],
                    "transport": "mutool",
                },
            )
            return []

    def _convert_pdf_to_images_with_sips(self, pdf_bytes: bytes) -> List[bytes]:
        # macOS fallback when pdf2image/poppler is unavailable.
        if sys.platform != "darwin" or not shutil.which("sips"):
            return []
        try:
            with tempfile.TemporaryDirectory(prefix="lc_pdf_sips_") as tmp_dir:
                pages = self._convert_pdf_to_images_with_sips_split(
                    pdf_bytes=pdf_bytes,
                    tmp_dir=Path(tmp_dir),
                )
                if pages:
                    return pages
                in_path = Path(tmp_dir) / "in.pdf"
                out_path = Path(tmp_dir) / "page_1.png"
                in_path.write_bytes(pdf_bytes)
                res = subprocess.run(
                    ["sips", "-s", "format", "png", str(in_path), "--out", str(out_path)],
                    capture_output=True,
                    check=False,
                    timeout=18.0,
                )
                if res.returncode != 0 or not out_path.exists():
                    return []
                blob = out_path.read_bytes()
                if blob:
                    return [blob]
        except Exception as exc:
            self.telemetry.log_event(
                "pdf_conversion_error",
                {
                    "error_type": type(exc).__name__,
                    "reason": str(exc)[:300],
                    "transport": "sips",
                },
            )
        return []

    def _convert_pdf_to_images_with_sips_split(self, *, pdf_bytes: bytes, tmp_dir: Path) -> List[bytes]:
        if PdfReader is None or PdfWriter is None:
            return []
        try:
            reader = PdfReader(io.BytesIO(pdf_bytes))
            page_count = min(int(len(reader.pages)), int(self.max_pages))
            if page_count <= 1:
                return []
            pages: List[bytes] = []
            for idx in range(page_count):
                single_pdf = tmp_dir / f"page_{idx + 1}.pdf"
                out_png = tmp_dir / f"page_{idx + 1}.png"
                writer = PdfWriter()
                writer.add_page(reader.pages[idx])
                with single_pdf.open("wb") as fp:
                    writer.write(fp)
                res = subprocess.run(
                    ["sips", "-s", "format", "png", str(single_pdf), "--out", str(out_png)],
                    capture_output=True,
                    check=False,
                    timeout=18.0,
                )
                if res.returncode != 0 or not out_png.exists():
                    continue
                blob = out_png.read_bytes()
                if blob:
                    pages.append(blob)
            return pages
        except Exception:
            return []

    def _detect_tables(self, pages: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        tables: List[Dict[str, Any]] = []
        for row in pages:
            text = str(row.get("raw_text", ""))
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            candidates = [line for line in lines if "|" in line or len(re.findall(r"\s{2,}", line)) >= 2]
            if candidates:
                tables.append({"page_number": int(row.get("page_number", 0)), "rows": candidates[:20]})
        return tables

    def _detect_equations(self, pages: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        equations: List[Dict[str, Any]] = []
        pattern = re.compile(r"[A-Za-z0-9_\+\-\*/\^\(\)]+\s*=\s*[A-Za-z0-9_\+\-\*/\^\(\)]+")
        for row in pages:
            text = str(row.get("math_normalized_text", ""))
            matches = pattern.findall(text)
            if matches:
                equations.append({"page_number": int(row.get("page_number", 0)), "expressions": matches[:40]})
        return equations

    def _average(self, values: Sequence[float]) -> float:
        arr = [float(v) for v in values]
        if not arr:
            return 0.0
        return float(sum(arr) / len(arr))
