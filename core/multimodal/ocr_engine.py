from __future__ import annotations

import asyncio
import io
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, Tuple

from core.multimodal.lc_iie_engine import LCIIEEngine
from core.multimodal.telemetry import DEFAULT_MULTIMODAL_TELEMETRY, MultimodalTelemetry


try:  # pragma: no cover - optional dependency
    import numpy as np
except Exception:  # pragma: no cover - optional dependency
    np = None

try:  # pragma: no cover - optional dependency
    from PIL import Image, ImageEnhance, ImageFilter, ImageOps
except Exception:  # pragma: no cover - optional dependency
    Image = None
    ImageEnhance = None
    ImageFilter = None
    ImageOps = None

try:  # pragma: no cover - optional dependency
    import pytesseract
except Exception:  # pragma: no cover - optional dependency
    pytesseract = None


_MATH_SYMBOL_MAP = {
    "√": "sqrt",
    "∫": "integral",
    "∑": "sum",
    "π": "pi",
    "∞": "infinity",
    "≤": "<=",
    "≥": ">=",
    "×": "*",
    "÷": "/",
    "−": "-",
    "→": "->",
    "≠": "!=",
    "≈": "~=",
    "∂": "d",
    "θ": "theta",
}


_SUPERSCRIPT_TR = str.maketrans(
    {
        "⁰": "0",
        "¹": "1",
        "²": "2",
        "³": "3",
        "⁴": "4",
        "⁵": "5",
        "⁶": "6",
        "⁷": "7",
        "⁸": "8",
        "⁹": "9",
        "⁻": "-",
    }
)


@dataclass(slots=True)
class OCRLayoutBlock:
    text: str
    bbox: List[int]
    confidence: float
    block_id: int
    page_number: int


class OCREngine:
    """
    OCR abstraction with quality-based model selection and LC-IIE post-processing.
    """

    def __init__(
        self,
        *,
        provider_preference: Sequence[str] = ("tesseract_best", "tesseract", "paddle", "heuristic"),
        math_aware_default: bool = True,
        enable_multicrop_refine: bool = True,
        max_region_candidates: int = 5,
        enable_handwritten_refine: bool = True,
        max_handwriting_variants: int = 3,
        telemetry: MultimodalTelemetry | None = None,
        lc_iie: LCIIEEngine | None = None,
    ) -> None:
        self.provider_preference = tuple(str(p).lower() for p in provider_preference)
        self.math_aware_default = bool(math_aware_default)
        self.enable_multicrop_refine = bool(enable_multicrop_refine)
        self.max_region_candidates = int(max(0, max_region_candidates))
        self.enable_handwritten_refine = bool(enable_handwritten_refine)
        self.max_handwriting_variants = int(max(0, max_handwriting_variants))
        self.telemetry = telemetry or DEFAULT_MULTIMODAL_TELEMETRY
        self.lc_iie = lc_iie or LCIIEEngine()

    async def extract_async(
        self,
        image_bytes: bytes,
        *,
        page_number: int = 1,
        math_aware: bool | None = None,
        optional_web_snippets: Sequence[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        return await asyncio.to_thread(
            self.extract,
            image_bytes,
            page_number=page_number,
            math_aware=math_aware,
            optional_web_snippets=optional_web_snippets,
        )

    def extract(
        self,
        image_bytes: bytes,
        *,
        page_number: int = 1,
        math_aware: bool | None = None,
        optional_web_snippets: Sequence[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        if not image_bytes:
            return {
                "raw_text": "",
                "layout_blocks": [],
                "bounding_boxes": [],
                "confidence": 0.0,
                "provider": "none",
                "page_number": int(page_number),
                "math_normalized_text": "",
                "clean_text": "",
                "lc_iie_questions": [],
                "lc_iie_metadata": {
                    "version": self.lc_iie.version,
                    "question_count": 0,
                    "web_validation": {"used": False, "confidence": 0.0, "urls": []},
                },
                "ocr_model": "",
            }

        use_math = self.math_aware_default if math_aware is None else bool(math_aware)
        prepared = self._prepare_image(image_bytes, math_aware=use_math)

        raw_text, blocks, provider, ocr_model = self._select_best_provider(prepared, image_bytes, page_number=page_number)
        raw_text, blocks, provider, ocr_model = self._refine_with_region_crops(
            original_image_bytes=image_bytes,
            page_number=page_number,
            math_aware=use_math,
            base_text=raw_text,
            base_blocks=blocks,
            base_provider=provider,
            base_model=ocr_model,
        )
        raw_text, blocks, provider, ocr_model = self._refine_with_handwriting_variants(
            original_image_bytes=image_bytes,
            page_number=page_number,
            math_aware=use_math,
            base_text=raw_text,
            base_blocks=blocks,
            base_provider=provider,
            base_model=ocr_model,
        )
        normalized = self.normalize_math_symbols(raw_text) if use_math else raw_text

        lc_iie_payload = self.lc_iie.run(
            raw_text=normalized or raw_text,
            page_number=page_number,
            optional_web_snippets=optional_web_snippets or [],
        )
        clean_text = str(lc_iie_payload.get("clean_text", "")).strip()
        if clean_text and use_math:
            normalized = self.normalize_math_symbols(clean_text)
        elif clean_text and not normalized.strip():
            normalized = clean_text

        bounding_boxes = [
            {
                "x1": int(block.bbox[0]),
                "y1": int(block.bbox[1]),
                "x2": int(block.bbox[2]),
                "y2": int(block.bbox[3]),
                "confidence": float(block.confidence),
                "page_number": int(block.page_number),
                "block_id": int(block.block_id),
            }
            for block in blocks
        ]

        confidence_raw = self._aggregate_confidence(blocks)
        quality_conf = self._quality_score(raw_text, blocks)
        confidence = float(
            max(
                confidence_raw,
                min(1.0, 0.35 * confidence_raw + 0.65 * quality_conf),
            )
        )
        clustered_blocks = self._cluster_blocks(blocks)
        questions = [dict(x) for x in (lc_iie_payload.get("questions") or []) if isinstance(x, dict)]

        payload = {
            "raw_text": raw_text,
            "layout_blocks": clustered_blocks,
            "bounding_boxes": bounding_boxes,
            "confidence": confidence,
            "provider": provider,
            "page_number": int(page_number),
            "math_normalized_text": normalized,
            "clean_text": clean_text,
            "lc_iie_questions": questions,
            "lc_iie_output": questions,
            "lc_iie_metadata": {
                "version": str(lc_iie_payload.get("version", self.lc_iie.version)),
                "question_count": int(lc_iie_payload.get("question_count", len(questions))),
                "web_validation": dict(lc_iie_payload.get("web_validation", {})),
                "math_context": dict(lc_iie_payload.get("math_context", {})),
            },
            "ocr_model": ocr_model,
        }

        self.telemetry.log_ocr_metrics(
            source=provider,
            confidence=confidence,
            block_count=len(clustered_blocks),
            math_normalized=use_math,
        )
        return payload

    def normalize_math_symbols(self, text: str) -> str:
        out = str(text or "")
        for source, target in _MATH_SYMBOL_MAP.items():
            out = out.replace(source, target)
        out = out.translate(_SUPERSCRIPT_TR)
        out = re.sub(r"sin\s*\^?\s*\(?-?1\)?", "asin", out, flags=re.IGNORECASE)
        out = re.sub(r"cos\s*\^?\s*\(?-?1\)?", "acos", out, flags=re.IGNORECASE)
        out = re.sub(r"tan\s*\^?\s*\(?-?1\)?", "atan", out, flags=re.IGNORECASE)
        out = re.sub(r"\s+", " ", out).strip()
        return out

    def _prepare_image(self, image_bytes: bytes, *, math_aware: bool) -> bytes:
        if not math_aware or Image is None:
            return image_bytes
        try:
            image = Image.open(io.BytesIO(image_bytes))
            gray = ImageOps.grayscale(image) if ImageOps is not None else image.convert("L")
            if ImageFilter is not None:
                gray = gray.filter(ImageFilter.MedianFilter(size=3))
            if np is not None:
                arr = np.array(gray)
                threshold = int(max(75, min(205, arr.mean())))
                bw = (arr > threshold) * 255
                gray = Image.fromarray(bw.astype("uint8"))
            out = io.BytesIO()
            gray.save(out, format="PNG")
            return out.getvalue()
        except Exception:
            return image_bytes

    def _refine_with_region_crops(
        self,
        *,
        original_image_bytes: bytes,
        page_number: int,
        math_aware: bool,
        base_text: str,
        base_blocks: Sequence[OCRLayoutBlock],
        base_provider: str,
        base_model: str,
    ) -> Tuple[str, List[OCRLayoutBlock], str, str]:
        if not self.enable_multicrop_refine or self.max_region_candidates <= 0:
            return base_text, list(base_blocks), base_provider, base_model
        variants = self._build_region_variants(original_image_bytes)
        if not variants:
            return base_text, list(base_blocks), base_provider, base_model

        base_score = self._quality_score(base_text, base_blocks)
        candidates: List[Dict[str, Any]] = [
            {
                "region": "full_page",
                "text": str(base_text or ""),
                "blocks": list(base_blocks),
                "provider": str(base_provider or "unknown"),
                "model": str(base_model or ""),
                "score": float(base_score),
            }
        ]
        region_pref = self._region_provider_preference()
        for region in variants[: self.max_region_candidates]:
            crop_bytes = bytes(region.get("bytes") or b"")
            if not crop_bytes:
                continue
            try:
                prepared_crop = self._prepare_image(crop_bytes, math_aware=math_aware)
                text, blocks, provider, model = self._select_best_provider(
                    prepared_crop,
                    crop_bytes,
                    page_number=page_number,
                    provider_preference=region_pref,
                )
                mapped_blocks = self._map_blocks_to_page(
                    blocks,
                    offset_x=int(region.get("x1", 0)),
                    offset_y=int(region.get("y1", 0)),
                    scale=float(region.get("scale", 1.0) or 1.0),
                    page_number=page_number,
                )
                score = self._quality_score(text, mapped_blocks)
                if text.strip():
                    candidates.append(
                        {
                            "region": str(region.get("name", "region")),
                            "text": str(text),
                            "blocks": mapped_blocks,
                            "provider": str(provider or "unknown"),
                            "model": str(model or ""),
                            "score": float(score),
                        }
                    )
            except Exception as exc:
                self.telemetry.log_event(
                    "ocr_region_error",
                    {
                        "error_type": type(exc).__name__,
                        "reason": str(exc)[:220],
                        "region": str(region.get("name", "region")),
                    },
                )

        candidates.sort(key=lambda row: float(row.get("score", 0.0)), reverse=True)
        best = candidates[0]
        base_guard = max(0.16, float(base_score) * 0.72)
        extra_texts = [
            str(row.get("text", ""))
            for row in candidates[1:4]
            if float(row.get("score", 0.0)) >= base_guard
        ]
        merged_text = self._merge_text_candidates(str(best.get("text", "")), extra_texts)
        merged_blocks = self._merge_block_candidates(
            best_blocks=[x for x in (best.get("blocks") or []) if isinstance(x, OCRLayoutBlock)],
            extra_block_sets=[
                [x for x in (row.get("blocks") or []) if isinstance(x, OCRLayoutBlock)]
                for row in candidates[1:4]
            ],
            page_number=page_number,
        )
        merged_score = self._quality_score(merged_text, merged_blocks)
        if merged_score >= float(best.get("score", 0.0)) * 1.02 and merged_text.strip():
            return (
                merged_text,
                merged_blocks,
                f"{str(best.get('provider') or 'unknown')}+multicrop",
                f"{str(best.get('model') or '')}+regions",
            )
        return (
            str(best.get("text", "")),
            [x for x in (best.get("blocks") or []) if isinstance(x, OCRLayoutBlock)],
            str(best.get("provider", base_provider)),
            str(best.get("model", base_model)),
        )

    def _refine_with_handwriting_variants(
        self,
        *,
        original_image_bytes: bytes,
        page_number: int,
        math_aware: bool,
        base_text: str,
        base_blocks: Sequence[OCRLayoutBlock],
        base_provider: str,
        base_model: str,
    ) -> Tuple[str, List[OCRLayoutBlock], str, str]:
        if not self._should_try_handwriting_refine(original_image_bytes, base_text, base_blocks):
            return base_text, list(base_blocks), base_provider, base_model
        variants = self._build_handwriting_variants(original_image_bytes)
        if not variants:
            return base_text, list(base_blocks), base_provider, base_model

        base_score = self._quality_score(base_text, base_blocks)
        likelihood = self._estimate_handwriting_likelihood(original_image_bytes)
        candidates: List[Dict[str, Any]] = [
            {
                "name": "base",
                "text": str(base_text or ""),
                "blocks": list(base_blocks),
                "provider": str(base_provider or "unknown"),
                "model": str(base_model or ""),
                "score": float(base_score),
            }
        ]
        pref = self._handwriting_provider_preference()
        for variant in variants[: self.max_handwriting_variants]:
            blob = bytes(variant.get("bytes") or b"")
            if not blob:
                continue
            try:
                prepared = self._prepare_image(blob, math_aware=math_aware)
                text, blocks, provider, model = self._select_best_provider(
                    prepared,
                    blob,
                    page_number=page_number,
                    provider_preference=pref,
                    handwritten_hint=True,
                )
                score = self._quality_score(text, blocks)
                # Slightly prefer handwriting variants when page likely contains handwriting.
                score *= 1.0 + (0.1 * likelihood)
                if text.strip():
                    candidates.append(
                        {
                            "name": str(variant.get("name", "hw")),
                            "text": str(text),
                            "blocks": [x for x in blocks if isinstance(x, OCRLayoutBlock)],
                            "provider": str(provider or "unknown"),
                            "model": str(model or ""),
                            "score": float(score),
                        }
                    )
            except Exception as exc:
                self.telemetry.log_event(
                    "ocr_handwriting_error",
                    {
                        "error_type": type(exc).__name__,
                        "reason": str(exc)[:220],
                        "variant": str(variant.get("name", "hw")),
                    },
                )

        candidates.sort(key=lambda row: float(row.get("score", 0.0)), reverse=True)
        best = candidates[0]
        if float(best.get("score", 0.0)) < (base_score * 1.03):
            return base_text, list(base_blocks), base_provider, base_model

        extra_texts = [
            str(row.get("text", ""))
            for row in candidates[1:4]
            if float(row.get("score", 0.0)) >= max(0.14, base_score * 0.72)
        ]
        merged_text = self._merge_text_candidates(str(best.get("text", "")), extra_texts)
        merged_blocks = self._merge_block_candidates(
            best_blocks=[x for x in (best.get("blocks") or []) if isinstance(x, OCRLayoutBlock)],
            extra_block_sets=[
                [x for x in (row.get("blocks") or []) if isinstance(x, OCRLayoutBlock)]
                for row in candidates[1:4]
            ],
            page_number=page_number,
        )
        merged_score = self._quality_score(merged_text, merged_blocks)
        if merged_score >= float(best.get("score", 0.0)) * 0.97 and merged_text.strip():
            return (
                merged_text,
                merged_blocks,
                f"{str(best.get('provider') or 'unknown')}+handwrite",
                f"{str(best.get('model') or '')}+hw",
            )
        return (
            str(best.get("text", "")),
            [x for x in (best.get("blocks") or []) if isinstance(x, OCRLayoutBlock)],
            str(best.get("provider", base_provider)),
            str(best.get("model", base_model)),
        )

    def _should_try_handwriting_refine(
        self,
        image_bytes: bytes,
        base_text: str,
        base_blocks: Sequence[OCRLayoutBlock],
    ) -> bool:
        if not self.enable_handwritten_refine or self.max_handwriting_variants <= 0:
            return False
        score = self._quality_score(base_text, base_blocks)
        if score < 0.78:
            return True
        if len(base_text or "") < 160:
            return True
        likelihood = self._estimate_handwriting_likelihood(image_bytes)
        return likelihood >= 0.52

    def _estimate_handwriting_likelihood(self, image_bytes: bytes) -> float:
        if Image is None or np is None:
            return 0.0
        try:
            image = Image.open(io.BytesIO(image_bytes)).convert("L")
            w, h = image.size
            # Keep computation bounded for mobile/CPU.
            if w * h > 1_400_000:
                scale = (1_400_000.0 / float(w * h)) ** 0.5
                nw = max(220, int(round(w * scale)))
                nh = max(220, int(round(h * scale)))
                resampling = getattr(getattr(Image, "Resampling", Image), "BILINEAR", None)
                if resampling is None:
                    resampling = getattr(Image, "BICUBIC", 3)
                image = image.resize((nw, nh), resample=resampling)
            arr = np.array(image, dtype="float32")
            dark_ratio = float(np.mean(arr < 185.0))
            if arr.shape[0] < 4 or arr.shape[1] < 4:
                return 0.0
            gx = np.abs(np.diff(arr, axis=1))
            gy = np.abs(np.diff(arr, axis=0))
            edge_density = float((np.mean(gx > 24.0) + np.mean(gy > 24.0)) / 2.0)
            row_ink = np.mean(arr < 170.0, axis=1)
            row_var = float(np.var(row_ink))
            # Handwriting tends to have higher edge complexity and irregular line-ink variance.
            score = (
                0.45 * min(1.0, edge_density * 3.2)
                + 0.35 * min(1.0, dark_ratio / 0.26)
                + 0.20 * min(1.0, row_var * 9.0)
            )
            return float(max(0.0, min(1.0, score)))
        except Exception:
            return 0.0

    def _build_handwriting_variants(self, image_bytes: bytes) -> List[Dict[str, Any]]:
        if Image is None:
            return []
        try:
            image = Image.open(io.BytesIO(image_bytes))
            image.load()
            gray = image.convert("L")
            gray = self._deskew_small_angles(gray)
            variants: List[Dict[str, Any]] = []

            # Variant 1: contrast + sharpen + upscale.
            v1 = gray
            if ImageOps is not None:
                v1 = ImageOps.autocontrast(v1, cutoff=1)
            if ImageEnhance is not None:
                v1 = ImageEnhance.Contrast(v1).enhance(1.45)
                v1 = ImageEnhance.Sharpness(v1).enhance(1.32)
            v1 = self._resize_for_handwritten(v1, scale=2.1)
            variants.append({"name": "hw_contrast_up", "bytes": self._image_to_png(v1)})

            # Variant 2: adaptive threshold for uneven pen pressure/shadows.
            v2 = self._adaptive_threshold_variant(gray, blur_radius=9.0, offset=11.0)
            v2 = self._resize_for_handwritten(v2, scale=2.0)
            variants.append({"name": "hw_adaptive_bw", "bytes": self._image_to_png(v2)})

            # Variant 3: thicker strokes after binarization.
            v3 = self._adaptive_threshold_variant(gray, blur_radius=6.0, offset=9.0)
            if ImageFilter is not None:
                v3 = v3.filter(ImageFilter.MaxFilter(size=3)).filter(ImageFilter.MedianFilter(size=3))
            v3 = self._resize_for_handwritten(v3, scale=2.2)
            variants.append({"name": "hw_thick_strokes", "bytes": self._image_to_png(v3)})

            # Variant 4: inversion-safe path (for light ink or photographed paper).
            inv = ImageOps.invert(gray) if ImageOps is not None else gray
            v4 = self._adaptive_threshold_variant(inv, blur_radius=8.0, offset=10.0)
            if ImageOps is not None:
                v4 = ImageOps.invert(v4)
            v4 = self._resize_for_handwritten(v4, scale=1.9)
            variants.append({"name": "hw_invert_adaptive", "bytes": self._image_to_png(v4)})

            return [row for row in variants if bytes(row.get("bytes") or b"")]
        except Exception:
            return []

    def _deskew_small_angles(self, gray_image: Any) -> Any:
        if Image is None or np is None:
            return gray_image
        try:
            candidates = [-2.0, -1.0, 0.0, 1.0, 2.0]
            best_img = gray_image
            best_score = -1.0
            for angle in candidates:
                rotated = gray_image.rotate(
                    angle,
                    resample=getattr(getattr(Image, "Resampling", Image), "BICUBIC", 3),
                    expand=False,
                    fillcolor=255,
                )
                arr = np.array(rotated, dtype="float32")
                ink = np.mean(arr < 175.0, axis=1)
                score = float(np.var(ink))
                if score > best_score:
                    best_score = score
                    best_img = rotated
            return best_img
        except Exception:
            return gray_image

    def _adaptive_threshold_variant(self, gray_image: Any, *, blur_radius: float, offset: float) -> Any:
        if Image is None:
            return gray_image
        if np is None or ImageFilter is None:
            return gray_image
        try:
            smooth = gray_image.filter(ImageFilter.GaussianBlur(radius=max(1.0, blur_radius)))
            arr = np.array(gray_image, dtype="float32")
            local = np.array(smooth, dtype="float32")
            threshold = local - float(offset)
            bw = np.where(arr < threshold, 0, 255).astype("uint8")
            return Image.fromarray(bw)
        except Exception:
            return gray_image

    def _resize_for_handwritten(self, image: Any, *, scale: float) -> Any:
        if Image is None or scale <= 1.0:
            return image
        try:
            resampling = getattr(getattr(Image, "Resampling", Image), "LANCZOS", None)
            if resampling is None:
                resampling = getattr(Image, "BICUBIC", 3)
            nw = max(96, int(round(image.width * scale)))
            nh = max(96, int(round(image.height * scale)))
            return image.resize((nw, nh), resample=resampling)
        except Exception:
            return image

    def _image_to_png(self, image: Any) -> bytes:
        out = io.BytesIO()
        try:
            image.save(out, format="PNG")
            return out.getvalue()
        except Exception:
            return b""

    def _build_region_variants(self, image_bytes: bytes) -> List[Dict[str, Any]]:
        if Image is None:
            return []
        try:
            image = Image.open(io.BytesIO(image_bytes))
            image.load()
            if image.mode not in ("L", "RGB"):
                image = image.convert("RGB")
            width, height = image.size
            if width < 300 or height < 300:
                return []

            specs = [
                ("top_band", 0.0, 0.0, 1.0, 0.30, 1.8),
                ("left_main", 0.0, 0.16, 0.56, 1.0, 1.45),
                ("right_main", 0.44, 0.16, 1.0, 1.0, 1.45),
                ("center_body", 0.08, 0.14, 0.92, 0.92, 1.35),
                ("lower_band", 0.0, 0.50, 1.0, 1.0, 1.4),
            ]
            variants: List[Dict[str, Any]] = []
            for name, x1f, y1f, x2f, y2f, scale in specs:
                x1 = int(max(0, min(width - 2, round(width * x1f))))
                y1 = int(max(0, min(height - 2, round(height * y1f))))
                x2 = int(max(x1 + 2, min(width, round(width * x2f))))
                y2 = int(max(y1 + 2, min(height, round(height * y2f))))
                if (x2 - x1) < 120 or (y2 - y1) < 90:
                    continue
                crop = image.crop((x1, y1, x2, y2))
                enhanced = self._enhance_crop_for_ocr(crop, scale=float(scale))
                out = io.BytesIO()
                enhanced.save(out, format="PNG")
                variants.append(
                    {
                        "name": name,
                        "x1": x1,
                        "y1": y1,
                        "scale": float(scale),
                        "bytes": out.getvalue(),
                    }
                )
            return variants
        except Exception:
            return []

    def _enhance_crop_for_ocr(self, image: Any, *, scale: float) -> Any:
        if Image is None:
            return image
        gray = image.convert("L")
        if ImageOps is not None:
            gray = ImageOps.autocontrast(gray, cutoff=2)
        if ImageEnhance is not None:
            gray = ImageEnhance.Contrast(gray).enhance(1.25)
            gray = ImageEnhance.Sharpness(gray).enhance(1.18)
        if ImageFilter is not None:
            gray = gray.filter(ImageFilter.MedianFilter(size=3))
        if np is not None:
            arr = np.array(gray)
            p35 = float(np.percentile(arr, 35))
            p70 = float(np.percentile(arr, 70))
            threshold = int(max(70, min(210, 0.44 * p35 + 0.56 * p70)))
            bw = (arr > threshold) * 255
            gray = Image.fromarray(bw.astype("uint8"))
        if scale > 1.01:
            resampling = getattr(getattr(Image, "Resampling", Image), "LANCZOS", None)
            if resampling is None:
                resampling = getattr(Image, "BICUBIC", 3)
            new_w = max(64, int(round(gray.width * scale)))
            new_h = max(64, int(round(gray.height * scale)))
            gray = gray.resize((new_w, new_h), resample=resampling)
        return gray

    def _map_blocks_to_page(
        self,
        blocks: Sequence[OCRLayoutBlock],
        *,
        offset_x: int,
        offset_y: int,
        scale: float,
        page_number: int,
    ) -> List[OCRLayoutBlock]:
        if not blocks:
            return []
        inv_scale = 1.0 / max(0.15, float(scale))
        mapped: List[OCRLayoutBlock] = []
        for idx, block in enumerate(blocks, start=1):
            x1 = int(round(offset_x + int(block.bbox[0]) * inv_scale))
            y1 = int(round(offset_y + int(block.bbox[1]) * inv_scale))
            x2 = int(round(offset_x + int(block.bbox[2]) * inv_scale))
            y2 = int(round(offset_y + int(block.bbox[3]) * inv_scale))
            mapped.append(
                OCRLayoutBlock(
                    text=str(block.text or ""),
                    bbox=[max(0, x1), max(0, y1), max(0, x2), max(0, y2)],
                    confidence=float(max(0.0, min(1.0, float(block.confidence)))),
                    block_id=idx,
                    page_number=int(page_number),
                )
            )
        return mapped

    def _merge_text_candidates(self, base_text: str, extra_texts: Sequence[str]) -> str:
        merged: List[str] = []
        seen: set[str] = set()

        def _emit(raw: str) -> None:
            for line in re.split(r"\r?\n+", str(raw or "")):
                clean = re.sub(r"\s+", " ", line).strip()
                if len(clean) < 3:
                    continue
                key = clean.lower()
                if key in seen:
                    continue
                seen.add(key)
                merged.append(clean)

        _emit(base_text)
        for chunk in extra_texts:
            _emit(chunk)
        return "\n".join(merged).strip()

    def _merge_block_candidates(
        self,
        *,
        best_blocks: Sequence[OCRLayoutBlock],
        extra_block_sets: Sequence[Sequence[OCRLayoutBlock]],
        page_number: int,
    ) -> List[OCRLayoutBlock]:
        merged: List[OCRLayoutBlock] = []
        seen: set[tuple[str, int, int]] = set()
        for group in [best_blocks, *extra_block_sets]:
            for block in group:
                key = (
                    re.sub(r"\s+", " ", str(block.text or "")).strip().lower(),
                    int(block.bbox[0] // 8),
                    int(block.bbox[1] // 8),
                )
                if not key[0] or key in seen:
                    continue
                seen.add(key)
                merged.append(
                    OCRLayoutBlock(
                        text=str(block.text or ""),
                        bbox=[int(block.bbox[0]), int(block.bbox[1]), int(block.bbox[2]), int(block.bbox[3])],
                        confidence=float(max(0.0, min(1.0, float(block.confidence)))),
                        block_id=len(merged) + 1,
                        page_number=int(page_number),
                    )
                )
                if len(merged) >= 2200:
                    return merged
        return merged

    def _region_provider_preference(self) -> Tuple[str, ...]:
        preferred = [
            p for p in self.provider_preference if p in ("tesseract_best", "tesseract", "paddle", "heuristic")
        ]
        if not preferred:
            return ("tesseract_best", "tesseract", "heuristic")
        return tuple(preferred[:3])

    def _handwriting_provider_preference(self) -> Tuple[str, ...]:
        preferred = [
            p for p in self.provider_preference if p in ("tesseract_best", "tesseract", "heuristic")
        ]
        if not preferred:
            return ("tesseract_best", "tesseract", "heuristic")
        return tuple(preferred[:3])

    def _select_best_provider(
        self,
        prepared_image_bytes: bytes,
        original_image_bytes: bytes,
        *,
        page_number: int,
        provider_preference: Sequence[str] | None = None,
        handwritten_hint: bool = False,
    ) -> Tuple[str, List[OCRLayoutBlock], str, str]:
        candidates: List[Tuple[float, str, str, str, List[OCRLayoutBlock]]] = []
        tesseract_cli_ok = bool(shutil.which("tesseract"))
        active_preference = tuple(str(p).lower() for p in (provider_preference or self.provider_preference))
        for provider_name in active_preference:
            try:
                if provider_name == "tesseract_best" and pytesseract is not None and Image is not None:
                    text, blocks, model = self._run_tesseract_best(
                        prepared_image_bytes,
                        page_number=page_number,
                        handwritten_hint=handwritten_hint,
                    )
                    score = self._quality_score(text, blocks)
                    candidates.append((score, text, "tesseract", model, blocks))
                elif provider_name == "tesseract_best" and tesseract_cli_ok:
                    text, blocks, model = self._run_tesseract_best_cli(
                        prepared_image_bytes,
                        page_number=page_number,
                        handwritten_hint=handwritten_hint,
                    )
                    score = self._quality_score(text, blocks)
                    candidates.append((score, text, "tesseract_cli", model, blocks))
                elif provider_name == "tesseract" and pytesseract is not None and Image is not None:
                    config = "--oem 1 --psm 4" if handwritten_hint else "--oem 1 --psm 6"
                    text, blocks = self._run_tesseract(
                        prepared_image_bytes,
                        page_number=page_number,
                        config=config,
                    )
                    score = self._quality_score(text, blocks)
                    candidates.append((score, text, "tesseract", "tesseract_default", blocks))
                elif provider_name == "tesseract" and tesseract_cli_ok:
                    cfg = ["--oem", "1", "--psm", "4"] if handwritten_hint else None
                    text, blocks = self._run_tesseract_cli(
                        prepared_image_bytes,
                        page_number=page_number,
                        config_args=cfg,
                    )
                    score = self._quality_score(text, blocks)
                    candidates.append((score, text, "tesseract_cli", "tesseract_cli_psm6", blocks))
                elif provider_name == "paddle":
                    text, blocks = self._heuristic_ocr(original_image_bytes, page_number=page_number)
                    score = self._quality_score(text, blocks) * 0.92
                    candidates.append((score, text, "paddle_stub", "paddle_stub_heuristic", blocks))
                elif provider_name == "heuristic":
                    text, blocks = self._heuristic_ocr(original_image_bytes, page_number=page_number)
                    score = self._quality_score(text, blocks) * 0.86
                    candidates.append((score, text, "heuristic", "heuristic_ascii_fallback", blocks))
            except Exception as exc:
                self.telemetry.log_event(
                    "ocr_provider_error",
                    {"provider": provider_name, "error_type": type(exc).__name__, "reason": str(exc)[:300]},
                )

        if not candidates:
            text, blocks = self._heuristic_ocr(original_image_bytes, page_number=page_number)
            return text, blocks, "heuristic", "heuristic_ascii_fallback"

        candidates.sort(key=lambda row: float(row[0]), reverse=True)
        _, best_text, provider, model, best_blocks = candidates[0]
        if not best_text.strip():
            text, blocks = self._heuristic_ocr(original_image_bytes, page_number=page_number)
            return text, blocks, "heuristic", "heuristic_ascii_fallback"
        return best_text, best_blocks, provider, model

    def _run_tesseract_best(
        self,
        image_bytes: bytes,
        *,
        page_number: int,
        handwritten_hint: bool = False,
    ) -> Tuple[str, List[OCRLayoutBlock], str]:
        # Best local model path: LSTM engine (OEM 1) with multiple segmentation attempts.
        configs = [
            ("--oem 1 --psm 6 -c preserve_interword_spaces=1", "tesseract_lstm_psm6"),
            ("--oem 1 --psm 11 -c preserve_interword_spaces=1", "tesseract_lstm_psm11"),
            ("--oem 3 --psm 6 -c preserve_interword_spaces=1", "tesseract_auto_psm6"),
        ]
        if handwritten_hint:
            configs.extend(
                [
                    ("--oem 1 --psm 4 -c preserve_interword_spaces=1 --dpi 300", "tesseract_hw_psm4"),
                    ("--oem 1 --psm 13 -c preserve_interword_spaces=1 --dpi 300", "tesseract_hw_psm13"),
                    ("--oem 3 --psm 11 -c preserve_interword_spaces=1 --dpi 300", "tesseract_hw_psm11"),
                ]
            )
        best_score = -1.0
        best_text = ""
        best_blocks: List[OCRLayoutBlock] = []
        best_model = "tesseract_lstm_psm6"
        for config, model_name in configs:
            text, blocks = self._run_tesseract(image_bytes, page_number=page_number, config=config)
            score = self._quality_score(text, blocks)
            if score > best_score:
                best_score = score
                best_text = text
                best_blocks = blocks
                best_model = model_name
        return best_text, best_blocks, best_model

    def _run_tesseract_best_cli(
        self,
        image_bytes: bytes,
        *,
        page_number: int,
        handwritten_hint: bool = False,
    ) -> Tuple[str, List[OCRLayoutBlock], str]:
        configs: List[Tuple[List[str], str]] = [
            (["--oem", "1", "--psm", "6"], "tesseract_cli_lstm_psm6"),
            (["--oem", "1", "--psm", "11"], "tesseract_cli_lstm_psm11"),
            (["--oem", "3", "--psm", "6"], "tesseract_cli_auto_psm6"),
        ]
        if handwritten_hint:
            configs.extend(
                [
                    (
                        [
                            "--oem",
                            "1",
                            "--psm",
                            "4",
                            "--dpi",
                            "300",
                            "-c",
                            "preserve_interword_spaces=1",
                            "-c",
                            "tessedit_do_invert=0",
                        ],
                        "tesseract_cli_hw_psm4",
                    ),
                    (
                        [
                            "--oem",
                            "1",
                            "--psm",
                            "13",
                            "--dpi",
                            "300",
                            "-c",
                            "preserve_interword_spaces=1",
                        ],
                        "tesseract_cli_hw_psm13",
                    ),
                    (
                        [
                            "--oem",
                            "3",
                            "--psm",
                            "11",
                            "--dpi",
                            "300",
                            "-c",
                            "preserve_interword_spaces=1",
                        ],
                        "tesseract_cli_hw_psm11",
                    ),
                ]
            )
        best_score = -1.0
        best_text = ""
        best_blocks: List[OCRLayoutBlock] = []
        best_model = "tesseract_cli_lstm_psm6"
        for config_args, model_name in configs:
            text, blocks = self._run_tesseract_cli(
                image_bytes,
                page_number=page_number,
                config_args=config_args,
            )
            score = self._quality_score(text, blocks)
            if score > best_score:
                best_score = score
                best_text = text
                best_blocks = blocks
                best_model = model_name
        return best_text, best_blocks, best_model

    def _run_tesseract_cli(
        self,
        image_bytes: bytes,
        *,
        page_number: int,
        config_args: List[str] | None = None,
        lang: str = "eng",
    ) -> Tuple[str, List[OCRLayoutBlock]]:
        if not shutil.which("tesseract"):
            return "", []
        cfg = [str(x) for x in (config_args or ["--oem", "1", "--psm", "6"]) if str(x)]
        with tempfile.NamedTemporaryFile(suffix=".png") as tmp:
            tmp.write(image_bytes)
            tmp.flush()
            text_cmd = ["tesseract", tmp.name, "stdout", "-l", lang, *cfg]
            text_run = subprocess.run(
                text_cmd,
                capture_output=True,
                check=False,
                timeout=18.0,
            )
            text = (
                text_run.stdout.decode("utf-8", errors="ignore")
                if text_run.returncode == 0
                else ""
            )

            tsv_cmd = ["tesseract", tmp.name, "stdout", "-l", lang, *cfg, "tsv"]
            tsv_run = subprocess.run(
                tsv_cmd,
                capture_output=True,
                check=False,
                timeout=18.0,
            )
            blocks: List[OCRLayoutBlock] = []
            if tsv_run.returncode == 0:
                lines = tsv_run.stdout.decode("utf-8", errors="ignore").splitlines()
                for idx, line in enumerate(lines[1:], start=1):
                    parts = line.split("\t")
                    if len(parts) < 12:
                        continue
                    token = str(parts[11] or "").strip()
                    if not token:
                        continue
                    try:
                        conf = max(0.0, min(1.0, float(parts[10]) / 100.0))
                    except Exception:
                        conf = 0.0
                    try:
                        x = int(float(parts[6]))
                        y = int(float(parts[7]))
                        w = int(float(parts[8]))
                        h = int(float(parts[9]))
                    except Exception:
                        x = y = w = h = 0
                    blocks.append(
                        OCRLayoutBlock(
                            text=token,
                            bbox=[x, y, x + max(0, w), y + max(0, h)],
                            confidence=conf,
                            block_id=idx,
                            page_number=int(page_number),
                        )
                    )
            if not text.strip() and blocks:
                text = " ".join(block.text for block in blocks)
            return text, blocks

    def _run_tesseract(
        self,
        image_bytes: bytes,
        *,
        page_number: int,
        config: str = "--oem 1 --psm 6",
        lang: str = "eng",
    ) -> Tuple[str, List[OCRLayoutBlock]]:
        if Image is None or pytesseract is None:
            return "", []
        image = Image.open(io.BytesIO(image_bytes))
        text = pytesseract.image_to_string(image, lang=lang, config=config) or ""
        data = pytesseract.image_to_data(image, lang=lang, config=config, output_type=pytesseract.Output.DICT)
        blocks: List[OCRLayoutBlock] = []
        n = len(data.get("text", []))
        for idx in range(n):
            token = str(data["text"][idx] or "").strip()
            if not token:
                continue
            conf_raw = str(data.get("conf", ["0"] * n)[idx])
            try:
                conf = max(0.0, min(1.0, float(conf_raw) / 100.0))
            except Exception:
                conf = 0.0
            x = int(data.get("left", [0] * n)[idx])
            y = int(data.get("top", [0] * n)[idx])
            w = int(data.get("width", [0] * n)[idx])
            h = int(data.get("height", [0] * n)[idx])
            blocks.append(
                OCRLayoutBlock(
                    text=token,
                    bbox=[x, y, x + max(0, w), y + max(0, h)],
                    confidence=conf,
                    block_id=idx + 1,
                    page_number=int(page_number),
                )
            )
        if not text.strip() and blocks:
            text = " ".join(block.text for block in blocks)
        return text, blocks

    def _looks_like_binary_payload(self, blob: bytes) -> bool:
        sample = bytes((blob or b"")[:16000])
        if not sample:
            return True
        if sample.startswith((b"\x89PNG\r\n\x1a\n", b"\xff\xd8\xff")):
            return True
        if sample.startswith(b"%PDF"):
            text_chars = sum(1 for b in sample if 32 <= b <= 126 or b in (9, 10, 13))
            ratio = text_chars / max(1, len(sample))
            low = sample[:4000].decode("latin-1", errors="ignore").lower()
            if ratio < 0.87 or ("stream" in low and "obj" in low):
                return True
            return False
        if sample.startswith((b"GIF87a", b"GIF89a", b"BM", b"II*\x00", b"MM\x00*")):
            return True
        text_chars = sum(1 for b in sample if 32 <= b <= 126 or b in (9, 10, 13))
        ratio = text_chars / max(1, len(sample))
        return ratio < 0.84

    def _looks_like_gibberish_text(self, text: str) -> bool:
        raw = str(text or "")
        if not raw.strip():
            return True
        sample = raw[:5000]
        low = sample.lower()
        marker_hits = sum(
            marker in low
            for marker in (
                "ihdr",
                "idat",
                "xmpmeta",
                "pdfcpu",
                "xml:com.adobe.xmp",
            )
        )
        if marker_hits >= 2 or ("ihdr" in low and "idat" in low):
            return True
        total = max(1, len(sample))
        letters = sum(1 for ch in sample if ch.isalpha())
        digits = sum(1 for ch in sample if ch.isdigit())
        spaces = sum(1 for ch in sample if ch.isspace())
        punct = sum(1 for ch in sample if ch in r"""!@#$%^&*()_+-=[]{};:'",.<>/?\|`~""")
        alpha_ratio = letters / total
        readable_ratio = (letters + digits + spaces) / total
        punct_ratio = punct / total
        if readable_ratio < 0.50:
            return True
        if alpha_ratio < 0.18 and punct_ratio > 0.22:
            return True
        odd_tokens = len(re.findall(r"[A-Za-z0-9]{1,3}[^\sA-Za-z0-9]{2,}[A-Za-z0-9]{1,3}", sample))
        if odd_tokens >= 10 and readable_ratio < 0.7:
            return True
        return False

    def _heuristic_ocr(self, image_bytes: bytes, *, page_number: int) -> Tuple[str, List[OCRLayoutBlock]]:
        if self._looks_like_binary_payload(image_bytes):
            return "", []
        decoded = image_bytes.decode("utf-8", errors="ignore")
        decoded = re.sub(r"[^\x20-\x7E\n\t]", " ", decoded)
        decoded = re.sub(r"\s+", " ", decoded).strip()
        if not decoded or self._looks_like_gibberish_text(decoded):
            return "", []
        words = decoded.split()
        blocks: List[OCRLayoutBlock] = []
        cursor_x = 8
        for idx, token in enumerate(words[:120], start=1):
            width = max(18, 7 * len(token))
            blocks.append(
                OCRLayoutBlock(
                    text=token,
                    bbox=[cursor_x, 10, cursor_x + width, 30],
                    confidence=0.34,
                    block_id=idx,
                    page_number=int(page_number),
                )
            )
            cursor_x += width + 6
        return decoded, blocks

    def _quality_score(self, raw_text: str, blocks: Sequence[OCRLayoutBlock]) -> float:
        text = str(raw_text or "")
        if not text.strip():
            return 0.0
        if self._looks_like_gibberish_text(text):
            return 0.01
        token_count = len([x for x in re.split(r"\s+", text) if x.strip()])
        math_hits = len(re.findall(r"[=+\-*/^]|sqrt|sin|cos|tan|log|ln", text, flags=re.IGNORECASE))
        block_conf = self._aggregate_confidence(blocks)
        length_score = min(1.0, len(text) / 1000.0)
        token_score = min(1.0, token_count / 140.0)
        math_score = min(1.0, math_hits / 24.0)
        return 0.35 * block_conf + 0.25 * length_score + 0.20 * token_score + 0.20 * math_score

    def _aggregate_confidence(self, blocks: Sequence[OCRLayoutBlock]) -> float:
        if not blocks:
            return 0.0
        weights = [max(0.05, float(block.confidence)) for block in blocks]
        return float(max(0.0, min(1.0, sum(weights) / len(weights))))

    def _cluster_blocks(self, blocks: Sequence[OCRLayoutBlock], *, y_tolerance: int = 14) -> List[Dict[str, Any]]:
        if not blocks:
            return []
        ordered = sorted(blocks, key=lambda b: (int(b.bbox[1]), int(b.bbox[0])))
        clusters: List[List[OCRLayoutBlock]] = []
        for block in ordered:
            if not clusters:
                clusters.append([block])
                continue
            prev = clusters[-1][-1]
            if abs(int(block.bbox[1]) - int(prev.bbox[1])) <= y_tolerance:
                clusters[-1].append(block)
            else:
                clusters.append([block])

        out: List[Dict[str, Any]] = []
        for idx, group in enumerate(clusters, start=1):
            text = " ".join(item.text for item in group).strip()
            x1 = min(item.bbox[0] for item in group)
            y1 = min(item.bbox[1] for item in group)
            x2 = max(item.bbox[2] for item in group)
            y2 = max(item.bbox[3] for item in group)
            conf = sum(float(item.confidence) for item in group) / max(1, len(group))
            out.append(
                {
                    "block_id": idx,
                    "text": text,
                    "bbox": [int(x1), int(y1), int(x2), int(y2)],
                    "confidence": float(max(0.0, min(1.0, conf))),
                    "page_number": int(group[0].page_number),
                }
            )
        return out
