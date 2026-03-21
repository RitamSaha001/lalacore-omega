from __future__ import annotations

import base64
import binascii
import mimetypes
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Literal, Sequence


InputType = Literal["auto", "text", "image", "pdf", "mixed"]
DetectedType = Literal["text", "image", "pdf", "mixed", "unknown"]


_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".bmp", ".gif", ".webp", ".tif", ".tiff"}
_PDF_EXTS = {".pdf"}


@dataclass(slots=True)
class IntakePayload:
    input_type: DetectedType
    text: str = ""
    image_bytes: bytes | None = None
    pdf_bytes: bytes | None = None
    files: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class MultimodalIntake:
    """
    Multimodal input normalization and type detection.
    """

    def __init__(self, *, max_input_bytes: int = 12_000_000) -> None:
        self.max_input_bytes = int(max(1024, max_input_bytes))

    def detect_type(self, input_data: Any, input_type: InputType = "auto") -> DetectedType:
        if input_type != "auto":
            return self._normalize_explicit_type(input_type)

        if input_data is None:
            return "unknown"

        if isinstance(input_data, (bytes, bytearray)):
            return self._detect_binary_type(bytes(input_data))

        if isinstance(input_data, str):
            candidate = input_data.strip()
            if not candidate:
                return "unknown"
            try:
                path = Path(candidate)
                if path.exists() and path.is_file():
                    return self._detect_path_type(path)
            except (OSError, ValueError):
                # Long/plain text prompts may be invalid as filesystem paths.
                pass
            return "text"

        if isinstance(input_data, dict):
            return self._detect_mapping_type(input_data)

        if isinstance(input_data, Sequence) and not isinstance(input_data, (str, bytes, bytearray)):
            seen = {self.detect_type(item, "auto") for item in input_data}
            seen.discard("unknown")
            if not seen:
                return "unknown"
            if len(seen) == 1:
                return seen.pop()  # type: ignore[return-value]
            return "mixed"

        return "unknown"

    def normalize(self, input_data: Any, input_type: InputType = "auto") -> IntakePayload:
        detected_type = self.detect_type(input_data, input_type)

        if detected_type == "text":
            text = self._extract_text(input_data)
            return IntakePayload(input_type="text", text=text, metadata={"detected_type": detected_type})

        if detected_type == "image":
            image_bytes = self._extract_first_image_bytes(input_data)
            return IntakePayload(
                input_type="image",
                image_bytes=image_bytes,
                files=[{"type": "image", "size": len(image_bytes or b"")}],
                metadata={"detected_type": detected_type},
            )

        if detected_type == "pdf":
            pdf_bytes = self._extract_pdf_bytes(input_data)
            return IntakePayload(
                input_type="pdf",
                pdf_bytes=pdf_bytes,
                files=[{"type": "pdf", "size": len(pdf_bytes or b"")}],
                metadata={"detected_type": detected_type},
            )

        if detected_type == "mixed":
            return self._normalize_mixed(input_data)

        return IntakePayload(input_type="unknown", metadata={"detected_type": "unknown"})

    def _normalize_mixed(self, input_data: Any) -> IntakePayload:
        text_parts: List[str] = []
        image_bytes: bytes | None = None
        pdf_bytes: bytes | None = None
        files: List[Dict[str, Any]] = []

        if isinstance(input_data, dict):
            if "text" in input_data:
                text_parts.append(str(input_data.get("text") or "").strip())
            if "image" in input_data or "image_bytes" in input_data:
                image_bytes = self._coerce_to_bytes(input_data.get("image") or input_data.get("image_bytes"))
                if image_bytes is not None:
                    files.append({"type": "image", "size": len(image_bytes)})
            if "pdf" in input_data or "pdf_bytes" in input_data:
                pdf_bytes = self._coerce_to_bytes(input_data.get("pdf") or input_data.get("pdf_bytes"))
                if pdf_bytes is not None:
                    files.append({"type": "pdf", "size": len(pdf_bytes)})

        if isinstance(input_data, Sequence) and not isinstance(input_data, (str, bytes, bytearray, dict)):
            for item in input_data:
                kind = self.detect_type(item, "auto")
                if kind == "text":
                    text_parts.append(self._extract_text(item))
                elif kind == "image" and image_bytes is None:
                    image_bytes = self._extract_first_image_bytes(item)
                    if image_bytes is not None:
                        files.append({"type": "image", "size": len(image_bytes)})
                elif kind == "pdf" and pdf_bytes is None:
                    pdf_bytes = self._extract_pdf_bytes(item)
                    if pdf_bytes is not None:
                        files.append({"type": "pdf", "size": len(pdf_bytes)})

        return IntakePayload(
            input_type="mixed",
            text="\n".join(part for part in text_parts if part),
            image_bytes=image_bytes,
            pdf_bytes=pdf_bytes,
            files=files,
            metadata={"detected_type": "mixed"},
        )

    def _detect_mapping_type(self, payload: Dict[str, Any]) -> DetectedType:
        if payload.get("input_type") in {"text", "image", "pdf", "mixed"}:
            return payload["input_type"]

        has_text = bool(payload.get("text"))
        has_image = payload.get("image") is not None or payload.get("image_bytes") is not None
        has_pdf = payload.get("pdf") is not None or payload.get("pdf_bytes") is not None

        if has_text and not has_image and not has_pdf:
            return "text"
        if has_image and not has_pdf and not has_text:
            return "image"
        if has_pdf and not has_image and not has_text:
            return "pdf"
        if has_text or has_image or has_pdf:
            return "mixed"

        return "unknown"

    def _detect_path_type(self, path: Path) -> DetectedType:
        suffix = path.suffix.lower()
        if suffix in _PDF_EXTS:
            return "pdf"
        if suffix in _IMAGE_EXTS:
            return "image"

        guessed, _ = mimetypes.guess_type(str(path))
        guessed = str(guessed or "").lower()
        if guessed == "application/pdf":
            return "pdf"
        if guessed.startswith("image/"):
            return "image"
        return "text"

    def _detect_binary_type(self, blob: bytes) -> DetectedType:
        if blob.startswith(b"%PDF"):
            return "pdf"
        if blob.startswith(b"\x89PNG") or blob.startswith(b"\xff\xd8\xff"):
            return "image"
        return "text"

    def _extract_text(self, input_data: Any) -> str:
        if input_data is None:
            return ""
        if isinstance(input_data, str):
            try:
                path = Path(input_data)
                if path.exists() and path.is_file():
                    try:
                        return path.read_text(encoding="utf-8")
                    except Exception:
                        return ""
            except (OSError, ValueError):
                # Treat invalid/oversized path-like strings as plain text input.
                pass
            return input_data
        if isinstance(input_data, dict):
            return str(input_data.get("text") or "")
        if isinstance(input_data, (bytes, bytearray)):
            try:
                return bytes(input_data).decode("utf-8", errors="ignore")
            except Exception:
                return ""
        return str(input_data)

    def _extract_first_image_bytes(self, input_data: Any) -> bytes | None:
        if isinstance(input_data, dict):
            for key in ("image", "image_bytes", "image_path"):
                if key in input_data:
                    blob = self._coerce_to_bytes(input_data.get(key))
                    if blob is not None:
                        return blob
            images = input_data.get("images")
            if isinstance(images, Sequence) and images:
                for item in images:
                    blob = self._coerce_to_bytes(item)
                    if blob is not None:
                        return blob

        return self._coerce_to_bytes(input_data)

    def _extract_pdf_bytes(self, input_data: Any) -> bytes | None:
        if isinstance(input_data, dict):
            for key in ("pdf", "pdf_bytes", "pdf_path"):
                if key in input_data:
                    blob = self._coerce_to_bytes(input_data.get(key))
                    if blob is not None:
                        return blob
        return self._coerce_to_bytes(input_data)

    def _coerce_to_bytes(self, value: Any) -> bytes | None:
        if value is None:
            return None
        if isinstance(value, bytes):
            return self._bounded(value)
        if isinstance(value, bytearray):
            return self._bounded(bytes(value))
        if isinstance(value, str):
            candidate = value.strip()
            if candidate.startswith("data:") and ";base64," in candidate:
                encoded = candidate.split(";base64,", 1)[1]
                decoded = self._decode_base64(encoded)
                if decoded is not None:
                    return self._bounded(decoded)
            elif self._looks_like_base64(candidate):
                decoded = self._decode_base64(candidate)
                if decoded is not None and self._looks_binary_blob(decoded):
                    return self._bounded(decoded)

            try:
                path = Path(candidate)
                if path.exists() and path.is_file():
                    return self._bounded(path.read_bytes())
            except (OSError, ValueError):
                # Not a valid filesystem path; continue treating as raw text.
                pass
            return self._bounded(candidate.encode("utf-8", errors="ignore"))
        return None

    def _decode_base64(self, text: str) -> bytes | None:
        try:
            compact = "".join(text.split())
            if not compact:
                return None
            return base64.b64decode(compact, validate=False)
        except (binascii.Error, ValueError):
            return None

    def _looks_like_base64(self, text: str) -> bool:
        compact = "".join(text.split())
        if len(compact) < 40 or len(compact) % 4 != 0:
            return False
        allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=")
        return all(ch in allowed for ch in compact)

    def _looks_binary_blob(self, blob: bytes) -> bool:
        if not blob:
            return False
        if blob.startswith(b"%PDF"):
            return True
        if blob.startswith(b"\x89PNG") or blob.startswith(b"\xff\xd8\xff"):
            return True
        if blob.startswith(b"GIF87a") or blob.startswith(b"GIF89a"):
            return True
        if blob[:4] == b"RIFF" and b"WEBP" in blob[:16]:
            return True
        if len(blob) > 512:
            printable = sum(32 <= b <= 126 or b in (9, 10, 13) for b in blob[:512])
            return printable / 512 < 0.92
        return False

    def _bounded(self, blob: bytes) -> bytes:
        if len(blob) > self.max_input_bytes:
            raise ValueError(f"Input exceeds maximum allowed size ({self.max_input_bytes} bytes)")
        return blob

    def _normalize_explicit_type(self, input_type: InputType) -> DetectedType:
        if input_type in {"text", "image", "pdf", "mixed"}:
            return input_type
        return "unknown"
