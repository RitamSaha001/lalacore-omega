from __future__ import annotations

import base64
import io
import os
import wave
from typing import Any

from core.network.resilient_http import request_sync


class BilingualSttService:
    def __init__(self) -> None:
        self._provider = (os.getenv("STT_PROVIDER") or "").strip().lower()
        self._deepgram_key = (os.getenv("DEEPGRAM_API_KEY") or "").strip()
        self._openai_key = (os.getenv("OPENAI_API_KEY") or "").strip()
        if not self._provider:
            if self._deepgram_key:
                self._provider = "deepgram"
            elif self._openai_key:
                self._provider = "openai"
            else:
                self._provider = "disabled"

    @property
    def enabled(self) -> bool:
        return self._provider in {"deepgram", "openai"}

    def transcribe_bytes(
        self,
        audio_bytes: bytes,
        *,
        content_type: str = "audio/wav",
        language_hint: str = "bn,en",
        sample_rate: int = 16000,
        channels: int = 1,
    ) -> dict[str, Any]:
        if not audio_bytes:
            return {"text": "", "confidence": 0.0}
        if self._provider == "deepgram":
            return self._transcribe_deepgram(
                audio_bytes, content_type=content_type, language_hint=language_hint
            )
        if self._provider == "openai":
            return self._transcribe_openai(
                audio_bytes,
                content_type=content_type,
                language_hint=language_hint,
                sample_rate=sample_rate,
                channels=channels,
            )
        return {"text": "", "confidence": 0.0}

    def transcribe_base64(
        self,
        audio_b64: str,
        *,
        content_type: str = "audio/wav",
        language_hint: str = "bn,en",
        sample_rate: int = 16000,
        channels: int = 1,
    ) -> dict[str, Any]:
        try:
            audio_bytes = base64.b64decode(audio_b64)
        except Exception:
            return {"text": "", "confidence": 0.0}
        return self.transcribe_bytes(
            audio_bytes,
            content_type=content_type,
            language_hint=language_hint,
            sample_rate=sample_rate,
            channels=channels,
        )

    def _wrap_pcm_as_wav(
        self, audio_bytes: bytes, *, sample_rate: int, channels: int
    ) -> bytes:
        buffer = io.BytesIO()
        with wave.open(buffer, "wb") as wav:
            wav.setnchannels(max(1, channels))
            wav.setsampwidth(2)
            wav.setframerate(max(8000, sample_rate))
            wav.writeframes(audio_bytes)
        return buffer.getvalue()

    def _transcribe_deepgram(
        self,
        audio_bytes: bytes,
        *,
        content_type: str,
        language_hint: str,
    ) -> dict[str, Any]:
        if not self._deepgram_key:
            return {"text": "", "confidence": 0.0}
        url = "https://api.deepgram.com/v1/listen"
        params = {
            "model": os.getenv("DEEPGRAM_MODEL", "nova-2"),
            "language": os.getenv("DEEPGRAM_LANGUAGE", "multi"),
            "punctuate": "true",
            "smart_format": "true",
            "diarize": "false",
        }
        if language_hint:
            params["detect_language"] = "true"
        try:
            response = request_sync(
                "POST",
                url,
                params=params,
                data=audio_bytes,
                headers={
                    "Authorization": f"Token {self._deepgram_key}",
                    "Content-Type": content_type,
                },
                timeout_s=20.0,
            )
            if response.status_code >= 400:
                return {"text": "", "confidence": 0.0}
            payload = response.json()
            channels = payload.get("results", {}).get("channels", [])
            if not channels:
                return {"text": "", "confidence": 0.0}
            alternatives = channels[0].get("alternatives", [])
            if not alternatives:
                return {"text": "", "confidence": 0.0}
            transcript = alternatives[0].get("transcript") or ""
            confidence = alternatives[0].get("confidence") or 0.0
            return {"text": transcript, "confidence": confidence}
        except Exception:
            return {"text": "", "confidence": 0.0}

    def _transcribe_openai(
        self,
        audio_bytes: bytes,
        *,
        content_type: str,
        language_hint: str,
        sample_rate: int,
        channels: int,
    ) -> dict[str, Any]:
        if not self._openai_key:
            return {"text": "", "confidence": 0.0}
        url = "https://api.openai.com/v1/audio/transcriptions"
        model = os.getenv("OPENAI_STT_MODEL", "gpt-4o-mini-transcribe")
        prompt = (
            "Mixed Bengali and English lecture. Preserve math terms. "
            "Use Bengali or English as spoken."
        )
        files = {
            "file": ("audio.wav", audio_bytes, content_type),
        }
        if "audio/raw" in content_type or "pcm" in content_type:
            audio_bytes = self._wrap_pcm_as_wav(
                audio_bytes, sample_rate=sample_rate, channels=channels
            )
            files = {
                "file": ("audio.wav", audio_bytes, "audio/wav"),
            }
        data = {"model": model, "prompt": prompt}
        try:
            response = request_sync(
                "POST",
                url,
                files=files,
                data=data,
                headers={"Authorization": f"Bearer {self._openai_key}"},
                timeout_s=30.0,
            )
            if response.status_code >= 400:
                return {"text": "", "confidence": 0.0}
            payload = response.json()
            transcript = payload.get("text") or ""
            return {"text": transcript, "confidence": 0.0}
        except Exception:
            return {"text": "", "confidence": 0.0}
