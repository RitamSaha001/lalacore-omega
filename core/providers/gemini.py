import os
import httpx
from typing import Dict, Any
from dotenv import load_dotenv

from core.providers.base import BaseProvider
from core.bootstrap import get_key_manager

load_dotenv()


class GeminiProvider(BaseProvider):

    SUPPORTED_MODELS = {
        "flash": "gemini-2.5-flash",
        "pro": "gemini-2.5-pro",
        "flash-lite": "gemini-2.5-flash-lite",
        "2.0-flash": "gemini-2.0-flash"
    }

    def __init__(self, provider_id: int, name: str, model_key: str = "flash"):
        super().__init__(provider_id, name)
        self.key_manager = get_key_manager()

        keys = os.getenv("GEMINI_KEYS")
        if not keys:
            raise RuntimeError("GEMINI_KEYS not set in .env")

        parsed_keys = [k.strip() for k in keys.split(",") if k.strip()]
        self.key_manager.register_provider_keys("gemini", parsed_keys)

        if model_key not in self.SUPPORTED_MODELS:
            raise RuntimeError(f"Unsupported Gemini model key: {model_key}")

        self.model_name = self.SUPPORTED_MODELS[model_key]

        self.url = "https://generativelanguage.googleapis.com/v1/models"

    async def generate(
        self,
        question: str,
        subject: str,
        difficulty: int
    ) -> Dict[str, Any]:

        payload = {
            "contents": [
                {
                    "parts": [
                        {"text": question}
                    ]
                }
            ],
            "generationConfig": {
                "temperature": 0,
                "topP": 0.95,
                "topK": 40
            }
        }

        key = self.key_manager.get_key("gemini")
        url = (
            f"{self.url}/{self.model_name}:generateContent?key={key}"
        )

        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(url, json=payload)

        try:
            data = response.json()
        except Exception:
            self.key_manager.report_failure(key)
            raise RuntimeError(
                f"Gemini returned non-JSON response: {response.text}"
            )

        if "candidates" not in data:
            self.key_manager.report_failure(key)
            raise RuntimeError(f"Gemini error response: {data}")

        content = data["candidates"][0]["content"]["parts"][0]["text"]
        self.key_manager.report_success(key)

        return {
            "answer": content.strip(),
            "raw": content,
            "confidence": 0.5  # calibration layer later
        }
