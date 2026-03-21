import os
import httpx
import logging
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

logger = logging.getLogger(__name__)


class GeminiProvider:
    """
    Async Gemini LLM Provider
    Compatible with Arena + Orchestrator
    Uses free-tier Gemini models
    """

    BASE_URL = "https://generativelanguage.googleapis.com/v1beta"

    def __init__(self, model: str = "models/gemini-1.5-flash"):
        self.model = model

        api_key = os.getenv("GEMINI_KEYS")
        if api_key and "," in api_key:
            api_key = api_key.split(",")[0]

        if not api_key:
            raise ValueError("GEMINI_KEYS not found in environment")

        self.api_key = api_key.strip()

        if not self.api_key.startswith("AIza"):
            raise ValueError("Invalid Gemini API key format")

    async def generate(
        self,
        prompt: str,
        temperature: float = 0.2,
        max_tokens: int = 1000,
        system_prompt: Optional[str] = None,
    ) -> str:

        url = f"{self.BASE_URL}/{self.model}:generateContent?key={self.api_key}"

        headers = {
            "Content-Type": "application/json",
        }

        contents = []

        if system_prompt:
            contents.append({
                "role": "user",
                "parts": [{"text": system_prompt}]
            })

        contents.append({
            "role": "user",
            "parts": [{"text": prompt}]
        })

        payload = {
            "contents": contents,
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": max_tokens,
            }
        }

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    url,
                    headers=headers,
                    json=payload,
                )
        except httpx.RequestError as e:
            logger.error(f"Network error contacting Gemini: {str(e)}")
            raise RuntimeError("Gemini network failure") from e

        if response.status_code != 200:
            logger.error(f"Gemini error {response.status_code}: {response.text}")
            raise RuntimeError(
                f"Gemini API error {response.status_code}: {response.text}"
            )

        try:
            data = response.json()
        except Exception:
            raise RuntimeError("Failed to parse Gemini JSON response")

        try:
            return (
                data["candidates"][0]
                ["content"]["parts"][0]
                ["text"]
                .strip()
            )
        except (KeyError, IndexError, TypeError):
            logger.error(f"Unexpected Gemini response format: {data}")
            raise RuntimeError("Invalid Gemini response format")
