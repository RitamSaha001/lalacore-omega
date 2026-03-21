import os
import httpx
import logging
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

logger = logging.getLogger(__name__)


class GroqProvider:
    """
    Async Groq LLM Provider
    Compatible with Arena + Orchestrator
    Uses GROQ_KEY from environment
    """

    BASE_URL = "https://api.groq.com/openai/v1/chat/completions"

    def __init__(self, model: str = "llama3-8b-8192"):
        self.model = model

        api_key = os.getenv("GROQ_KEY") or os.getenv("GROQ_KEYS")
        if api_key and "," in api_key:
            api_key = api_key.split(",")[0]

        if not api_key:
            raise ValueError("GROQ_KEY not found in environment")

        self.api_key = api_key.strip()

        if not self.api_key.startswith("gsk_"):
            raise ValueError("Invalid Groq API key format")

    async def generate(
        self,
        prompt: str,
        temperature: float = 0.2,
        max_tokens: int = 1000,
        system_prompt: Optional[str] = None,
    ) -> str:

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        messages = []

        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})

        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    self.BASE_URL,
                    headers=headers,
                    json=payload,
                )
        except httpx.RequestError as e:
            logger.error(f"Network error contacting Groq: {str(e)}")
            raise RuntimeError("Groq network failure") from e

        if response.status_code != 200:
            logger.error(f"Groq error {response.status_code}: {response.text}")
            raise RuntimeError(
                f"Groq API error {response.status_code}: {response.text}"
            )

        try:
            data = response.json()
        except Exception:
            raise RuntimeError("Failed to parse Groq JSON response")

        try:
            return data["choices"][0]["message"]["content"].strip()
        except (KeyError, IndexError, TypeError):
            logger.error(f"Unexpected Groq response format: {data}")
            raise RuntimeError("Invalid Groq response format")
