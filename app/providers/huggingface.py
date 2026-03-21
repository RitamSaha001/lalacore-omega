import os
import httpx
import logging
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

logger = logging.getLogger(__name__)


class HuggingFaceProvider:
    """
    Async HuggingFace Inference API Provider
    Compatible with Arena + Orchestrator
    Uses HF_KEYS from environment
    """

    BASE_URL = "https://api-inference.huggingface.co/models"

    def __init__(self, model: str = "mistralai/Mistral-7B-Instruct-v0.2"):
        self.model = model

        api_key = os.getenv("HF_KEYS")
        if api_key and "," in api_key:
            api_key = api_key.split(",")[0]

        if not api_key:
            raise ValueError("HF_KEYS not found in environment")

        self.api_key = api_key.strip()

        if not self.api_key.startswith("hf_"):
            raise ValueError("Invalid HuggingFace API key format")

    async def generate(
        self,
        prompt: str,
        temperature: float = 0.2,
        max_tokens: int = 1000,
        system_prompt: Optional[str] = None,
    ) -> str:

        url = f"{self.BASE_URL}/{self.model}"

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        # HF expects full prompt string (not role-based structure)
        if system_prompt:
            full_prompt = f"{system_prompt}\n\nUser:\n{prompt}\n\nAssistant:"
        else:
            full_prompt = prompt

        payload = {
            "inputs": full_prompt,
            "parameters": {
                "temperature": temperature,
                "max_new_tokens": max_tokens,
                "return_full_text": False,
            },
        }

        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(
                    url,
                    headers=headers,
                    json=payload,
                )
        except httpx.RequestError as e:
            logger.error(f"Network error contacting HuggingFace: {str(e)}")
            raise RuntimeError("HuggingFace network failure") from e

        if response.status_code == 503:
            raise RuntimeError("HuggingFace model is loading (cold start)")

        if response.status_code != 200:
            logger.error(f"HuggingFace error {response.status_code}: {response.text}")
            raise RuntimeError(
                f"HuggingFace API error {response.status_code}: {response.text}"
            )

        try:
            data = response.json()
        except Exception:
            raise RuntimeError("Failed to parse HuggingFace JSON response")

        try:
            # HF usually returns list of dicts
            return data[0]["generated_text"].strip()
        except (KeyError, IndexError, TypeError):
            logger.error(f"Unexpected HuggingFace response format: {data}")
            raise RuntimeError("Invalid HuggingFace response format")
