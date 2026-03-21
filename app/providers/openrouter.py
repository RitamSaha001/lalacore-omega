import os
import json
import httpx
import logging
from dotenv import load_dotenv
from typing import Optional, List, Any

load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_FALLBACK_MODELS = (
    "meta-llama/llama-3.1-8b-instruct:free",
    "openai/gpt-4o-mini",
    "google/gemini-2.0-flash-001",
)


class OpenRouterProvider:
    """
    Async OpenRouter LLM Provider
    Compatible with Arena + Orchestrator
    Hardened for production usage inside LalaCore Omega
    """

    BASE_URL = "https://openrouter.ai/api/v1/chat/completions"

    def __init__(self, model: str):
        self.model = model

        # Strict single-key loading (no comma fallbacks)
        api_key = os.getenv("OPENROUTER_API_KEY") or os.getenv("OPENROUTER_KEYS")
        if api_key and "," in api_key:
            api_key = api_key.split(",")[0]

        if not api_key:
            raise ValueError("OPENROUTER_API_KEY not found in environment")

        # Clean whitespace / accidental newline issues
        self.api_key = api_key.strip()

        if not self.api_key.startswith("sk-"):
            raise ValueError("Invalid OpenRouter API key format")

    def _candidate_models(self) -> List[str]:
        configured = os.getenv("OPENROUTER_FALLBACK_MODELS", "")
        extra_models = [m.strip() for m in configured.split(",") if m.strip()]
        candidates = [self.model, *extra_models, *DEFAULT_FALLBACK_MODELS]
        deduped: List[str] = []
        seen = set()
        for model in candidates:
            if model in seen:
                continue
            seen.add(model)
            deduped.append(model)
        return deduped

    @staticmethod
    def _error_payload(response: httpx.Response) -> Optional[dict[str, Any]]:
        try:
            payload = response.json()
            if isinstance(payload, dict):
                return payload
        except Exception:
            return None
        return None

    @staticmethod
    def _is_model_unavailable(
        status_code: int,
        response_text: str,
        payload: Optional[dict[str, Any]],
    ) -> bool:
        if status_code not in {400, 404}:
            return False

        haystack_parts = [response_text.lower()]
        if payload:
            haystack_parts.append(json.dumps(payload).lower())
            raw = (
                payload.get("error", {})
                .get("metadata", {})
                .get("raw")
            )
            if isinstance(raw, str):
                haystack_parts.append(raw.lower())

        haystack = "\n".join(haystack_parts)
        markers = (
            "model_not_available",
            "unable to access non-serverless model",
            "no endpoints found",
            "unknown model",
        )
        return any(marker in haystack for marker in markers)

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
            "HTTP-Referer": "http://localhost",
            "X-Title": "LalaCore-Omega",
        }

        messages = []

        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})

        messages.append({"role": "user", "content": prompt})

        last_error: Optional[RuntimeError] = None
        candidate_models = self._candidate_models()

        for index, model_name in enumerate(candidate_models):
            payload = {
                "model": model_name,
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
            }
            is_last_model = index == len(candidate_models) - 1

            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    response = await client.post(
                        self.BASE_URL,
                        headers=headers,
                        json=payload,
                    )
            except httpx.RequestError as e:
                logger.error("Network error contacting OpenRouter: %s", str(e))
                raise RuntimeError("OpenRouter network failure") from e

            if response.status_code == 200:
                try:
                    data = response.json()
                except Exception:
                    raise RuntimeError("Failed to parse OpenRouter JSON response")

                try:
                    return data["choices"][0]["message"]["content"].strip()
                except (KeyError, IndexError, TypeError):
                    logger.error("Unexpected OpenRouter response format: %s", data)
                    raise RuntimeError("Invalid OpenRouter response format")

            payload_data = self._error_payload(response)
            if (
                not is_last_model
                and self._is_model_unavailable(
                    response.status_code, response.text, payload_data
                )
            ):
                logger.warning(
                    "OpenRouter model '%s' unavailable; retrying with fallback.",
                    model_name,
                )
                continue

            last_error = RuntimeError(
                f"OpenRouter API error {response.status_code}: {response.text}"
            )
            logger.error(str(last_error))
            break

        if last_error:
            raise last_error
        raise RuntimeError("OpenRouter request failed with all configured models")
