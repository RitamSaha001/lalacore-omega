import os
import json
import httpx
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

from core.providers.base import BaseProvider
from core.bootstrap import get_key_manager

load_dotenv()

DEFAULT_FALLBACK_MODELS = (
    "meta-llama/llama-3.1-8b-instruct:free",
    "openai/gpt-4o-mini",
    "google/gemini-2.0-flash-001",
)


class OpenRouterProvider(BaseProvider):

    def __init__(self, provider_id: int, name: str):
        super().__init__(provider_id, name)
        self.key_manager = get_key_manager()

        keys = os.getenv("OPENROUTER_KEYS")
        if not keys:
            raise RuntimeError("OPENROUTER_KEYS not set in .env")

        parsed_keys = [k.strip() for k in keys.split(",") if k.strip()]
        self.key_manager.register_provider_keys("openrouter", parsed_keys)
        self.url = "https://openrouter.ai/api/v1/chat/completions"

    def _candidate_models(self) -> List[str]:
        configured = os.getenv("OPENROUTER_FALLBACK_MODELS", "")
        extra_models = [m.strip() for m in configured.split(",") if m.strip()]
        candidates = [
            "meta-llama/llama-3.1-8b-instruct:free",
            *extra_models,
            *DEFAULT_FALLBACK_MODELS,
        ]
        deduped: List[str] = []
        seen = set()
        for model in candidates:
            if model in seen:
                continue
            seen.add(model)
            deduped.append(model)
        return deduped

    @staticmethod
    def _error_payload(response: httpx.Response) -> Optional[Dict[str, Any]]:
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
        payload: Optional[Dict[str, Any]],
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
        question: str,
        subject: str,
        difficulty: int
    ) -> Dict[str, Any]:

        key = self.key_manager.get_key("openrouter")

        headers = {
            "Authorization": f"Bearer {key}",
            "HTTP-Referer": "http://localhost",
            "X-Title": "Omega",
            "Content-Type": "application/json"
        }

        last_error: Optional[RuntimeError] = None
        candidate_models = self._candidate_models()

        for idx, model_name in enumerate(candidate_models):
            payload = {
                "model": model_name,
                "messages": [
                    {"role": "user", "content": question}
                ],
                "temperature": 0
            }
            is_last_model = idx == len(candidate_models) - 1

            async with httpx.AsyncClient(timeout=60) as client:
                response = await client.post(
                    self.url,
                    headers=headers,
                    json=payload
                )

            if response.status_code == 200:
                try:
                    data = response.json()
                except Exception:
                    self.key_manager.report_failure(key)
                    raise RuntimeError(
                        f"OpenRouter returned non-JSON response: {response.text}"
                    )

                if "choices" not in data:
                    self.key_manager.report_failure(key)
                    raise RuntimeError(f"OpenRouter error response: {data}")

                content = data["choices"][0]["message"]["content"]
                self.key_manager.report_success(key)
                return {
                    "answer": content.strip(),
                    "raw": content,
                    "confidence": 0.5
                }

            payload_data = self._error_payload(response)
            if (
                not is_last_model
                and self._is_model_unavailable(
                    response.status_code, response.text, payload_data
                )
            ):
                continue

            self.key_manager.report_failure(key)
            last_error = RuntimeError(
                f"OpenRouter API error {response.status_code}: {response.text}"
            )
            break

        if last_error:
            raise last_error
        self.key_manager.report_failure(key)
        raise RuntimeError("OpenRouter request failed with all configured models")
