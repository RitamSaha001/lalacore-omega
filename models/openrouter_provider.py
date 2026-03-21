import httpx
import time
from models.provider_base import BaseProvider
from core.bootstrap import get_key_manager, initialize_keys


initialize_keys()
key_manager = get_key_manager()


class OpenRouterProvider(BaseProvider):

    async def generate(self, question: str, context: str | None = None):

        key = key_manager.get_key("openrouter")

        start = time.time()

        headers = {
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": "mistralai/mistral-7b-instruct",
            "messages": [
                {
                    "role": "system",
                    "content": "Solve the problem and return ONLY the final answer."
                },
                {
                    "role": "user",
                    "content": question
                }
            ]
        }

        try:
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers=headers,
                    json=payload
                )

            latency = time.time() - start
            response.raise_for_status()
            data = response.json()

            output = data["choices"][0]["message"]["content"].strip()

            key_manager.report_success(key)

            return {
                "provider": "openrouter",
                "reasoning": None,
                "final_answer": output,
                "latency": latency,
                "raw": data
            }

        except Exception as e:
            key_manager.report_failure(key)
            return {
                "provider": "openrouter",
                "reasoning": None,
                "final_answer": "",
                "latency": 0,
                "raw": {"error": str(e)}
            }
