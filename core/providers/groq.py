import os
import httpx
from typing import Dict, Any
from dotenv import load_dotenv

from core.providers.base import BaseProvider
from core.bootstrap import get_key_manager

load_dotenv()


class GroqProvider(BaseProvider):

    def __init__(self, provider_id: int, name: str):
        super().__init__(provider_id, name)
        self.key_manager = get_key_manager()
        keys = os.getenv("GROQ_KEYS")
        if not keys:
            raise RuntimeError("GROQ_KEYS not set in .env")
        parsed_keys = [k.strip() for k in keys.split(",") if k.strip()]
        self.key_manager.register_provider_keys("groq", parsed_keys)
        self.url = "https://api.groq.com/openai/v1/chat/completions"

    async def generate(self, question: str, subject: str, difficulty: int) -> Dict[str, Any]:

        key = self.key_manager.get_key("groq")

        headers = {
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": "llama3-70b-8192",
            "messages": [
                {"role": "user", "content": question}
            ]
        }

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(self.url, headers=headers, json=payload)

        data = response.json()
        if "choices" not in data:
            self.key_manager.report_failure(key)
            raise RuntimeError(f"Groq error response: {data}")
        content = data["choices"][0]["message"]["content"]
        self.key_manager.report_success(key)

        return {
            "answer": content.strip(),
            "raw": content,
            "confidence": 0.5
        }
