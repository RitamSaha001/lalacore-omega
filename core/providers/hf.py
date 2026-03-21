import os
import httpx
from typing import Dict, Any
from dotenv import load_dotenv

from core.providers.base import BaseProvider
from core.bootstrap import get_key_manager

load_dotenv()


class HFProvider(BaseProvider):

    def __init__(self, provider_id: int, name: str):
        super().__init__(provider_id, name)
        self.key_manager = get_key_manager()
        keys = os.getenv("HF_KEYS")
        if not keys:
            raise RuntimeError("HF_KEYS not set in .env")
        parsed_keys = [k.strip() for k in keys.split(",") if k.strip()]
        self.key_manager.register_provider_keys("hf", parsed_keys)
        self.url = "https://api-inference.huggingface.co/models/meta-llama/Meta-Llama-3-70B-Instruct"

    async def generate(self, question: str, subject: str, difficulty: int) -> Dict[str, Any]:

        key = self.key_manager.get_key("hf")

        headers = {
            "Authorization": f"Bearer {key}"
        }

        payload = {
            "inputs": question
        }

        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(self.url, headers=headers, json=payload)

        data = response.json()

        if isinstance(data, list):
            content = data[0]["generated_text"]
        else:
            self.key_manager.report_failure(key)
            content = str(data)
            return {
                "answer": content.strip(),
                "raw": content,
                "confidence": 0.1
            }

        self.key_manager.report_success(key)

        return {
            "answer": content.strip(),
            "raw": content,
            "confidence": 0.5
        }
