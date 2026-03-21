from typing import Dict
from core.providers.openrouter import OpenRouterProvider
from core.providers.groq import GroqProvider
from core.providers.gemini import GeminiProvider
from core.providers.hf import HFProvider


class ProviderRegistry:

    def __init__(self):
        self._providers: Dict[int, object] = {}

    def register(self, provider_id: int, name: str):

        if name == "openrouter":
            instance = OpenRouterProvider(provider_id, name)

        elif name == "groq":
            instance = GroqProvider(provider_id, name)

        elif name == "gemini":
            instance = GeminiProvider(provider_id, name)

        elif name in {"hf", "huggingface"}:
            instance = HFProvider(provider_id, name)

        else:
            raise RuntimeError(f"Unknown provider: {name}")

        self._providers[provider_id] = instance

    def get(self, provider_id: int):
        if provider_id not in self._providers:
            raise RuntimeError(f"Provider {provider_id} not registered.")
        return self._providers[provider_id]
