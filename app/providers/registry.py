from __future__ import annotations

from typing import Dict

from app.providers.gemini import GeminiProvider
from app.providers.groq import GroqProvider
from app.providers.huggingface import HuggingFaceProvider
from app.providers.openrouter import OpenRouterProvider


def build_app_provider_registry() -> Dict[str, object]:
    providers: Dict[str, object] = {}

    try:
        providers["openrouter"] = OpenRouterProvider(
            model="meta-llama/llama-3.1-8b-instruct:free"
        )
    except Exception:
        pass

    try:
        providers["groq"] = GroqProvider()
    except Exception:
        pass

    try:
        providers["gemini"] = GeminiProvider()
    except Exception:
        pass

    try:
        providers["hf"] = HuggingFaceProvider()
    except Exception:
        pass

    return providers
