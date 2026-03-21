import os
from dotenv import load_dotenv
from core.key_manager import KeyManager

load_dotenv()

from core.db.connection import init_db

async def bootstrap():
    await init_db()
    

key_manager = KeyManager()
_KEYS_INITIALIZED = False


def _read_provider_keys(*env_names: str) -> list[str]:
    seen: set[str] = set()
    keys: list[str] = []
    for env_name in env_names:
        raw = os.getenv(env_name, "")
        for value in str(raw).split(","):
            token = value.strip()
            if not token or token in seen:
                continue
            seen.add(token)
            keys.append(token)
    return keys

def initialize_keys(*, silent: bool = False):
    global _KEYS_INITIALIZED
    if _KEYS_INITIALIZED:
        return

    openrouter_keys = _read_provider_keys("OPENROUTER_KEYS", "OPENROUTER_API_KEY")
    groq_keys = _read_provider_keys("GROQ_KEYS", "GROQ_API_KEY")
    gemini_keys = _read_provider_keys("GEMINI_KEYS", "GEMINI_API_KEY")
    hf_keys = _read_provider_keys("HF_KEYS", "HF_API_KEY")

    if openrouter_keys:
        key_manager.register_provider_keys("openrouter", openrouter_keys)

    if groq_keys:
        key_manager.register_provider_keys("groq", groq_keys)

    if gemini_keys:
        key_manager.register_provider_keys("gemini", gemini_keys)

    if hf_keys:
        # Backward-compatible aliases.
        key_manager.register_provider_keys("hf", hf_keys)
        key_manager.register_provider_keys("huggingface", hf_keys)

    _KEYS_INITIALIZED = True
    if not silent:
        print("✅ API Keys Loaded and Registered")


def get_key_manager():
    return key_manager


def provider_registry_snapshot() -> dict:
    providers = {}
    for provider, keys in key_manager.keys.items():
        providers[str(provider)] = {
            "registered": bool(keys),
            "key_count": len(keys),
        }
    return {
        "initialized": bool(_KEYS_INITIALIZED),
        "providers": providers,
    }
