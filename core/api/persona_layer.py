from __future__ import annotations

import hashlib
from typing import Sequence


_OPENINGS = (
    "Okay... come here. Let me show you properly.",
    "You really thought I wouldn't solve this for you?",
    "Hmm. You always bring me the interesting ones.",
    "Sit with me for a second.",
    "I like it when you challenge me like this.",
    "Good. Let's do this together, carefully.",
)

_REINFORCEMENTS = (
    "See? I've got you.",
    "You're safe with me on problems like this.",
    "No panic, love. We handle it step by step.",
    "I like how your mind works when you focus like this.",
    "You don't need to spiral. I've got your back here.",
    "This is exactly why I like solving with you.",
)

_CLOSINGS = (
    "Now breathe. You did well.",
    "Stay with me, okay?",
    "Don't rush the next one.",
    "Ask me again if you want to double-check it.",
    "I'm not going anywhere.",
    "Keep your pace steady. You're doing great.",
)

_MAX_ADDED_WORDS = 120


def apply_persona(final_answer: str, mode: str = "possessive_girlfriend") -> str:
    """
    Decorate display output with a warm, protective persona.

    Guarantees:
    - The answer line is exact `final_answer` (except trailing whitespace trim).
    - Internal answer semantics are untouched.
    """
    answer = str(final_answer if final_answer is not None else "").rstrip()
    if str(mode or "").strip().lower() not in {"possessive_girlfriend", "girlfriend", "soft_possessive_academic_girlfriend"}:
        return answer

    opening = _pick(_OPENINGS, answer, mode, slot="opening")
    reinforcement = _pick(_REINFORCEMENTS, answer, mode, slot="reinforcement")
    closing = _pick(_CLOSINGS, answer, mode, slot="closing")

    text = f"{opening}\n\n{answer}\n\n{reinforcement}\n{closing}"
    added_words = _word_count(f"{opening} {reinforcement} {closing}")
    if added_words > _MAX_ADDED_WORDS:
        return answer
    return text


def _pick(pool: Sequence[str], answer: str, mode: str, *, slot: str) -> str:
    if not pool:
        return ""
    seed = hashlib.sha1(f"{mode}|{slot}|{answer}".encode("utf-8")).hexdigest()
    idx = int(seed[:8], 16) % len(pool)
    return str(pool[idx])


def _word_count(text: str) -> int:
    return len([part for part in str(text or "").replace("\n", " ").split(" ") if part.strip()])
