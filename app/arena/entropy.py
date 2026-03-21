from collections import defaultdict
from difflib import SequenceMatcher

from core.safe_math import clipped_division, safe_log


def _norm(text):
    return " ".join(str(text or "").strip().lower().split())


def _pairwise_disagreement(texts):
    if len(texts) <= 1:
        return 0.0
    total = 0.0
    count = 0
    for i in range(len(texts)):
        for j in range(i + 1, len(texts)):
            a = texts[i]
            b = texts[j]
            if not a and not b:
                continue
            similarity = float(SequenceMatcher(a=a, b=b).ratio())
            total += max(0.0, min(1.0, 1.0 - similarity))
            count += 1
    if count == 0:
        return 0.0
    return total / count


def compute_entropy(responses):
    if not responses:
        return 0.0
    clusters = defaultdict(int)

    for r in responses:
        key = _norm(r.get("final_answer", ""))
        clusters[key] += 1

    total = len(responses)
    entropy = 0.0

    for count in clusters.values():
        p = clipped_division(count, total, fallback=0.0)
        entropy -= p * safe_log(p + 1e-9)

    # Additional structural disagreement signal using reasoning+answer text distance.
    combined = [
        _norm(
            str(r.get("final_answer", ""))
            + " || "
            + str(r.get("reasoning", ""))[:260]
        )
        for r in responses
    ]
    text_disagreement = _pairwise_disagreement(combined)

    # If all providers collapse into the same short fragment, entropy remains low.
    # This keeps confidence conservative for echo-like convergence.
    same_answer_collapse = 1.0 if len(clusters) == 1 else 0.0
    collapse_penalty = 0.08 if same_answer_collapse else 0.0

    entropy += 0.45 * text_disagreement
    entropy = max(0.0, entropy - collapse_penalty)
    return entropy
