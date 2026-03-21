from __future__ import annotations

import math
from typing import Iterable, List, Sequence


EPS = 1e-12
MAX_EXP_INPUT = 700.0
MIN_EXP_INPUT = -700.0


def _finite_or(value: float, fallback: float) -> float:
    if not math.isfinite(value):
        return float(fallback)
    return float(value)


def safe_exp(x: float, fallback: float = 0.0) -> float:
    try:
        x = _finite_or(float(x), 0.0)
        if x > MAX_EXP_INPUT:
            x = MAX_EXP_INPUT
        elif x < MIN_EXP_INPUT:
            x = MIN_EXP_INPUT
        value = math.exp(x)
        return _finite_or(value, fallback)
    except Exception:
        return float(fallback)


def safe_log(x: float, fallback: float | None = None) -> float:
    try:
        x = _finite_or(float(x), EPS)
        if x <= 0.0:
            x = EPS
        value = math.log(x)
        return _finite_or(value, fallback if fallback is not None else math.log(EPS))
    except Exception:
        return float(fallback if fallback is not None else math.log(EPS))


def clipped_division(
    numerator: float,
    denominator: float,
    *,
    fallback: float = 0.0,
    min_abs_denom: float = EPS,
) -> float:
    try:
        num = _finite_or(float(numerator), fallback)
        den = _finite_or(float(denominator), 0.0)
        if abs(den) < float(min_abs_denom):
            return float(fallback)
        out = num / den
        return _finite_or(out, fallback)
    except Exception:
        return float(fallback)


def safe_sigmoid(x: float) -> float:
    x = _finite_or(float(x), 0.0)
    if x >= 0.0:
        z = safe_exp(-x, fallback=0.0)
        return clipped_division(1.0, 1.0 + z, fallback=0.5)
    z = safe_exp(x, fallback=0.0)
    return clipped_division(z, 1.0 + z, fallback=0.5)


def stable_logsumexp(values: Sequence[float], fallback: float = 0.0) -> float:
    if not values:
        return float(fallback)
    finite = [_finite_or(float(v), float("-inf")) for v in values]
    max_v = max(finite)
    if not math.isfinite(max_v):
        return float(fallback)
    shifted = [safe_exp(v - max_v, fallback=0.0) for v in finite]
    total = sum(shifted)
    if total <= EPS:
        return float(fallback)
    return max_v + safe_log(total, fallback=0.0)


def safe_softmax(values: Sequence[float]) -> List[float]:
    if not values:
        return []
    log_z = stable_logsumexp(values, fallback=0.0)
    out = [safe_exp(float(v) - log_z, fallback=0.0) for v in values]
    s = sum(out)
    if s <= EPS or not math.isfinite(s):
        return [1.0 / len(values)] * len(values)
    return [clipped_division(v, s, fallback=1.0 / len(values)) for v in out]

