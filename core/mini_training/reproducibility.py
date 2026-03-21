"""Reproducibility helpers for offline Mini training runs."""

from __future__ import annotations

import os
import random
from typing import Dict

try:  # pragma: no cover - optional dependency
    import numpy as np
except Exception:  # pragma: no cover - optional dependency
    np = None


try:  # pragma: no cover - optional dependency
    import torch
except Exception:  # pragma: no cover - optional dependency
    torch = None


def set_global_seed(seed: int, *, deterministic: bool = True) -> Dict[str, object]:
    """Set random seeds across libraries and return applied seed metadata."""
    normalized_seed = int(max(0, seed)) % (2**31 - 1)

    random.seed(normalized_seed)
    if np is not None:
        np.random.seed(normalized_seed)
    os.environ["PYTHONHASHSEED"] = str(normalized_seed)

    torch_available = torch is not None
    cuda_available = False
    if torch is not None:
        try:
            torch.manual_seed(normalized_seed)
            if torch.cuda.is_available():
                torch.cuda.manual_seed_all(normalized_seed)
                cuda_available = True
            if deterministic:
                torch.backends.cudnn.deterministic = True
                torch.backends.cudnn.benchmark = False
                try:
                    torch.use_deterministic_algorithms(True)
                except Exception:
                    pass
        except Exception:
            torch_available = False
            cuda_available = False

    return {
        "seed": normalized_seed,
        "deterministic": bool(deterministic),
        "numpy_available": bool(np is not None),
        "torch_available": bool(torch_available),
        "cuda_available": bool(cuda_available),
    }
