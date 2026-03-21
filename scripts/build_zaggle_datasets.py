from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.training.dataset_builder import ZaggleDatasetBuilder


def main() -> None:
    manifest = ZaggleDatasetBuilder().build_all()
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
