from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.training.dataset_builder import ZaggleDatasetBuilder
from core.lalacore_x.weekly import WeeklyEvolutionJob


def main() -> None:
    weekly = WeeklyEvolutionJob().run()
    manifest = ZaggleDatasetBuilder().build_all()

    print(json.dumps({"weekly": weekly, "datasets": manifest}, indent=2))


if __name__ == "__main__":
    main()
