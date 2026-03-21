"""Checkpoint versioning and metadata persistence for Mini training."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict



def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class CheckpointPaths:
    run_dir: Path
    checkpoints_dir: Path
    best_model_dir: Path
    calibration_head_dir: Path
    tokenizer_dir: Path
    logs_dir: Path


class CheckpointManager:
    """Creates versioned run directories and writes structured checkpoint artifacts."""

    def __init__(self, *, root: str = "data/mini_training/checkpoints", run_prefix: str = "mini") -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.run_prefix = str(run_prefix)

    def create_run(self) -> CheckpointPaths:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        prefix = f"{self.run_prefix}_{stamp}"
        pattern = re.compile(rf"^{re.escape(prefix)}_v(\d+)$")

        max_version = 0
        for item in self.root.iterdir():
            if not item.is_dir():
                continue
            match = pattern.match(item.name)
            if match:
                max_version = max(max_version, int(match.group(1)))

        version = max_version + 1
        run_dir = self.root / f"{prefix}_v{version:03d}"
        checkpoints_dir = run_dir / "checkpoints"
        best_model_dir = run_dir / "best_model"
        calibration_head_dir = run_dir / "calibration_head"
        tokenizer_dir = run_dir / "tokenizer"
        logs_dir = run_dir / "logs"

        for directory in (run_dir, checkpoints_dir, best_model_dir, calibration_head_dir, tokenizer_dir, logs_dir):
            directory.mkdir(parents=True, exist_ok=True)

        return CheckpointPaths(
            run_dir=run_dir,
            checkpoints_dir=checkpoints_dir,
            best_model_dir=best_model_dir,
            calibration_head_dir=calibration_head_dir,
            tokenizer_dir=tokenizer_dir,
            logs_dir=logs_dir,
        )

    def write_json(self, path: Path, payload: Dict[str, Any]) -> None:
        data = {"ts": _utc_now(), **payload}
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")

    def append_jsonl(self, path: Path, payload: Dict[str, Any]) -> None:
        row = {"ts": _utc_now(), **payload}
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")

    def write_hyperparameters(self, run: CheckpointPaths, hyperparameters: Dict[str, Any]) -> Path:
        path = run.run_dir / "hyperparameters.json"
        self.write_json(path, hyperparameters)
        return path

    def write_training_metadata(self, run: CheckpointPaths, metadata: Dict[str, Any]) -> Path:
        path = run.run_dir / "training_metadata.json"
        self.write_json(path, metadata)
        return path

    def write_epoch_checkpoint(
        self,
        run: CheckpointPaths,
        *,
        epoch: int,
        model_state: Dict[str, Any],
        metrics: Dict[str, Any],
        ema_state: Dict[str, Any] | None = None,
    ) -> Path:
        path = run.checkpoints_dir / f"epoch_{int(epoch):03d}.json"
        payload: Dict[str, Any] = {
            "epoch": int(epoch),
            "model_state": model_state,
            "metrics": metrics,
        }
        if ema_state is not None:
            payload["ema_state"] = ema_state
        self.write_json(path, payload)
        return path

    def write_best_model(self, run: CheckpointPaths, payload: Dict[str, Any]) -> Path:
        path = run.best_model_dir / "model_state.json"
        self.write_json(path, payload)
        return path

    def write_calibration_head(self, run: CheckpointPaths, payload: Dict[str, Any]) -> Path:
        path = run.calibration_head_dir / "calibration_head.json"
        self.write_json(path, payload)
        return path

    def write_tokenizer_stub(self, run: CheckpointPaths, payload: Dict[str, Any]) -> Path:
        path = run.tokenizer_dir / "tokenizer_config.json"
        self.write_json(path, payload)
        return path
