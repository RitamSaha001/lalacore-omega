#!/bin/bash
set -euo pipefail

ROOT="/Users/ritamsaha/lalacore_omega"
PY="$ROOT/venv/bin/python"
SCRIPT="$ROOT/scripts/run_manual_questions.py"
LOCK_DIR="$ROOT/.locks/serial_queue_lock"
PID_FILE="$LOCK_DIR/pid"

mkdir -p "$ROOT/.locks"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  if [ -f "$PID_FILE" ]; then
    OLD_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
      exit 0
    fi
  fi
  rm -rf "$LOCK_DIR"
  mkdir "$LOCK_DIR"
fi
echo "$$" > "$PID_FILE"
trap 'rm -rf "$LOCK_DIR"' EXIT

export PYTHONUNBUFFERED=1
# Ban-safe mode: providers are called serially with a small gap.
export LC9_PROVIDER_SERIAL=1
export LC9_PROVIDER_MIN_GAP_S=1.25
export LC9_FEEDER_DAILY_CAP=500

cd "$ROOT"

exec "$PY" "$SCRIPT" \
  --queue-path "$ROOT/data/lc9/LC9_FEEDER_QUEUE.jsonl" \
  --training-cases-path "$ROOT/data/lc9/LC9_FEEDER_CASES.jsonl" \
  --replay-cases-path "$ROOT/data/replay/feeder_cases.jsonl" \
  process \
  --mode full-solver \
  --max-items 1
