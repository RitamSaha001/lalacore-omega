#!/bin/bash
set -euo pipefail

ROOT="/Users/ritamsaha/lalacore_omega"
PY="$ROOT/venv/bin/python"
SCRIPT="$ROOT/scripts/google_sheets_monitor.py"
LOCK_DIR="$ROOT/.locks/daily_health_lock"
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

exec "$PY" "$SCRIPT" \
  --once \
  --sheet-id 1huHNzGPCDE2zuS5w8rHSoPnfApbOAnoE2EdqtZoIXeI \
  --service-account-file "$ROOT/credentials/google_service_account.json" \
  --state-path "$ROOT/data/lc9/LC9_GOOGLE_SHEETS_SYNC_STATE.json" \
  --sync-log-path "$ROOT/data/lc9/LC9_GOOGLE_SHEETS_SYNC_LOG.jsonl" \
  --queue-path "$ROOT/data/lc9/LC9_FEEDER_QUEUE.jsonl" \
  --debug-path "$ROOT/data/lc9/LC9_SOLVER_DEBUG.jsonl" \
  --runtime-path "$ROOT/data/lc9/LC9_RUNTIME_TELEMETRY.jsonl" \
  --provider-stats-path "$ROOT/data/metrics/provider_stats.json" \
  --provider-circuit-path "$ROOT/data/metrics/provider_circuit.json" \
  --token-budget-path "$ROOT/data/metrics/token_budget.json"
