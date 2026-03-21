#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export PYTHONPATH="${PYTHONPATH:-.}"

INPUT_BANK="${1:-data/app/import_question_bank.json}"
TOKEN_BUDGET="${TOKEN_BUDGET:-1800000}"
MAX_AI_ROWS="${MAX_AI_ROWS:-0}"
MIN_SIM="${MIN_SIM:-0.78}"
AI_MAX_RETRIES="${AI_MAX_RETRIES:-3}"
AI_RETRY_DELAY_S="${AI_RETRY_DELAY_S:-3}"
AI_TIMEOUT_S="${AI_TIMEOUT_S:-20}"
REQUIRE_AI_ALL="${REQUIRE_AI_ALL:-1}"

echo "[refine] input bank: $INPUT_BANK"
echo "[refine] token budget: $TOKEN_BUDGET"
echo "[refine] max ai rows: $MAX_AI_ROWS"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
  echo "[refine] loaded .env"
fi

./venv/bin/python tools/reclassify_import_chapters.py \
  --bank "$INPUT_BANK" \
  --teacher-id "" \
  --min-updates 0

./venv/bin/python tools/build_layer3_verified_bank.py \
  --input "$INPUT_BANK" \
  --output data/app/import_question_bank_layer3_verified.live.json \
  --best-output data/app/import_question_bank_layer3_best.live.json \
  --report data/app/repair_report_layer3.live.json \
  --snapshot data/app/import_question_bank.layer3_snapshot.live.json \
  --integrity-safe-threshold 0.80 \
  --progress-every 500

./venv/bin/python tools/build_layer4_ai_salvage_bank.py \
  --input data/app/import_question_bank_layer3_verified.live.json \
  --output data/app/import_question_bank_final.live.json \
  --dropped-output data/app/import_question_bank_layer4_dropped.live.json \
  --report data/app/repair_report_layer4.live.json \
  --progress-file data/app/repair_report_layer4.progress.live.json \
  --ai-review-all \
  $( [ "$REQUIRE_AI_ALL" = "1" ] && echo "--require-ai-check-all" ) \
  --max-ai-rows "$MAX_AI_ROWS" \
  --token-budget "$TOKEN_BUDGET" \
  --avg-response-tokens 220 \
  --ai-max-retries "$AI_MAX_RETRIES" \
  --ai-retry-delay-s "$AI_RETRY_DELAY_S" \
  --ai-timeout-s "$AI_TIMEOUT_S" \
  --min-question-similarity "$MIN_SIM" \
  --delete-unusable \
  --progress-every 300

./venv/bin/python tools/build_layer7_postvalidate_bank.py \
  --input data/app/import_question_bank_final.live.json \
  --output data/app/import_question_bank_layer7_final.live.json \
  --dropped-output data/app/import_question_bank_layer7_dropped.live.json \
  --report data/app/repair_report_layer7.live.json \
  --progress-file data/app/repair_report_layer7.progress.live.json \
  --delete-unusable \
  --progress-every 300

cp data/app/import_question_bank_layer7_final.live.json data/app/import_question_bank.json
echo "[refine] active bank replaced: data/app/import_question_bank.json"

./venv/bin/python tools/build_jee_bank_x.py \
  --input data/app/import_question_bank_layer7_final.live.json \
  --output data/app/JEE_BANK_X.json \
  --report data/app/JEE_BANK_X.report.json \
  --progress-file data/app/JEE_BANK_X.progress.json \
  --progress-every 300
echo "[refine] JEE BANK X written: data/app/JEE_BANK_X.json"
