#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_FILE="${LIVE_CLASSES_STRESS_REPORT:-${ROOT_DIR}/build/reports/live_class_stress_report.json}"
STUDENTS="${LIVE_CLASSES_STRESS_STUDENTS:-50}"

cd "${ROOT_DIR}"
dart run tool/stress_test_50_students.dart --students "${STUDENTS}" --out "${OUT_FILE}"
