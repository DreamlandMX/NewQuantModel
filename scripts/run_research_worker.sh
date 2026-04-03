#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT_DIR/storage/logs"
PID_FILE="$LOG_DIR/research-worker.pid"
LOG_FILE="$LOG_DIR/research-worker.log"

mkdir -p "$LOG_DIR"

if [[ -f "$PID_FILE" ]]; then
  EXISTING_PID="$(cat "$PID_FILE")"
  if [[ -n "$EXISTING_PID" ]] && kill -0 "$EXISTING_PID" 2>/dev/null; then
    echo "Research worker already running with PID $EXISTING_PID"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

echo $$ > "$PID_FILE"

export PYTHONPATH="$ROOT_DIR/apps/research/src:$ROOT_DIR/packages/shared-types/python"

echo "[$(date -Is)] Starting research worker" >> "$LOG_FILE"
exec python3 -m newquantmodel.cli.main worker --root "$ROOT_DIR" --years 5 --poll-seconds 60 >> "$LOG_FILE" 2>&1
