#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/likang/cszy2000w-ultimate"
LOG_DIR="$ROOT/logs"
LOCK_DIR="$ROOT/.local_strategy_b_levels_weekly.lock"
LOG_FILE="$LOG_DIR/strategy_b_levels_weekly.log"

mkdir -p "$LOG_DIR"

exec >>"$LOG_FILE" 2>&1

ts() { date '+%Y-%m-%d %H:%M:%S'; }

echo "[$(ts)] ===== strategy_b_levels_weekly start ====="

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "[$(ts)] another strategy_b_levels_weekly run is active; skip."
  exit 0
fi
trap 'rm -rf "$LOCK_DIR"' EXIT

cd "$ROOT"

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"

set -a
source "$ROOT/.env"
set +a

# Local Mac -> Docker MySQL port mapping.
export DB_HOST="${LOCAL_B_LEVELS_DB_HOST:-127.0.0.1}"
export DB_PORT="${LOCAL_B_LEVELS_DB_PORT:-3307}"

if command -v docker >/dev/null 2>&1; then
  if ! docker compose up -d mysql >/dev/null; then
    echo "[$(ts)] WARNING: could not start/check Docker MySQL; continuing with DB=$DB_HOST:$DB_PORT."
  fi
else
  echo "[$(ts)] WARNING: docker not found in PATH; assuming MySQL is already running."
fi

echo "[$(ts)] DB=$DB_HOST:$DB_PORT/$DB_NAME"

"$ROOT/.venv/bin/python" -u "$ROOT/app/strategy_b_build_levels_v57.py"

echo "[$(ts)] ===== strategy_b_levels_weekly done ====="
