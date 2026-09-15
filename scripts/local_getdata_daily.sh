#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/likang/cszy2000w-ultimate"
LOG_DIR="$ROOT/logs"
LOCK_DIR="$ROOT/.local_getdata_daily.lock"
LOG_FILE="$LOG_DIR/getdata_daily_local.log"

mkdir -p "$LOG_DIR"

exec >>"$LOG_FILE" 2>&1

ts() { date '+%Y-%m-%d %H:%M:%S'; }

echo "[$(ts)] ===== local_getdata_daily start ====="

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "[$(ts)] another local_getdata_daily run is active; skip."
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
export DB_HOST="${LOCAL_GETDATA_DB_HOST:-127.0.0.1}"
export DB_PORT="${LOCAL_GETDATA_DB_PORT:-3307}"

# Local filesystem paths, replacing the Docker /app paths from .env.
export SYMBOLS_CSV="${LOCAL_GETDATA_SYMBOLS_CSV:-$ROOT/data/symbols/low_price_symbols.csv}"
export FAILED_OUT="${LOCAL_GETDATA_FAILED_OUT:-$ROOT/data/symbols/failed_symbols_today.csv}"
export LOG_DIR="$LOG_DIR"

# Daily history refresh defaults.
export DAILY="${LOCAL_GETDATA_DAILY:-1}"
export DAILY_DAYS="${LOCAL_GETDATA_DAILY_DAYS:-3}"
export GETDATA_TABLE="${LOCAL_GETDATA_TABLE:-${GETDATA_TABLE:-stock_prices_pool}}"
export BATCH_SIZE="${LOCAL_GETDATA_BATCH_SIZE:-${BATCH_SIZE:-200}}"
export MAX_TICKERS="${LOCAL_GETDATA_MAX_TICKERS:-${MAX_TICKERS:-0}}"
export INTERVAL="${LOCAL_GETDATA_INTERVAL:-${INTERVAL:-1d}}"
export ALPACA_DATA_FEED="${LOCAL_GETDATA_ALPACA_DATA_FEED:-${ALPACA_DATA_FEED:-sip}}"

if command -v docker >/dev/null 2>&1; then
  if ! docker compose up -d mysql >/dev/null; then
    echo "[$(ts)] WARNING: could not start/check Docker MySQL; continuing with DB=$DB_HOST:$DB_PORT."
  fi
else
  echo "[$(ts)] WARNING: docker not found in PATH; assuming MySQL is already running."
fi

echo "[$(ts)] DB=$DB_HOST:$DB_PORT/$DB_NAME TABLE=$GETDATA_TABLE SYMBOLS_CSV=$SYMBOLS_CSV DAILY_DAYS=$DAILY_DAYS"

"$ROOT/.venv/bin/python" -u "$ROOT/app/getdata_alpaca.py"

if [[ "${LOCAL_GETDATA_REFRESH_B_CANDIDATES:-1}" != "0" ]]; then
  echo "[$(ts)] ===== refresh Strategy B candidates ====="
  "$ROOT/.venv/bin/python" -u "$ROOT/scripts/refresh_b_candidates_to_ops.py"
  echo "[$(ts)] ===== refresh Strategy B candidates done ====="
fi

if [[ "${LOCAL_GETDATA_CLEANUP_B_CANDIDATES:-1}" != "0" ]]; then
  echo "[$(ts)] ===== cleanup stale Strategy B candidates ====="
  "$ROOT/.venv/bin/python" -u "$ROOT/scripts/cleanup_b_stock_operations.py"
  echo "[$(ts)] ===== cleanup stale Strategy B candidates done ====="
fi

echo "[$(ts)] ===== local_getdata_daily done ====="
