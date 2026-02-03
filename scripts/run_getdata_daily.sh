#!/usr/bin/env bash
set -euo pipefail

echo "[$(date '+%F %T')] run_getdata_daily.sh start"

python -u /app/app/getdata_alpaca.py

echo "[$(date '+%F %T')] run_getdata_daily.sh done"