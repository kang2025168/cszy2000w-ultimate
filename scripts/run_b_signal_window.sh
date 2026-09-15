#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export DB_HOST="${DB_HOST:-138.197.75.51}"
export DB_PORT="${DB_PORT:-3307}"
export DB_USER="${DB_USER:-tradebot}"
export DB_NAME="${DB_NAME:-cszy2000}"

exec .venv/bin/python scripts/local_b_signal_yahoo_window.py
