#!/usr/bin/env bash
set -euo pipefail

cd /app
export PYTHONPATH="/app${PYTHONPATH:+:$PYTHONPATH}"

ts() { date '+[%Y-%m-%d %H:%M:%S]'; }

cmd="${1:-main}"

echo "$(ts) ===== START healthcheck ====="
if [ -f "app/healthcheck.py" ]; then
  python -u app/healthcheck.py
else
  echo "$(ts) healthcheck.py not found, skip."
fi
echo "$(ts) ===== DONE healthcheck ====="

case "$cmd" in
  main)
    echo "$(ts) ===== START tradebot main ====="
    exec python -u app/trade_bot_main.py
    ;;
  getdata_full)
    exec python -u app/getdata_alpaca.py
    ;;
  strategy_a)
    echo "$(ts) ===== START tradebot main (strategy_a) ====="
    exec python -u app/trade_bot_main.py
    ;;
  buy_bot)
    echo "$(ts) ===== START independent buy bot ====="
    exec python -u app/buy_bot.py
    ;;
  sell_bot)
    echo "$(ts) ===== START independent sell bot ====="
    exec python -u app/sell_bot.py
    ;;
  ops_volume)
    echo "$(ts) ===== START local ops intraday volume sync ====="
    exec python -u app/sync_ops_intraday_volume.py
    ;;
  unlock_can_sell)
    exec python -u app/unlock_can_sell.py
    ;;
  healthcheck)
    echo "$(ts) healthcheck only done."
    exit 0
    ;;
  *)
    echo "Usage: ./scripts/run.sh {main|getdata_full|strategy_a|buy_bot|sell_bot|ops_volume|unlock_can_sell|healthcheck}" >&2
    exit 2
    ;;
esac
