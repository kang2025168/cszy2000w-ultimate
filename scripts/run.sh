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
    echo "$(ts) old tradebot main has been removed. Use b_buy_bot/b_sell_bot/f_buy_bot/f_sell_bot." >&2
    exit 2
    ;;
  getdata_full)
    exec python -u app/getdata_alpaca.py
    ;;
  strategy_a)
    echo "$(ts) old strategy_a main entry has been removed. Use independent bots." >&2
    exit 2
    ;;
  b_buy_bot)
    echo "$(ts) ===== START independent B buy bot ====="
    exec python -u -m app.bots.b_buy_bot
    ;;
  b_sell_bot)
    echo "$(ts) ===== START independent B sell bot ====="
    exec python -u -m app.bots.b_sell_bot
    ;;
  f_buy_bot)
    echo "$(ts) ===== START independent F buy bot ====="
    exec python -u -m app.bots.f_buy_bot
    ;;
  f_sell_bot)
    echo "$(ts) ===== START independent F sell bot ====="
    exec python -u -m app.bots.f_sell_bot
    ;;
  ops_volume)
    echo "$(ts) ===== START local ops intraday volume sync ====="
    exec python -u app/sync_ops_intraday_volume.py
    ;;
  price_categories_once)
    echo "$(ts) ===== START stock price category snapshot refresh ====="
    exec python -u scripts/refresh_stock_price_categories.py
    ;;
  price_categories_loop)
    echo "$(ts) ===== START daily stock price category snapshot refresher ====="
    exec python -u scripts/refresh_stock_price_categories.py --loop
    ;;
  unlock_can_sell)
    exec python -u app/unlock_can_sell.py
    ;;
  ultimate_startup)
    echo "$(ts) ===== START ultimate_v1 startup ====="
    exec python -u -m ultimate_v1.main
    ;;
  ultimate_web)
    echo "$(ts) ===== START ultimate_v1 web ====="
    exec python -u -m ultimate_v1.web_app
    ;;
  ultimate_sync_positions)
    echo "$(ts) ===== START ultimate_v1 position sync ====="
    exec python -u -m ultimate_v1.sync_positions
    ;;
  ultimate_flatten_d)
    echo "$(ts) ===== START ultimate_v1 D flatten ====="
    exec python -u -m ultimate_v1.intraday_flatten
    ;;
  ultimate_rebalance)
    echo "$(ts) ===== START ultimate_v1 rebalance report ====="
    exec python -u -m ultimate_v1.rebalance_monthly
    ;;
  ultimate_strategy)
    echo "$(ts) ===== START ultimate_v1 strategy runner ====="
    shift
    exec python -u -m ultimate_v1.strategy_runner "$@"
    ;;
  ultimate_dashboard_bot)
    echo "$(ts) ===== START dashboard bot ====="
    shift
    exec python -u -m app.bots.dashboard_bot "$@"
    ;;
  ultimate_risk_bot)
    echo "$(ts) ===== START risk bot ====="
    shift
    exec python -u -m app.bots.risk_bot "$@"
    ;;
  ultimate_ac_bot)
    echo "$(ts) ===== START AC bot ====="
    shift
    exec python -u -m app.bots.ac_bot "$@"
    ;;
  ultimate_d_buy_bot)
    echo "$(ts) ===== START D buy bot ====="
    shift
    exec python -u -m app.bots.d_buy_bot "$@"
    ;;
  ultimate_d_sell_bot)
    echo "$(ts) ===== START D sell bot ====="
    shift
    exec python -u -m app.bots.d_sell_bot "$@"
    ;;
  healthcheck)
    echo "$(ts) healthcheck only done."
    exit 0
    ;;
  *)
    echo "Usage: ./scripts/run.sh {main|getdata_full|strategy_a|b_buy_bot|b_sell_bot|f_buy_bot|f_sell_bot|ops_volume|price_categories_once|price_categories_loop|unlock_can_sell|ultimate_startup|ultimate_web|ultimate_sync_positions|ultimate_flatten_d|ultimate_rebalance|ultimate_strategy|ultimate_dashboard_bot|ultimate_risk_bot|ultimate_ac_bot|ultimate_d_buy_bot|ultimate_d_sell_bot|healthcheck}" >&2
    exit 2
    ;;
esac
