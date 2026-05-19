from __future__ import annotations

"""看板状态机器人。

负责刷新账户快照、资金池、持仓占用和 capital_state，
供网页看板和其它机器人读取。
"""

import argparse
import time

from ultimate_v1.capital_manager import get_capital_allocation, get_strategy_used_capital
from ultimate_v1.config import settings
from ultimate_v1.schema import ensure_schema
from ultimate_v1.state_store import heartbeat, replace_capital_state, write_account_snapshot
from ultimate_v1.sync_positions import sync_position_holdings

BOT_NAME = "dashboard_bot"


def refresh_dashboard_state(sync_positions: bool = False) -> list[dict]:
    """刷新资金池状态，并可选同步 Alpaca 当前持仓。"""
    ensure_schema()
    heartbeat(BOT_NAME, "running", "刷新资金和仓位状态")
    if sync_positions and settings().enable_position_holdings:
        sync_position_holdings()

    allocation = get_capital_allocation()
    if allocation is None:
        heartbeat(BOT_NAME, "blocked", "账户快照失败，资金状态不可用")
        return []
    write_account_snapshot(allocation.equity, allocation.buying_power, allocation.cash, allocation.portfolio_value)

    rows = []
    for group in ("A", "B", "C", "D"):
        target = allocation.target_for(group)
        used = allocation.used.get(group, get_strategy_used_capital(group))
        available = allocation.available.get(group, max(0.0, target - used))
        can_open = True
        reason = "allow"
        if available <= 0:
            can_open = False
            reason = "no_available_capital"
        rows.append(
            {
                "strategy_group": group,
                "target_capital": round(target, 2),
                "used_capital": round(used, 2),
                "available_capital": round(available, 2),
                "risk_adjusted_target": round(target, 2),
                "can_open_new": can_open,
                "reason": reason,
            }
        )
    replace_capital_state(rows)
    heartbeat(BOT_NAME, "running", "资金和仓位状态已刷新")
    print(f"[DASHBOARD BOT] refreshed groups={len(rows)}", flush=True)
    return rows


def loop(interval_sec: int) -> None:
    """循环刷新看板状态。"""
    while True:
        refresh_dashboard_state(sync_positions=True)
        time.sleep(interval_sec)


def main() -> None:
    parser = argparse.ArgumentParser(description="看板状态机器人")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=int, default=settings().position_sync_interval_sec)
    args = parser.parse_args()
    if args.loop:
        loop(args.interval)
    else:
        refresh_dashboard_state(sync_positions=True)


if __name__ == "__main__":
    main()
