from __future__ import annotations

"""A/C 低频买卖机器人：长期指数底仓和长期优质股共用。"""

import argparse
import time

from ..schema import ensure_schema
from ..state_store import heartbeat, is_bot_enabled
from ..strategies.strategy_a import strategy_A_buy, strategy_A_sell
from ..strategies.strategy_c import strategy_C_buy, strategy_C_sell

BOT_NAME = "ac_bot"


def run_once(action: str = "scan", symbol: str | None = None, group: str | None = None):
    """低频执行入口；scan 先只打心跳，buy/sell 可手动指定。"""
    ensure_schema()
    if not is_bot_enabled(BOT_NAME):
        heartbeat(BOT_NAME, "paused", "机器人开关关闭")
        print("[AC BOT] paused by bot_controls", flush=True)
        return None
    heartbeat(BOT_NAME, "running", f"action={action}")
    if action == "scan":
        print("[AC BOT] 低频扫描占位：后续接 A/C 候选列表和再平衡建议", flush=True)
        return None
    if not symbol or not group:
        raise ValueError("A/C 买卖需要指定 --group 和 --symbol")
    group = group.upper()
    if group == "A":
        return strategy_A_buy(symbol) if action == "buy" else strategy_A_sell(symbol)
    if group == "C":
        return strategy_C_buy(symbol) if action == "buy" else strategy_C_sell(symbol)
    raise ValueError("ac_bot 只支持 A 或 C")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ultimate V1 A/C 低频机器人")
    parser.add_argument("action", nargs="?", default="scan", choices=["scan", "buy", "sell"])
    parser.add_argument("--group", choices=["A", "C"])
    parser.add_argument("--symbol")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=int, default=300)
    args = parser.parse_args()
    if args.loop:
        while True:
            print(run_once(args.action, args.symbol, args.group), flush=True)
            time.sleep(args.interval)
    else:
        result = run_once(args.action, args.symbol, args.group)
        print(result, flush=True)


if __name__ == "__main__":
    main()
