from __future__ import annotations

"""B 买入机器人：只负责寻找和执行 B 类买入机会。"""

import argparse

from ..schema import ensure_schema
from ..state_store import heartbeat, is_bot_enabled
from ..strategies.strategy_b import strategy_B_buy

BOT_NAME = "b_buy_bot"


def run_once(symbol: str | None = None):
    """执行一次 B 买入；未指定 symbol 时先保留为扫描占位。"""
    ensure_schema()
    if not is_bot_enabled(BOT_NAME):
        heartbeat(BOT_NAME, "paused", "机器人开关关闭")
        print("[B BUY BOT] paused by bot_controls", flush=True)
        return None
    heartbeat(BOT_NAME, "running", f"symbol={symbol or 'scan'}")
    if not symbol:
        print("[B BUY BOT] 扫描占位：后续接 pressure breakout 候选池", flush=True)
        return None
    return strategy_B_buy(symbol)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ultimate V1 B 买入机器人")
    parser.add_argument("symbol", nargs="?")
    args = parser.parse_args()
    print(run_once(args.symbol), flush=True)


if __name__ == "__main__":
    main()
