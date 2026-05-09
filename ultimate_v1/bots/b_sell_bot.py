from __future__ import annotations

"""B 卖出机器人：只负责 B 类持仓退出，不受资金池限制。"""

import argparse

from ..schema import ensure_schema
from ..state_store import heartbeat, is_bot_enabled
from ..strategies.strategy_b import strategy_B_sell

BOT_NAME = "b_sell_bot"


def run_once(symbol: str | None = None):
    """执行一次 B 卖出；未指定 symbol 时先保留为扫描占位。"""
    ensure_schema()
    if not is_bot_enabled(BOT_NAME):
        heartbeat(BOT_NAME, "paused", "机器人开关关闭")
        print("[B SELL BOT] paused by bot_controls", flush=True)
        return None
    heartbeat(BOT_NAME, "running", f"symbol={symbol or 'scan'}")
    if not symbol:
        print("[B SELL BOT] 扫描占位：后续接 B 持仓动态止损、b_stage、pending stop", flush=True)
        return None
    return strategy_B_sell(symbol)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ultimate V1 B 卖出机器人")
    parser.add_argument("symbol", nargs="?")
    args = parser.parse_args()
    print(run_once(args.symbol), flush=True)


if __name__ == "__main__":
    main()
