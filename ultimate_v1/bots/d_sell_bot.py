from __future__ import annotations

"""D 卖出机器人：负责 D 类止盈止损和收盘前强制平仓。"""

import argparse

from ..intraday_flatten import flatten_d_positions
from ..schema import ensure_schema
from ..state_store import heartbeat, is_bot_enabled
from ..strategies.strategy_d import strategy_D_sell

BOT_NAME = "d_sell_bot"


def run_once(symbol: str | None = None, flatten: bool = False):
    """执行一次 D 卖出或强平检查。"""
    ensure_schema()
    if not is_bot_enabled(BOT_NAME):
        heartbeat(BOT_NAME, "paused", "机器人开关关闭")
        print("[D SELL BOT] paused by bot_controls", flush=True)
        return None
    heartbeat(BOT_NAME, "running", "flatten" if flatten else f"symbol={symbol or 'scan'}")
    if flatten:
        return flatten_d_positions(force=True)
    if not symbol:
        count = flatten_d_positions(force=False)
        print(f"[D SELL BOT] 强平检查完成 count={count}", flush=True)
        return count
    return strategy_D_sell(symbol)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ultimate V1 D 卖出机器人")
    parser.add_argument("symbol", nargs="?")
    parser.add_argument("--flatten", action="store_true")
    args = parser.parse_args()
    print(run_once(args.symbol, args.flatten), flush=True)


if __name__ == "__main__":
    main()
