from __future__ import annotations

"""D 卖出机器人。

负责 D 策略止盈止损和收盘前强制平仓。
未指定 symbol 时会执行 D 强平检查。
"""

import argparse
import time

from ultimate_v1.intraday_flatten import flatten_d_positions
from ultimate_v1.schema import ensure_schema
from ultimate_v1.state_store import heartbeat, is_bot_enabled
from app.strategies.abcd_strategy import strategy_D_sell

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
    parser = argparse.ArgumentParser(description="D 卖出机器人")
    parser.add_argument("symbol", nargs="?")
    parser.add_argument("--flatten", action="store_true")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=int, default=30)
    args = parser.parse_args()
    if args.loop:
        while True:
            print(run_once(args.symbol, args.flatten), flush=True)
            time.sleep(args.interval)
    else:
        print(run_once(args.symbol, args.flatten), flush=True)


if __name__ == "__main__":
    main()
