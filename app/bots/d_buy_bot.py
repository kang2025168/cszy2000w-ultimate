from __future__ import annotations

"""D 买入机器人。

负责 D 策略日内开仓入口。必须听 D 资金池和风险状态；
当前未指定 symbol 时只保留扫描占位。
"""

import argparse
import time

from ultimate_v1.schema import ensure_schema
from ultimate_v1.state_store import heartbeat, is_bot_enabled
from app.strategies.abcd_strategy import strategy_D_buy

BOT_NAME = "d_buy_bot"


def run_once(symbol: str | None = None):
    """执行一次 D 买入；未指定 symbol 时先保留为扫描占位。"""
    ensure_schema()
    if not is_bot_enabled(BOT_NAME):
        heartbeat(BOT_NAME, "paused", "机器人开关关闭")
        print("[D BUY BOT] paused by bot_controls", flush=True)
        return None
    heartbeat(BOT_NAME, "running", f"symbol={symbol or 'scan'}")
    if not symbol:
        print("[D BUY BOT] 扫描占位：后续接日内信号；接近收盘禁止新开仓", flush=True)
        return None
    return strategy_D_buy(symbol)


def main() -> None:
    parser = argparse.ArgumentParser(description="D 买入机器人")
    parser.add_argument("symbol", nargs="?")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=int, default=30)
    args = parser.parse_args()
    if args.loop:
        while True:
            print(run_once(args.symbol), flush=True)
            time.sleep(args.interval)
    else:
        print(run_once(args.symbol), flush=True)


if __name__ == "__main__":
    main()
