from __future__ import annotations

"""Q 卖出机器人。

Q 只负责期权仓位，且这个机器人只做卖出/平仓动作：
- 不主动开新期权仓；
- 只扫描 Q 页面手动开出的 option_spreads；
- 开仓单未确认成交前，不触发平仓。
"""

import argparse
import time

from ultimate_v1.d_tactical import q_sell_once
from ultimate_v1.schema import ensure_schema
from ultimate_v1.state_store import heartbeat, is_bot_enabled

BOT_NAME = "q_sell_bot"


def run_once():
    """执行一次 Q 期权平仓扫描。"""
    ensure_schema()
    if not is_bot_enabled(BOT_NAME):
        heartbeat(BOT_NAME, "paused", "机器人开关关闭")
        print("[Q SELL BOT] paused by bot_controls", flush=True)
        return None

    heartbeat(BOT_NAME, "running", "scan Q option exits")
    count = q_sell_once()
    heartbeat(BOT_NAME, "running", f"scan done closed_or_planned={count}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Q 期权卖出机器人")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=int, default=30)
    args = parser.parse_args()
    if args.loop:
        while True:
            print(run_once(), flush=True)
            time.sleep(args.interval)
    else:
        print(run_once(), flush=True)


if __name__ == "__main__":
    main()
