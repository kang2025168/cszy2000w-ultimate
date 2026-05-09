from __future__ import annotations

"""风险机器人：计算风险状态，写入 risk_state，影响看板机器人。"""

import argparse
import time

from ..risk_controller import get_risk_state, log_risk_state
from ..schema import ensure_schema
from ..state_store import heartbeat, write_risk_state

BOT_NAME = "risk_bot"


def refresh_risk_state():
    """计算并写入最新风险状态。"""
    ensure_schema()
    state = log_risk_state()
    write_risk_state(state)
    heartbeat(BOT_NAME, "running", f"risk_multiplier={state.risk_multiplier:.2f} reason={state.reason or 'allow'}")
    return state


def loop(interval_sec: int) -> None:
    """循环刷新风险状态。"""
    while True:
        refresh_risk_state()
        time.sleep(interval_sec)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ultimate V1 风险机器人")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=int, default=60)
    args = parser.parse_args()
    if args.loop:
        loop(args.interval)
    else:
        refresh_risk_state()


if __name__ == "__main__":
    main()

