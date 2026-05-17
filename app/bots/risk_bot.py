from __future__ import annotations

"""风险机器人。

负责计算最新风险状态并写入 risk_state，
看板和资金池会根据这里的结果限制新开仓。
"""

import argparse
import time

from ultimate_v1.risk_controller import log_risk_state
from ultimate_v1.schema import ensure_schema
from ultimate_v1.state_store import heartbeat, write_risk_state

BOT_NAME = "risk_bot"


def refresh_risk_state():
    """计算并写入最新风险状态。"""
    ensure_schema()
    state = log_risk_state()
    write_risk_state(state)
    heartbeat(
        BOT_NAME,
        "running",
        f"risk={state.risk_multiplier:.2f} trend={state.market_trend} vix={state.vix:.1f} exposure={state.recommended_exposure:.0%}",
    )
    return state


def loop(interval_sec: int) -> None:
    """循环刷新风险状态。"""
    while True:
        refresh_risk_state()
        time.sleep(interval_sec)


def main() -> None:
    parser = argparse.ArgumentParser(description="风险机器人")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=int, default=60)
    args = parser.parse_args()
    if args.loop:
        loop(args.interval)
    else:
        refresh_risk_state()


if __name__ == "__main__":
    main()
