from __future__ import annotations

"""自动调仓机器人。

根据风险机器人给出的目标总仓位，按当前 A/B/C/D/F 持仓市值等比例
生成调仓计划。默认 SUGGEST 模式只写建议；AUTO 模式才会按权限下单。
"""

import argparse
import os
import time

from ultimate_v1.exposure_manager import refresh_exposure_plan
from ultimate_v1.schema import ensure_schema
from ultimate_v1.state_store import heartbeat, is_bot_enabled


BOT_NAME = "rebalance_bot"


def run_once(mode: str | None = None):
    """执行一次总仓位检查和调仓规划。"""
    ensure_schema()
    if not is_bot_enabled(BOT_NAME):
        heartbeat(BOT_NAME, "paused", "机器人开关关闭")
        print("[REBALANCE BOT] paused by bot_controls", flush=True)
        return None

    mode = (mode or os.getenv("REBALANCE_BOT_MODE", "SUGGEST")).strip().upper()
    if mode not in {"OFF", "SUGGEST", "AUTO"}:
        mode = "SUGGEST"

    if mode == "OFF":
        heartbeat(BOT_NAME, "paused", "REBALANCE_BOT_MODE=OFF")
        print("[REBALANCE BOT] off", flush=True)
        return None

    plan = refresh_exposure_plan(mode=mode, execute=True)
    heartbeat(
        BOT_NAME,
        "running",
        (
            f"mode={mode} action={plan.action} "
            f"cur={plan.current_exposure_pct:.1%} target={plan.target_exposure_pct:.1%} "
            f"actions={len(plan.actions)}"
        ),
    )
    print(
        f"[REBALANCE BOT] mode={mode} action={plan.action} "
        f"equity={plan.equity:.2f} current={plan.current_market_value:.2f} "
        f"cur_pct={plan.current_exposure_pct:.2%} target_pct={plan.target_exposure_pct:.2%} "
        f"gap={plan.exposure_gap_value:.2f} actions={len(plan.actions)}",
        flush=True,
    )
    for action in plan.actions:
        print(
            f"[REBALANCE ACTION] {action['side'].upper()} {action['strategy_group']} "
            f"{action['symbol']} value={float(action['delta_value']):.2f} "
            f"qty≈{float(action['qty']):.4f} status={action.get('status')}",
            flush=True,
        )
    return plan


def loop(interval_sec: int, mode: str | None = None) -> None:
    """循环运行自动调仓机器人。"""
    while True:
        run_once(mode)
        time.sleep(interval_sec)


def main() -> None:
    parser = argparse.ArgumentParser(description="自动调仓机器人")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=int, default=int(os.getenv("REBALANCE_INTERVAL_SEC", "900")))
    parser.add_argument("--mode", choices=["OFF", "SUGGEST", "AUTO"])
    args = parser.parse_args()
    if args.loop:
        loop(args.interval, args.mode)
    else:
        run_once(args.mode)


if __name__ == "__main__":
    main()
