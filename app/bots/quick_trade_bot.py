from __future__ import annotations

"""Quick trading bot entrypoint."""

import argparse
import os
import time

from app.quick_trade import DRY_RUN, heartbeat_message, run_once
from ultimate_v1.schema import ensure_schema
from ultimate_v1.state_store import heartbeat, is_bot_enabled


BOT_NAME = "quick_trade_bot"


def run_quick_once(execute: bool = True):
    ensure_schema()
    if not is_bot_enabled(BOT_NAME):
        heartbeat(BOT_NAME, "paused", "机器人开关关闭")
        print("[QUICK BOT] paused by bot_controls", flush=True)
        return None
    heartbeat(BOT_NAME, "running", "quick pass start")
    payload = run_once(dry_run=DRY_RUN, execute=execute)
    heartbeat(BOT_NAME, "running", heartbeat_message(payload))
    print(f"[QUICK BOT] {heartbeat_message(payload)} dry_run={DRY_RUN}", flush=True)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="快交易机器人")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=int, default=int(os.getenv("QUICK_BOT_INTERVAL_SEC", "5")))
    parser.add_argument("--preview", action="store_true", help="只生成计划，不提交订单")
    args = parser.parse_args()
    if args.loop:
        while True:
            run_quick_once(execute=not args.preview)
            time.sleep(max(args.interval, 1))
    else:
        print(run_quick_once(execute=not args.preview), flush=True)


if __name__ == "__main__":
    main()
