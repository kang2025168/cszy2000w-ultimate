from __future__ import annotations

"""A/C core-position intraday T bot."""

import argparse
import time

from ultimate_v1.schema import ensure_schema
from ultimate_v1.state_store import heartbeat, is_bot_enabled
from app.strategy_ac_t import run_strategy_ac_t_once

BOT_NAME = "ac_bot"


def run_once(action: str = "scan", symbol: str | None = None, group: str | None = None):
    """Run one AC T-strategy pass. buy/sell are kept as aliases for one symbol pass."""
    ensure_schema()
    if not is_bot_enabled(BOT_NAME):
        heartbeat(BOT_NAME, "paused", "机器人开关关闭")
        print("[AC BOT] paused by bot_controls", flush=True)
        return None
    heartbeat(BOT_NAME, "running", f"action={action}")
    if action == "scan":
        return run_strategy_ac_t_once(symbol=symbol)
    if action in {"buy", "sell"}:
        if not symbol:
            raise ValueError("A/C manual pass needs --symbol")
        print(f"[AC BOT] action={action} is handled by AC_T state machine for {symbol}", flush=True)
        return run_strategy_ac_t_once(symbol=symbol)
    raise ValueError("ac_bot only supports scan/buy/sell")


def main() -> None:
    parser = argparse.ArgumentParser(description="A/C intraday T bot")
    parser.add_argument("action", nargs="?", default="scan", choices=["scan", "buy", "sell"])
    parser.add_argument("--group", choices=["A", "C"])
    parser.add_argument("--symbol")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=int, default=300)
    args = parser.parse_args()
    if args.loop:
        while True:
            print(run_once(args.action, args.symbol, args.group), flush=True)
            time.sleep(args.interval)
    else:
        result = run_once(args.action, args.symbol, args.group)
        print(result, flush=True)


if __name__ == "__main__":
    main()
