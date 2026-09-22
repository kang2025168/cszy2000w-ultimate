from __future__ import annotations

"""Single-process runner for the Strategy D grid state machine."""

import argparse
import time

from ultimate_v1.d_grid import run_all
from ultimate_v1.schema import ensure_schema
from ultimate_v1.state_store import heartbeat, is_bot_enabled

BOT_NAME = "d_grid_bot"


def run_once() -> list[dict]:
    ensure_schema()
    if not is_bot_enabled(BOT_NAME):
        heartbeat(BOT_NAME, "paused", "机器人开关关闭")
        return []
    results = run_all()
    failed = sum(1 for row in results if not row["ok"])
    detail = "; ".join(
        f"{row['symbol']}={str(row['message'])[:120]}" for row in results
    ) or "no_enabled_symbol"
    heartbeat(
        BOT_NAME,
        "running" if not failed else "warning",
        f"symbols={len(results)} failed={failed} {detail}"[:512],
    )
    for row in results:
        print(f"[D GRID] {row['symbol']} ok={row['ok']} {row['message']}", flush=True)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="D 单循环网格机器人")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=float, default=3)
    args = parser.parse_args()
    if not args.loop:
        print(run_once(), flush=True)
        return
    while True:
        try:
            run_once()
        except Exception as exc:
            heartbeat(BOT_NAME, "error", str(exc))
            print(f"[D GRID ERROR] {exc}", flush=True)
        time.sleep(max(args.interval, 1))


if __name__ == "__main__":
    main()
