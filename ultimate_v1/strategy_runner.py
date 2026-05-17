from __future__ import annotations

"""ABCD 策略统一调度器。

命令示例：
python -m ultimate_v1.strategy_runner B buy QQQ
python -m ultimate_v1.strategy_runner B sell QQQ
python -m ultimate_v1.strategy_runner D flatten
"""

import argparse

from app.strategies.abcd_strategy import run_strategy


def main() -> None:
    parser = argparse.ArgumentParser(description="Ultimate V1 ABCD 策略调度器")
    parser.add_argument("strategy_group", choices=["A", "B", "C", "D"])
    parser.add_argument("action", choices=["buy", "sell", "flatten"])
    parser.add_argument("symbol", nargs="?")
    args = parser.parse_args()
    result = run_strategy(args.strategy_group, args.action, args.symbol)
    print(result, flush=True)


if __name__ == "__main__":
    main()
