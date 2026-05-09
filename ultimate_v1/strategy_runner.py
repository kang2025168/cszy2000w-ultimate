from __future__ import annotations

"""ABCD 策略统一调度器。

命令示例：
python -m ultimate_v1.strategy_runner B buy QQQ
python -m ultimate_v1.strategy_runner B sell QQQ
python -m ultimate_v1.strategy_runner D flatten
"""

import argparse

from .schema import ensure_schema
from .strategies.strategy_a import strategy_A_buy, strategy_A_sell
from .strategies.strategy_b import strategy_B_buy, strategy_B_sell
from .strategies.strategy_c import strategy_C_buy, strategy_C_sell
from .strategies.strategy_d import force_flatten, strategy_D_buy, strategy_D_sell


BUY_HANDLERS = {
    "A": strategy_A_buy,
    "B": strategy_B_buy,
    "C": strategy_C_buy,
    "D": strategy_D_buy,
}

SELL_HANDLERS = {
    "A": strategy_A_sell,
    "B": strategy_B_sell,
    "C": strategy_C_sell,
    "D": strategy_D_sell,
}


def run_strategy(strategy_group: str, action: str, symbol: str | None = None):
    """根据策略组和动作调用对应策略入口。"""
    ensure_schema()
    group = strategy_group.upper()
    action = action.lower()
    if group == "D" and action == "flatten":
        count = force_flatten()
        print(f"[STRATEGY RUNNER] D flatten count={count}", flush=True)
        return count
    if not symbol:
        raise ValueError("买入/卖出必须提供 symbol")
    if action == "buy":
        return BUY_HANDLERS[group](symbol)
    if action == "sell":
        return SELL_HANDLERS[group](symbol)
    raise ValueError(f"不支持的动作: {action}")


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

