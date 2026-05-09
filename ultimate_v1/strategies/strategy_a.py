from __future__ import annotations

"""A 类策略：指数底仓占位实现。

目标：
1. 只做 QQQ / SPY / VOO / SCHG 这类指数或成长 ETF。
2. 长期持有，不使用 margin。
3. 不做日内高频，不追涨杀跌。
4. 买入前必须经过 Ultimate V1 的风控和资金池检查。

后续真实逻辑可以按这个伪代码落地：
1. 每月或每周检查 A 资金池是否低配。
2. 如果低配，优先买入目标 ETF 中当前偏离最低的一只。
3. 单笔金额不超过 A 池可用资金的一部分。
4. 卖出只在极端风控或人工再平衡时执行，不自动清长期仓。
"""

from ..trading_gate import can_open_position
from .base import StrategyResult, default_notional

A_SYMBOLS = {"QQQ", "SPY", "VOO", "SCHG"}


def strategy_A_buy(symbol: str) -> StrategyResult:
    """A 类买入占位：现在只做检查和日志，不真实下单。"""
    symbol = symbol.upper()
    if symbol not in A_SYMBOLS:
        return StrategyResult(False, "A", symbol, "buy", "not_a_allowed_symbol")
    notional = default_notional("A")
    allow, reason = can_open_position("A", notional)
    if not allow:
        return StrategyResult(False, "A", symbol, "buy", reason)
    print(f"[A TODO] {symbol} 通过资金/风控检查，后续接入指数底仓买入逻辑 notional={notional:.2f}", flush=True)
    return StrategyResult(True, "A", symbol, "buy", "placeholder_passed")


def strategy_A_sell(symbol: str) -> StrategyResult:
    """A 类卖出占位：长期仓默认不自动卖出。"""
    symbol = symbol.upper()
    print(f"[A TODO] {symbol} 长期底仓不自动卖出，后续只接人工/再平衡降低风险逻辑", flush=True)
    return StrategyResult(True, "A", symbol, "sell", "placeholder_no_auto_sell")

