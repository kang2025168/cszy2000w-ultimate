from __future__ import annotations

"""C 类策略：长期优质股占位实现。

目标：
1. 只做 NVDA / MSFT / AAPL / AMZN / META 等长期优质股。
2. 中长期持有，不追妖股，不使用 margin。
3. 买入前受 C 资金池限制。
4. 后续可以接入基本面评分、趋势过滤、估值区间和分批买入。

伪代码：
1. 每天收盘后扫描 C_SYMBOLS。
2. 过滤财报风险、极端高估、趋势破坏的股票。
3. 如果 C 资金池低配，按评分分批买入。
4. 如果基本面恶化或大幅超配，只给出降低风险建议，不自动乱卖。
"""

from ..trading_gate import can_open_position
from .base import StrategyResult, default_notional

C_SYMBOLS = {"NVDA", "MSFT", "AAPL", "AMZN", "META", "GOOGL", "AVGO", "TSLA"}


def strategy_C_buy(symbol: str) -> StrategyResult:
    """C 类买入占位：现在只做检查和日志，不真实下单。"""
    symbol = symbol.upper()
    if symbol not in C_SYMBOLS:
        return StrategyResult(False, "C", symbol, "buy", "not_a_quality_symbol")
    notional = default_notional("C")
    allow, reason = can_open_position("C", notional)
    if not allow:
        return StrategyResult(False, "C", symbol, "buy", reason)
    print(f"[C TODO] {symbol} 通过资金/风控检查，后续接入长期优质股买入逻辑 notional={notional:.2f}", flush=True)
    return StrategyResult(True, "C", symbol, "buy", "placeholder_passed")


def strategy_C_sell(symbol: str) -> StrategyResult:
    """C 类卖出占位：默认只输出建议，不自动卖长期优质股。"""
    symbol = symbol.upper()
    print(f"[C TODO] {symbol} 长期优质股不自动卖出，后续接入基本面恶化/人工再平衡逻辑", flush=True)
    return StrategyResult(True, "C", symbol, "sell", "placeholder_no_auto_sell")

