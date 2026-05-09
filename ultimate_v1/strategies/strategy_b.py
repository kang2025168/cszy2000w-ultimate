from __future__ import annotations

"""B 类策略适配器：把旧项目里的成熟 B 策略接入 Ultimate V1。

说明：
1. 旧 B 策略逻辑目前保留在 `app.strategy_b`，避免一次性搬动三千多行代码造成风险。
2. 新入口先走 Ultimate V1 的风控和资金池检查。
3. 检查通过后，再调用旧的 `strategy_B_buy/strategy_B_sell`。
4. 后续可以逐步把旧 B 内部函数拆到本目录，最终完全脱离旧 app。
"""

from ..config import env_float
from ..trading_gate import can_open_position
from .base import StrategyResult, mark_strategy_group


def _estimated_notional() -> float:
    """读取 B 单笔计划金额，用于下单前资金池检查。"""
    return env_float("B_TARGET_NOTIONAL_USD", env_float("B_MAX_NOTIONAL_USD", 2500.0))


def strategy_B_buy(symbol: str) -> StrategyResult:
    """B 类买入：先过 V1 总控，再调用旧 B 买入函数。"""
    symbol = symbol.upper()
    notional = _estimated_notional()
    allow, reason = can_open_position("B", notional)
    if not allow:
        print(f"[B V1 BLOCK] symbol={symbol} reason={reason}", flush=True)
        return StrategyResult(False, "B", symbol, "buy", reason)

    mark_strategy_group(symbol, "B", capital_pool="B", margin_used=0)
    from app.strategy_b import strategy_B_buy as legacy_strategy_B_buy

    ok = bool(legacy_strategy_B_buy(symbol))
    return StrategyResult(ok, "B", symbol, "buy", "legacy_b_buy_called")


def strategy_B_sell(symbol: str) -> StrategyResult:
    """B 类卖出：卖出不受新开仓资金池限制，直接调用旧 B 卖出函数。"""
    symbol = symbol.upper()
    mark_strategy_group(symbol, "B", capital_pool="B", margin_used=0)
    from app.strategy_b import strategy_B_sell as legacy_strategy_B_sell

    ok = bool(legacy_strategy_B_sell(symbol))
    return StrategyResult(ok, "B", symbol, "sell", "legacy_b_sell_called")

