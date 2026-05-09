from __future__ import annotations

"""D 类策略：日内交易占位实现。

目标：
1. 只使用保证金额度剩余部分，即 D 资金池。
2. 禁止隔夜，接近收盘必须强制平仓。
3. 买入前必须经过风控和 D 资金池检查。
4. SAFE / RISK_OFF 模式下禁止开 D。

伪代码：
1. 盘中扫描高流动性标的。
2. 只在明确日内信号出现时开仓。
3. 设置非常短的止损和时间止损。
4. 到 MARKET_CLOSE_FLATTEN_TIME 时调用 intraday_flatten 强制清空 D。
"""

from ..intraday_flatten import flatten_d_positions
from ..trading_gate import can_open_position
from .base import StrategyResult, default_notional


def strategy_D_buy(symbol: str) -> StrategyResult:
    """D 类买入占位：现在只做检查和日志，不真实下单。"""
    symbol = symbol.upper()
    notional = default_notional("D")
    allow, reason = can_open_position("D", notional)
    if not allow:
        return StrategyResult(False, "D", symbol, "buy", reason)
    print(f"[D TODO] {symbol} 通过资金/风控检查，后续接入日内买入逻辑 notional={notional:.2f}", flush=True)
    return StrategyResult(True, "D", symbol, "buy", "placeholder_passed")


def strategy_D_sell(symbol: str) -> StrategyResult:
    """D 类卖出占位：普通卖出逻辑后续接入，强平由 flatten_d_positions 负责。"""
    symbol = symbol.upper()
    print(f"[D TODO] {symbol} 后续接入日内主动卖出逻辑；收盘强平已有独立模块", flush=True)
    return StrategyResult(True, "D", symbol, "sell", "placeholder_no_order")


def force_flatten() -> int:
    """强制执行 D 类日内清仓。"""
    return flatten_d_positions(force=True)

