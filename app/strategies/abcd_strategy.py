from __future__ import annotations

"""ABCD 买卖策略统一入口。

本文件集中管理 A/B/C/D 四类策略的买入、卖出和 D 类强平入口。
机器人只调用这里的函数，不再分散引用多个旧策略文件。
"""

from dataclasses import dataclass

from ultimate_v1.config import env_float, settings
from ultimate_v1.db import db_conn
from ultimate_v1.intraday_flatten import flatten_d_positions
from ultimate_v1.schema import ensure_schema
from ultimate_v1.trading_gate import can_open_position

A_SYMBOLS = {"QQQ", "SPY", "VOO", "SCHG"}
C_SYMBOLS = {"NVDA", "MSFT", "AAPL", "AMZN", "META", "GOOGL", "AVGO", "TSLA"}


@dataclass
class StrategyResult:
    """策略执行结果，方便调度器和日志统一处理。"""

    ok: bool
    strategy_group: str
    symbol: str
    action: str
    reason: str = ""


def default_notional(strategy_group: str) -> float:
    """按策略组读取默认单笔金额，用来做开仓前资金检查。"""
    group = (strategy_group or "").upper()
    return env_float(f"{group}_TARGET_NOTIONAL_USD", env_float("DEFAULT_TARGET_NOTIONAL_USD", 500.0))


def mark_strategy_group(symbol: str, strategy_group: str, *, capital_pool: str | None = None, margin_used: int = 0) -> None:
    """把旧表记录补上 strategy_group/capital_pool，方便资金池统计。"""
    s = settings()
    with db_conn(s) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE `{s.ops_table}`
                SET strategy_group=%s,
                    capital_pool=%s,
                    margin_used=%s,
                    last_capital_check_at=NOW()
                WHERE stock_code=%s
                  AND UPPER(COALESCE(NULLIF(strategy_group, ''), stock_type))=%s
                """,
                (strategy_group.upper(), capital_pool or strategy_group.upper(), margin_used, symbol.upper(), strategy_group.upper()),
            )


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


def _b_buy_plan() -> dict:
    """读取 B 买入计划；失败时退回静态金额。"""
    try:
        from app.strategy_b import get_b_buy_plan_for_gate

        return get_b_buy_plan_for_gate()
    except Exception as exc:
        print(f"[B V1 PLAN] fallback static notional: {exc}", flush=True)
    return {"dynamic": False}


def _estimated_b_notional() -> float:
    """读取 B 单笔计划金额，用于下单前资金池检查。"""
    plan = _b_buy_plan()
    if bool(plan.get("dynamic")):
        return float(plan.get("target_notional") or 0.0)
    return env_float("B_TARGET_NOTIONAL_USD", env_float("B_MAX_NOTIONAL_USD", 2500.0))


def strategy_B_buy(symbol: str) -> StrategyResult:
    """B 类买入：先过 V1 总控，再调用旧 B 买入函数。"""
    symbol = symbol.upper()
    plan = _b_buy_plan()
    if bool(plan.get("dynamic")) and int(plan.get("remaining_slots") or 0) <= 0:
        return StrategyResult(False, "B", symbol, "buy", "max_b_positions")
    notional = _estimated_b_notional()
    allow, reason = can_open_position("B", notional)
    if not allow:
        print(f"[B V1 BLOCK] symbol={symbol} reason={reason}", flush=True)
        return StrategyResult(False, "B", symbol, "buy", reason)

    from app.strategy_b import strategy_B_buy as legacy_strategy_B_buy

    ok = bool(legacy_strategy_B_buy(symbol))
    if ok:
        mark_strategy_group(symbol, "B", capital_pool="B", margin_used=0)
    return StrategyResult(ok, "B", symbol, "buy", "legacy_b_buy_ok" if ok else "legacy_b_buy_blocked")


def strategy_B_sell(symbol: str) -> StrategyResult:
    """B 类卖出：卖出不受新开仓资金池限制，直接调用旧 B 卖出函数。"""
    symbol = symbol.upper()
    from app.strategy_b import strategy_B_sell as legacy_strategy_B_sell

    ok = bool(legacy_strategy_B_sell(symbol))
    if ok:
        mark_strategy_group(symbol, "B", capital_pool="B", margin_used=0)
    return StrategyResult(ok, "B", symbol, "sell", "legacy_b_sell_ok" if ok else "legacy_b_sell_no_action")


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

