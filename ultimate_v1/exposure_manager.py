from __future__ import annotations

"""总仓位管理器：按风险目标仓位，对各策略当前持仓做等比例调仓规划。"""

import math
import os
from dataclasses import dataclass
from datetime import datetime

from . import alpaca_gateway
from .config import env_float, env_str
from .db import db_conn, fetch_all, fetch_one
from .risk_controller import get_risk_state


GROUPS = ("A", "B", "C", "D", "F")


@dataclass
class Holding:
    symbol: str
    strategy_group: str
    qty: float
    price: float
    market_value: float


@dataclass
class ExposurePlan:
    round_id: str
    mode: str
    risk_mode: str
    market_trend: str
    vix: float
    equity: float
    current_market_value: float
    current_exposure_pct: float
    target_market_value: float
    target_exposure_pct: float
    exposure_gap_value: float
    exposure_gap_pct: float
    scale_ratio: float
    action: str
    reason: str
    actions: list[dict]


def _safe_float(v, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _target_exposure_pct() -> tuple[float, str]:
    """根据当前风险状态给出总仓位目标。"""
    risk = get_risk_state()
    if risk.mode == "RISK_OFF" or risk.block_all_new or risk.risk_multiplier <= 0:
        return env_float("REBALANCE_TARGET_RISK_OFF", 0.05), "risk_off"
    if risk.market_trend == "向上" and risk.vix < env_float("REBALANCE_LOW_VIX", 20.0):
        return env_float("REBALANCE_TARGET_UP", 0.85), "up_low_vix"
    if risk.market_trend == "向下":
        return env_float("REBALANCE_TARGET_DOWN", 0.25), "downtrend"
    return env_float("REBALANCE_TARGET_SIDEWAYS", 0.55), "sideways"


def _load_open_holdings() -> list[Holding]:
    rows = fetch_all(
        """
        SELECT
            symbol,
            CASE
                WHEN strategy_group IN ('A','B','C','D','F') THEN strategy_group
                WHEN stock_type IN ('A','B','C','D','F') THEN stock_type
                ELSE strategy_group
            END AS strategy_group,
            qty,
            avg_entry_price,
            current_price,
            market_value,
            cost_basis
        FROM position_holdings
        WHERE status='open'
          AND qty > 0
        """
    )
    holdings: list[Holding] = []
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        group = str(row.get("strategy_group") or "").strip().upper()
        if not symbol or group not in GROUPS:
            continue
        qty = _safe_float(row.get("qty"))
        price = _safe_float(row.get("current_price")) or _safe_float(row.get("avg_entry_price"))
        market_value = abs(_safe_float(row.get("market_value")))
        if market_value <= 0 and qty > 0 and price > 0:
            market_value = abs(qty * price)
        if qty <= 0 or price <= 0 or market_value <= 0:
            continue
        holdings.append(Holding(symbol, group, qty, price, market_value))
    return holdings


def _account_equity() -> float:
    snap = alpaca_gateway.get_account_snapshot()
    if snap and snap.equity > 0:
        return float(snap.equity)
    row = fetch_one("SELECT equity FROM account_equity_snapshots ORDER BY created_at DESC LIMIT 1")
    return _safe_float((row or {}).get("equity"))


def _latest_position_map() -> dict[str, float]:
    """读取 Alpaca 真实持仓，自动执行时用它限制卖出数量。"""
    try:
        positions = alpaca_gateway.list_positions()
    except Exception as exc:
        print(f"[REBALANCE] cannot load Alpaca positions: {exc}", flush=True)
        return {}
    out = {}
    for pos in positions:
        symbol = str(getattr(pos, "symbol", "") or "").upper()
        qty = _safe_float(getattr(pos, "qty", 0))
        if symbol and qty > 0:
            out[symbol] = qty
    return out


def build_exposure_plan(mode: str | None = None) -> ExposurePlan:
    """生成一次等比例总仓位调仓计划。"""
    mode = (mode or env_str("REBALANCE_BOT_MODE", "SUGGEST")).strip().upper()
    risk = get_risk_state()
    equity = _account_equity()
    holdings = _load_open_holdings()
    current_value = sum(h.market_value for h in holdings)
    current_pct = current_value / equity if equity > 0 else 0.0
    target_pct, target_reason = _target_exposure_pct()
    target_pct = max(0.0, min(1.0, float(target_pct)))
    target_value = equity * target_pct if equity > 0 else 0.0
    gap_value = target_value - current_value
    gap_pct = target_pct - current_pct
    tolerance = env_float("REBALANCE_TOLERANCE_PCT", 0.03)
    min_trade = env_float("REBALANCE_MIN_TRADE_USD", 100.0)
    round_id = datetime.now().strftime("%Y%m%d%H%M%S")

    if equity <= 0:
        return ExposurePlan(round_id, mode, risk.mode, risk.market_trend, risk.vix, equity, current_value, current_pct, target_value, target_pct, gap_value, gap_pct, 1.0, "HOLD", "equity_unavailable", [])
    if current_value <= 0:
        return ExposurePlan(round_id, mode, risk.mode, risk.market_trend, risk.vix, equity, current_value, current_pct, target_value, target_pct, gap_value, gap_pct, 1.0, "HOLD", "no_open_holdings", [])
    if abs(gap_pct) <= tolerance:
        return ExposurePlan(round_id, mode, risk.mode, risk.market_trend, risk.vix, equity, current_value, current_pct, target_value, target_pct, gap_value, gap_pct, 1.0, "HOLD", f"within_tolerance {target_reason}", [])

    scale_ratio = target_value / current_value if current_value > 0 else 1.0
    action = "BUY" if scale_ratio > 1 else "SELL"
    actions: list[dict] = []

    for holding in holdings:
        target_holding_value = holding.market_value * scale_ratio
        delta_value = target_holding_value - holding.market_value
        if abs(delta_value) < min_trade:
            continue
        side = "buy" if delta_value > 0 else "sell"
        qty = abs(delta_value) / holding.price if holding.price > 0 else 0.0
        if qty <= 0:
            continue
        actions.append(
            {
                "round_id": round_id,
                "symbol": holding.symbol,
                "strategy_group": holding.strategy_group,
                "side": side,
                "current_value": round(holding.market_value, 2),
                "target_value": round(target_holding_value, 2),
                "delta_value": round(abs(delta_value), 2),
                "qty": qty,
                "price": holding.price,
                "status": "planned",
                "reason": f"proportional_{action.lower()} scale={scale_ratio:.4f} target={target_pct:.0%} {target_reason}",
            }
        )

    return ExposurePlan(
        round_id=round_id,
        mode=mode,
        risk_mode=risk.mode,
        market_trend=risk.market_trend,
        vix=risk.vix,
        equity=equity,
        current_market_value=current_value,
        current_exposure_pct=current_pct,
        target_market_value=target_value,
        target_exposure_pct=target_pct,
        exposure_gap_value=gap_value,
        exposure_gap_pct=gap_pct,
        scale_ratio=scale_ratio,
        action=action,
        reason=f"{target_reason}; proportional resize current={current_pct:.1%} target={target_pct:.1%}",
        actions=actions,
    )


def persist_exposure_plan(plan: ExposurePlan) -> None:
    """保存总仓位状态和本轮调仓建议。"""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO exposure_state (
                    mode, risk_mode, market_trend, vix, equity,
                    current_market_value, current_exposure_pct,
                    target_market_value, target_exposure_pct,
                    exposure_gap_value, exposure_gap_pct, scale_ratio,
                    action, reason, created_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                """,
                (
                    plan.mode,
                    plan.risk_mode,
                    plan.market_trend,
                    plan.vix,
                    plan.equity,
                    plan.current_market_value,
                    plan.current_exposure_pct,
                    plan.target_market_value,
                    plan.target_exposure_pct,
                    plan.exposure_gap_value,
                    plan.exposure_gap_pct,
                    plan.scale_ratio,
                    plan.action,
                    plan.reason,
                ),
            )
            for action in plan.actions:
                cur.execute(
                    """
                    INSERT INTO rebalance_actions (
                        round_id, symbol, strategy_group, side,
                        current_value, target_value, delta_value,
                        qty, price, status, reason, created_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    """,
                    (
                        action["round_id"],
                        action["symbol"],
                        action["strategy_group"],
                        action["side"],
                        action["current_value"],
                        action["target_value"],
                        action["delta_value"],
                        action["qty"],
                        action["price"],
                        action["status"],
                        action["reason"][:255],
                    ),
                )


def _group_allowed(side: str, group: str) -> bool:
    side = side.upper()
    group = group.upper()
    if side == "BUY" and os.getenv("REBALANCE_ALLOW_BUY", "0") != "1":
        return False
    if side == "SELL" and os.getenv("REBALANCE_ALLOW_SELL", "1") != "1":
        return False
    return os.getenv(f"REBALANCE_ALLOW_{group}_{side}", "1" if group in {"B", "F", "D"} or side == "BUY" else "0") == "1"


def _submit_market(symbol: str, side: str, qty: int):
    from alpaca.trading.enums import OrderSide, TimeInForce
    from alpaca.trading.requests import MarketOrderRequest

    tc = alpaca_gateway.trading_client()
    req = MarketOrderRequest(
        symbol=symbol,
        qty=int(qty),
        side=OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL,
        time_in_force=TimeInForce.DAY,
    )
    return tc.submit_order(order_data=req)


def execute_exposure_plan(plan: ExposurePlan) -> list[dict]:
    """AUTO 模式下按计划提交订单；SUGGEST/OFF 只返回 planned/skipped。"""
    mode = plan.mode.upper()
    if mode != "AUTO":
        return [{**a, "status": "planned"} for a in plan.actions]

    max_buy = env_float("REBALANCE_MAX_BUY_PER_ROUND_USD", 1500.0)
    max_sell = env_float("REBALANCE_MAX_SELL_PER_ROUND_USD", 1500.0)
    min_trade = env_float("REBALANCE_MIN_TRADE_USD", 100.0)
    bought = 0.0
    sold = 0.0
    real_qty = _latest_position_map()
    results: list[dict] = []

    for action in plan.actions:
        side = str(action["side"]).lower()
        group = str(action["strategy_group"]).upper()
        value = float(action["delta_value"])
        status = "skipped"
        order_id = ""
        reason = str(action["reason"])

        if value < min_trade:
            reason = "below_min_trade"
        elif not _group_allowed(side, group):
            reason = f"permission_denied {group}_{side}"
        elif side == "buy" and bought + value > max_buy:
            reason = "round_buy_limit"
        elif side == "sell" and sold + value > max_sell:
            reason = "round_sell_limit"
        else:
            qty = int(math.floor(float(action["qty"])))
            if side == "sell":
                qty = min(qty, int(math.floor(real_qty.get(str(action["symbol"]), 0.0))))
            if qty <= 0:
                reason = "qty_floor_zero"
            else:
                try:
                    order = _submit_market(str(action["symbol"]), side, qty)
                    order_id = str(getattr(order, "id", "") or getattr(order, "order_id", "") or "")
                    status = "submitted"
                    if side == "buy":
                        bought += value
                    else:
                        sold += value
                except Exception as exc:
                    status = "failed"
                    reason = f"submit_error {str(exc)[:180]}"

        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE rebalance_actions
                    SET status=%s, reason=%s, order_id=%s,
                        executed_at=IF(%s IN ('submitted','failed','skipped'), NOW(), executed_at)
                    WHERE round_id=%s AND symbol=%s AND strategy_group=%s AND side=%s
                    """,
                    (
                        status,
                        reason[:255],
                        order_id,
                        status,
                        action["round_id"],
                        action["symbol"],
                        action["strategy_group"],
                        action["side"],
                    ),
                )
        results.append({**action, "status": status, "order_id": order_id, "reason": reason})
    return results


def refresh_exposure_plan(mode: str | None = None, execute: bool = True) -> ExposurePlan:
    """生成、保存并按配置执行一次自动调仓计划。"""
    plan = build_exposure_plan(mode)
    persist_exposure_plan(plan)
    if execute:
        plan.actions = execute_exposure_plan(plan)
    return plan


def latest_exposure_state() -> dict | None:
    return fetch_one("SELECT * FROM exposure_state ORDER BY id DESC LIMIT 1")


def latest_rebalance_actions(limit: int = 100) -> list[dict]:
    return fetch_all(
        """
        SELECT *
        FROM rebalance_actions
        ORDER BY id DESC
        LIMIT %s
        """,
        (int(limit),),
    )
