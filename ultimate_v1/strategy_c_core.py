from __future__ import annotations

"""Automatic Strategy C core-position builder.

The builder uses pool C budget and broker buying power, including margin.
It fills the ETF
foundation first, then core leaders, then the remaining growth/diversifier
sleeve. Filled positions are handed to the existing AC-T state machine.
"""

import argparse
import json
import math
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, time as day_time
from zoneinfo import ZoneInfo

from . import alpaca_gateway
from .capital_manager import get_capital_allocation
from .config import env_bool, env_float, env_int, settings
from .db import db_conn
from .schema import ensure_schema
from .strategy_c_watchlist import STRATEGY_C_WATCHLIST
from .trading_gate import can_open_position


LA_TZ = ZoneInfo(settings().timezone or "America/Los_Angeles")
BUY_START = day_time(6, 40)
BUY_END = day_time(12, 30)
ACTIVE_ORDER_STATUSES = {
    "new",
    "accepted",
    "pending_new",
    "accepted_for_bidding",
    "partially_filled",
    "calculated",
    "held",
    "pending_replace",
    "pending_cancel",
}


@dataclass(frozen=True)
class CoreBuyPlan:
    symbol: str
    tier: int
    priority: int
    weight: float
    current_value: float
    target_value: float
    deficit: float
    notional: float


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value if value is not None else default)
    except Exception:
        return default


def _status_text(value) -> str:
    return str(value or "").split(".")[-1].strip().lower()


def _today():
    return datetime.now(LA_TZ).date()


def _within_buy_window() -> bool:
    now = datetime.now(LA_TZ).time()
    return BUY_START <= now < BUY_END


def _candidate_deficits(target_capital: float, current_values: dict[str, float]) -> list[dict]:
    rows = []
    for item in STRATEGY_C_WATCHLIST:
        target = max(0.0, float(target_capital) * item.weight)
        current = max(0.0, float(current_values.get(item.symbol, 0.0) or 0.0))
        deficit = max(0.0, target - current)
        rows.append(
            {
                "item": item,
                "current": current,
                "target": target,
                "deficit": deficit,
                "deficit_ratio": deficit / target if target > 0 else 0.0,
            }
        )
    return rows


def build_c_core_buy_plan(
    *,
    target_capital: float,
    available_capital: float,
    buying_power: float,
    cash: float = 0.0,
    current_values: dict[str, float],
    daily_spent: float = 0.0,
    excluded_symbols: set[str] | None = None,
    min_order: float = 25.0,
    daily_budget_pct: float = 0.10,
    daily_budget_max: float = 250.0,
    cash_reserve: float = 25.0,
    max_orders: int = 3,
    tier_fill_ratio: float = 0.90,
) -> list[CoreBuyPlan]:
    """Create a target-gap plan bounded by pool budget and broker buying power.

    cash/cash_reserve remain accepted for compatibility, but do not cap C
    margin purchases. Retirement pool A has its own cash-only planner.
    """
    target_capital = max(0.0, float(target_capital or 0.0))
    min_order = max(1.0, float(min_order or 0.0))
    if target_capital <= 0:
        return []

    daily_cap = target_capital * max(0.0, float(daily_budget_pct or 0.0))
    if daily_budget_max > 0:
        daily_cap = min(daily_cap, float(daily_budget_max))
    daily_remaining = max(0.0, daily_cap - max(0.0, float(daily_spent or 0.0)))
    usable = min(
        max(0.0, float(available_capital or 0.0)),
        max(0.0, float(buying_power or 0.0)),
        daily_remaining,
    )
    if usable < min_order:
        return []

    excluded = {str(symbol).upper() for symbol in (excluded_symbols or set())}
    deficits = [row for row in _candidate_deficits(target_capital, current_values) if row["item"].symbol not in excluded]
    active_tier = None
    for tier in sorted({row["item"].tier for row in deficits}):
        tier_rows = [row for row in deficits if row["item"].tier == tier]
        tier_target = sum(row["target"] for row in tier_rows)
        tier_current = sum(min(row["current"], row["target"]) for row in tier_rows)
        completion = tier_current / tier_target if tier_target > 0 else 1.0
        if completion < tier_fill_ratio and sum(row["deficit"] for row in tier_rows) >= min_order:
            active_tier = tier
            break
    if active_tier is None:
        available_rows = [row for row in deficits if row["deficit"] >= min_order]
    else:
        available_rows = [
            row for row in deficits if row["item"].tier == active_tier and row["deficit"] >= min_order
        ]
    if not available_rows:
        return []

    available_rows.sort(key=lambda row: (-row["deficit_ratio"], row["item"].priority, row["item"].symbol))
    selected = available_rows[: max(1, int(max_orders or 1))]

    while len(selected) > 1:
        total_deficit = sum(row["deficit"] for row in selected)
        allocations = [min(row["deficit"], usable * row["deficit"] / total_deficit) for row in selected]
        if all(value >= min_order for value in allocations):
            break
        smallest = min(range(len(selected)), key=lambda index: allocations[index])
        selected.pop(smallest)

    total_deficit = sum(row["deficit"] for row in selected)
    if total_deficit <= 0:
        return []
    allocations = [min(row["deficit"], usable * row["deficit"] / total_deficit) for row in selected]
    plans = []
    for row, allocation in zip(selected, allocations):
        amount = math.floor(float(allocation) * 100) / 100.0
        if amount < min_order:
            continue
        item = row["item"]
        plans.append(
            CoreBuyPlan(
                symbol=item.symbol,
                tier=item.tier,
                priority=item.priority,
                weight=item.weight,
                current_value=round(row["current"], 2),
                target_value=round(row["target"], 2),
                deficit=round(row["deficit"], 2),
                notional=round(amount, 2),
            )
        )
    return plans


def _current_c_values() -> dict[str, float]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT UPPER(symbol) AS symbol,
                       SUM(CASE
                             WHEN COALESCE(market_value,0)>0 THEN market_value
                             ELSE COALESCE(qty,0)*COALESCE(NULLIF(current_price,0),avg_entry_price,0)
                           END) AS value
                FROM position_holdings
                WHERE status='open'
                  AND COALESCE(qty,0)>0
                  AND UPPER(COALESCE(NULLIF(strategy_group,''),stock_type))='C'
                GROUP BY UPPER(symbol)
                """
            )
            return {str(row["symbol"]): _safe_float(row.get("value")) for row in cur.fetchall() or []}


def _busy_c_symbols() -> set[str]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT UPPER(stock_code) AS symbol
                FROM `{settings().ops_table}`
                WHERE UPPER(COALESCE(NULLIF(strategy_group,''),stock_type))='C'
                  AND UPPER(COALESCE(ac_t_state,'IDLE'))<>'IDLE'
                """
            )
            return {str(row["symbol"]) for row in cur.fetchall() or []}


def _daily_spent() -> float:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(filled_qty*filled_avg_price),0) AS spent
                FROM strategy_c_core_buys
                WHERE trade_date=%s AND filled_qty>0 AND filled_avg_price>0
                """,
                (_today(),),
            )
            return _safe_float((cur.fetchone() or {}).get("spent"))


def _record_attempt(plan: CoreBuyPlan, *, status: str, reason: str = "", order_id: str = "", limit_price: float = 0.0, filled_qty: float = 0.0, filled_avg: float = 0.0) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO strategy_c_core_buys (
                    trade_date,symbol,tier,target_weight,planned_notional,limit_price,
                    filled_qty,filled_avg_price,status,reason,order_id
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    _today(), plan.symbol, plan.tier, plan.weight, plan.notional, limit_price,
                    filled_qty, filled_avg, status[:32], reason[:255], order_id or None,
                ),
            )


@contextmanager
def _execution_lock():
    """Prevent overlapping C core-buy passes from submitting duplicate orders."""
    lock_name = "cszy:strategy_c_core_buy"
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT GET_LOCK(%s, 0) AS acquired", (lock_name,))
            acquired = int((cur.fetchone() or {}).get("acquired") or 0) == 1
            try:
                if acquired:
                    from .order_journal import execution_lock as account_lock
                    from .account_config import profile_for_pool
                    with account_lock(profile_for_pool("C")):
                        yield True
                else:
                    yield False
            finally:
                if acquired:
                    cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))


def _has_open_buy(client, symbol: str) -> bool:
    try:
        from alpaca.trading.enums import QueryOrderStatus
        from alpaca.trading.requests import GetOrdersRequest

        orders = client.get_orders(filter=GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[symbol]))
        return any(str(getattr(order, "side", "")).lower().endswith("buy") for order in orders or [])
    except Exception as exc:
        print(f"[C CORE] open-order check failed {symbol}: {exc}", flush=True)
        return True


def _stock_qty_for_notional(notional: float, price: float) -> float:
    """All automated stock buys use 0.1-share lots; never create dust below 0.1."""
    if notional <= 0 or price <= 0:
        return 0.0
    return math.floor((float(notional) / float(price)) * 10) / 10.0


def _submit_and_wait(client, plan: CoreBuyPlan, limit_price: float, qty: float) -> tuple[str, str, float, float, str]:
    from alpaca.trading.enums import OrderSide, TimeInForce
    from alpaca.trading.requests import LimitOrderRequest

    client_order_id = f"c-core-{_today().strftime('%y%m%d')}-{plan.symbol.replace('.', '')}-{uuid.uuid4().hex[:8]}"[:48]
    request = LimitOrderRequest(
        symbol=plan.symbol,
        qty=round(qty, 1),
        side=OrderSide.BUY,
        limit_price=alpaca_gateway.stock_limit_price(limit_price),
        time_in_force=TimeInForce.DAY,
        client_order_id=client_order_id,
    )
    order = client.submit_order(order_data=request)
    order_id = str(getattr(order, "id", "") or "")
    status = _status_text(getattr(order, "status", "submitted"))
    filled_qty = _safe_float(getattr(order, "filled_qty", 0))
    filled_avg = _safe_float(getattr(order, "filled_avg_price", 0))
    poll_error = ""
    deadline = time.time() + max(2.0, env_float("C_CORE_FILL_WAIT_SEC", 8.0))
    while time.time() < deadline:
        try:
            fresh = client.get_order_by_id(order_id)
        except Exception as exc:
            poll_error = f"poll_failed:{exc}"
            time.sleep(0.5)
            continue
        status = _status_text(getattr(fresh, "status", status))
        filled_qty = _safe_float(getattr(fresh, "filled_qty", 0))
        filled_avg = _safe_float(getattr(fresh, "filled_avg_price", 0))
        if status not in ACTIVE_ORDER_STATUSES:
            break
        time.sleep(0.5)
    if status in ACTIVE_ORDER_STATUSES:
        try:
            client.cancel_order_by_id(order_id)
            status = "cancel_requested"
            cancel_deadline = time.time() + 2.0
            while time.time() < cancel_deadline:
                try:
                    fresh = client.get_order_by_id(order_id)
                    status = _status_text(getattr(fresh, "status", status))
                    filled_qty = _safe_float(getattr(fresh, "filled_qty", filled_qty))
                    filled_avg = _safe_float(getattr(fresh, "filled_avg_price", filled_avg))
                    if status not in ACTIVE_ORDER_STATUSES:
                        break
                except Exception as exc:
                    poll_error = f"cancel_poll_failed:{exc}"
                time.sleep(0.25)
        except Exception as exc:
            return order_id, status, filled_qty, filled_avg, f"cancel_failed:{exc}"
    return order_id, status, filled_qty, filled_avg, poll_error


def _record_fill(plan: CoreBuyPlan, filled_qty: float, filled_avg: float, order_id: str) -> None:
    table = settings().ops_table
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id,qty,avg_entry_price
                FROM position_holdings
                WHERE UPPER(symbol)=%s AND strategy_group='C' AND status='open'
                ORDER BY id DESC LIMIT 1
                """,
                (plan.symbol,),
            )
            holding = cur.fetchone() or {}
            adopted_default_holding = False
            if not holding:
                cur.execute(
                    """
                    SELECT id,qty,avg_entry_price
                    FROM position_holdings
                    WHERE UPPER(symbol)=%s
                      AND strategy_group='B'
                      AND status='open'
                      AND notes='auto-created from Alpaca sync default=B'
                      AND last_order_id IS NULL
                    ORDER BY id DESC LIMIT 1
                    """,
                    (plan.symbol,),
                )
                holding = cur.fetchone() or {}
                adopted_default_holding = bool(holding)
            cur.execute(
                f"""
                SELECT id,qty,cost_price
                FROM `{table}`
                WHERE UPPER(stock_code)=%s AND stock_type='C'
                ORDER BY id DESC LIMIT 1
                """,
                (plan.symbol,),
            )
            operation = cur.fetchone() or {}
            if not operation:
                cur.execute(
                    f"""
                    INSERT INTO `{table}` (
                        stock_code,stock_type,weight,is_bought,can_buy,can_sell,qty,
                        strategy_group,capital_pool,margin_used,
                        ac_t_enabled,ac_t_type,ac_t_state,
                        last_order_intent,created_at,updated_at
                    ) VALUES (
                        %s,'C',%s,0,1,0,0,
                        'C','C',0,
                        0,'C','IDLE',
                        %s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP
                    )
                    """,
                    (plan.symbol, plan.weight, f"C:CORE_AUTO_RECOVERY tier={plan.tier}"[:80]),
                )
                operation = {"id": int(cur.lastrowid or 0), "qty": 0, "cost_price": 0}

            if adopted_default_holding:
                # The broker sync can observe a fill before this worker records it.
                # That row already contains the broker's post-fill total, so adding
                # filled_qty again would double count the newest fill.
                total_qty = _safe_float(holding.get("qty"), filled_qty)
                blended_avg = _safe_float(holding.get("avg_entry_price"), filled_avg)
            else:
                previous_qty = _safe_float(holding.get("qty"), _safe_float(operation.get("qty")))
                previous_avg = _safe_float(holding.get("avg_entry_price"), _safe_float(operation.get("cost_price")))
                total_qty = previous_qty + filled_qty
                blended_avg = (
                    (previous_qty * previous_avg + filled_qty * filled_avg) / total_qty
                    if total_qty > 0 else filled_avg
                )
            cur.execute(
                f"""
                UPDATE `{table}`
                SET is_bought=1,can_buy=1,can_sell=1,
                    qty=%s,cost_price=%s,current_price=%s,close_price=%s,
                    strategy_group='C',capital_pool='C',margin_used=0,
                    ac_t_enabled=1,ac_t_type='C',
                    ac_t_core_qty=%s,
                    last_order_side='buy',last_order_intent=%s,
                    last_order_id=%s,last_order_time=NOW(),updated_at=NOW()
                WHERE id=%s
                """,
                (
                    total_qty, blended_avg, filled_avg, filled_avg,
                    int(math.floor(total_qty)),
                    f"C:CORE_AUTO tier={plan.tier} notional={plan.notional:.2f}"[:80],
                    order_id, operation.get("id"),
                ),
            )
            if cur.rowcount != 1:
                raise RuntimeError(f"failed to update C operation row for {plan.symbol}")
            if holding:
                cur.execute(
                    """
                    UPDATE position_holdings
                    SET qty=%s,avg_entry_price=%s,current_price=%s,
                        cost_basis=%s,market_value=%s,
                        unrealized_pnl=%s,
                        unrealized_pnl_pct=CASE WHEN %s>0 THEN (%s-%s)/%s ELSE 0 END,
                        stock_type='C',strategy_group='C',capital_pool='C',margin_used=0,
                        last_order_id=%s,last_order_side='buy',last_update_time=NOW(),
                        notes=%s
                    WHERE id=%s
                    """,
                    (
                        total_qty, blended_avg, filled_avg,
                        total_qty * blended_avg, total_qty * filled_avg,
                        total_qty * (filled_avg - blended_avg),
                        blended_avg, filled_avg, blended_avg, blended_avg,
                        order_id,
                        "reclassified from default B after C core fill" if adopted_default_holding
                        else "updated by C automatic core builder",
                        holding["id"],
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO position_holdings (
                        symbol,strategy_group,stock_type,status,qty,
                        initial_entry_price,avg_entry_price,current_price,
                        market_value,cost_basis,unrealized_pnl,unrealized_pnl_pct,
                        entry_time,capital_pool,margin_used,last_order_id,last_order_side,
                        last_update_time,notes
                    ) VALUES (%s,'C','C','open',%s,%s,%s,%s,%s,%s,0,0,NOW(),'C',0,%s,'buy',NOW(),%s)
                    """,
                    (
                        plan.symbol, total_qty, blended_avg, blended_avg, filled_avg,
                        total_qty * filled_avg, total_qty * blended_avg,
                        order_id, "created by C automatic core builder",
                    ),
                )


def _run_strategy_c_core_buy_locked(*, dry_run: bool | None = None, ignore_market_hours: bool = False) -> dict:
    """Plan or execute one automatic C core-building pass."""
    ensure_schema()
    enabled = env_bool("C_CORE_AUTO_BUY_ENABLED", False)
    dry_run = env_bool("C_CORE_AUTO_BUY_DRY_RUN", False) if dry_run is None else bool(dry_run)
    result = {"ok": True, "enabled": enabled, "dry_run": dry_run, "orders": [], "reason": ""}
    if not enabled and not dry_run:
        result["reason"] = "disabled"
        return result
    if not ignore_market_hours and not _within_buy_window():
        result["reason"] = "outside_buy_window"
        return result

    allocation = get_capital_allocation()
    if allocation is None:
        return {**result, "ok": False, "reason": "capital_allocation_unavailable"}
    client = alpaca_gateway.trading_client(pool="C")
    if not ignore_market_hours:
        try:
            if not bool(getattr(client.get_clock(), "is_open", False)):
                result["reason"] = "market_closed"
                return result
        except Exception as exc:
            return {**result, "ok": False, "reason": f"clock_unavailable:{exc}"}
    account = client.get_account()
    block_reason = alpaca_gateway.account_trade_block_reason(
        alpaca_gateway.get_account_snapshot(pool="C")
    )
    if block_reason:
        return {**result, "ok": False, "reason": block_reason}

    current_values = _current_c_values()
    plans = build_c_core_buy_plan(
        target_capital=float(allocation.C_target or 0.0),
        available_capital=float(allocation.available.get("C", 0.0) or 0.0),
        buying_power=_safe_float(getattr(account, "buying_power", 0)),
        current_values=current_values,
        daily_spent=_daily_spent(),
        excluded_symbols=_busy_c_symbols(),
        min_order=env_float("C_CORE_MIN_ORDER_USD", 25.0),
        daily_budget_pct=env_float("C_CORE_DAILY_BUDGET_PCT", 1.0),
        daily_budget_max=env_float("C_CORE_DAILY_BUDGET_MAX_USD", 0.0),
        max_orders=env_int("C_CORE_MAX_ORDERS_PER_RUN", 3),
        tier_fill_ratio=env_float("C_CORE_TIER_FILL_RATIO", 0.90),
    )
    result["target_capital"] = round(float(allocation.C_target or 0.0), 2)
    result["available_capital"] = round(float(allocation.available.get("C", 0.0) or 0.0), 2)
    result["plans"] = [asdict(plan) for plan in plans]
    if not plans:
        result["reason"] = "no_eligible_budget_or_deficit"
        return result
    allow, gate_reason = can_open_position(
        "C",
        sum(plan.notional for plan in plans),
        available_override=float(allocation.available.get("C", 0.0) or 0.0),
    )
    result["gate_reason"] = gate_reason
    if not allow:
        result["reason"] = f"blocked:{gate_reason}"
        return result
    if dry_run:
        result["reason"] = "dry_run"
        return result

    for plan in plans:
        price = alpaca_gateway.get_latest_stock_price(plan.symbol, pool="C")
        if price <= 0:
            _record_attempt(plan, status="skipped", reason="price_unavailable")
            result["orders"].append({"symbol": plan.symbol, "status": "skipped", "reason": "price_unavailable"})
            continue
        try:
            asset = client.get_asset(plan.symbol)
            if not bool(getattr(asset, "tradable", False)) or not bool(getattr(asset, "fractionable", False)):
                reason = "asset_not_tradable_or_fractionable"
                _record_attempt(plan, status="skipped", reason=reason, limit_price=price)
                result["orders"].append({"symbol": plan.symbol, "status": "skipped", "reason": reason})
                continue
        except Exception as exc:
            reason = f"asset_check_failed:{exc}"
            _record_attempt(plan, status="error", reason=reason, limit_price=price)
            result["orders"].append({"symbol": plan.symbol, "status": "error", "reason": reason})
            continue
        if _has_open_buy(client, plan.symbol):
            _record_attempt(plan, status="skipped", reason="open_buy_exists", limit_price=price)
            result["orders"].append({"symbol": plan.symbol, "status": "skipped", "reason": "open_buy_exists"})
            continue
        order_qty = _stock_qty_for_notional(plan.notional, price)
        if order_qty < 0.1:
            reason = "calculated_qty_below_0.1"
            _record_attempt(plan, status="skipped", reason=reason, limit_price=price)
            result["orders"].append({"symbol": plan.symbol, "status": "skipped", "reason": reason})
            continue
        try:
            order_id, status, filled_qty, filled_avg, error = _submit_and_wait(client, plan, price, order_qty)
            final_status = "filled" if filled_qty > 0 and filled_avg > 0 else status
            _record_attempt(
                plan,
                status=final_status,
                reason=error,
                order_id=order_id,
                limit_price=price,
                filled_qty=filled_qty,
                filled_avg=filled_avg,
            )
            local_record_error = ""
            if filled_qty > 0 and filled_avg > 0:
                try:
                    _record_fill(plan, filled_qty, filled_avg, order_id)
                except Exception as exc:
                    local_record_error = str(exc)
                    print(
                        f"[C CORE] broker fill recorded but local holding update failed "
                        f"{plan.symbol} order={order_id}: {exc}",
                        flush=True,
                    )
            result["orders"].append(
                {
                    "symbol": plan.symbol,
                    "status": final_status,
                    "order_id": order_id,
                    "qty": order_qty,
                    "target_notional": plan.notional,
                    "notional": round(order_qty * price, 2),
                    "filled_qty": filled_qty,
                    "filled_avg_price": filled_avg,
                    "error": local_record_error or error,
                }
            )
        except Exception as exc:
            reason = str(exc)
            _record_attempt(plan, status="error", reason=reason, limit_price=price)
            result["orders"].append({"symbol": plan.symbol, "status": "error", "reason": reason})
    result["reason"] = "executed"
    return result


def run_strategy_c_core_buy_once(*, dry_run: bool | None = None, ignore_market_hours: bool = False) -> dict:
    """Run one serialized C core-building pass."""
    ensure_schema()
    with _execution_lock() as acquired:
        if not acquired:
            return {
                "ok": True,
                "enabled": env_bool("C_CORE_AUTO_BUY_ENABLED", False),
                "dry_run": bool(dry_run),
                "orders": [],
                "reason": "another_pass_is_running",
            }
        return _run_strategy_c_core_buy_locked(
            dry_run=dry_run,
            ignore_market_hours=ignore_market_hours,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Strategy C automatic core-position builder")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--ignore-market-hours", action="store_true")
    args = parser.parse_args()
    print(
        json.dumps(
            run_strategy_c_core_buy_once(
                dry_run=args.dry_run,
                ignore_market_hours=args.ignore_market_hours,
            ),
            ensure_ascii=False,
            default=str,
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
