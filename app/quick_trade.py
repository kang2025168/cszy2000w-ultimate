from __future__ import annotations

"""Quick intraday trading engine.

The engine has two jobs:
- read a small preselected stock pool from stock_operations
- during regular trading, buy the strongest tight-spread stock with a protected
  limit order, and sell existing quick positions at take-profit/stop-loss.
"""

import json
import math
import os
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Callable, Iterable

from pymysql.cursors import DictCursor

from ultimate_v1.alpaca_gateway import account_trade_block_reason, get_account_snapshot, stock_limit_price, trading_client
from ultimate_v1.config import settings
from ultimate_v1.db import db_conn


OPS_TABLE = os.getenv("OPS_TABLE", "stock_operations")
POOL_TYPES = tuple(x.strip().upper() for x in os.getenv("QUICK_POOL_TYPES", "B,C,QUICK").split(",") if x.strip())
MAX_CANDIDATES = int(os.getenv("QUICK_MAX_CANDIDATES", "40"))
MAX_SPREAD_PCT = float(os.getenv("QUICK_MAX_SPREAD_PCT", "0.0012"))
MAX_SLIPPAGE_PCT = float(os.getenv("QUICK_MAX_SLIPPAGE_PCT", "0.0008"))
LIMIT_BUFFER_CENTS = float(os.getenv("QUICK_LIMIT_BUFFER_CENTS", "0.01"))
MIN_DAY_UP_PCT = float(os.getenv("QUICK_MIN_DAY_UP_PCT", "0.003"))
MAX_DAY_UP_PCT = float(os.getenv("QUICK_MAX_DAY_UP_PCT", "0.12"))
MAX_PULLBACK_FROM_HIGH_PCT = float(os.getenv("QUICK_MAX_PULLBACK_FROM_HIGH_PCT", "0.012"))
MIN_PRICE = float(os.getenv("QUICK_MIN_PRICE", "2.0"))
MAX_PRICE = float(os.getenv("QUICK_MAX_PRICE", "2500.0"))
TRADE_NOTIONAL = float(os.getenv("QUICK_TRADE_NOTIONAL", "500.0"))
MAX_POSITION_NOTIONAL = float(os.getenv("QUICK_MAX_POSITION_NOTIONAL", "1000.0"))
MIN_TRADE_NOTIONAL = float(os.getenv("QUICK_MIN_TRADE_NOTIONAL", "50.0"))
TAKE_PROFIT_PCT = float(os.getenv("QUICK_TAKE_PROFIT_PCT", "0.0025"))
STOP_LOSS_PCT = float(os.getenv("QUICK_STOP_LOSS_PCT", "0.0018"))
FILL_WAIT_SEC = float(os.getenv("QUICK_FILL_WAIT_SEC", "2.0"))
DRY_RUN = os.getenv("QUICK_DRY_RUN", "1").strip().lower() not in {"0", "false", "no", "off"}
OPTION_SYMBOL_RE = re.compile(r"^[A-Z]{1,6}\d{6}[CP]\d{8}$")
EQUITY_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.]{0,9}$")


@dataclass(frozen=True)
class QuickQuote:
    symbol: str
    last: float
    bid: float
    ask: float
    day_open: float = 0.0
    day_high: float = 0.0
    prev_close: float = 0.0
    feed: str = ""


@dataclass(frozen=True)
class QuickPlan:
    action: str
    symbol: str
    stock_type: str
    score: float
    reason: str
    qty: int = 0
    limit_price: float = 0.0
    last: float = 0.0
    bid: float = 0.0
    ask: float = 0.0
    spread_pct: float = 0.0
    day_up_pct: float = 0.0
    pullback_pct: float = 0.0
    trigger_price: float = 0.0
    take_profit_price: float = 0.0
    stop_loss_price: float = 0.0
    operation_id: int = 0


def _float(value, default: float = 0.0) -> float:
    try:
        return float(value if value is not None else default)
    except Exception:
        return default


def _int(value, default: int = 0) -> int:
    try:
        return int(float(value if value is not None else default))
    except Exception:
        return default


def is_equity_symbol(symbol: str) -> bool:
    symbol = (symbol or "").strip().upper()
    if not symbol or OPTION_SYMBOL_RE.match(symbol):
        return False
    return bool(EQUITY_SYMBOL_RE.match(symbol))


def default_quote_provider(symbol: str) -> QuickQuote:
    from app.strategy_b import get_snapshot_quote_realtime

    q = get_snapshot_quote_realtime(symbol)
    return QuickQuote(
        symbol=symbol,
        last=_float(q.get("last_price")),
        bid=_float(q.get("bid")),
        ask=_float(q.get("ask")),
        day_open=_float(q.get("day_open")),
        day_high=_float(q.get("day_high")),
        prev_close=_float(q.get("prev_close")),
        feed=str(q.get("feed") or ""),
    )


def _spread_pct(q: QuickQuote) -> float:
    if q.bid <= 0 or q.ask <= 0 or q.last <= 0 or q.ask < q.bid:
        return 999.0
    return (q.ask - q.bid) / q.last


def _day_up_pct(q: QuickQuote) -> float:
    return (q.last - q.prev_close) / q.prev_close if q.last > 0 and q.prev_close > 0 else 0.0


def _pullback_pct(q: QuickQuote) -> float:
    return (q.day_high - q.last) / q.day_high if q.day_high > 0 and q.last > 0 else 0.0


def protected_limit_price(side: str, q: QuickQuote) -> float:
    """Return a marketable limit with a hard slippage cap."""
    side = (side or "").strip().lower()
    if q.last <= 0:
        return 0.0
    if side == "buy":
        natural = q.ask if q.ask > 0 else q.last
        buffered = natural + LIMIT_BUFFER_CENTS
        cap = q.last * (1.0 + MAX_SLIPPAGE_PCT)
        return stock_limit_price(min(buffered, cap))
    natural = q.bid if q.bid > 0 else q.last
    buffered = max(natural - LIMIT_BUFFER_CENTS, 0.0)
    floor_price = q.last * (1.0 - MAX_SLIPPAGE_PCT)
    return stock_limit_price(max(buffered, floor_price))


def score_entry(row: dict, q: QuickQuote) -> QuickPlan | None:
    symbol = str(row.get("stock_code") or "").strip().upper()
    stock_type = str(row.get("stock_type") or row.get("strategy_group") or "").strip().upper()
    trigger = _float(row.get("trigger_price"))
    if not is_equity_symbol(symbol):
        return QuickPlan("SKIP", symbol, stock_type, -999.0, "not_equity_symbol", operation_id=_int(row.get("id")))
    if q.last < MIN_PRICE or q.last > MAX_PRICE:
        return QuickPlan("SKIP", symbol, stock_type, -900.0, "price_out_of_range", last=q.last, operation_id=_int(row.get("id")))
    spread = _spread_pct(q)
    day_up = _day_up_pct(q)
    pullback = _pullback_pct(q)
    if spread > MAX_SPREAD_PCT:
        return QuickPlan("SKIP", symbol, stock_type, -800.0, "spread_too_wide", last=q.last, bid=q.bid, ask=q.ask, spread_pct=spread, operation_id=_int(row.get("id")))
    if day_up < MIN_DAY_UP_PCT:
        return QuickPlan("SKIP", symbol, stock_type, -700.0, "not_strong_now", last=q.last, day_up_pct=day_up, operation_id=_int(row.get("id")))
    if day_up > MAX_DAY_UP_PCT:
        return QuickPlan("SKIP", symbol, stock_type, -650.0, "too_extended", last=q.last, day_up_pct=day_up, operation_id=_int(row.get("id")))
    if pullback > MAX_PULLBACK_FROM_HIGH_PCT:
        return QuickPlan("SKIP", symbol, stock_type, -600.0, "pulled_back_from_high", last=q.last, pullback_pct=pullback, operation_id=_int(row.get("id")))
    if trigger > 0 and q.last < trigger:
        return QuickPlan("SKIP", symbol, stock_type, -500.0, "below_trigger", last=q.last, trigger_price=trigger, operation_id=_int(row.get("id")))

    score = day_up * 1000.0 - spread * 2500.0 - pullback * 700.0
    if trigger > 0:
        score += min(max((q.last - trigger) / trigger, 0.0), 0.04) * 500.0
    if q.day_open > 0 and q.last > q.day_open:
        score += min((q.last - q.day_open) / q.day_open, 0.03) * 350.0

    return QuickPlan(
        action="BUY",
        symbol=symbol,
        stock_type=stock_type,
        score=round(score, 4),
        reason="strong_tight_spread",
        last=q.last,
        bid=q.bid,
        ask=q.ask,
        spread_pct=spread,
        day_up_pct=day_up,
        pullback_pct=pullback,
        trigger_price=trigger,
        operation_id=_int(row.get("id")),
    )


def build_buy_plan(row: dict, q: QuickQuote, buying_power: float) -> QuickPlan | None:
    base = score_entry(row, q)
    if not base or base.action != "BUY":
        return base
    limit_price = protected_limit_price("buy", q)
    if limit_price <= 0:
        return QuickPlan("SKIP", base.symbol, base.stock_type, base.score, "bad_limit_price", operation_id=base.operation_id)
    notional = min(TRADE_NOTIONAL, MAX_POSITION_NOTIONAL, max(buying_power * 0.98, 0.0))
    qty = int(math.floor(notional / limit_price))
    if qty <= 0 or qty * limit_price < MIN_TRADE_NOTIONAL:
        return QuickPlan("SKIP", base.symbol, base.stock_type, base.score, "not_enough_buying_power", limit_price=limit_price, operation_id=base.operation_id)
    entry = limit_price
    return QuickPlan(
        **{
            **asdict(base),
            "qty": qty,
            "limit_price": limit_price,
            "take_profit_price": stock_limit_price(entry * (1.0 + TAKE_PROFIT_PCT)),
            "stop_loss_price": stock_limit_price(entry * (1.0 - STOP_LOSS_PCT)),
        }
    )


def build_sell_plan(row: dict, q: QuickQuote) -> QuickPlan | None:
    symbol = str(row.get("stock_code") or "").strip().upper()
    stock_type = str(row.get("stock_type") or row.get("strategy_group") or "").strip().upper()
    if not is_equity_symbol(symbol):
        return None
    qty = _int(row.get("qty"))
    if qty <= 0:
        return None
    cost = _float(row.get("cost_price") or row.get("initial_entry_price") or row.get("trigger_price"))
    take_profit = _float(row.get("take_profit_price")) or (cost * (1.0 + TAKE_PROFIT_PCT) if cost > 0 else 0.0)
    stop_loss = _float(row.get("stop_loss_price")) or (cost * (1.0 - STOP_LOSS_PCT) if cost > 0 else 0.0)
    action = ""
    reason = ""
    if take_profit > 0 and q.last >= take_profit:
        action = "SELL"
        reason = "take_profit"
    elif stop_loss > 0 and q.last <= stop_loss:
        action = "SELL"
        reason = "stop_loss"
    if not action:
        return None
    limit_price = protected_limit_price("sell", q)
    return QuickPlan(
        action=action,
        symbol=symbol,
        stock_type=stock_type,
        score=0.0,
        reason=reason,
        qty=qty,
        limit_price=limit_price,
        last=q.last,
        bid=q.bid,
        ask=q.ask,
        spread_pct=_spread_pct(q),
        day_up_pct=_day_up_pct(q),
        pullback_pct=_pullback_pct(q),
        trigger_price=_float(row.get("trigger_price")),
        take_profit_price=take_profit,
        stop_loss_price=stop_loss,
        operation_id=_int(row.get("id")),
    )


def _pool_placeholders(values: Iterable[str]) -> tuple[str, tuple]:
    vals = tuple(values)
    return ", ".join(["%s"] * len(vals)), vals


def load_candidate_rows(conn) -> list[dict]:
    placeholders, args = _pool_placeholders(POOL_TYPES)
    with conn.cursor(DictCursor) as cur:
        cur.execute(
            f"""
            SELECT id, stock_code, stock_type, strategy_group, trigger_price, current_price,
                   close_price, entry_close, entry_open, is_bought, can_buy
            FROM `{OPS_TABLE}`
            WHERE stock_type IN ({placeholders})
              AND COALESCE(can_buy, 0)=1
              AND COALESCE(is_bought, 0)=0
            ORDER BY updated_at DESC, id DESC
            LIMIT %s
            """,
            (*args, MAX_CANDIDATES),
        )
        return list(cur.fetchall() or [])


def load_holding_rows(conn) -> list[dict]:
    placeholders, args = _pool_placeholders(POOL_TYPES)
    with conn.cursor(DictCursor) as cur:
        cur.execute(
            f"""
            SELECT id, stock_code, stock_type, strategy_group, qty, cost_price,
                   trigger_price, stop_loss_price, take_profit_price, is_bought, can_sell
            FROM `{OPS_TABLE}`
            WHERE stock_type IN ({placeholders})
              AND COALESCE(is_bought, 0)=1
              AND COALESCE(can_sell, 0)=1
            ORDER BY updated_at DESC, id DESC
            LIMIT %s
            """,
            (*args, MAX_CANDIDATES),
        )
        return list(cur.fetchall() or [])


def rank_entry_candidates(rows: list[dict], quote_provider: Callable[[str], QuickQuote] = default_quote_provider) -> tuple[list[QuickPlan], list[QuickPlan]]:
    accepted: list[QuickPlan] = []
    skipped: list[QuickPlan] = []
    for row in rows:
        symbol = str(row.get("stock_code") or "").strip().upper()
        if not symbol:
            continue
        try:
            plan = score_entry(row, quote_provider(symbol))
        except Exception as exc:
            plan = QuickPlan("SKIP", symbol, str(row.get("stock_type") or ""), -999.0, f"quote_error:{exc}", operation_id=_int(row.get("id")))
        if plan and plan.action == "BUY":
            accepted.append(plan)
        elif plan:
            skipped.append(plan)
    accepted.sort(key=lambda p: p.score, reverse=True)
    return accepted, skipped


def _submit_limit_order(symbol: str, side: str, qty: int, limit_price: float, dry_run: bool) -> dict:
    if dry_run:
        return {"status": "DRY_RUN", "order_id": "DRY_RUN", "filled_qty": 0, "filled_avg_price": 0.0}

    from alpaca.trading.enums import OrderSide, TimeInForce
    from alpaca.trading.requests import LimitOrderRequest

    client = trading_client()
    req = LimitOrderRequest(
        symbol=symbol,
        qty=int(qty),
        side=OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL,
        limit_price=stock_limit_price(limit_price),
        time_in_force=TimeInForce.IOC,
    )
    order = client.submit_order(order_data=req)
    order_id = str(getattr(order, "id", "") or "")
    status = str(getattr(order, "status", "") or "")
    filled_qty = _int(getattr(order, "filled_qty", 0))
    filled_avg = _float(getattr(order, "filled_avg_price", 0.0))

    deadline = time.time() + max(FILL_WAIT_SEC, 0.0)
    while order_id and time.time() < deadline and filled_qty <= 0 and status.lower() not in {"filled", "canceled", "cancelled", "expired", "rejected"}:
        time.sleep(0.25)
        fresh = client.get_order_by_id(order_id)
        status = str(getattr(fresh, "status", "") or status)
        filled_qty = _int(getattr(fresh, "filled_qty", filled_qty))
        filled_avg = _float(getattr(fresh, "filled_avg_price", filled_avg))

    return {"status": status, "order_id": order_id, "filled_qty": filled_qty, "filled_avg_price": filled_avg}


def _record_event(conn, plan: QuickPlan, result: dict) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quick_trade_events
                    (symbol, stock_type, action, status, reason, qty, last_price,
                     limit_price, score, order_id, payload, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                """,
                (
                    plan.symbol,
                    plan.stock_type,
                    plan.action,
                    str(result.get("status") or ""),
                    plan.reason,
                    plan.qty,
                    plan.last,
                    plan.limit_price,
                    plan.score,
                    str(result.get("order_id") or ""),
                    json.dumps({"plan": asdict(plan), "result": result}, ensure_ascii=False, default=str)[:4000],
                ),
            )
    except Exception as exc:
        print(f"[QUICK EVENT] write failed: {exc}", flush=True)


def _apply_filled_buy(conn, plan: QuickPlan, result: dict) -> None:
    filled_qty = _int(result.get("filled_qty"))
    filled_avg = _float(result.get("filled_avg_price")) or plan.limit_price
    if filled_qty <= 0:
        return
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE `{OPS_TABLE}`
            SET is_bought=1,
                can_buy=0,
                can_sell=1,
                qty=%s,
                cost_price=%s,
                current_price=%s,
                stop_loss_price=%s,
                take_profit_price=%s,
                last_order_side='BUY',
                last_order_time=NOW(),
                last_order_intent=%s
            WHERE id=%s
            """,
            (
                filled_qty,
                filled_avg,
                plan.last,
                stock_limit_price(filled_avg * (1.0 - STOP_LOSS_PCT)),
                stock_limit_price(filled_avg * (1.0 + TAKE_PROFIT_PCT)),
                f"QUICK BUY filled={filled_qty}@{filled_avg:.2f} score={plan.score:.2f}",
                plan.operation_id,
            ),
        )


def _apply_filled_sell(conn, plan: QuickPlan, result: dict) -> None:
    filled_qty = _int(result.get("filled_qty"))
    if filled_qty <= 0:
        return
    remaining = max(plan.qty - filled_qty, 0)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE `{OPS_TABLE}`
            SET is_bought=%s,
                can_sell=%s,
                qty=%s,
                current_price=%s,
                stop_loss_price=%s,
                take_profit_price=%s,
                last_order_side='SELL',
                last_order_time=NOW(),
                last_order_intent=%s
            WHERE id=%s
            """,
            (
                1 if remaining > 0 else 0,
                1 if remaining > 0 else 0,
                remaining,
                plan.last,
                plan.stop_loss_price if remaining > 0 else None,
                plan.take_profit_price if remaining > 0 else None,
                f"QUICK {plan.reason} sold={filled_qty}@{plan.limit_price:.2f}",
                plan.operation_id,
            ),
        )


def execute_plan(conn, plan: QuickPlan, dry_run: bool = DRY_RUN) -> dict:
    if plan.action not in {"BUY", "SELL"}:
        return {"status": "SKIPPED", "reason": plan.reason}
    side = "buy" if plan.action == "BUY" else "sell"
    print(
        f"[QUICK {plan.action}] {plan.symbol} 符合{plan.reason} qty={plan.qty} "
        f"last={plan.last:.2f} bid={plan.bid:.2f} ask={plan.ask:.2f} "
        f"limit={plan.limit_price:.2f} score={plan.score:.2f}",
        flush=True,
    )
    result = _submit_limit_order(plan.symbol, side, plan.qty, plan.limit_price, dry_run=dry_run)
    _record_event(conn, plan, result)
    status = str(result.get("status") or "")
    print(f"[QUICK {plan.action}] {plan.symbol} 下单 status={status} order_id={result.get('order_id')}", flush=True)
    if not dry_run:
        if plan.action == "BUY":
            _apply_filled_buy(conn, plan, result)
        else:
            _apply_filled_sell(conn, plan, result)
    return result


def run_once(
    *,
    dry_run: bool = DRY_RUN,
    quote_provider: Callable[[str], QuickQuote] = default_quote_provider,
    execute: bool = True,
) -> dict:
    """Run one quick-trading pass. Returns a serializable status payload."""
    snap = get_account_snapshot()
    block = account_trade_block_reason(snap)
    if block:
        return {"ok": False, "blocked": block, "plans": []}
    buying_power = _float(getattr(snap, "buying_power", 0.0))

    with db_conn(settings()) as conn:
        sell_plans: list[QuickPlan] = []
        for row in load_holding_rows(conn):
            symbol = str(row.get("stock_code") or "").strip().upper()
            try:
                plan = build_sell_plan(row, quote_provider(symbol))
            except Exception as exc:
                print(f"[QUICK SELL] {symbol} quote error: {exc}", flush=True)
                plan = None
            if plan:
                sell_plans.append(plan)

        results: list[dict] = []
        if sell_plans:
            for plan in sell_plans:
                results.append({"plan": asdict(plan), "result": execute_plan(conn, plan, dry_run=dry_run) if execute else None})
            return {"ok": True, "dry_run": dry_run, "mode": "sell", "plans": results}

        candidates = load_candidate_rows(conn)
        accepted, skipped = rank_entry_candidates(candidates, quote_provider)
        plan = None
        if accepted:
            top_row_by_id = {int(r.get("id") or 0): r for r in candidates}
            top = accepted[0]
            plan = build_buy_plan(top_row_by_id.get(top.operation_id, {}), quote_provider(top.symbol), buying_power)
        if not plan or plan.action != "BUY":
            return {
                "ok": True,
                "dry_run": dry_run,
                "mode": "scan",
                "buying_power": buying_power,
                "accepted": [asdict(p) for p in accepted[:8]],
                "skipped": [asdict(p) for p in skipped[:8]],
                "message": "no_buy_plan",
            }
        result = execute_plan(conn, plan, dry_run=dry_run) if execute else None
        return {
            "ok": True,
            "dry_run": dry_run,
            "mode": "buy",
            "buying_power": buying_power,
            "plan": asdict(plan),
            "result": result,
            "accepted": [asdict(p) for p in accepted[:8]],
        }


def latest_events(limit: int = 30) -> list[dict]:
    with db_conn(settings()) as conn:
        with conn.cursor(DictCursor) as cur:
            cur.execute(
                """
                SELECT symbol, stock_type, action, status, reason, qty, last_price,
                       limit_price, score, order_id, created_at
                FROM quick_trade_events
                ORDER BY id DESC
                LIMIT %s
                """,
                (int(limit),),
            )
            return list(cur.fetchall() or [])


def heartbeat_message(payload: dict) -> str:
    if not payload.get("ok"):
        return str(payload.get("blocked") or payload.get("error") or "blocked")
    if payload.get("mode") == "sell":
        return f"sell plans={len(payload.get('plans') or [])}"
    if payload.get("mode") == "buy":
        p = payload.get("plan") or {}
        return f"buy {p.get('symbol')} qty={p.get('qty')} limit={p.get('limit_price')}"
    return f"scan accepted={len(payload.get('accepted') or [])}"


if __name__ == "__main__":
    print(json.dumps(run_once(dry_run=DRY_RUN), ensure_ascii=False, default=str, indent=2))
