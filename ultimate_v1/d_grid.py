from __future__ import annotations

"""Strategy D single-cycle intraday grid engine.

Each symbol owns one durable state row. A new buy cannot be submitted until the
previous buy and sell have both reached a terminal state.
"""

import argparse
import math
import time as time_module
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

from .alpaca_gateway import StockQuote, get_latest_stock_quote, stock_limit_price, trading_client
from .capital_manager import get_capital_allocation
from .config import env_bool, env_float, env_int, env_str, settings
from .db import db_conn, fetch_all
from .schema import ensure_schema
from .state_store import get_app_setting, set_app_setting
from .trading_gate import can_open_position
from .yahoo_market_data import get_yahoo_stock_quote

ACTIVE_STATES = {"BUY_WORKING", "SELL_WORKING", "CLOSING", "BUY_SUBMITTING", "SELL_SUBMITTING"}
TERMINAL_ORDER_STATES = {"filled", "canceled", "cancelled", "expired", "rejected"}
D_AUTO_EXCLUDED_SYMBOLS = {
    "TQQQ", "SQQQ", "SOXL", "SOXS", "SPXL", "SPXS", "UPRO", "UVXY", "VXX",
    "LABU", "LABD", "NUGT", "DUST", "FNGU", "FNGD", "BITX", "BITI",
}


def _runtime_bool(key: str, env_name: str, default: bool) -> bool:
    saved = get_app_setting(key, "").strip().lower()
    if saved:
        return saved in {"1", "true", "yes", "on"}
    return env_bool(env_name, default)


def _runtime_text(key: str, env_name: str, default: str) -> str:
    return get_app_setting(key, "").strip() or env_str(env_name, default)


def _cycle_budget(config: dict) -> float:
    """每轮使用 D 当前可用资金，但不超过单轮上限。"""
    fallback = max(0.0, float(config.get("lot_notional") or 0.0))
    if not _runtime_bool("D_GRID_USE_AVAILABLE_CAPITAL", "D_GRID_USE_AVAILABLE_CAPITAL", True):
        return fallback
    allocation = get_capital_allocation()
    if allocation is None:
        return 0.0
    available = max(0.0, float(allocation.available.get("D", 0.0) or 0.0))
    cap = max(0.0, float(_runtime_text("D_GRID_MAX_CYCLE_NOTIONAL_USD", "D_GRID_MAX_CYCLE_NOTIONAL_USD", "10000")))
    return min(available, cap) if cap > 0 else available


@dataclass(frozen=True)
class GridPlan:
    anchor_price: float
    buy_limit: float
    sell_limit: float
    qty: int
    notional: float


def _money(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def build_grid_plan(
    anchor_price: float,
    lot_notional: float,
    entry_offset: float = 0.0,
    profit_offset: float = 0.0,
    *,
    entry_pct: float = 0.0,
    profit_pct: float = 0.0,
) -> GridPlan:
    """Build a whole-share cycle plan; live fills may improve either limit."""
    anchor = stock_limit_price(anchor_price)
    entry_gap = anchor * max(entry_pct, 0.0) if entry_pct > 0 else max(entry_offset, 0.0)
    buy_limit = stock_limit_price(anchor - entry_gap)
    qty = int(math.floor(max(lot_notional, 0.0) / buy_limit)) if buy_limit > 0 else 0
    profit_gap = buy_limit * max(profit_pct, 0.0) if profit_pct > 0 else max(profit_offset, 0.0)
    sell_limit = stock_limit_price(buy_limit + profit_gap)
    return GridPlan(anchor, buy_limit, sell_limit, qty, _money(qty * buy_limit))


def sell_limit_from_fill(fill_price: float, profit_offset: float = 0.0, *, profit_pct: float = 0.0) -> float:
    fill = float(fill_price)
    profit_gap = fill * max(float(profit_pct), 0.0) if profit_pct > 0 else max(float(profit_offset), 0.0)
    return stock_limit_price(fill + profit_gap)


def _time_setting(name: str, default: str) -> time:
    raw = _runtime_text(name, name, default)
    hour, minute = raw.split(":", 1)
    return time(int(hour), int(minute))


def _now_la() -> datetime:
    return datetime.now(ZoneInfo(settings().timezone))


def _status(order) -> str:
    value = getattr(order, "status", "")
    return str(getattr(value, "value", value) or "").lower()


def _order_snapshot(order) -> tuple[str, float, float]:
    return (
        _status(order),
        float(getattr(order, "filled_qty", 0) or 0),
        float(getattr(order, "filled_avg_price", 0) or 0),
    )


def _event(cur, symbol: str, cycle_no: int, event_type: str, state: str, *, order_id: str = "", qty: float = 0, price: float = 0, message: str = "") -> None:
    cur.execute(
        """
        INSERT INTO d_grid_events
          (symbol, cycle_no, event_type, state, order_id, qty, price, message)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (symbol, cycle_no, event_type, state, order_id or None, qty, price, message[:512]),
    )


def configure_symbol(
    symbol: str,
    *,
    enabled: bool = False,
    lot_notional: float | None = None,
    entry_offset: float | None = None,
    profit_offset: float | None = None,
    max_spread: float | None = None,
) -> None:
    ensure_schema()
    symbol = symbol.strip().upper()
    if not symbol:
        raise ValueError("symbol is required")
    lot_notional = lot_notional or env_float("D_GRID_DEFAULT_NOTIONAL_USD", 250.0)
    entry_offset = entry_offset if entry_offset is not None else env_float("D_GRID_ENTRY_OFFSET", 0.03)
    profit_offset = profit_offset if profit_offset is not None else env_float("D_GRID_PROFIT_OFFSET", 0.06)
    max_spread = max_spread if max_spread is not None else env_float("D_GRID_MAX_SPREAD", 0.05)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO d_grid_symbols
                  (symbol, enabled, lot_notional, entry_offset, profit_offset, max_spread)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE enabled=VALUES(enabled),
                  lot_notional=VALUES(lot_notional), entry_offset=VALUES(entry_offset),
                  profit_offset=VALUES(profit_offset), max_spread=VALUES(max_spread)
                """,
                (symbol, 1 if enabled else 0, lot_notional, entry_offset, profit_offset, max_spread),
            )
            cur.execute("INSERT IGNORE INTO d_grid_cycles (symbol) VALUES (%s)", (symbol,))


def config_payload() -> dict:
    ensure_schema()
    controls = fetch_all("SELECT enabled FROM bot_controls WHERE bot_name='d_grid_bot' LIMIT 1")
    candidate_summary = (
        fetch_all(
            """SELECT COUNT(*) AS n, MIN(signal_date) AS min_date, MAX(signal_date) AS max_date
               FROM d_candidate_pool WHERE enabled=1"""
        )
        or [{}]
    )[0]
    return {
        "ok": True,
        "enabled": _runtime_bool("D_GRID_ENABLED", "D_GRID_ENABLED", False),
        "dry_run": _runtime_bool("D_GRID_DRY_RUN", "D_GRID_DRY_RUN", True),
        "bot_enabled": bool(controls and int(controls[0].get("enabled") or 0) == 1),
        "open_time": _runtime_text("D_GRID_OPEN_TIME_LA", "D_GRID_OPEN_TIME_LA", "06:35"),
        "last_entry_time": _runtime_text("D_GRID_LAST_ENTRY_TIME_LA", "D_GRID_LAST_ENTRY_TIME_LA", "12:30"),
        "flatten_time": _runtime_text("D_GRID_FLATTEN_TIME_LA", "D_GRID_FLATTEN_TIME_LA", settings().market_close_flatten_time),
        "cooldown_seconds": int(float(_runtime_text("D_GRID_COOLDOWN_SEC", "D_GRID_COOLDOWN_SEC", "5"))),
        "buy_timeout_seconds": _buy_timeout_seconds(),
        "entry_pct": float(_runtime_text("D_GRID_ENTRY_PCT", "D_GRID_ENTRY_PCT", "0.0025")),
        "profit_pct": float(_runtime_text("D_GRID_PROFIT_PCT", "D_GRID_PROFIT_PCT", "0.01")),
        "use_available_capital": _runtime_bool("D_GRID_USE_AVAILABLE_CAPITAL", "D_GRID_USE_AVAILABLE_CAPITAL", True),
        "max_cycle_notional": float(_runtime_text("D_GRID_MAX_CYCLE_NOTIONAL_USD", "D_GRID_MAX_CYCLE_NOTIONAL_USD", "10000")),
        "auto_select_enabled": _runtime_bool("D_AUTO_SELECT_ENABLED", "D_AUTO_SELECT_ENABLED", True),
        "auto_select_interval_seconds": int(float(_runtime_text("D_AUTO_SELECT_INTERVAL_SEC", "D_AUTO_SELECT_INTERVAL_SEC", "3600"))),
        "auto_selected_symbol": _runtime_text("D_AUTO_SELECTED_SYMBOL", "D_AUTO_SELECTED_SYMBOL", ""),
        "candidate_count": int(candidate_summary.get("n") or 0),
        "candidate_min_date": str(candidate_summary.get("min_date") or ""),
        "candidate_max_date": str(candidate_summary.get("max_date") or ""),
        "candidate_retain_trading_days": max(1, int(env_float("D_CANDIDATE_RETAIN_TRADING_DAYS", 2))),
        "symbols": status_rows(),
        "state_flow": ["IDLE", "BUY_WORKING", "SELL_WORKING", "COOLDOWN"],
    }


def save_config(payload: dict) -> dict:
    ensure_schema()
    symbols = payload.get("symbols") if isinstance(payload.get("symbols"), list) else []
    normalized = []
    seen = set()
    for raw in symbols:
        if not isinstance(raw, dict):
            continue
        symbol = str(raw.get("symbol") or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        if not symbol.replace(".", "").replace("-", "").isalnum():
            raise ValueError(f"股票代码格式错误: {symbol}")
        seen.add(symbol)
        normalized.append(
            {
                "symbol": symbol,
                "enabled": bool(raw.get("enabled")),
                "lot_notional": max(1.0, float(raw.get("lot_notional") or 250)),
                "entry_offset": max(0.01, float(raw.get("entry_offset") or 0.03)),
                "profit_offset": max(0.01, float(raw.get("profit_offset") or 0.06)),
                "max_spread": max(0.01, float(raw.get("max_spread") or 0.05)),
            }
        )
    if len(normalized) > 2:
        raise ValueError("基础版最多配置 2 只 D 股票")
    times = {
        "D_GRID_OPEN_TIME_LA": str(payload.get("open_time") or "06:35"),
        "D_GRID_LAST_ENTRY_TIME_LA": str(payload.get("last_entry_time") or "12:30"),
        "D_GRID_FLATTEN_TIME_LA": str(payload.get("flatten_time") or "12:50"),
    }
    parsed_times = {key: _parse_time_value(value) for key, value in times.items()}
    if not parsed_times["D_GRID_OPEN_TIME_LA"] < parsed_times["D_GRID_LAST_ENTRY_TIME_LA"] < parsed_times["D_GRID_FLATTEN_TIME_LA"]:
        raise ValueError("时间必须满足：开始交易 < 停止开仓 < 收盘平仓")
    set_app_setting("D_GRID_ENABLED", "1" if bool(payload.get("enabled")) else "0")
    set_app_setting("D_GRID_DRY_RUN", "1" if bool(payload.get("dry_run", True)) else "0")
    for key, value in times.items():
        set_app_setting(key, value)
    set_app_setting("D_GRID_COOLDOWN_SEC", str(max(1, int(float(payload.get("cooldown_seconds") or 5)))))
    set_app_setting("D_GRID_BUY_TIMEOUT_SEC", str(max(300, int(float(payload.get("buy_timeout_seconds") or 600)))))
    entry_pct = min(0.05, max(0.0001, float(payload.get("entry_pct") or 0.0025)))
    profit_pct = min(0.20, max(0.0001, float(payload.get("profit_pct") or 0.01)))
    set_app_setting("D_GRID_ENTRY_PCT", str(entry_pct))
    set_app_setting("D_GRID_PROFIT_PCT", str(profit_pct))
    set_app_setting("D_AUTO_SELECT_ENABLED", "1" if bool(payload.get("auto_select_enabled", True)) else "0")
    set_app_setting("D_AUTO_SELECT_INTERVAL_SEC", str(max(300, int(float(payload.get("auto_select_interval_seconds") or 3600)))))
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol, state FROM d_grid_cycles")
            active = {str(row["symbol"]): str(row.get("state") or "IDLE") for row in cur.fetchall()}
            keep = {row["symbol"] for row in normalized}
            for symbol, state in active.items():
                if symbol not in keep and state in ACTIVE_STATES:
                    raise ValueError(f"{symbol} 正在 {state}，完成本轮前不能移除")
            for index, row in enumerate(normalized):
                cur.execute(
                    """
                    INSERT INTO d_grid_symbols
                      (symbol, enabled, lot_notional, entry_offset, profit_offset, max_spread, sort_order)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE enabled=VALUES(enabled), lot_notional=VALUES(lot_notional),
                      entry_offset=VALUES(entry_offset), profit_offset=VALUES(profit_offset),
                      max_spread=VALUES(max_spread), sort_order=VALUES(sort_order)
                    """,
                    (row["symbol"], 1 if row["enabled"] else 0, row["lot_notional"], row["entry_offset"], row["profit_offset"], row["max_spread"], index),
                )
                cur.execute("INSERT IGNORE INTO d_grid_cycles (symbol) VALUES (%s)", (row["symbol"],))
            removable = [symbol for symbol, state in active.items() if symbol not in keep and state not in ACTIVE_STATES]
            if removable:
                placeholders = ",".join(["%s"] * len(removable))
                cur.execute(f"DELETE FROM d_grid_symbols WHERE symbol IN ({placeholders})", tuple(removable))
                cur.execute(f"DELETE FROM d_grid_cycles WHERE symbol IN ({placeholders})", tuple(removable))
    return config_payload()


def _parse_time_value(value: str) -> time:
    try:
        hour, minute = str(value).split(":", 1)
        return time(int(hour), int(minute))
    except Exception as exc:
        raise ValueError(f"时间格式错误: {value}") from exc


def _valid_quote(quote: StockQuote, _max_spread: float) -> tuple[bool, str, float]:
    """Use the latest trade as anchor; bid/ask is optional with the current feed."""
    anchor = float(quote.last or 0)
    if anchor <= 0 and quote.bid > 0 and quote.ask > 0:
        anchor = (quote.bid + quote.ask) / 2.0
    if anchor <= 0:
        return False, "missing_realtime_price", 0.0
    return True, "ok", anchor


def _latest_execution_quote(symbol: str) -> StockQuote:
    """Prefer Alpaca, but keep D operable when its latest endpoint is empty."""
    quote = get_latest_stock_quote(symbol, pool="D")
    valid, _, _ = _valid_quote(quote, 0.0)
    if valid:
        return quote
    yahoo = get_yahoo_stock_quote(symbol)
    return StockQuote(
        symbol=symbol,
        last=float(yahoo.last or 0),
        bid=float(yahoo.bid or 0),
        ask=float(yahoo.ask or 0),
        timestamp=yahoo.as_of or None,
    )


def _auto_select_candidate(*, force: bool = False, exclude: set[str] | None = None) -> dict | None:
    """Select one idle D symbol at most once per hour from the retained pool."""
    if not _runtime_bool("D_AUTO_SELECT_ENABLED", "D_AUTO_SELECT_ENABLED", True):
        return None
    interval = max(300, int(float(_runtime_text("D_AUTO_SELECT_INTERVAL_SEC", "D_AUTO_SELECT_INTERVAL_SEC", "3600"))))
    last_check = float(_runtime_text("D_AUTO_SELECT_LAST_EPOCH", "D_AUTO_SELECT_LAST_EPOCH", "0") or 0)
    if not force and time_module.time() - last_check < interval:
        return None
    active = fetch_all("SELECT symbol, state FROM d_grid_cycles WHERE state IN ('BUY_WORKING','SELL_WORKING','CLOSING','BUY_SUBMITTING','SELL_SUBMITTING') LIMIT 1")
    if active:
        return {"kept": active[0]["symbol"], "reason": "active_cycle_locked"}
    rows = fetch_all(
        """SELECT * FROM d_candidate_pool
           WHERE enabled=1
           ORDER BY signal_date DESC, base_score DESC, signal_dollar_volume DESC LIMIT 120"""
    )
    from .d_entry_filter import check_entry
    scored = []
    for row in rows:
        symbol = str(row.get('symbol') or '').upper()
        if not symbol or symbol in (exclude or set()):
            continue
        # Pool admission already applied liquidity/price/range filters.
        # Intraday selection uses the same signal check as actual buy entry.
        entry = check_entry(symbol)
        if not entry['ok']:
            continue
        price = entry['price']
        gain = entry['day_gain_pct'] / 100
        score = float(row.get('base_score') or 0) + gain * 300
        scored.append((score, symbol, price, gain, 0.0))
    if not scored:
        return {"reason": "no_candidate_passed_entry_filter"}
    scored.sort(reverse=True)
    chosen = scored[0]
    set_app_setting("D_AUTO_SELECT_LAST_EPOCH", str(int(time_module.time())))
    score, symbol, price, gain, drawdown = chosen
    step = max(0.03, price * 0.001)
    notional = env_float("D_GRID_DEFAULT_NOTIONAL_USD", 250.0)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE d_grid_symbols SET enabled=0 WHERE symbol<>%s", (symbol,))
            cur.execute(
                """INSERT INTO d_grid_symbols
                   (symbol, enabled, lot_notional, entry_offset, profit_offset, max_spread, sort_order)
                   VALUES (%s,1,%s,%s,%s,999,0)
                   ON DUPLICATE KEY UPDATE enabled=1, lot_notional=VALUES(lot_notional),
                    entry_offset=VALUES(entry_offset), profit_offset=VALUES(profit_offset), sort_order=0""",
                (symbol, notional, step, step),
            )
            cur.execute("INSERT IGNORE INTO d_grid_cycles (symbol) VALUES (%s)", (symbol,))
            cur.execute(
                "UPDATE d_candidate_pool SET selected_count=selected_count+1,last_selected_at=NOW() WHERE symbol=%s",
                (symbol,),
            )
    set_app_setting("D_AUTO_SELECTED_SYMBOL", symbol)
    return {"symbol": symbol, "price": price, "gain": gain, "drawdown": drawdown, "score": score}


def _submit_limit(client, symbol: str, side: str, qty: float, price: float, client_order_id: str):
    from alpaca.trading.enums import OrderSide, TimeInForce
    from alpaca.trading.requests import LimitOrderRequest

    request = LimitOrderRequest(
        symbol=symbol,
        qty=str(qty),
        side=OrderSide.BUY if side == "buy" else OrderSide.SELL,
        limit_price=stock_limit_price(price),
        time_in_force=TimeInForce.DAY,
        extended_hours=False,
        client_order_id=client_order_id[:48],
    )
    from . import order_journal as journal
    from .account_config import profile_for_pool
    row = journal.get_intent(client_order_id)
    if row is None:
        row = journal.prepare(client_order_id, profile_for_pool("D"), "D", symbol, side,
            {"symbol": symbol, "side": side, "qty": qty, "price": price})
    return journal.submit_prepared(client, request, row)


def _set_cycle(cur, symbol: str, **values) -> None:
    if not values:
        return
    assignments = [f"`{key}`=%s" for key in values]
    assignments.extend(["state_changed_at=NOW()", "updated_at=NOW()"])
    cur.execute(
        f"UPDATE d_grid_cycles SET {', '.join(assignments)} WHERE symbol=%s",
        (*values.values(), symbol),
    )


def _start_cycle(cur, config: dict, cycle: dict, quote: StockQuote, dry_run: bool, client) -> str:
    symbol = config["symbol"]
    from .d_entry_filter import check_entry
    entry = check_entry(symbol, refresh=True)
    if not entry['ok']:
        return f"entry_filter:{entry['reason']}"
    anchor = entry["price"]
    cycle_budget = _cycle_budget(config)
    if cycle_budget <= 0:
        return "no_d_available_capital"
    plan = build_grid_plan(
        anchor,
        cycle_budget,
        float(config["entry_offset"]),
        float(config["profit_offset"]),
        entry_pct=float(_runtime_text("D_GRID_ENTRY_PCT", "D_GRID_ENTRY_PCT", "0.0025")),
        profit_pct=float(_runtime_text("D_GRID_PROFIT_PCT", "D_GRID_PROFIT_PCT", "0.01")),
    )
    if plan.qty <= 0:
        return "lot_notional_below_one_share"
    allowed, reason = can_open_position("D", plan.notional)
    if not allowed:
        return f"risk_block:{reason}"
    cycle_no = int(cycle.get("cycle_no") or 0) + 1
    if dry_run:
        order_id = f"DRY-D-{symbol}-{cycle_no}-B"
    else:
        cid = f"dgrid-{symbol}-{cycle_no}-b"
        from . import order_journal as journal
        from .account_config import profile_for_pool
        journal.prepare(cid, profile_for_pool("D"), "D", symbol, "buy",
            {"symbol": symbol, "side": "buy", "qty": plan.qty, "price": plan.buy_limit}, plan.notional)
        _set_cycle(cur, symbol, state="BUY_SUBMITTING", cycle_no=cycle_no,
                   anchor_price=plan.anchor_price, buy_limit=plan.buy_limit,
                   buy_qty=plan.qty, sell_limit=plan.sell_limit, pending_client_order_id=cid)
        cur.connection.commit()  # Persist intent and release row locks before HTTP.
        order = _submit_limit(client, symbol, "buy", plan.qty, plan.buy_limit, cid)
        order_id = str(getattr(order, "id", "") or "")
        if not order_id:
            raise RuntimeError("Alpaca did not return a buy order id")
    _set_cycle(
        cur,
        symbol,
        state="BUY_WORKING",
        cycle_no=cycle_no,
        anchor_price=plan.anchor_price,
        buy_limit=plan.buy_limit,
        buy_order_id=order_id,
        buy_qty=plan.qty,
        buy_filled_qty=0,
        buy_filled_price=0,
        sell_limit=plan.sell_limit,
        sell_order_id=None,
        sell_filled_price=0,
        realized_pnl=0,
        cooldown_until=None,
        last_error=None,
    )
    event_message = f"{'dry_run' if dry_run else 'live'} budget={cycle_budget:.2f} notional={plan.notional:.2f}"
    _event(cur, symbol, cycle_no, "BUY_SUBMITTED", "BUY_WORKING", order_id=order_id, qty=plan.qty, price=plan.buy_limit, message=event_message)
    return f"buy_working qty={plan.qty} limit={plan.buy_limit:.2f} budget={cycle_budget:.2f} notional={plan.notional:.2f}"


def _submit_sell(cur, config: dict, cycle: dict, qty: float, fill_price: float, dry_run: bool, client, *, closing: bool = False, quote: StockQuote | None = None) -> str:
    symbol = config["symbol"]
    cycle_no = int(cycle["cycle_no"])
    target = stock_limit_price(
        (quote.bid if closing and quote else 0)
        or sell_limit_from_fill(
            fill_price,
            float(config["profit_offset"]),
            profit_pct=float(_runtime_text("D_GRID_PROFIT_PCT", "D_GRID_PROFIT_PCT", "0.01")),
        )
    )
    if dry_run:
        order_id = f"DRY-D-{symbol}-{cycle_no}-S"
    else:
        previous = str(cycle.get("sell_order_id") or "")
        suffix = ("-c-" + previous[-8:]) if closing else "-s"
        cid = f"dgrid-{symbol}-{cycle_no}{suffix}"
        _set_cycle(cur, symbol, state="SELL_SUBMITTING", buy_filled_qty=qty,
                   buy_filled_price=fill_price, sell_limit=target,
                   pending_client_order_id=cid, last_error="near_close_exit" if closing else None)
        cur.connection.commit()
        order = _submit_limit(client, symbol, "sell", qty, target, cid)
        order_id = str(getattr(order, "id", "") or "")
        if not order_id:
            raise RuntimeError("Alpaca did not return a sell order id")
    state = "CLOSING" if closing else "SELL_WORKING"
    _set_cycle(
        cur,
        symbol,
        state=state,
        buy_filled_qty=qty,
        buy_filled_price=fill_price,
        sell_limit=target,
        sell_order_id=order_id,
        last_error="near_close_exit" if closing else None,
    )
    _event(cur, symbol, cycle_no, "CLOSE_SUBMITTED" if closing else "SELL_SUBMITTED", state, order_id=order_id, qty=qty, price=target)
    return f"{state.lower()} qty={qty:g} limit={target:.2f}"


def _finish_cycle(cur, config: dict, cycle: dict, sell_price: float) -> str:
    qty = float(cycle.get("buy_filled_qty") or 0)
    buy_price = float(cycle.get("buy_filled_price") or 0)
    pnl = _money((sell_price - buy_price) * qty)
    cooldown = _now_la().replace(tzinfo=None) + timedelta(seconds=int(float(_runtime_text("D_GRID_COOLDOWN_SEC", "D_GRID_COOLDOWN_SEC", "5"))))
    _set_cycle(cur, config["symbol"], state="COOLDOWN", sell_filled_price=sell_price, realized_pnl=pnl, cooldown_until=cooldown, last_error=None)
    from .order_journal import update
    update(f"dgrid-{config['symbol']}-{cycle['cycle_no']}-b", reserved_notional=0, state="closed")
    _event(cur, config["symbol"], int(cycle["cycle_no"]), "CYCLE_FILLED", "COOLDOWN", order_id=str(cycle.get("sell_order_id") or ""), qty=qty, price=sell_price, message=f"gross_pnl={pnl:.2f}")
    return f"cycle_filled gross_pnl={pnl:.2f}"


def _buy_timeout_seconds() -> int:
    """Keep pullback orders resting long enough to have a realistic fill chance."""
    return max(300, int(float(_runtime_text("D_GRID_BUY_TIMEOUT_SEC", "D_GRID_BUY_TIMEOUT_SEC", "600"))))


def _buy_retry_cooldown_seconds() -> int:
    return max(15, int(float(_runtime_text("D_GRID_REPRICE_COOLDOWN_SEC", "D_GRID_REPRICE_COOLDOWN_SEC", "60"))))


def _advance_buy(cur, config: dict, cycle: dict, quote: StockQuote, dry_run: bool, client) -> str:
    if dry_run:
        if quote.ask <= 0 or quote.ask > float(cycle["buy_limit"]):
            return "waiting_buy_cross"
        return _submit_sell(cur, config, cycle, float(cycle["buy_qty"]), min(quote.ask, float(cycle["buy_limit"])), True, client)
    order = client.get_order_by_id(str(cycle["buy_order_id"]))
    status, filled_qty, fill_price = _order_snapshot(order)
    if status == "filled":
        return _submit_sell(cur, config, cycle, filled_qty, fill_price, False, client)
    if status in TERMINAL_ORDER_STATES:
        if filled_qty > 0:
            return _submit_sell(cur, config, cycle, filled_qty, fill_price or float(cycle["buy_limit"]), False, client)
        from .order_journal import update
        update(f"dgrid-{config['symbol']}-{cycle['cycle_no']}-b", reserved_notional=0, state=status)
        cooldown = _now_la().replace(tzinfo=None) + timedelta(seconds=_buy_retry_cooldown_seconds())
        _set_cycle(
            cur,
            config["symbol"],
            state="COOLDOWN",
            buy_order_id=None,
            cooldown_until=cooldown,
            last_error=f"buy_{status}",
        )
        _event(cur, config["symbol"], int(cycle["cycle_no"]), "BUY_TERMINAL", "COOLDOWN", message=status)
        return f"buy_{status}_cooldown"
    age = (_now_la().replace(tzinfo=None) - cycle["state_changed_at"]).total_seconds()
    if age >= _buy_timeout_seconds():
        client.cancel_order_by_id(str(cycle["buy_order_id"]))
        return "buy_cancel_requested"
    return f"waiting_buy status={status or 'unknown'} filled={filled_qty:g}"


def _advance_sell(cur, config: dict, cycle: dict, quote: StockQuote, dry_run: bool, client, closing: bool = False) -> str:
    if dry_run:
        if quote.bid <= 0 or quote.bid < float(cycle["sell_limit"]):
            return "waiting_sell_cross"
        return _finish_cycle(cur, config, cycle, max(float(cycle["sell_limit"]), quote.bid))
    order = client.get_order_by_id(str(cycle["sell_order_id"]))
    status, filled_qty, fill_price = _order_snapshot(order)
    if status == "filled":
        return _finish_cycle(cur, config, cycle, fill_price)
    if status in TERMINAL_ORDER_STATES:
        expected = float(cycle.get("buy_filled_qty") or 0)
        if filled_qty >= expected > 0:
            return _finish_cycle(cur, config, cycle, fill_price or float(cycle["sell_limit"]))
        _set_cycle(cur, config["symbol"], state="ERROR", last_error=f"sell_{status}_filled={filled_qty:g}")
        _event(cur, config["symbol"], int(cycle["cycle_no"]), "SELL_TERMINAL", "ERROR", message=f"{status} filled={filled_qty:g}")
        return f"sell_{status}_manual_review"
    return f"waiting_{'close' if closing else 'sell'} status={status or 'unknown'} filled={filled_qty:g}"


def run_symbol(symbol: str, *, now: datetime | None = None) -> str:
    from .order_journal import execution_lock
    from .account_config import profile_for_pool
    ensure_schema()
    with execution_lock(profile_for_pool("D")):
        return _run_symbol_locked(symbol, now=now)


def _run_symbol_locked(symbol: str, *, now: datetime | None = None) -> str:
    ensure_schema()
    symbol = symbol.strip().upper()
    dry_run = _runtime_bool("D_GRID_DRY_RUN", "D_GRID_DRY_RUN", True)
    now = now or _now_la()
    naive_now = now.replace(tzinfo=None)
    client = None if dry_run else trading_client(pool="D")
    quote: StockQuote | None = None
    cycle: dict = {}
    with db_conn() as conn:
        with conn.cursor() as cur:
            lock_name = f"d_grid:{symbol}"
            cur.execute("SELECT GET_LOCK(%s, 0) AS acquired", (lock_name,))
            if int((cur.fetchone() or {}).get("acquired") or 0) != 1:
                return "locked_by_other_worker"
            try:
                cur.execute("SELECT * FROM d_grid_symbols WHERE symbol=%s", (symbol,))
                config = cur.fetchone()
                if not config:
                    return "symbol_missing"
                cur.execute("INSERT IGNORE INTO d_grid_cycles (symbol) VALUES (%s)", (symbol,))
                cur.execute("SELECT * FROM d_grid_cycles WHERE symbol=%s FOR UPDATE", (symbol,))
                cycle = cur.fetchone()
                state = str(cycle.get("state") or "IDLE").upper()
                conn.commit()  # Named lock owns execution; do not hold row locks across network calls.
                if state in {"BUY_SUBMITTING", "SELL_SUBMITTING"}:
                    if dry_run:
                        return "live_pending_order_requires_live_reconciliation"
                    buying = state == "BUY_SUBMITTING"
                    order = _submit_limit(client, symbol, "buy" if buying else "sell",
                        float(cycle["buy_qty"] if buying else cycle["buy_filled_qty"]),
                        float(cycle["buy_limit"] if buying else cycle["sell_limit"]),
                        str(cycle["pending_client_order_id"]))
                    target_state = "BUY_WORKING" if buying else ("CLOSING" if "-c-" in str(cycle.get("pending_client_order_id") or "") else "SELL_WORKING")
                    _set_cycle(cur, symbol, state=target_state, **{("buy_order_id" if buying else "sell_order_id"): str(order.id)})
                    return "recovered_" + target_state.lower()
                if state == "COOLDOWN":
                    until = cycle.get("cooldown_until")
                    if until and naive_now < until:
                        return "cooldown"
                    _set_cycle(cur, symbol, state="IDLE", cooldown_until=None)
                    state = "IDLE"
                    cycle["state"] = state
                    conn.commit()
                last_entry = _time_setting("D_GRID_LAST_ENTRY_TIME_LA", "12:30")
                flatten_at = _time_setting("D_GRID_FLATTEN_TIME_LA", settings().market_close_flatten_time)
                in_entry_window = now.weekday() < 5 and _time_setting("D_GRID_OPEN_TIME_LA", "06:35") <= now.time() < last_entry
                if state == "IDLE":
                    if int(config.get("enabled") or 0) != 1:
                        return "symbol_disabled"
                    if not _runtime_bool("D_GRID_ENABLED", "D_GRID_ENABLED", False):
                        return "grid_disabled"
                    if not in_entry_window:
                        return "outside_entry_window"
                    quote = StockQuote(symbol, 0, 0, 0)  # Entry refreshes a timestamped D snapshot.
                    return _start_cycle(cur, config, cycle, quote, dry_run, client)
                if state == "BUY_WORKING":
                    if now.time() >= last_entry:
                        if dry_run:
                            _set_cycle(cur, symbol, state="IDLE", buy_order_id=None, last_error="entry_window_closed")
                            return "dry_buy_canceled_at_cutoff"
                        order = client.get_order_by_id(str(cycle["buy_order_id"]))
                        status, filled_qty, fill_price = _order_snapshot(order)
                        if status == "filled" or (status in TERMINAL_ORDER_STATES and filled_qty > 0):
                            return _submit_sell(
                                cur,
                                config,
                                cycle,
                                filled_qty,
                                fill_price or float(cycle["buy_limit"]),
                                False,
                                client,
                                closing=now.time() >= flatten_at,
                                quote=get_latest_stock_quote(symbol, pool="D"),
                            )
                        if status in TERMINAL_ORDER_STATES:
                            from .order_journal import update
                            update(f"dgrid-{symbol}-{cycle['cycle_no']}-b", reserved_notional=0, state=status)
                            _set_cycle(cur, symbol, state="IDLE", buy_order_id=None, last_error=f"buy_{status}_at_cutoff")
                            return f"buy_{status}_at_cutoff"
                        client.cancel_order_by_id(str(cycle["buy_order_id"]))
                        return "buy_cancel_requested_at_cutoff"
                    quote = get_latest_stock_quote(symbol, pool="D")
                    return _advance_buy(cur, config, cycle, quote, dry_run, client)
                if state == "SELL_WORKING" and now.time() >= flatten_at:
                    quote = get_latest_stock_quote(symbol, pool="D")
                    if dry_run:
                        return _finish_cycle(cur, config, cycle, quote.bid or float(cycle["sell_limit"]))
                    fresh = client.get_order_by_id(str(cycle["sell_order_id"]))
                    status, filled_qty, fill_price = _order_snapshot(fresh)
                    total_qty = float(cycle.get("buy_filled_qty") or 0)
                    if status == "filled" or filled_qty >= total_qty:
                        return _finish_cycle(cur, config, cycle, fill_price or float(cycle["sell_limit"]))
                    if status not in TERMINAL_ORDER_STATES:
                        client.cancel_order_by_id(str(cycle["sell_order_id"]))
                        return "sell_cancel_requested_before_closeout"
                    remaining = max(0.0, total_qty - filled_qty)
                    if remaining <= 0:
                        return _finish_cycle(cur, config, cycle, fill_price or float(cycle["sell_limit"]))
                    return _submit_sell(cur, config, cycle, remaining, float(cycle["buy_filled_price"]), False, client, closing=True, quote=quote)
                if state == "SELL_WORKING":
                    quote = get_latest_stock_quote(symbol, pool="D")
                    return _advance_sell(cur, config, cycle, quote, dry_run, client)
                if state == "CLOSING":
                    quote = get_latest_stock_quote(symbol, pool="D")
                    return _advance_sell(cur, config, cycle, quote, dry_run, client, closing=True)
                return f"state_requires_review:{state}"
            except Exception as exc:
                conn.rollback()
                # Keep the last durable state, especially SUBMITTING, recoverable.
                _set_cycle(cur, symbol, last_error=str(exc)[:512])
                _event(cur, symbol, int((cycle or {}).get("cycle_no") or 0), "ERROR", str((cycle or {}).get("state") or "UNKNOWN"), message=str(exc))
                conn.commit()  # Error evidence survives the context manager rollback.
                raise
            finally:
                cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))


def run_all() -> list[dict]:
    ensure_schema()
    from .d_entry_filter import sample_tick
    sample_tick()
    selection = _auto_select_candidate()
    rows = fetch_all("SELECT s.symbol FROM d_grid_symbols s LEFT JOIN d_grid_cycles c ON c.symbol=s.symbol WHERE s.enabled=1 OR c.state IN ('BUY_WORKING','SELL_WORKING','CLOSING','BUY_SUBMITTING','SELL_SUBMITTING') ORDER BY s.sort_order, s.symbol")
    results = [{"symbol": "AUTO", "ok": True, "message": str(selection)}] if selection else []
    for row in rows:
        symbol = str(row["symbol"])
        try:
            message = run_symbol(symbol)
            results.append({"symbol": symbol, "ok": True, "message": message})
        except Exception as exc:
            results.append({"symbol": symbol, "ok": False, "message": str(exc)})
    rejected = {r['symbol'] for r in results if str(r.get('message', '')).startswith('entry_filter:') and r.get('message') != 'entry_filter:collecting_prices'}
    if rejected:
        # Do not wait for the hourly rotation after a failed pre-buy check.
        replacement = _auto_select_candidate(force=True, exclude=rejected)
        if replacement:
            results.append({'symbol':'AUTO', 'ok':True, 'message':str(replacement)})
    return results


def status_rows() -> list[dict]:
    ensure_schema()
    return fetch_all(
        """
        SELECT s.*, c.state, c.cycle_no, c.buy_limit, c.buy_filled_qty,
               c.buy_filled_price, c.sell_limit, c.realized_pnl, c.last_error,
               c.updated_at AS cycle_updated_at
        FROM d_grid_symbols s
        LEFT JOIN d_grid_cycles c ON c.symbol=s.symbol
        ORDER BY s.sort_order, s.symbol
        """
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Strategy D single-cycle grid")
    sub = parser.add_subparsers(dest="command", required=True)
    config_parser = sub.add_parser("configure")
    config_parser.add_argument("symbol")
    config_parser.add_argument("--notional", type=float)
    config_parser.add_argument("--enable", action="store_true")
    sub.add_parser("run")
    sub.add_parser("status")
    args = parser.parse_args()
    if args.command == "configure":
        configure_symbol(args.symbol, enabled=args.enable, lot_notional=args.notional)
        print(status_rows(), flush=True)
    elif args.command == "run":
        print(run_all(), flush=True)
    else:
        print(status_rows(), flush=True)


if __name__ == "__main__":
    main()
