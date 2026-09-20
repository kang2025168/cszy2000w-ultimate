# -*- coding: utf-8 -*-
"""
Local Yahoo-driven signal writer for Strategy B.

This tool is intended to run on the local machine, where Yahoo access is
available and Alpaca market-data delay is not used for signal decisions. It
reads B candidates/positions from the cloud MySQL `stock_operations` table,
calculates buy/sell signals from Yahoo quotes, and writes only signal fields
back to MySQL. Alpaca remains responsible for account, orders, and fills.

Typical dry run:
  DB_HOST=138.197.75.51 DB_PORT=3307 DB_USER=tradebot DB_PASS='...' DB_NAME=cszy2000 \
  .venv/bin/python scripts/local_b_signal_yahoo.py --dry-run --once

Loop:
  .venv/bin/python scripts/local_b_signal_yahoo.py --loop --interval 15
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pymysql

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ultimate_v1.yahoo_market_data import get_yahoo_stock_quote  # noqa: E402


OPS_TABLE = os.getenv("OPS_TABLE", "stock_operations")

DB = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "3307")),
    user=os.getenv("DB_USER", "tradebot"),
    password=os.getenv("DB_PASS", ""),
    database=os.getenv("DB_NAME", "cszy2000"),
    charset="utf8mb4",
    autocommit=True,
    cursorclass=pymysql.cursors.DictCursor,
)

B_MIN_UP_PCT = float(os.getenv("B_MIN_UP_PCT", "0.03"))
B_MAX_BUY_UP_PCT = float(os.getenv("B_MAX_BUY_UP_PCT", "0.10"))
B_MAX_ENTRY_UP_PCT = float(os.getenv("B_MAX_ENTRY_UP_PCT", "0.4"))
B_MIN_PRICE = float(os.getenv("B_MIN_PRICE", "5.0"))
B_MAX_BELOW_OPEN_PCT = float(os.getenv("B_MAX_BELOW_OPEN_PCT", "0.015"))
B_MAX_PULLBACK_FROM_HIGH_PCT = float(os.getenv("B_MAX_PULLBACK_FROM_HIGH_PCT", "0.03"))
B_INITIAL_STOP_MULT = float(os.getenv("B_INITIAL_STOP_MULT", "0.95"))
B_CATASTROPHIC_STOP_LOSS_PCT = float(os.getenv("B_CATASTROPHIC_STOP_LOSS_PCT", "-0.08"))
B_SIGNAL_STALE_RESET_SEC = int(os.getenv("B_SIGNAL_STALE_RESET_SEC", "300"))
B_SIGNAL_LIMIT = int(os.getenv("B_SIGNAL_LIMIT", "300"))
B_SIGNAL_TZ = os.getenv("B_SIGNAL_TZ", "America/Los_Angeles")
B_SIGNAL_BUY_START_LA = os.getenv("B_SIGNAL_BUY_START_LA", "06:50")
B_SIGNAL_BUY_END_LA = os.getenv("B_SIGNAL_BUY_END_LA", "12:55")
B_SIGNAL_SELL_START_LA = os.getenv("B_SIGNAL_SELL_START_LA", "06:40")
B_SIGNAL_SELL_END_LA = os.getenv("B_SIGNAL_SELL_END_LA", "13:00")
B_SIGNAL_IGNORE_WINDOW = os.getenv("B_SIGNAL_IGNORE_WINDOW", "0").strip().lower() in {"1", "true", "yes", "on"}


SIGNAL_COLUMNS = {
    "current_price": "DECIMAL(18,6) NULL",
    "intraday_volume": "BIGINT NULL",
    "buy_signal": "TINYINT DEFAULT 0",
    "sell_signal": "TINYINT DEFAULT 0",
    "signal_action": "VARCHAR(32) NULL",
    "signal_price": "DECIMAL(18,6) NULL",
    "signal_prev_close": "DECIMAL(18,6) NULL",
    "signal_day_open": "DECIMAL(18,6) NULL",
    "signal_day_high": "DECIMAL(18,6) NULL",
    "signal_day_low": "DECIMAL(18,6) NULL",
    "signal_volume": "BIGINT NULL",
    "signal_source": "VARCHAR(64) NULL",
    "signal_reason": "VARCHAR(512) NULL",
    "signal_at": "DATETIME NULL",
}


@dataclass
class SignalDecision:
    symbol: str
    role: str
    buy_signal: int
    sell_signal: int
    signal_action: str
    signal_price: float
    signal_prev_close: float
    signal_day_open: float
    signal_day_high: float
    signal_day_low: float
    signal_volume: int
    signal_source: str
    signal_reason: str


def _connect():
    return pymysql.connect(**DB)


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value, default: int = 0) -> int:
    try:
        if value is None or str(value).strip() == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _hhmm_minutes(value: str, default: str) -> int:
    raw = (value or default).strip()
    try:
        hh, mm = raw.split(":", 1)
        return int(hh) * 60 + int(mm)
    except Exception:
        hh, mm = default.split(":", 1)
        return int(hh) * 60 + int(mm)


def _now_signal_tz() -> datetime:
    try:
        return datetime.now(ZoneInfo(B_SIGNAL_TZ))
    except Exception:
        return datetime.now()


def _in_window(start: str, end: str, default_start: str, default_end: str) -> tuple[bool, str]:
    if B_SIGNAL_IGNORE_WINDOW:
        return True, "ignored"
    now = _now_signal_tz()
    if now.weekday() >= 5:
        return False, f"weekend now={now.strftime('%a %H:%M')}"
    now_min = now.hour * 60 + now.minute
    start_min = _hhmm_minutes(start, default_start)
    end_min = _hhmm_minutes(end, default_end)
    if start_min <= end_min:
        ok = start_min <= now_min <= end_min
    else:
        ok = now_min >= start_min or now_min <= end_min
    return ok, f"now={now.strftime('%H:%M')} window={start}-{end}"


def _ensure_columns(conn) -> None:
    sql = """
    SELECT COLUMN_NAME
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (DB["database"], OPS_TABLE))
        existing = {str(row.get("COLUMN_NAME")) for row in cur.fetchall() or []}
        for name, ddl in SIGNAL_COLUMNS.items():
            if name not in existing:
                cur.execute(f"ALTER TABLE `{OPS_TABLE}` ADD COLUMN `{name}` {ddl}")
                print(f"[SCHEMA] added {OPS_TABLE}.{name}", flush=True)


def _symbol_filter_sql(symbols: list[str], params: list) -> str:
    if not symbols:
        return ""
    placeholders = ",".join(["%s"] * len(symbols))
    params.extend(symbols)
    return f" AND UPPER(stock_code) IN ({placeholders})"


def _load_b_rows(conn, symbols: list[str] | None = None) -> list[dict]:
    symbols = [s.strip().upper() for s in symbols or [] if s.strip()]
    params: list = []
    symbol_sql = _symbol_filter_sql(symbols, params)
    sql = f"""
    SELECT *
    FROM `{OPS_TABLE}`
    WHERE UPPER(stock_type)='B'
      AND (
        (COALESCE(can_buy,0)=1 AND COALESCE(is_bought,0)<>1)
        OR
        (COALESCE(is_bought,0)=1 AND COALESCE(can_sell,0)=1)
      )
      {symbol_sql}
    ORDER BY COALESCE(is_bought,0) DESC, UPPER(stock_code) ASC
    LIMIT %s
    """
    params.append(B_SIGNAL_LIMIT)
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        return list(cur.fetchall() or [])


def _prev_close_from_db(conn, symbol: str) -> float:
    sql = """
    SELECT `close`
    FROM stock_prices_pool
    WHERE UPPER(symbol)=UPPER(%s)
      AND `close` > 0
    ORDER BY `date` DESC
    LIMIT 1
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (symbol,))
            row = cur.fetchone() or {}
        return _safe_float(row.get("close"))
    except Exception:
        return 0.0


def _reject_buy(row: dict, price: float, day_open: float, day_high: float, prev_close: float) -> str:
    symbol = str(row.get("stock_code") or "").upper()
    trigger = _safe_float(row.get("trigger_price"))
    entry_close = _safe_float(row.get("entry_close")) or _safe_float(row.get("close_price")) or trigger

    if _safe_int(row.get("can_buy")) != 1:
        return f"can_buy={_safe_int(row.get('can_buy'))}"
    if _safe_int(row.get("is_bought")) == 1:
        return "already_bought"
    if price <= 0:
        return "invalid_price"
    if price < B_MIN_PRICE:
        return f"price {price:.2f} < min_price {B_MIN_PRICE:.2f}"
    if prev_close <= 0:
        return "invalid_prev_close"
    if trigger <= 0:
        return "invalid_trigger"
    if entry_close <= 0:
        return "invalid_entry_close"
    if price <= trigger:
        return f"price {price:.2f} <= trigger {trigger:.2f}"

    day_up_pct = (price - prev_close) / prev_close
    entry_up_pct = (price - entry_close) / entry_close
    if day_up_pct <= B_MIN_UP_PCT:
        return f"day_up {day_up_pct:.2%} <= min {B_MIN_UP_PCT:.2%}"
    if day_up_pct >= B_MAX_BUY_UP_PCT:
        return f"day_up {day_up_pct:.2%} >= max {B_MAX_BUY_UP_PCT:.2%}"
    if entry_up_pct >= B_MAX_ENTRY_UP_PCT:
        return f"entry_up {entry_up_pct:.2%} >= max {B_MAX_ENTRY_UP_PCT:.2%}"
    if day_open > 0 and price < day_open * (1.0 - B_MAX_BELOW_OPEN_PCT):
        return f"below_open price {price:.2f} open {day_open:.2f}"
    if day_high > 0 and price < day_high * (1.0 - B_MAX_PULLBACK_FROM_HIGH_PCT):
        return f"pullback_from_high price {price:.2f} high {day_high:.2f}"
    return ""


def _buy_reason(row: dict, price: float, prev_close: float) -> str:
    trigger = _safe_float(row.get("trigger_price"))
    entry_close = _safe_float(row.get("entry_close")) or _safe_float(row.get("close_price")) or trigger
    day_up_pct = (price - prev_close) / prev_close if prev_close > 0 else 0.0
    entry_up_pct = (price - entry_close) / entry_close if entry_close > 0 else 0.0
    return (
        f"BUY_SIGNAL price={price:.2f} trigger={trigger:.2f} "
        f"day_up={day_up_pct:.2%} entry_up={entry_up_pct:.2%}"
    )


def _sell_reason(row: dict, price: float) -> tuple[int, str, str]:
    qty = _safe_int(row.get("qty"))
    cost = _safe_float(row.get("cost_price"))
    sl = _safe_float(row.get("stop_loss_price"))
    if _safe_int(row.get("is_bought")) != 1:
        return 0, "HOLD", "not_bought"
    if _safe_int(row.get("can_sell")) != 1:
        return 0, "HOLD", f"can_sell={_safe_int(row.get('can_sell'))}"
    if qty <= 0 or cost <= 0:
        return 0, "HOLD", f"invalid_qty_cost qty={qty} cost={cost:.2f}"
    if price <= 0:
        return 0, "HOLD", "invalid_price"

    up_pct = (price - cost) / cost
    effective_sl = sl if sl > 0 else cost * B_INITIAL_STOP_MULT
    if up_pct <= B_CATASTROPHIC_STOP_LOSS_PCT:
        return 1, "SELL_STOP", f"CATASTROPHIC price={price:.2f} cost={cost:.2f} up={up_pct:.2%}"
    if effective_sl > 0 and price <= effective_sl:
        return 1, "SELL_STOP", f"STOP price={price:.2f} <= sl={effective_sl:.2f} up={up_pct:.2%}"
    return 0, "HOLD", f"HOLD price={price:.2f} cost={cost:.2f} up={up_pct:.2%} sl={effective_sl:.2f}"


def _decide_row(conn, row: dict) -> SignalDecision:
    symbol = str(row.get("stock_code") or "").strip().upper()
    role = "sell" if _safe_int(row.get("is_bought")) == 1 else "buy"
    quote = get_yahoo_stock_quote(symbol)
    price = _safe_float(quote.last)
    prev_close = _safe_float(quote.prev_close) or _safe_float(quote.regular_close) or _prev_close_from_db(conn, symbol)
    day_open = _safe_float(quote.day_open)
    day_high = _safe_float(quote.day_high)
    day_low = _safe_float(quote.day_low)
    volume = _safe_int(quote.day_volume)

    if role == "buy":
        in_buy_window, window_reason = _in_window(B_SIGNAL_BUY_START_LA, B_SIGNAL_BUY_END_LA, "06:50", "12:55")
        reject = "" if in_buy_window else f"outside_buy_window {window_reason}"
        if not reject:
            reject = _reject_buy(row, price, day_open, day_high, prev_close)
        buy_signal = 0 if reject else 1
        action = "BUY" if buy_signal else "WAIT_BUY"
        reason = reject or _buy_reason(row, price, prev_close)
        sell_signal = 0
    else:
        in_sell_window, window_reason = _in_window(B_SIGNAL_SELL_START_LA, B_SIGNAL_SELL_END_LA, "06:40", "13:00")
        if in_sell_window:
            sell_signal, action, reason = _sell_reason(row, price)
        else:
            sell_signal, action, reason = 0, "HOLD", f"outside_sell_window {window_reason}"
        buy_signal = 0

    return SignalDecision(
        symbol=symbol,
        role=role,
        buy_signal=buy_signal,
        sell_signal=sell_signal,
        signal_action=action,
        signal_price=round(price, 6),
        signal_prev_close=round(prev_close, 6),
        signal_day_open=round(day_open, 6),
        signal_day_high=round(day_high, 6),
        signal_day_low=round(day_low, 6),
        signal_volume=volume,
        signal_source=str(quote.source or "yahoo")[:64],
        signal_reason=reason[:512],
    )


def _quote_only_decision(conn, symbol: str) -> SignalDecision:
    symbol = (symbol or "").strip().upper()
    quote = get_yahoo_stock_quote(symbol)
    price = _safe_float(quote.last)
    prev_close = _safe_float(quote.prev_close) or _safe_float(quote.regular_close) or _prev_close_from_db(conn, symbol)
    day_open = _safe_float(quote.day_open)
    day_high = _safe_float(quote.day_high)
    day_low = _safe_float(quote.day_low)
    volume = _safe_int(quote.day_volume)
    day_up_pct = (price - prev_close) / prev_close if price > 0 and prev_close > 0 else 0.0
    return SignalDecision(
        symbol=symbol,
        role="watch",
        buy_signal=0,
        sell_signal=0,
        signal_action="QUOTE",
        signal_price=round(price, 6),
        signal_prev_close=round(prev_close, 6),
        signal_day_open=round(day_open, 6),
        signal_day_high=round(day_high, 6),
        signal_day_low=round(day_low, 6),
        signal_volume=volume,
        signal_source=str(quote.source or "yahoo")[:64],
        signal_reason=f"QUOTE price={price:.2f} prev_close={prev_close:.2f} day_up={day_up_pct:.2%}",
    )


def _write_signal(conn, decision: SignalDecision) -> None:
    sql = f"""
    UPDATE `{OPS_TABLE}`
    SET buy_signal=%s,
        sell_signal=%s,
        signal_action=%s,
        signal_price=%s,
        signal_prev_close=%s,
        signal_day_open=%s,
        signal_day_high=%s,
        signal_day_low=%s,
        signal_volume=%s,
        signal_source=%s,
        signal_reason=%s,
        signal_at=CURRENT_TIMESTAMP,
        current_price=%s,
        intraday_volume=CASE WHEN %s > 0 THEN %s ELSE intraday_volume END,
        updated_at=CURRENT_TIMESTAMP
    WHERE UPPER(stock_code)=UPPER(%s)
      AND UPPER(stock_type)='B'
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                decision.buy_signal,
                decision.sell_signal,
                decision.signal_action,
                decision.signal_price,
                decision.signal_prev_close,
                decision.signal_day_open,
                decision.signal_day_high,
                decision.signal_day_low,
                decision.signal_volume,
                decision.signal_source,
                decision.signal_reason,
                decision.signal_price,
                decision.signal_volume,
                decision.signal_volume,
                decision.symbol,
            ),
        )


def _clear_stale_signals(conn) -> int:
    sql = f"""
    UPDATE `{OPS_TABLE}`
    SET buy_signal=0,
        sell_signal=0,
        signal_action='STALE',
        signal_reason='signal stale reset',
        updated_at=CURRENT_TIMESTAMP
    WHERE UPPER(stock_type)='B'
      AND signal_at IS NOT NULL
      AND signal_at < (CURRENT_TIMESTAMP - INTERVAL %s SECOND)
      AND (COALESCE(buy_signal,0)=1 OR COALESCE(sell_signal,0)=1)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (B_SIGNAL_STALE_RESET_SEC,))
        return int(cur.rowcount or 0)


def run_once(args) -> dict:
    started = time.time()
    rows_out: list[dict] = []
    errors: list[dict] = []
    symbols = [s.strip().upper() for s in (args.symbols or "").split(",") if s.strip()]

    with _connect() as conn:
        if not args.dry_run:
            _ensure_columns(conn)
        stale_reset = 0 if args.dry_run else _clear_stale_signals(conn)
        rows = _load_b_rows(conn, symbols=symbols)
        seen_symbols = {str(row.get("stock_code") or "").strip().upper() for row in rows}
        for row in rows:
            symbol = str(row.get("stock_code") or "").strip().upper()
            try:
                decision = _decide_row(conn, row)
                if not args.dry_run:
                    _write_signal(conn, decision)
                rows_out.append(asdict(decision))
                print(
                    f"[B SIGNAL] {decision.symbol} {decision.signal_action} "
                    f"buy={decision.buy_signal} sell={decision.sell_signal} "
                    f"price={decision.signal_price:.2f} reason={decision.signal_reason}",
                    flush=True,
                )
            except Exception as exc:
                err = {"symbol": symbol, "error": str(exc)}
                errors.append(err)
                print(f"[B SIGNAL] {symbol} ERROR {exc}", flush=True)
        for symbol in [s for s in symbols if s not in seen_symbols]:
            try:
                decision = _quote_only_decision(conn, symbol)
                rows_out.append(asdict(decision))
                print(
                    f"[B SIGNAL] {decision.symbol} QUOTE "
                    f"price={decision.signal_price:.2f} reason={decision.signal_reason}",
                    flush=True,
                )
            except Exception as exc:
                err = {"symbol": symbol, "error": str(exc)}
                errors.append(err)
                print(f"[B SIGNAL] {symbol} ERROR {exc}", flush=True)

    return {
        "ok": not errors,
        "dry_run": bool(args.dry_run),
        "count": len(rows_out),
        "buy_signals": sum(1 for r in rows_out if r["buy_signal"]),
        "sell_signals": sum(1 for r in rows_out if r["sell_signal"]),
        "stale_reset": stale_reset,
        "errors": errors,
        "rows": rows_out,
        "elapsed_sec": round(time.time() - started, 3),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Write Strategy B Yahoo buy/sell signals into stock_operations.")
    parser.add_argument("--dry-run", action="store_true", help="Print decisions without writing signal fields.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--loop", action="store_true", help="Run continuously.")
    parser.add_argument("--interval", type=float, default=float(os.getenv("B_SIGNAL_INTERVAL_SEC", "15")), help="Loop interval seconds.")
    parser.add_argument("--symbols", default="", help="Comma-separated symbols to process.")
    parser.add_argument("--json", action="store_true", help="Print JSON summary after each run.")
    args = parser.parse_args()

    if not args.once and not args.loop:
        args.once = True

    while True:
        summary = run_once(args)
        if args.json or args.once:
            print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
        if args.once:
            return 0 if summary["ok"] else 1
        time.sleep(max(float(args.interval or 1), 1.0))


if __name__ == "__main__":
    raise SystemExit(main())
