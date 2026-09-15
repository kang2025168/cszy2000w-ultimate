# -*- coding: utf-8 -*-
"""
Refresh Strategy B pre-selected stocks into stock_operations.

This reuses scripts/select_strong_trend_stocks.py to find recent daily
strong-trend candidates, then writes them as stock_type='B' and can_buy=1.
By default, candidates selected during the latest 5 trading days stay in the
ready pool. Existing bought B positions are left untouched. Existing unbought B
candidates that are no longer selected are disabled by default so the B queue
stays clean.
"""

from __future__ import annotations

import argparse
import csv
import os
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pymysql

import select_strong_trend_stocks as strong


DB = dict(
    host=os.getenv("DB_HOST", "mysql"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "tradebot"),
    password=os.getenv("DB_PASS", ""),
    database=os.getenv("DB_NAME", "cszy2000"),
    charset="utf8mb4",
    autocommit=True,
    cursorclass=pymysql.cursors.DictCursor,
)

OPS_TABLE = os.getenv("OPS_TABLE", "stock_operations")
OUT_DIR = Path(os.getenv("OUT_DIR", "data"))

B_MIN_UP_PCT = float(os.getenv("B_MIN_UP_PCT", "0.03"))
B_READY_LIMIT = int(os.getenv("B_READY_LIMIT", "50"))
B_READY_WINDOW_TRADING_DAYS = int(os.getenv("B_READY_WINDOW_TRADING_DAYS", "5"))
B_READY_REPLACE = os.getenv("B_READY_REPLACE", "1").strip().lower() not in {"0", "false", "no", "off"}
B_READY_MIN_DAY_VOLUME = int(float(os.getenv("B_READY_MIN_DAY_VOLUME", os.getenv("B_READY_MIN_VOLUME", "3000000"))))


def _connect():
    return pymysql.connect(**DB)


def _select_args(cli_args) -> SimpleNamespace:
    return SimpleNamespace(
        avg_volume_days=cli_args.avg_volume_days,
        min_price=cli_args.min_price,
        min_volume=cli_args.min_volume,
        min_dollar_volume=cli_args.min_dollar_volume,
        min_gain_pct=cli_args.min_gain_pct,
        max_gain_pct=cli_args.max_gain_pct,
        min_up_streak=cli_args.min_up_streak,
        max_up_streak=cli_args.max_up_streak,
        min_close_position=cli_args.min_close_position,
        min_volume_ratio=cli_args.min_volume_ratio,
        require_green=not cli_args.no_require_green,
    )


def _candidate_intent(row: dict) -> str:
    text = (
        f"B:READY date={row['trade_date']} score={row['score']:.2f} "
        f"chg={row['change_pct']:.2%} streak={row['up_streak']} "
        f"volr={row['volume_ratio']:.2f}"
    )
    return text[:80]


def _trigger_price(row: dict) -> float:
    prev_close = float(row.get("prev_close") or 0)
    if prev_close <= 0:
        return round(float(row.get("close") or 0), 2)
    return round(prev_close * (1.0 + B_MIN_UP_PCT), 2)


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "symbol", "trade_date", "score", "change_pct", "up_streak",
        "close_position", "volume_ratio", "volume", "avg_volume",
        "dollar_volume", "open", "high", "low", "close", "prev_close",
        "day_range_pct", "trigger_price",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _disable_stale_candidates(conn, selected_symbols: list[str]) -> int:
    if not B_READY_REPLACE:
        return 0

    params: list = []
    keep_sql = ""
    if selected_symbols:
        placeholders = ",".join(["%s"] * len(selected_symbols))
        keep_sql = f"AND stock_code NOT IN ({placeholders})"
        params.extend(selected_symbols)

    sql = f"""
    UPDATE `{OPS_TABLE}`
    SET can_buy=0,
        can_sell=0,
        last_order_intent=%s,
        updated_at=CURRENT_TIMESTAMP
    WHERE stock_type='B'
      AND is_bought=0
      {keep_sql};
    """
    params.insert(0, "B:READY refresh disabled stale candidate")
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        return int(cur.rowcount or 0)


def _upsert_candidate(conn, row: dict) -> bool:
    symbol = str(row.get("symbol") or "").strip().upper()
    if not symbol:
        return False

    trigger = _trigger_price(row)
    close_price = round(float(row.get("close") or 0), 2)
    entry_open = round(float(row.get("open") or 0), 2)
    entry_close = close_price
    entry_date = row.get("trade_date")
    intent = _candidate_intent(row)
    intraday_volume = int(float(row.get("volume") or 0))

    sql = f"""
    INSERT INTO `{OPS_TABLE}` (
        stock_code, stock_type, is_bought, can_buy, can_sell,
        trigger_price, close_price, entry_open, entry_close, entry_date,
        cost_price, stop_loss_price, take_profit_price, qty,
        b_stage, b_peak_price, b_peak_profit, b_last_profit,
        intraday_volume, strategy_group, capital_pool, margin_used,
        last_order_intent, created_at, updated_at
    )
    VALUES (
        %s, 'B', 0, 1, 0,
        %s, %s, %s, %s, %s,
        NULL, NULL, NULL, 0,
        0, NULL, 0, 0,
        %s, 'B', 'B', 0,
        %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    )
    ON DUPLICATE KEY UPDATE
        can_buy=IF(is_bought=1, can_buy, VALUES(can_buy)),
        can_sell=IF(is_bought=1, can_sell, VALUES(can_sell)),
        trigger_price=IF(is_bought=1, trigger_price, VALUES(trigger_price)),
        close_price=IF(is_bought=1, close_price, VALUES(close_price)),
        entry_open=IF(is_bought=1, entry_open, VALUES(entry_open)),
        entry_close=IF(is_bought=1, entry_close, VALUES(entry_close)),
        entry_date=IF(is_bought=1, entry_date, VALUES(entry_date)),
        qty=IF(is_bought=1, qty, 0),
        b_stage=IF(is_bought=1, b_stage, 0),
        b_peak_price=IF(is_bought=1, b_peak_price, NULL),
        b_peak_profit=IF(is_bought=1, b_peak_profit, 0),
        b_last_profit=IF(is_bought=1, b_last_profit, 0),
        intraday_volume=IF(is_bought=1, intraday_volume, VALUES(intraday_volume)),
        strategy_group='B',
        capital_pool='B',
        margin_used=IF(is_bought=1, margin_used, 0),
        last_order_intent=IF(is_bought=1, last_order_intent, VALUES(last_order_intent)),
        updated_at=CURRENT_TIMESTAMP;
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                symbol,
                trigger,
                close_price,
                entry_open,
                entry_close,
                entry_date,
                intraday_volume,
                intent,
            ),
        )
    return True


def _dedupe_latest_by_symbol(rows: list[dict]) -> list[dict]:
    latest: dict[str, dict] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        current = latest.get(symbol)
        if current is None:
            latest[symbol] = row
            continue
        current_key = (str(current.get("trade_date") or ""), float(current.get("score") or 0))
        row_key = (str(row.get("trade_date") or ""), float(row.get("score") or 0))
        if row_key > current_key:
            latest[symbol] = row
    return sorted(
        latest.values(),
        key=lambda r: (str(r.get("trade_date") or ""), float(r.get("score") or 0), str(r.get("symbol") or "")),
        reverse=True,
    )


def _load_candidates(cli_args) -> tuple[date, date, list[dict], int]:
    with strong._connect() as conn:
        dates = strong._latest_dates(conn, cli_args.lookback_days)
        if not dates:
            raise RuntimeError(f"{strong.SRC_TABLE} has no data")
        snapshot_date = date.fromisoformat(cli_args.date) if cli_args.date else dates[-1]
        if snapshot_date not in dates:
            dates = sorted(set(dates + [snapshot_date]))
        rows = strong._fetch_prices(conn, dates)

    target_dates = [d for d in dates if d <= snapshot_date]
    window = max(1, cli_args.window_trading_days)
    target_dates = target_dates[-window:]
    if not target_dates:
        raise RuntimeError(f"{strong.SRC_TABLE} has no data on or before {snapshot_date}")

    per_day_selected: list[dict] = []
    select_args = _select_args(cli_args)
    for target_date in target_dates:
        rows_as_of = [row for row in rows if row.get("trade_date") <= target_date]
        daily_candidates = strong._build_candidates(rows_as_of, target_date, select_args)
        daily_candidates = [
            row for row in daily_candidates
            if int(float(row.get("volume") or 0)) > int(cli_args.min_day_volume)
        ]
        daily_selected = daily_candidates if cli_args.limit <= 0 else daily_candidates[: cli_args.limit]
        per_day_selected.extend(daily_selected)

    candidates = _dedupe_latest_by_symbol(per_day_selected)
    for row in candidates:
        row["trigger_price"] = _trigger_price(row)
    return snapshot_date, target_dates[0], candidates, len(target_dates)


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh Strategy B candidates into stock_operations.")
    parser.add_argument("--date", default="", help="Snapshot date YYYY-MM-DD. Defaults to latest date in stock_prices_pool.")
    parser.add_argument("--lookback-days", type=int, default=int(os.getenv("B_READY_LOOKBACK_DAYS", "35")))
    parser.add_argument("--avg-volume-days", type=int, default=int(os.getenv("B_READY_AVG_VOLUME_DAYS", "20")))
    parser.add_argument("--min-price", type=float, default=float(os.getenv("B_READY_MIN_PRICE", os.getenv("STRONG_MIN_PRICE", "5"))))
    parser.add_argument("--min-volume", type=float, default=float(os.getenv("B_READY_MIN_VOLUME", os.getenv("STRONG_MIN_VOLUME", "3000000"))))
    parser.add_argument("--min-day-volume", type=float, default=float(B_READY_MIN_DAY_VOLUME), help="Candidate trade-day volume must be greater than this value.")
    parser.add_argument("--min-dollar-volume", type=float, default=float(os.getenv("B_READY_MIN_DOLLAR_VOLUME", os.getenv("STRONG_MIN_DOLLAR_VOLUME", "5000000"))))
    parser.add_argument("--min-gain-pct", type=float, default=float(os.getenv("B_READY_MIN_GAIN_PCT", os.getenv("STRONG_MIN_GAIN_PCT", "0.05"))))
    parser.add_argument("--max-gain-pct", type=float, default=float(os.getenv("B_READY_MAX_GAIN_PCT", os.getenv("STRONG_MAX_GAIN_PCT", "0.15"))))
    parser.add_argument("--min-up-streak", type=int, default=int(os.getenv("B_READY_MIN_UP_STREAK", os.getenv("STRONG_MIN_UP_STREAK", "2"))))
    parser.add_argument("--max-up-streak", type=int, default=int(os.getenv("B_READY_MAX_UP_STREAK", os.getenv("STRONG_MAX_UP_STREAK", "4"))))
    parser.add_argument("--min-close-position", type=float, default=float(os.getenv("B_READY_MIN_CLOSE_POSITION", os.getenv("STRONG_MIN_CLOSE_POSITION", "0.80"))))
    parser.add_argument("--min-volume-ratio", type=float, default=float(os.getenv("B_READY_MIN_VOLUME_RATIO", os.getenv("STRONG_MIN_VOLUME_RATIO", "1.2"))))
    parser.add_argument("--no-require-green", action="store_true", help="Do not require close >= open.")
    parser.add_argument("--limit", type=int, default=B_READY_LIMIT, help="Maximum candidates per trading day to write into stock_operations. 0 means all.")
    parser.add_argument("--window-trading-days", type=int, default=B_READY_WINDOW_TRADING_DAYS, help="Keep candidates selected within this many recent trading days.")
    parser.add_argument("--dry-run", action="store_true", help="Print selected candidates without writing stock_operations.")
    args = parser.parse_args()

    snapshot_date, window_start, selected, window_days = _load_candidates(args)

    out_path = OUT_DIR / f"strategy_b_candidates_{snapshot_date:%Y%m%d}.csv"
    latest_path = OUT_DIR / "strategy_b_candidates_latest.csv"
    _write_csv(out_path, selected)
    _write_csv(latest_path, selected)

    if args.dry_run:
        print(
            f"[B READY] dry_run date={snapshot_date} window={window_start}..{snapshot_date} "
            f"trading_days={window_days} min_day_volume>{int(args.min_day_volume)} selected={len(selected)} out={out_path}",
            flush=True,
        )
        for row in selected[:20]:
            print(
                f"  {row['symbol']} date={row['trade_date']} score={row['score']:.2f} chg={row['change_pct']:.2%} "
                f"close={row['close']:.2f} trigger={row['trigger_price']:.2f}",
                flush=True,
            )
        return

    with _connect() as conn:
        symbols = [row["symbol"] for row in selected]
        disabled = _disable_stale_candidates(conn, symbols)
        written = 0
        for row in selected:
            if _upsert_candidate(conn, row):
                written += 1

    print(
        f"[B READY] date={snapshot_date} window={window_start}..{snapshot_date} "
        f"trading_days={window_days} selected={len(selected)} "
        f"written={written} disabled_stale={disabled} out={out_path}",
        flush=True,
    )
    for row in selected[:20]:
        print(
            f"[B READY] {row['symbol']} date={row['trade_date']} score={row['score']:.2f} chg={row['change_pct']:.2%} "
            f"close={row['close']:.2f} trigger={row['trigger_price']:.2f}",
            flush=True,
        )


if __name__ == "__main__":
    main()
