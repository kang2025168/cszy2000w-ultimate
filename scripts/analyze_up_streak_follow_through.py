# -*- coding: utf-8 -*-
"""
Analyze next-day follow-through probability after up streaks.

For each trading day in a date range:
    - up2_next_up: symbols that had already risen 2 consecutive trading days
      through the previous trading day; probability they rise again today.
    - up3_next_up: same for 3 consecutive up days.
    - up4_next_up: same for 4 consecutive up days.

Rise is defined as today's close > previous trading day's close.

Example:
    DB_HOST=138.197.75.51 DB_PORT=3307 DB_USER=tradebot DB_PASS='***' DB_NAME=cszy2000 \
    .venv/bin/python scripts/analyze_up_streak_follow_through.py --start 2026-04-01 --end 2026-05-01
"""

from __future__ import annotations

import argparse
import csv
import os
from collections import defaultdict
from datetime import date, timedelta

import pymysql


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

SRC_TABLE = os.getenv("SRC_TABLE", "stock_prices_pool")
OUT_DIR = os.getenv("OUT_DIR", "data")
MIN_CLOSE_PRICE = float(os.getenv("FOLLOW_THROUGH_MIN_CLOSE_PRICE", os.getenv("PRICE_CATEGORY_MIN_CLOSE_PRICE", "3")))


def _connect():
    return pymysql.connect(**DB)


def _fetch_prices(conn, start: date, end: date):
    # Need several pre-start trading days so Apr 1 can still have a valid prior streak.
    lookback_start = start - timedelta(days=20)
    sql = f"""
    SELECT UPPER(symbol) AS symbol, DATE(`date`) AS trade_date, `close`
    FROM `{SRC_TABLE}`
    WHERE DATE(`date`) BETWEEN %s AND %s
      AND symbol IS NOT NULL
      AND symbol <> ''
      AND `close` IS NOT NULL
    ORDER BY symbol ASC, trade_date ASC;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (lookback_start, end))
        return cur.fetchall() or []


def _as_float(v):
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _pct(numer: int, denom: int):
    if denom <= 0:
        return None
    return numer / denom


def _fmt_pct(v):
    if v is None:
        return ""
    return f"{v * 100:.2f}%"


def _analyze(rows, start: date, end: date):
    by_symbol = defaultdict(list)
    trading_dates = set()
    for row in rows:
        sym = str(row.get("symbol") or "").strip().upper()
        d = row.get("trade_date")
        close = _as_float(row.get("close"))
        if not sym or d is None or close is None:
            continue
        by_symbol[sym].append((d, close))
        if start <= d <= end:
            trading_dates.add(d)

    daily = {
        d: {
            "trade_date": d.isoformat(),
            "up2_base": 0,
            "up2_success": 0,
            "up3_base": 0,
            "up3_success": 0,
            "up4_base": 0,
            "up4_success": 0,
        }
        for d in sorted(trading_dates)
    }

    for sym_rows in by_symbol.values():
        sym_rows.sort(key=lambda x: x[0])
        streak_before_today = 0
        for i in range(1, len(sym_rows)):
            d, close = sym_rows[i]
            prev_d, prev_close = sym_rows[i - 1]
            today_up = close > prev_close

            if start <= d <= end and close > MIN_CLOSE_PRICE:
                day = daily.get(d)
                if day is not None:
                    for n in (2, 3, 4):
                        if streak_before_today >= n:
                            day[f"up{n}_base"] += 1
                            if today_up:
                                day[f"up{n}_success"] += 1

            streak_before_today = streak_before_today + 1 if today_up else 0

    for day in daily.values():
        for n in (2, 3, 4):
            day[f"up{n}_prob"] = _pct(day[f"up{n}_success"], day[f"up{n}_base"])

    totals = {}
    for n in (2, 3, 4):
        base = sum(day[f"up{n}_base"] for day in daily.values())
        success = sum(day[f"up{n}_success"] for day in daily.values())
        daily_probs = [day[f"up{n}_prob"] for day in daily.values() if day[f"up{n}_prob"] is not None]
        totals[f"up{n}"] = {
            "base": base,
            "success": success,
            "weighted_prob": _pct(success, base),
            "daily_avg_prob": (sum(daily_probs) / len(daily_probs)) if daily_probs else None,
        }

    return list(daily.values()), totals


def _write_csv(path: str, daily_rows):
    cols = [
        "trade_date",
        "up2_base", "up2_success", "up2_prob",
        "up3_base", "up3_success", "up3_prob",
        "up4_base", "up4_success", "up4_prob",
    ]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for row in daily_rows:
            out = dict(row)
            for n in (2, 3, 4):
                out[f"up{n}_prob"] = _fmt_pct(row[f"up{n}_prob"])
            writer.writerow({c: out.get(c, "") for c in cols})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="Start date, YYYY-MM-DD.")
    parser.add_argument("--end", required=True, help="End date, YYYY-MM-DD.")
    parser.add_argument("--out", default="", help="Output CSV path. Defaults to data/up_streak_follow_through_START_END.csv.")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    if end < start:
        raise SystemExit("--end must be >= --start")

    with _connect() as conn:
        rows = _fetch_prices(conn, start, end)

    daily_rows, totals = _analyze(rows, start, end)
    out_path = args.out or os.path.join(OUT_DIR, f"up_streak_follow_through_{start:%Y%m%d}_{end:%Y%m%d}.csv")
    _write_csv(out_path, daily_rows)

    print(f"[OK] range={start}..{end} trading_days={len(daily_rows)} min_close>{MIN_CLOSE_PRICE:g} -> {out_path}", flush=True)
    print("date, 连涨2天后第3天上涨, 连涨3天后第4天上涨, 连涨4天后第5天上涨", flush=True)
    for row in daily_rows:
        print(
            f"{row['trade_date']}, "
            f"{row['up2_success']}/{row['up2_base']}={_fmt_pct(row['up2_prob'])}, "
            f"{row['up3_success']}/{row['up3_base']}={_fmt_pct(row['up3_prob'])}, "
            f"{row['up4_success']}/{row['up4_base']}={_fmt_pct(row['up4_prob'])}",
            flush=True,
        )
    print("averages:", flush=True)
    for n, label in ((2, "连涨2天后第3天上涨"), (3, "连涨3天后第4天上涨"), (4, "连涨4天后第5天上涨")):
        t = totals[f"up{n}"]
        print(
            f"  {label}: weighted={_fmt_pct(t['weighted_prob'])} "
            f"({t['success']}/{t['base']}), daily_avg={_fmt_pct(t['daily_avg_prob'])}",
            flush=True,
        )


if __name__ == "__main__":
    main()
