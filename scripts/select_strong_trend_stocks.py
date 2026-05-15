# -*- coding: utf-8 -*-
"""
Select stocks with a strong-but-not-overheated daily trend from stock_prices_pool.

Default shape:
  - latest day gain between 5% and 15%
  - 2 to 4 consecutive up closes
  - close is near the intraday high
  - latest volume is meaningfully above recent average volume
  - minimum price and volume filters

Example:
  DB_HOST=127.0.0.1 DB_PORT=3307 DB_USER=tradebot DB_PASS='***' DB_NAME=cszy2000 \
  .venv/bin/python scripts/select_strong_trend_stocks.py
"""

from __future__ import annotations

import argparse
import csv
import os
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

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
OUT_DIR = Path(os.getenv("OUT_DIR", "data"))


def _connect():
    return pymysql.connect(**DB)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _latest_dates(conn, limit: int) -> list[date]:
    sql = f"""
    SELECT DISTINCT DATE(`date`) AS d
    FROM `{SRC_TABLE}`
    WHERE `date` IS NOT NULL
    ORDER BY d DESC
    LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall() or []
    return sorted(row["d"] for row in rows if row.get("d"))


def _fetch_prices(conn, dates: list[date]) -> list[dict]:
    placeholders = ",".join(["%s"] * len(dates))
    sql = f"""
    SELECT UPPER(symbol) AS symbol, DATE(`date`) AS trade_date,
           `open`, high, low, `close`, volume
    FROM `{SRC_TABLE}`
    WHERE DATE(`date`) IN ({placeholders})
      AND symbol IS NOT NULL
      AND symbol <> ''
      AND `open` IS NOT NULL
      AND high IS NOT NULL
      AND low IS NOT NULL
      AND `close` IS NOT NULL
      AND volume IS NOT NULL
    ORDER BY symbol ASC, trade_date ASC
    """
    with conn.cursor() as cur:
        cur.execute(sql, tuple(dates))
        return cur.fetchall() or []


def _up_streak(rows: list[dict]) -> int:
    streak = 0
    for idx in range(len(rows) - 1, 0, -1):
        cur_close = _as_float(rows[idx].get("close"))
        prev_close = _as_float(rows[idx - 1].get("close"))
        if cur_close > prev_close > 0:
            streak += 1
            continue
        break
    return streak


def _close_position(row: dict) -> float:
    high = _as_float(row.get("high"))
    low = _as_float(row.get("low"))
    close = _as_float(row.get("close"))
    if high <= low:
        return 1.0 if close >= high else 0.0
    return max(0.0, min(1.0, (close - low) / (high - low)))


def _avg(values: list[float]) -> float:
    values = [v for v in values if v > 0]
    return sum(values) / len(values) if values else 0.0


def _build_candidates(rows: list[dict], snapshot_date: date, args) -> list[dict]:
    by_symbol: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        if symbol:
            by_symbol[symbol].append(row)

    candidates = []
    for symbol, sym_rows in by_symbol.items():
        sym_rows.sort(key=lambda r: r["trade_date"])
        if len(sym_rows) < max(3, args.avg_volume_days + 1):
            continue
        latest = sym_rows[-1]
        if latest.get("trade_date") != snapshot_date:
            continue

        prev = sym_rows[-2]
        open_price = _as_float(latest.get("open"))
        high = _as_float(latest.get("high"))
        low = _as_float(latest.get("low"))
        close = _as_float(latest.get("close"))
        prev_close = _as_float(prev.get("close"))
        volume = _as_float(latest.get("volume"))
        if close <= 0 or prev_close <= 0:
            continue

        change_pct = (close - prev_close) / prev_close
        streak = _up_streak(sym_rows)
        close_pos = _close_position(latest)
        prior_volumes = [_as_float(r.get("volume")) for r in sym_rows[-(args.avg_volume_days + 1):-1]]
        avg_volume = _avg(prior_volumes)
        volume_ratio = volume / avg_volume if avg_volume > 0 else 0.0
        dollar_volume = close * volume
        day_range_pct = (high - low) / prev_close if prev_close > 0 else 0.0

        if close < args.min_price:
            continue
        if volume < args.min_volume:
            continue
        if dollar_volume < args.min_dollar_volume:
            continue
        if change_pct < args.min_gain_pct or change_pct > args.max_gain_pct:
            continue
        if streak < args.min_up_streak or streak > args.max_up_streak:
            continue
        if close_pos < args.min_close_position:
            continue
        if volume_ratio < args.min_volume_ratio:
            continue
        if close < open_price and args.require_green:
            continue

        score = (
            change_pct * 100.0
            + min(volume_ratio, 5.0) * 8.0
            + close_pos * 12.0
            + min(streak, 3) * 5.0
            + min(day_range_pct, 0.20) * 20.0
        )
        candidates.append({
            "symbol": symbol,
            "trade_date": snapshot_date.isoformat(),
            "score": round(score, 2),
            "change_pct": round(change_pct, 4),
            "up_streak": streak,
            "close_position": round(close_pos, 4),
            "volume_ratio": round(volume_ratio, 2),
            "volume": int(volume),
            "avg_volume": int(avg_volume),
            "dollar_volume": round(dollar_volume, 2),
            "open": round(open_price, 2),
            "high": round(high, 2),
            "low": round(low, 2),
            "close": round(close, 2),
            "prev_close": round(prev_close, 2),
            "day_range_pct": round(day_range_pct, 4),
        })

    candidates.sort(key=lambda r: (-r["score"], -r["change_pct"], r["symbol"]))
    return candidates


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "symbol", "trade_date", "score", "change_pct", "up_streak",
        "close_position", "volume_ratio", "volume", "avg_volume",
        "dollar_volume", "open", "high", "low", "close", "prev_close",
        "day_range_pct",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _print_rows(rows: list[dict], limit: int) -> None:
    print("symbol,date,score,chg%,streak,close_pos,vol_ratio,close,volume")
    for row in rows[:limit]:
        print(
            f"{row['symbol']},{row['trade_date']},{row['score']:.2f},"
            f"{row['change_pct']:.2%},{row['up_streak']},"
            f"{row['close_position']:.0%},{row['volume_ratio']:.2f},"
            f"{row['close']:.2f},{row['volume']}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Select strong trend stock candidates.")
    parser.add_argument("--date", default="", help="Snapshot date YYYY-MM-DD. Defaults to latest date in stock_prices_pool.")
    parser.add_argument("--lookback-days", type=int, default=35)
    parser.add_argument("--avg-volume-days", type=int, default=20)
    parser.add_argument("--min-price", type=float, default=float(os.getenv("STRONG_MIN_PRICE", "3")))
    parser.add_argument("--min-volume", type=float, default=float(os.getenv("STRONG_MIN_VOLUME", "1000000")))
    parser.add_argument("--min-dollar-volume", type=float, default=float(os.getenv("STRONG_MIN_DOLLAR_VOLUME", "5000000")))
    parser.add_argument("--min-gain-pct", type=float, default=float(os.getenv("STRONG_MIN_GAIN_PCT", "0.05")))
    parser.add_argument("--max-gain-pct", type=float, default=float(os.getenv("STRONG_MAX_GAIN_PCT", "0.15")))
    parser.add_argument("--min-up-streak", type=int, default=int(os.getenv("STRONG_MIN_UP_STREAK", "2")))
    parser.add_argument("--max-up-streak", type=int, default=int(os.getenv("STRONG_MAX_UP_STREAK", "4")))
    parser.add_argument("--min-close-position", type=float, default=float(os.getenv("STRONG_MIN_CLOSE_POSITION", "0.80")))
    parser.add_argument("--min-volume-ratio", type=float, default=float(os.getenv("STRONG_MIN_VOLUME_RATIO", "1.2")))
    parser.add_argument("--no-require-green", action="store_true", help="Do not require close >= open.")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--out", default="", help="CSV output path. Defaults to data/strong_trend_candidates_YYYYMMDD.csv.")
    args = parser.parse_args()
    args.require_green = not args.no_require_green

    with _connect() as conn:
        dates = _latest_dates(conn, args.lookback_days)
        if not dates:
            raise RuntimeError(f"{SRC_TABLE} has no data")
        snapshot_date = date.fromisoformat(args.date) if args.date else dates[-1]
        if snapshot_date not in dates:
            dates = sorted(set(dates + [snapshot_date]))
        rows = _fetch_prices(conn, dates)

    candidates = _build_candidates(rows, snapshot_date, args)
    out_path = Path(args.out) if args.out else OUT_DIR / f"strong_trend_candidates_{snapshot_date:%Y%m%d}.csv"
    _write_csv(out_path, candidates)

    print(
        f"[OK] date={snapshot_date} candidates={len(candidates)} out={out_path} "
        f"gain={args.min_gain_pct:.0%}-{args.max_gain_pct:.0%} "
        f"streak={args.min_up_streak}-{args.max_up_streak} "
        f"close_pos>={args.min_close_position:.0%} vol_ratio>={args.min_volume_ratio:g}",
        flush=True,
    )
    _print_rows(candidates, args.limit)


if __name__ == "__main__":
    main()
