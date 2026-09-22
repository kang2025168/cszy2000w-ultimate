#!/usr/bin/env python3
"""Build Strategy D's next-session candidate pool from completed daily bars."""

from __future__ import annotations

import argparse
import os
from collections import defaultdict

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

MIN_GAIN = float(os.getenv("D_CANDIDATE_MIN_GAIN_PCT", "0.05"))
MIN_PRICE = float(os.getenv("D_CANDIDATE_MIN_PRICE", "5"))
MIN_VOLUME = int(float(os.getenv("D_CANDIDATE_MIN_VOLUME", "3000000")))
MIN_DOLLAR_VOLUME = float(os.getenv("D_CANDIDATE_MIN_DOLLAR_VOLUME", "30000000"))
MIN_AVG_RANGE = float(os.getenv("D_CANDIDATE_MIN_AVG_RANGE_PCT", "0.015"))
MAX_AVG_RANGE = float(os.getenv("D_CANDIDATE_MAX_AVG_RANGE_PCT", "0.04"))
LOOKBACK_CALENDAR_DAYS = max(35, int(os.getenv("D_CANDIDATE_LOOKBACK_CALENDAR_DAYS", "60")))
RETAIN_TRADING_DAYS = max(1, int(os.getenv("D_CANDIDATE_RETAIN_TRADING_DAYS", "2")))

def _f(value) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def _eligible_symbol(symbol: str) -> bool:
    symbol = symbol.strip().upper()
    return bool(symbol and len(symbol) <= 16)


def build_candidates(rows: list[dict], snapshot_date) -> list[dict]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("symbol") or "").upper()].append(row)
    selected = []
    for symbol, bars in grouped.items():
        if not _eligible_symbol(symbol):
            continue
        bars.sort(key=lambda row: row["trade_date"])
        if len(bars) < 2 or bars[-1]["trade_date"] != snapshot_date:
            continue
        latest, previous = bars[-1], bars[-2]
        close, open_price = _f(latest["close"]), _f(latest["open"])
        high, low, volume = _f(latest["high"]), _f(latest["low"]), int(_f(latest["volume"]))
        if close <= 0 or open_price <= 0:
            continue
        gain = (close - open_price) / open_price
        dollar_volume = close * volume
        recent = bars[-20:]
        ranges = [(_f(row["high"]) - _f(row["low"])) / _f(row["close"]) for row in recent if _f(row["close"]) > 0]
        avg_range = sum(ranges) / len(ranges) if ranges else 0.0
        if not (gain >= MIN_GAIN and close >= MIN_PRICE and volume >= MIN_VOLUME):
            continue
        if dollar_volume < MIN_DOLLAR_VOLUME:
            continue
        liquidity_score = min(dollar_volume / 100_000_000.0, 3.0) * 25.0
        range_score = max(0.0, 1.0 - abs(avg_range - 0.025) / 0.015) * 20.0
        gain_score = min(gain, 0.15) / 0.15 * 30.0
        volume_score = min(volume / 10_000_000.0, 2.0) * 12.5
        selected.append({
            "symbol": symbol, "signal_date": snapshot_date, "close": close, "gain": gain,
            "volume": volume, "dollar_volume": dollar_volume, "avg_range": avg_range,
            "score": gain_score + liquidity_score + range_score + volume_score,
        })
    return sorted(selected, key=lambda row: (-row["score"], -row["dollar_volume"], row["symbol"]))


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh Strategy D candidate pool")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    conn = pymysql.connect(**DB)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(DATE(`date`)) AS d FROM stock_prices_pool")
            snapshot = (cur.fetchone() or {}).get("d")
            if not snapshot:
                raise RuntimeError("stock_prices_pool has no daily data")
            cur.execute(
                """SELECT DISTINCT DATE(`date`) AS d
                   FROM stock_prices_pool
                   WHERE DATE(`date`) <= %s
                   ORDER BY d DESC
                   LIMIT %s""",
                (snapshot, RETAIN_TRADING_DAYS),
            )
            retained_dates = [row["d"] for row in (cur.fetchall() or []) if row.get("d")]
            cutoff_date = min(retained_dates) if retained_dates else snapshot
            cur.execute(
                """SELECT UPPER(symbol) symbol, DATE(`date`) trade_date, `open`, high, low, `close`, volume
                   FROM stock_prices_pool
                   WHERE DATE(`date`) BETWEEN DATE_SUB(%s, INTERVAL %s DAY) AND %s
                   ORDER BY symbol, `date`""",
                (snapshot, LOOKBACK_CALENDAR_DAYS, snapshot),
            )
            rows = cur.fetchall() or []
        candidates = build_candidates(rows, snapshot)
        print(
            f"[D POOL] date={snapshot} selected={len(candidates)} "
            f"retain_trading_days={RETAIN_TRADING_DAYS} cutoff={cutoff_date}",
            flush=True,
        )
        for row in candidates[:30]:
            print(f"  {row['symbol']} score={row['score']:.1f} gain={row['gain']:.2%} volume={row['volume']:,}", flush=True)
        if args.dry_run:
            return
        with conn.cursor() as cur:
            for row in candidates:
                cur.execute(
                    """INSERT INTO d_candidate_pool
                       (symbol, signal_date, signal_close, signal_gain_pct, signal_volume,
                        signal_dollar_volume, avg_range_pct, base_score, enabled)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,1)
                       ON DUPLICATE KEY UPDATE signal_date=VALUES(signal_date),
                        signal_close=VALUES(signal_close), signal_gain_pct=VALUES(signal_gain_pct),
                        signal_volume=VALUES(signal_volume), signal_dollar_volume=VALUES(signal_dollar_volume),
                        avg_range_pct=VALUES(avg_range_pct), base_score=VALUES(base_score), enabled=1""",
                    (row["symbol"], row["signal_date"], row["close"], row["gain"], row["volume"],
                     row["dollar_volume"], row["avg_range"], row["score"]),
                )
            cur.execute(
                "UPDATE d_candidate_pool SET enabled=CASE WHEN signal_date >= %s THEN 1 ELSE 0 END",
                (cutoff_date,),
            )
            cur.execute("SELECT COUNT(*) AS n FROM d_candidate_pool WHERE enabled=1")
            active_count = int((cur.fetchone() or {}).get("n") or 0)
        print(f"[D POOL] active_recent_candidates={active_count}", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
