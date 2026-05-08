# -*- coding: utf-8 -*-
"""
Local intraday volume sync for stock_operations.

Purpose:
- stock_prices_pool remains the after-hours daily history table.
- This local-only bot updates today's cumulative intraday volume directly on stock_operations
  so live buy rules can later use current volume without depending on cloud
  Yahoo access or Alpaca market-data entitlements.
"""
from __future__ import annotations

import os
import time
from datetime import datetime

import pandas as pd
import pymysql
import requests
import yfinance as yf

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


OPS_TABLE = os.getenv("OPS_TABLE", "stock_operations")

DB = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "3307")),
    user=os.getenv("DB_USER", "tradebot"),
    password=os.getenv("DB_PASS", "TradeBot#2026!"),
    database=os.getenv("DB_NAME", "cszy2000"),
    charset="utf8mb4",
    autocommit=True,
    cursorclass=pymysql.cursors.DictCursor,
)

BATCH_SIZE = int(os.getenv("OPS_VOLUME_BATCH_SIZE", "120"))
INTERVAL = os.getenv("OPS_VOLUME_INTERVAL", "1d")
PERIOD = os.getenv("OPS_VOLUME_PERIOD", "7d")
SOURCE = os.getenv("OPS_VOLUME_SOURCE", "daily_bar").strip().lower()
YAHOO_TIMEOUT = float(os.getenv("OPS_VOLUME_YAHOO_TIMEOUT", "6"))
SLEEP_SECONDS = float(os.getenv("OPS_VOLUME_SLEEP_SECONDS", "300"))
RUN_ONCE = int(os.getenv("OPS_VOLUME_RUN_ONCE", "0"))
STOCK_TYPES = os.getenv("OPS_VOLUME_STOCK_TYPES", "A,B,F").strip()
LA_TZ_NAME = os.getenv("OPS_VOLUME_TZ", "America/Los_Angeles")
LA_TZ = ZoneInfo(LA_TZ_NAME) if ZoneInfo else None
START_LA = os.getenv("OPS_VOLUME_START_LA", "06:00")
END_LA = os.getenv("OPS_VOLUME_END_LA", "17:00")
IGNORE_WINDOW = int(os.getenv("OPS_VOLUME_IGNORE_WINDOW", "0"))


def _connect():
    return pymysql.connect(**DB)


def _chunked(items, n):
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _hhmm_to_minutes(value: str, default: str) -> int:
    raw = (value or default).strip()
    try:
        hh, mm = raw.split(":", 1)
        return int(hh) * 60 + int(mm)
    except Exception:
        hh, mm = default.split(":", 1)
        return int(hh) * 60 + int(mm)


def _now_la():
    if LA_TZ:
        return datetime.now(LA_TZ)
    return datetime.now()


def _in_active_window():
    if IGNORE_WINDOW:
        return True, _now_la().strftime("%H:%M")

    now = _now_la()
    now_min = now.hour * 60 + now.minute
    start_min = _hhmm_to_minutes(START_LA, "06:00")
    end_min = _hhmm_to_minutes(END_LA, "17:00")
    if start_min <= end_min:
        ok = start_min <= now_min <= end_min
    else:
        ok = now_min >= start_min or now_min <= end_min
    return ok, now.strftime("%H:%M")


def _ensure_columns(conn):
    wanted = {
        "intraday_volume": "BIGINT NULL",
    }
    sql = """
    SELECT COLUMN_NAME
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (DB["database"], OPS_TABLE))
        existing = {str(r.get("COLUMN_NAME")) for r in (cur.fetchall() or [])}

    missing = [(name, ddl) for name, ddl in wanted.items() if name not in existing]
    if not missing:
        return

    with conn.cursor() as cur:
        for name, ddl in missing:
            cur.execute(f"ALTER TABLE `{OPS_TABLE}` ADD COLUMN `{name}` {ddl};")
            print(f"[SCHEMA] added {OPS_TABLE}.{name}", flush=True)


def _load_symbols(conn):
    stock_types = [x.strip().upper() for x in STOCK_TYPES.split(",") if x.strip()]
    if stock_types and "ALL" not in stock_types:
        placeholders = ",".join(["%s"] * len(stock_types))
        sql = f"""
        SELECT DISTINCT UPPER(stock_code) AS symbol
        FROM `{OPS_TABLE}`
        WHERE stock_code IS NOT NULL
          AND stock_code <> ''
          AND stock_type IN ({placeholders})
        ORDER BY symbol;
        """
        args = tuple(stock_types)
    else:
        sql = f"""
        SELECT DISTINCT UPPER(stock_code) AS symbol
        FROM `{OPS_TABLE}`
        WHERE stock_code IS NOT NULL
          AND stock_code <> ''
        ORDER BY symbol;
        """
        args = ()

    with conn.cursor() as cur:
        cur.execute(sql, args)
        rows = cur.fetchall() or []

    return [str(r.get("symbol") or "").strip().upper() for r in rows if r.get("symbol")]


def _extract_symbol_frame(df: pd.DataFrame, symbol: str):
    if df is None or df.empty:
        return None
    if not isinstance(df.columns, pd.MultiIndex):
        return df

    lvl0 = df.columns.get_level_values(0)
    lvl1 = df.columns.get_level_values(1)
    if symbol in lvl0:
        return df[symbol]
    if symbol in lvl1:
        return df.xs(symbol, axis=1, level=1, drop_level=True)
    return None


def _volume_tuple(symbol: str, subdf: pd.DataFrame):
    if subdf is None or subdf.empty:
        return None
    if "Volume" not in subdf.columns:
        return None

    subdf = subdf.dropna(subset=["Volume"])
    if subdf.empty:
        return None

    subdf = subdf.dropna(how="any")
    if subdf.empty:
        return None

    # Match the old local getdata script: take the latest valid daily bar and
    # use its Volume. During market hours Yahoo's latest 1d bar is the current
    # trading day; after close it is the final daily volume.
    row = subdf.iloc[-1]
    volume = int(row["Volume"]) if pd.notna(row["Volume"]) else 0
    return symbol, volume


def _fetch_yahoo_chart_volume(symbol: str):
    """
    Prefer Yahoo chart meta.regularMarketVolume.

    Summing 1m bars can be lower than broker quote screens because the latest
    partial minute may be missing and Yahoo intraday bars are often delayed.
    The chart meta value is Yahoo's own current cumulative regular-market
    volume, so it is usually closer to what quote panels show.
    """
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {
        "range": PERIOD,
        "interval": INTERVAL,
        "includePrePost": "false",
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }
    try:
        r = requests.get(url, params=params, headers=headers, timeout=YAHOO_TIMEOUT)
        if r.status_code != 200:
            return None
        js = r.json()
        result = ((js.get("chart") or {}).get("result") or [None])[0]
        if not result:
            return None

        meta = result.get("meta") or {}
        meta_volume = meta.get("regularMarketVolume")
        if meta_volume is not None:
            return symbol, int(float(meta_volume))

        quote = ((result.get("indicators") or {}).get("quote") or [None])[0] or {}
        volumes = quote.get("volume") or []
        volume = int(sum(int(float(v or 0)) for v in volumes))
        return (symbol, volume) if volume > 0 else None
    except Exception:
        return None


def _download_batch(symbols):
    try:
        return yf.download(
            tickers=" ".join(symbols),
            period=PERIOD,
            interval=INTERVAL,
            group_by="ticker",
            auto_adjust=False,
            threads=True,
            progress=False,
        )
    except Exception as e:
        print(f"[YF] batch failed n={len(symbols)} err={e}", flush=True)
        return None


def _fetch_batch(symbols):
    rows = []
    failed = []

    if SOURCE == "chart_meta":
        fallback_symbols = []
        for symbol in symbols:
            item = _fetch_yahoo_chart_volume(symbol)
            if item:
                rows.append(item)
            else:
                fallback_symbols.append(symbol)
    else:
        fallback_symbols = symbols

    if not fallback_symbols:
        return rows, failed

    df = _download_batch(fallback_symbols)
    for symbol in fallback_symbols:
        try:
            subdf = _extract_symbol_frame(df, symbol)
            item = _volume_tuple(symbol, subdf)
            if item:
                rows.append(item)
            else:
                failed.append(symbol)
        except Exception:
            failed.append(symbol)
    return rows, failed


def _update_ops(conn, rows):
    if not rows:
        return 0
    sql = f"""
    UPDATE `{OPS_TABLE}`
    SET intraday_volume=%s
    WHERE stock_code=%s;
    """
    args = [(volume, symbol) for symbol, volume in rows]
    with conn.cursor() as cur:
        return cur.executemany(sql, args)


def sync_once():
    conn = _connect()
    try:
        _ensure_columns(conn)
        symbols = _load_symbols(conn)
        print(
            f"[OPS VOL] symbols={len(symbols)} types={STOCK_TYPES or 'ALL'} "
            f"period={PERIOD} interval={INTERVAL}",
            flush=True,
        )
        if not symbols:
            return 0, 0

        total_rows = 0
        failed_all = []
        for batch_no, batch in enumerate(_chunked(symbols, BATCH_SIZE), start=1):
            t0 = time.perf_counter()
            rows, failed = _fetch_batch(batch)
            affected = _update_ops(conn, rows)
            total_rows += len(rows)
            failed_all.extend(failed)
            print(
                f"[OPS VOL] batch={batch_no} requested={len(batch)} "
                f"fetched={len(rows)} updated={affected} failed={len(failed)} "
                f"elapsed={time.perf_counter() - t0:.1f}s",
                flush=True,
            )

        if failed_all:
            print(f"[OPS VOL] failed={','.join(sorted(set(failed_all))[:50])}", flush=True)
        print(f"[OPS VOL] done updated_symbols={total_rows} failed={len(set(failed_all))}", flush=True)
        return total_rows, len(set(failed_all))
    finally:
        conn.close()


def main():
    while True:
        in_window, now_hhmm = _in_active_window()
        if not in_window:
            print(
                f"[OPS VOL] outside window now={now_hhmm} "
                f"window={START_LA}-{END_LA} tz={LA_TZ_NAME}; sleep {SLEEP_SECONDS:.0f}s",
                flush=True,
            )
            if RUN_ONCE:
                break
            time.sleep(SLEEP_SECONDS)
            continue

        try:
            sync_once()
        except Exception as e:
            print(f"[OPS VOL] error: {e}", flush=True)

        if RUN_ONCE:
            break
        print(f"[OPS VOL] sleep {SLEEP_SECONDS:.0f}s", flush=True)
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()

# nohup env \
# DB_HOST=138.197.75.51 \
# DB_PORT=3307 \
# DB_USER=tradebot \
# DB_PASS='TradeBot#2026!' \
# DB_NAME=cszy2000 \
# OPS_VOLUME_STOCK_TYPES=A,B,F \
# OPS_VOLUME_PERIOD=7d \
# OPS_VOLUME_INTERVAL=1d \
# OPS_VOLUME_START_LA=06:00 \
# OPS_VOLUME_END_LA=17:00 \
# OPS_VOLUME_SLEEP_SECONDS=30 \
# .venv/bin/python app/sync_ops_intraday_volume.py \
# > logs/ops_volume_local.log 2>&1 &


# tail -f logs/ops_volume_local.log

# pkill -f sync_ops_intraday_volume.py
