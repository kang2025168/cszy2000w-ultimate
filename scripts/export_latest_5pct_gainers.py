# -*- coding: utf-8 -*-
"""
导出最新交易日涨幅 >= 5% 的股票列表。

规则：
    使用 stock_prices_pool 最新交易日数据，
    计算 (close - open) / open >= 阈值。

输出：
    data/latest_5pct_gainers_YYYYMMDD.txt
    data/latest_5pct_gainers.txt

每行只有一个股票代码，方便导入自选列表。

示例：
    DB_HOST=138.197.75.51 DB_PORT=3307 DB_USER=tradebot DB_PASS='***' DB_NAME=cszy2000 \
    .venv/bin/python scripts/export_latest_5pct_gainers.py
"""

from __future__ import annotations

import os

import pymysql


DB = dict(
    host=os.getenv("DB_HOST", "138.197.75.51"),
    port=int(os.getenv("DB_PORT", "3307")),
    user=os.getenv("DB_USER", "tradebot"),
    password=os.getenv("DB_PASS", ""),
    database=os.getenv("DB_NAME", "cszy2000"),
    charset="utf8mb4",
    autocommit=True,
    cursorclass=pymysql.cursors.DictCursor,
)

SRC_TABLE = os.getenv("SRC_TABLE", "stock_prices_pool")
OUT_DIR = os.getenv("OUT_DIR", "data")
MIN_UP_PCT = float(os.getenv("GAINERS_MIN_UP_PCT", "0.05"))


def _connect():
    return pymysql.connect(**DB)


def _fetch_latest_day(conn):
    sql = f"SELECT MAX(DATE(`date`)) AS d FROM `{SRC_TABLE}`;"
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone() or {}
    if not row.get("d"):
        raise RuntimeError(f"{SRC_TABLE} has no data")
    return str(row["d"])


def _fetch_symbols(conn, latest_day: str):
    sql = f"""
    SELECT symbol
    FROM `{SRC_TABLE}`
    WHERE DATE(`date`) = DATE(%s)
      AND `open` > 0
      AND (`close` - `open`) / `open` >= %s
    ORDER BY ((`close` - `open`) / `open`) DESC, symbol ASC;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (latest_day, float(MIN_UP_PCT)))
        rows = cur.fetchall() or []

    out = []
    seen = set()
    for r in rows:
        sym = str(r.get("symbol") or "").strip().upper()
        if sym and sym not in seen:
            seen.add(sym)
            out.append(sym)
    return out


def _write_symbols(path: str, symbols: list[str]):
    with open(path, "w", encoding="utf-8") as f:
        for sym in symbols:
            f.write(sym + "\n")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    conn = _connect()
    try:
        latest_day = _fetch_latest_day(conn)
        symbols = _fetch_symbols(conn, latest_day)
    finally:
        conn.close()

    tag = latest_day.replace("-", "")
    dated_path = os.path.join(OUT_DIR, f"latest_5pct_gainers_{tag}.txt")
    latest_path = os.path.join(OUT_DIR, "latest_5pct_gainers.txt")

    _write_symbols(dated_path, symbols)
    _write_symbols(latest_path, symbols)

    print(
        f"[OK] latest_day={latest_day} min_up={MIN_UP_PCT * 100:.2f}% "
        f"symbols={len(symbols)} -> {dated_path}",
        flush=True,
    )
    print(f"[OK] latest copy -> {latest_path}", flush=True)


if __name__ == "__main__":
    main()
