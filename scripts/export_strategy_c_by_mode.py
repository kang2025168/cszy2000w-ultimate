# -*- coding: utf-8 -*-
"""
按策略 C 的四种期权模式导出股票代码。

来源表：
    strategy_c_candidates

输出文件：
    strategy_c_BULL_CALL_YYYYMMDD.txt
    strategy_c_BEAR_PUT_YYYYMMDD.txt
    strategy_c_BULL_PUT_YYYYMMDD.txt
    strategy_c_BEAR_CALL_YYYYMMDD.txt
    strategy_c_watchlist_YYYYMMDD.txt

每个文件每行只有一个股票代码，方便导入自选列表。

示例：
    DB_HOST=138.197.75.51 DB_PORT=3307 python scripts/export_strategy_c_by_mode.py
"""

from __future__ import annotations

import os
from datetime import datetime

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

CANDIDATES_TABLE = os.getenv("C_CANDIDATES_TABLE", "strategy_c_candidates")
OUT_DIR = os.getenv("OUT_DIR", "data")
AS_OF = os.getenv("C_EXPORT_AS_OF", "").strip()
MIN_SCORE = float(os.getenv("C_EXPORT_MIN_SCORE", "0"))

MODES = ("BULL_CALL", "BEAR_PUT", "BULL_PUT", "BEAR_CALL")


def _connect():
    return pymysql.connect(**DB)


def _fetch_as_of(conn):
    if AS_OF:
        return AS_OF
    sql = f"SELECT MAX(as_of) AS as_of FROM `{CANDIDATES_TABLE}`;"
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone() or {}
    if not row.get("as_of"):
        raise RuntimeError(f"{CANDIDATES_TABLE} has no data")
    return str(row["as_of"])


def _fetch_symbols(conn, as_of: str, mode: str):
    sql = f"""
    SELECT symbol
    FROM `{CANDIDATES_TABLE}`
    WHERE as_of=%s
      AND option_mode=%s
      AND score >= %s
    ORDER BY score DESC, symbol ASC;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (as_of, mode, float(MIN_SCORE)))
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
        as_of = _fetch_as_of(conn)
        date_tag = str(as_of).replace("-", "")
        all_symbols = []

        print(
            f"[INFO] table={CANDIDATES_TABLE} as_of={as_of} min_score={MIN_SCORE} out_dir={OUT_DIR}",
            flush=True,
        )

        for mode in MODES:
            symbols = _fetch_symbols(conn, as_of, mode)
            all_symbols.extend(symbols)
            path = os.path.join(OUT_DIR, f"strategy_c_{mode}_{date_tag}.txt")
            _write_symbols(path, symbols)
            print(f"[OK] {mode}: {len(symbols)} -> {path}", flush=True)

        merged = []
        seen = set()
        for sym in all_symbols:
            if sym not in seen:
                seen.add(sym)
                merged.append(sym)

        merged_path = os.path.join(OUT_DIR, f"strategy_c_watchlist_{date_tag}.txt")
        _write_symbols(merged_path, merged)
        latest_path = os.path.join(OUT_DIR, "strategy_c_watchlist.txt")
        _write_symbols(latest_path, merged)

        print(f"[OK] ALL: {len(merged)} -> {merged_path}", flush=True)
        print(f"[OK] latest copy -> {latest_path}", flush=True)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
