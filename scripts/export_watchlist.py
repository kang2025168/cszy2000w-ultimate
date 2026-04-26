# -*- coding: utf-8 -*-
"""
导出股票自选列表 -> Webull 可导入格式
生成两个文件：
  - watchlist_buy_queue.txt  待买入队列（can_buy=1 且未买入）
  - watchlist_holdings.txt   当前持仓（is_bought=1）
  - watchlist_all.txt        两个合并（去重）
"""

import os
import pymysql
from datetime import datetime

DB = dict(
    host=os.getenv("DB_HOST", "mysql"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "tradebot"),
    password=os.getenv("DB_PASS", "TradeBot#2026!"),
    database=os.getenv("DB_NAME", "cszy2000"),
    charset="utf8mb4",
    autocommit=True,
    cursorclass=pymysql.cursors.DictCursor,
)

OPS_TABLE = os.getenv("OPS_TABLE", "stock_operations")
OUT_DIR   = os.getenv("OUT_DIR", ".")


def fetch_buy_queue(conn):
    """待买入队列：can_buy=1 且未买入"""
    sql = f"""
    SELECT stock_code
    FROM `{OPS_TABLE}`
    WHERE can_buy = 1
      AND (is_bought IS NULL OR is_bought <> 1)
      AND stock_type IN ('A','B','C','D','E')
    ORDER BY stock_code;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return [r["stock_code"].strip().upper() for r in cur.fetchall()]


def fetch_holdings(conn):
    """当前持仓：is_bought=1"""
    sql = f"""
    SELECT stock_code
    FROM `{OPS_TABLE}`
    WHERE is_bought = 1
      AND stock_type IN ('A','B','C','D','E')
    ORDER BY stock_code;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return [r["stock_code"].strip().upper() for r in cur.fetchall()]


def write_file(path, symbols, label):
    with open(path, "w", encoding="utf-8") as f:
        for sym in symbols:
            f.write(sym + "\n")
    print(f"[OK] {label}: {len(symbols)} 只 -> {path}")


def main():
    conn = pymysql.connect(**DB)
    try:
        buy_queue = fetch_buy_queue(conn)
        holdings  = fetch_holdings(conn)
        all_syms  = sorted(set(buy_queue + holdings))

        ts = datetime.now().strftime("%Y%m%d")

        write_file(
            os.path.join(OUT_DIR, f"watchlist_buy_queue_{ts}.txt"),
            buy_queue,
            "待买入队列"
        )
        write_file(
            os.path.join(OUT_DIR, f"watchlist_holdings_{ts}.txt"),
            holdings,
            "当前持仓"
        )
        write_file(
            os.path.join(OUT_DIR, f"watchlist_all_{ts}.txt"),
            all_syms,
            "全部合并"
        )

        print(f"\n共导出 {len(all_syms)} 只股票（去重后）")
        print("导入方法：Webull -> 自选 -> 右上角菜单 -> 导入")

    finally:
        conn.close()


if __name__ == "__main__":
    main()

# DB_HOST=138.197.75.51 DB_PORT=3307 python scripts/export_watchlist.py
