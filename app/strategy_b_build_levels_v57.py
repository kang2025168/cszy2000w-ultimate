# -*- coding: utf-8 -*-
"""
[MARK] strategy_b_build_levels_v57
策略B Step1：写 strategy_b_levels（MySQL 5.7 兼容）
"""

import os
import pymysql
import pandas as pd

DB = dict(
    host=os.getenv("DB_HOST", "mysql"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASS", "mlp009988"),
    database=os.getenv("DB_NAME", "cszy2000"),
    charset="utf8mb4",
    autocommit=True,
)

SRC_TABLE = "stock_prices_pool"
DST_TABLE = "strategy_b_levels"
LOOKBACK_DAYS = 180


def main():
    print("[MARK] running v57 file OK")

    conn = pymysql.connect(**DB)

    latest_day = pd.read_sql(
        f"SELECT MAX(DATE(`date`)) AS d FROM {SRC_TABLE};", conn
    )["d"].iloc[0]

    if pd.isna(latest_day):
        raise RuntimeError("stock_prices_pool 里没有数据")

    print("[INFO] 最新交易日:", latest_day)

    pick_sql = f"""
    SELECT
      p.symbol,
      p.`open` AS pressure_price,
      DATE(p.`date`) AS pressure_date
    FROM {SRC_TABLE} p
    WHERE DATE(p.`date`) BETWEEN DATE_SUB(DATE('{latest_day}'), INTERVAL {LOOKBACK_DAYS-1} DAY)
                             AND DATE('{latest_day}')
      AND p.`open` > p.`close`
      AND p.`close` < (
        SELECT pp.`close`
        FROM {SRC_TABLE} pp
        WHERE pp.symbol = p.symbol
          AND DATE(pp.`date`) < DATE(p.`date`)
        ORDER BY DATE(pp.`date`) DESC
        LIMIT 1
      )
      AND p.volume = (
        SELECT MAX(p2.volume)
        FROM {SRC_TABLE} p2
        WHERE p2.symbol = p.symbol
          AND DATE(p2.`date`) BETWEEN DATE_SUB(DATE('{latest_day}'), INTERVAL {LOOKBACK_DAYS-1} DAY)
                                   AND DATE('{latest_day}')
          AND p2.`open` > p2.`close`
      );
    """

    df = pd.read_sql(pick_sql, conn)

    print("[INFO] 命中股票数:", len(df))

    if df.empty:
        print("[WARN] 没有符合条件的数据")
        return

    upsert_sql = f"""
    INSERT INTO {DST_TABLE} (symbol, pressure_price, pressure_date)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
      pressure_price = VALUES(pressure_price),
      pressure_date  = VALUES(pressure_date);
    """

    rows = [(r.symbol, float(r.pressure_price), r.pressure_date) for r in df.itertuples()]

    with conn.cursor() as cur:
        cur.executemany(upsert_sql, rows)
        print("[OK] 写入完成，影响行数:", cur.rowcount)

    preview = pd.read_sql(
        f"SELECT * FROM {DST_TABLE} ORDER BY symbol LIMIT 10;", conn
    )
    print(preview)

    conn.close()


if __name__ == "__main__":
    main()