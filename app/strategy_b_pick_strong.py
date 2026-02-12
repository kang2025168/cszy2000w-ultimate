# -*- coding: utf-8 -*-
"""
策略B：硬条件过滤后写入 stock_operations（最小字段 + close_price）

筛选条件：
1) 最新收盘价 close > MIN_PRICE（默认 2）
2) 最新成交量 volume > MIN_VOLUME（默认 1,000,000）
3) 连续3天上涨：close[-3] < close[-2] < close[-1]
4) 价格必须在压力位范围：pressure*LOW_PCT <= close <= pressure*HIGH_PCT（默认 0.9~1.3）

写入字段：
- stock_code
- close_price  = 最新收盘价（2位小数）
- trigger_price = 压力位价格（2位小数）
- created_at   = 压力位日期 00:00:00
- is_bought    = 0（但如果已买入=1，不改回0）
- stock_type   = 'B'

保护：
- A 优先：如果已存在 stock_type='A'，不改动
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

SRC_TABLE = os.getenv("SRC_TABLE", "stock_prices_pool")
LEVELS_TABLE = os.getenv("LEVELS_TABLE", "strategy_b_levels")
OPS_TABLE = os.getenv("OPS_TABLE", "stock_operations")

MIN_PRICE = float(os.getenv("B_MIN_PRICE", "2.0"))
MIN_VOLUME = int(float(os.getenv("B_MIN_VOLUME", "1000000")))

LOW_PCT = float(os.getenv("B_LOW_PCT", "0.90"))
HIGH_PCT = float(os.getenv("B_HIGH_PCT", "1.30"))

LOOKBACK_DAYS = int(os.getenv("B_TREND_LOOKBACK_DAYS", "15"))  # 给多点，避免停牌/节假日


def main():
    conn = pymysql.connect(**DB)

    as_of = pd.read_sql(
        f"SELECT MAX(DATE(`date`)) AS d FROM `{SRC_TABLE}`;", conn
    )["d"].iloc[0]
    if pd.isna(as_of):
        raise RuntimeError(f"{SRC_TABLE} 没有数据")

    print(
        f"[INFO] as_of_date={as_of}  MIN_PRICE>{MIN_PRICE}  MIN_VOLUME>{MIN_VOLUME}  "
        f"RANGE=[{LOW_PCT},{HIGH_PCT}]"
    )

    # 拉最近窗口（只拉 levels 里的票）
    hist_sql = f"""
    SELECT
      p.symbol,
      DATE(p.`date`) AS d,
      p.`close`,
      p.`volume`,
      lv.pressure_price,
      lv.pressure_date
    FROM `{SRC_TABLE}` p
    JOIN `{LEVELS_TABLE}` lv
      ON p.symbol = lv.symbol
    WHERE DATE(p.`date`) BETWEEN DATE_SUB(DATE('{as_of}'), INTERVAL {LOOKBACK_DAYS} DAY) AND DATE('{as_of}')
      AND p.`close` IS NOT NULL
      AND p.`volume` IS NOT NULL
    ORDER BY p.symbol, d;
    """
    hist = pd.read_sql(hist_sql, conn)

    if hist.empty:
        print("[WARN] 最近窗口内没有任何数据（或 levels 为空）")
        conn.close()
        return

    hist["close"] = pd.to_numeric(hist["close"], errors="coerce")
    hist["volume"] = pd.to_numeric(hist["volume"], errors="coerce")
    hist["pressure_price"] = pd.to_numeric(hist["pressure_price"], errors="coerce")

    selected = []
    for sym, g in hist.groupby("symbol", sort=False):
        g = g.dropna(subset=["close", "volume", "pressure_price"]).sort_values("d")
        if len(g) < 3:
            continue

        last3 = g.tail(3)
        c1, c2, c3 = last3["close"].iloc[0], last3["close"].iloc[1], last3["close"].iloc[2]
        last_close = float(c3)
        last_vol = float(last3["volume"].iloc[2])

        # 3连涨
        if not (c1 < c2 < c3):
            continue

        # 价/量阈值
        if not (last_close > MIN_PRICE and last_vol > MIN_VOLUME):
            continue

        # 压力位范围
        pressure_price = float(g["pressure_price"].iloc[-1])
        if pressure_price <= 0:
            continue

        low_bound = pressure_price * LOW_PCT
        high_bound = pressure_price * HIGH_PCT
        if not (low_bound <= last_close <= high_bound):
            continue

        pressure_date = str(g["pressure_date"].iloc[-1])
        selected.append((sym, pressure_price, pressure_date, last_close))

    print(f"[INFO] 命中股票数: {len(selected)}")
    if not selected:
        conn.close()
        return

    upsert_sql = f"""
    INSERT INTO `{OPS_TABLE}` (stock_code, close_price, trigger_price, created_at, is_bought, stock_type)
    VALUES (%s, %s, %s, %s, 0, 'B')
    ON DUPLICATE KEY UPDATE
      close_price   = IF(stock_type='A', close_price, VALUES(close_price)),
      trigger_price = IF(stock_type='A', trigger_price, VALUES(trigger_price)),
      created_at    = IF(stock_type='A', created_at, VALUES(created_at)),
      stock_type    = IF(stock_type='A', stock_type, 'B'),
      is_bought     = IF(stock_type='A', is_bought, IF(is_bought=1, 1, 0));
    """

    rows = []
    for sym, pressure_price, pressure_date, last_close in selected:
        close_price = round(float(last_close), 2)
        trigger_price = round(float(pressure_price), 2)
        created_at = f"{pressure_date} 00:00:00"
        rows.append((sym, close_price, trigger_price, created_at))

    with conn.cursor() as cur:
        cur.executemany(upsert_sql, rows)

    print(f"[OK] 写入 stock_operations：{len(rows)} 只 (stock_type='B')")

    conn.close()


if __name__ == "__main__":
    main()