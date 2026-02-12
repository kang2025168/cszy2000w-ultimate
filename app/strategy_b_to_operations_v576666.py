# -*- coding: utf-8 -*-
"""
策略B：把入池股票写入 stock_operations（极简版）

写入字段：
- stock_code
- trigger_price (= pressure_price)
- created_at (= pressure_date 当天 00:00:00)
- stock_type = 'B'
- is_bought = 0

保护：
- 如果已存在 stock_type='A' 的记录，不更新（A优先）
"""

import os
import pymysql

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

LOW_PCT = float(os.getenv("B_LOW_PCT", "0.90"))
HIGH_PCT = float(os.getenv("B_HIGH_PCT", "1.30"))


def main():
    conn = pymysql.connect(**DB)
    with conn.cursor() as cur:
        # as_of = 行情表最大日期（你库里拉到哪天就用哪天）
        cur.execute(f"SELECT MAX(DATE(`date`)) FROM `{SRC_TABLE}`;")
        as_of = cur.fetchone()[0]
        if not as_of:
            raise RuntimeError(f"{SRC_TABLE} 没数据")

        print(f"[INFO] as_of_date={as_of}  range=[{LOW_PCT},{HIGH_PCT}]")

        # MySQL 5.7 兼容：用 join (symbol, max(date)) 取最新close
        sql = f"""
        INSERT INTO `{OPS_TABLE}` (
          stock_code, trigger_price, stock_type, is_bought, created_at
        )
        SELECT
          lv.symbol AS stock_code,
          ROUND(lv.pressure_price, 2) AS trigger_price,
          'B' AS stock_type,
          0 AS is_bought,
          CONCAT(lv.pressure_date, ' 00:00:00') AS created_at
        FROM `{LEVELS_TABLE}` lv
        JOIN (
          SELECT p.symbol,
                 DATE(p.`date`) AS last_date,
                 p.`close` AS last_close
          FROM `{SRC_TABLE}` p
          JOIN (
            SELECT symbol, MAX(DATE(`date`)) AS last_date
            FROM `{SRC_TABLE}`
            WHERE DATE(`date`) <= DATE(%s)
            GROUP BY symbol
          ) t
            ON p.symbol = t.symbol AND DATE(p.`date`) = t.last_date
          WHERE p.`close` IS NOT NULL
        ) lb
          ON lv.symbol = lb.symbol
        WHERE lb.last_close BETWEEN (lv.pressure_price * {LOW_PCT})
                               AND (lv.pressure_price * {HIGH_PCT})
        ON DUPLICATE KEY UPDATE
          -- A 优先：如果已是 A，就完全不动
          trigger_price = IF(stock_type='A', trigger_price, VALUES(trigger_price)),
          created_at    = IF(stock_type='A', created_at,    VALUES(created_at)),
          stock_type    = IF(stock_type='A', stock_type,    VALUES(stock_type)),
          is_bought     = IF(stock_type='A', is_bought,     VALUES(is_bought));
        """

        affected = cur.execute(sql, (as_of,))
        print(f"[OK] affected rows={affected}  (含插入+更新)")

    conn.close()


if __name__ == "__main__":
    main()