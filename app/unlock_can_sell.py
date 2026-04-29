# -*- coding: utf-8 -*-
import os
import pymysql
from datetime import datetime

MYSQL_CFG = dict(
    host=os.getenv("DB_HOST", "mysql"),
    user=os.getenv("DB_USER", "tradebot"),
    password=os.getenv("DB_PASS", "TradeBot#2026!"),
    database=os.getenv("DB_NAME", "cszy2000"),
)

# 卖出后第二天重置 can_buy=1，允许重新买入
SQL_RESET_CAN_BUY = """
UPDATE stock_operations
SET can_buy = 1
WHERE is_bought = 0
  AND can_buy = 0
  AND stock_type IN ('A','B','C','D','E')
  AND last_order_side = 'sell'
  AND DATE(last_order_time) <= (CURDATE() - INTERVAL 1 DAY);
"""

# 买入后第二天解锁 can_sell=1，允许卖出
SQL_UNLOCK_CAN_SELL = """
UPDATE stock_operations
SET can_sell = 1
WHERE is_bought = 1
  AND can_sell = 0
  AND stock_type IN ('A','B','C','D','E')
  AND last_order_side = 'buy'
  AND DATE(last_order_time) <= (CURDATE() - INTERVAL 1 DAY);
"""

# 删除 entry_date 为 4 天前及更早、且仍未买入的 B 池股票
SQL_DELETE_OLD_UNBOUGHT_B = """
DELETE FROM stock_operations
WHERE is_bought = 0
  AND stock_type = 'B'
  AND entry_date IS NOT NULL
  AND DATE(entry_date) <= (CURDATE() - INTERVAL 4 DAY);
"""

def main():
    conn = pymysql.connect(**MYSQL_CFG)
    cursor = conn.cursor()
    try:
        cursor.execute(SQL_RESET_CAN_BUY)
        n1 = cursor.rowcount

        cursor.execute(SQL_UNLOCK_CAN_SELL)
        n2 = cursor.rowcount

        cursor.execute(SQL_DELETE_OLD_UNBOUGHT_B)
        n3 = cursor.rowcount

        conn.commit()
        print(
            f"[UNLOCK] {datetime.now()} "
            f"重置 can_buy=1 行数={n1} | "
            f"解锁 can_sell=1 行数={n2} | "
            f"删除 entry_date 4天前未买入B票行数={n3}"
        )
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] 解锁失败: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()