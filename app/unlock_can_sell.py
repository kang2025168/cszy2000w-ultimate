import pymysql
from datetime import datetime

MYSQL_CFG = dict(
    host="mysql",          # ⚠️ 容器里必须是 mysql
    user="tradebot",
    password="TradeBot#2026!",
    database="cszy2000",
)

SQL_UNLOCK = """
UPDATE stock_operations
SET can_sell = 1
WHERE can_sell = 0
  AND is_bought = 1
  AND DATE(updated_at) <= (CURDATE() - INTERVAL 1 DAY);
"""

def main():
    conn = pymysql.connect(**MYSQL_CFG)
    cursor = conn.cursor()
    try:
        cursor.execute(SQL_UNLOCK)
        affected = cursor.rowcount
        conn.commit()
        print(f"[UNLOCK] {datetime.now()} 跨天解锁 can_sell 行数 = {affected}")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] 解锁 can_sell 失败: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()