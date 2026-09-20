# -*- coding: utf-8 -*-
import json
import os
import pymysql
from datetime import date, datetime
from decimal import Decimal

MYSQL_CFG = dict(
    host=os.getenv("DB_HOST", "mysql"),
    user=os.getenv("DB_USER", "tradebot"),
    password=os.getenv("DB_PASS", ""),
    database=os.getenv("DB_NAME", "cszy2000"),
    charset="utf8mb4",
    autocommit=False,
    cursorclass=pymysql.cursors.DictCursor,
)

OPS_TABLE = os.getenv("OPS_TABLE", "stock_operations")
PRICE_TABLE = os.getenv("STOCK_PRICE_TABLE", "stock_prices_pool")
B_ARCHIVE_TABLE = os.getenv("B_ARCHIVE_TABLE", "stock_operations_b_archive")
B_KEEP_TRADING_DAYS = max(1, int(os.getenv("B_KEEP_TRADING_DAYS", "5")))

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

def _json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def _ensure_b_archive_table(cursor) -> None:
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{B_ARCHIVE_TABLE}` (
          archive_id BIGINT AUTO_INCREMENT PRIMARY KEY,
          original_operation_id BIGINT NOT NULL,
          stock_code VARCHAR(64) NOT NULL,
          stock_type VARCHAR(8) NOT NULL DEFAULT 'B',
          entry_date DATE NULL,
          archived_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          archive_reason VARCHAR(255) NOT NULL,
          row_data JSON NOT NULL,
          INDEX idx_b_archive_symbol (stock_code),
          INDEX idx_b_archive_entry_date (entry_date),
          INDEX idx_b_archive_archived_at (archived_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )


def _b_window_start(cursor) -> date | None:
    cursor.execute(
        f"""
        SELECT DISTINCT `date`
        FROM `{PRICE_TABLE}`
        WHERE `date` <= CURDATE()
        ORDER BY `date` DESC
        LIMIT %s
        """,
        (B_KEEP_TRADING_DAYS,),
    )
    days = [row.get("date") for row in cursor.fetchall() if row.get("date")]
    return min(days) if len(days) >= B_KEEP_TRADING_DAYS else None


def _archive_expired_b_candidates(cursor) -> tuple[int, date | None]:
    """归档并删除不在最近 N 个交易日窗口内、且尚未买入的 B 候选。"""
    _ensure_b_archive_table(cursor)
    window_start = _b_window_start(cursor)
    if window_start is None:
        return 0, None

    cursor.execute(
        f"""
        SELECT *
        FROM `{OPS_TABLE}`
        WHERE stock_type='B'
          AND COALESCE(is_bought, 0)=0
          AND COALESCE(entry_date, DATE(created_at)) < %s
          AND (
                last_order_id IS NULL
                OR last_order_time IS NULL
                OR last_order_time < (NOW() - INTERVAL 1 DAY)
              )
        ORDER BY COALESCE(entry_date, DATE(created_at)), id
        FOR UPDATE
        """,
        (window_start,),
    )
    rows = cursor.fetchall() or []
    if not rows:
        return 0, window_start

    reason = f"B候选超过最近{B_KEEP_TRADING_DAYS}个交易日；窗口起点={window_start}"
    archive_values = [
        (
            int(row["id"]),
            str(row.get("stock_code") or "").upper(),
            str(row.get("stock_type") or "B").upper(),
            row.get("entry_date"),
            reason,
            json.dumps(row, ensure_ascii=False, default=_json_default),
        )
        for row in rows
    ]
    cursor.executemany(
        f"""
        INSERT INTO `{B_ARCHIVE_TABLE}` (
            original_operation_id, stock_code, stock_type,
            entry_date, archive_reason, row_data
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """,
        archive_values,
    )

    ids = [int(row["id"]) for row in rows]
    placeholders = ",".join(["%s"] * len(ids))
    cursor.execute(
        f"""
        DELETE FROM `{OPS_TABLE}`
        WHERE id IN ({placeholders})
          AND stock_type='B'
          AND COALESCE(is_bought, 0)=0
        """,
        tuple(ids),
    )
    if cursor.rowcount != len(rows):
        raise RuntimeError(f"B候选归档数量={len(rows)}，删除数量={cursor.rowcount}，事务已回滚")
    return len(rows), window_start

def main():
    conn = pymysql.connect(**MYSQL_CFG)
    cursor = conn.cursor()
    try:
        cursor.execute(SQL_RESET_CAN_BUY)
        n1 = cursor.rowcount

        cursor.execute(SQL_UNLOCK_CAN_SELL)
        n2 = cursor.rowcount

        n3, window_start = _archive_expired_b_candidates(cursor)

        conn.commit()
        print(
            f"[UNLOCK] {datetime.now()} "
            f"重置 can_buy=1 行数={n1} | "
            f"解锁 can_sell=1 行数={n2} | "
            f"归档删除过期B候选={n3} | "
            f"保留最近{B_KEEP_TRADING_DAYS}个交易日（起点={window_start or '--'}）"
        )
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] 解锁失败: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
