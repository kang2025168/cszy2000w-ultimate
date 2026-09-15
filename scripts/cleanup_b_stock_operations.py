# -*- coding: utf-8 -*-
"""
Archive and delete stale Strategy B rows from stock_operations.

Safe defaults:
- Never touch bought rows.
- Never touch current buy/sell queue rows.
- Only delete disabled, unbought B rows whose original signal date is old.
- Keep rows with recent order history for cooldown/audit.
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta

import pymysql


DB = dict(
    host=os.getenv("DB_HOST", "mysql"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "tradebot"),
    password=os.getenv("DB_PASS", ""),
    database=os.getenv("DB_NAME", "cszy2000"),
    charset="utf8mb4",
    autocommit=False,
    cursorclass=pymysql.cursors.DictCursor,
)

OPS_TABLE = os.getenv("OPS_TABLE", "stock_operations")
ARCHIVE_TABLE = os.getenv("B_CLEANUP_ARCHIVE_TABLE", "stock_operations_b_cleanup_archive")
KEEP_SIGNAL_DAYS = int(os.getenv("B_CLEANUP_KEEP_SIGNAL_DAYS", "14"))
KEEP_ORDER_DAYS = int(os.getenv("B_CLEANUP_KEEP_ORDER_DAYS", "45"))
LIMIT = int(os.getenv("B_CLEANUP_LIMIT", "1000"))


def _connect():
    return pymysql.connect(**DB)


def _source_columns(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(f"SHOW COLUMNS FROM `{OPS_TABLE}`")
        return [str(row["Field"]) for row in cur.fetchall()]


def _column_exists(conn, table: str, column: str) -> bool:
    sql = """
    SELECT COUNT(*) AS n
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (DB["database"], table, column))
        row = cur.fetchone() or {}
    return int(row.get("n") or 0) > 0


def _ensure_archive_table(conn) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{ARCHIVE_TABLE}` AS
    SELECT s.*, CURRENT_TIMESTAMP AS archived_at, CAST('' AS CHAR(160)) AS cleanup_reason
    FROM `{OPS_TABLE}` s
    WHERE 1=0;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        if not _column_exists(conn, ARCHIVE_TABLE, "archived_at"):
            cur.execute(f"ALTER TABLE `{ARCHIVE_TABLE}` ADD COLUMN archived_at DATETIME NULL")
        if not _column_exists(conn, ARCHIVE_TABLE, "cleanup_reason"):
            cur.execute(f"ALTER TABLE `{ARCHIVE_TABLE}` ADD COLUMN cleanup_reason VARCHAR(160) NULL")


def _cutoffs(args) -> tuple[str, str]:
    now = datetime.now()
    signal_cutoff = (now - timedelta(days=args.keep_signal_days)).date().isoformat()
    order_cutoff = (now - timedelta(days=args.keep_order_days)).strftime("%Y-%m-%d %H:%M:%S")
    return signal_cutoff, order_cutoff


def _candidate_rows(conn, args) -> list[dict]:
    signal_cutoff, order_cutoff = _cutoffs(args)
    limit_sql = "" if args.limit <= 0 else "LIMIT %s"
    params: list = [signal_cutoff, order_cutoff]
    if args.limit > 0:
        params.append(args.limit)
    sql = f"""
    SELECT id, stock_code, entry_date, updated_at, last_order_side, last_order_time, last_order_intent
    FROM `{OPS_TABLE}`
    WHERE stock_type='B'
      AND is_bought=0
      AND can_buy=0
      AND can_sell=0
      AND COALESCE(entry_date, DATE(created_at), DATE(updated_at), '1900-01-01') < %s
      AND (last_order_time IS NULL OR last_order_time < %s)
    ORDER BY COALESCE(entry_date, DATE(created_at), DATE(updated_at), '1900-01-01') ASC, id ASC
    {limit_sql};
    """
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        return cur.fetchall() or []


def _archive_and_delete(conn, rows: list[dict], reason: str) -> int:
    if not rows:
        return 0
    _ensure_archive_table(conn)
    ids = [int(row["id"]) for row in rows]
    placeholders = ",".join(["%s"] * len(ids))
    columns = _source_columns(conn)
    src_cols = ", ".join(f"`{col}`" for col in columns)
    dst_cols = ", ".join([*(f"`{col}`" for col in columns), "`archived_at`", "`cleanup_reason`"])
    archive_sql = f"""
    INSERT INTO `{ARCHIVE_TABLE}` ({dst_cols})
    SELECT {src_cols}, CURRENT_TIMESTAMP, %s
    FROM `{OPS_TABLE}`
    WHERE id IN ({placeholders});
    """
    delete_sql = f"DELETE FROM `{OPS_TABLE}` WHERE id IN ({placeholders});"
    with conn.cursor() as cur:
        cur.execute(archive_sql, tuple([reason] + ids))
        cur.execute(delete_sql, tuple(ids))
        return int(cur.rowcount or 0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Cleanup stale disabled Strategy B stock_operations rows.")
    parser.add_argument("--keep-signal-days", type=int, default=KEEP_SIGNAL_DAYS)
    parser.add_argument("--keep-order-days", type=int, default=KEEP_ORDER_DAYS)
    parser.add_argument("--limit", type=int, default=LIMIT, help="Max rows to delete per run. 0 means no limit.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    signal_cutoff, order_cutoff = _cutoffs(args)
    reason = f"B stale disabled candidate; entry_date<{signal_cutoff}; last_order_time<{order_cutoff}"

    conn = _connect()
    try:
        rows = _candidate_rows(conn, args)
        print(
            f"[B CLEANUP] candidates={len(rows)} keep_signal_days={args.keep_signal_days} "
            f"keep_order_days={args.keep_order_days} limit={args.limit} dry_run={args.dry_run}",
            flush=True,
        )
        for row in rows[:30]:
            print(
                f"[B CLEANUP] {row.get('stock_code')} entry={row.get('entry_date')} "
                f"last_order={row.get('last_order_side')} {row.get('last_order_time')} "
                f"intent={row.get('last_order_intent')}",
                flush=True,
            )
        if args.dry_run:
            conn.rollback()
            return
        deleted = _archive_and_delete(conn, rows, reason)
        conn.commit()
        print(f"[B CLEANUP] archived_deleted={deleted} archive_table={ARCHIVE_TABLE}", flush=True)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
