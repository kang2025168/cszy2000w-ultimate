from __future__ import annotations

"""从 Alpaca 同步真实持仓。

position_holdings 负责网页展示和复盘。
stock_operations 负责老策略买卖控制。
"""

from datetime import datetime
from typing import Any

from . import alpaca_gateway
from .config import settings
from .db import db_conn
from .position_holdings import mark_missing_from_alpaca, summary_counts, sync_open_holding_from_position
from .schema import ensure_schema

LAST_SYNC_ERROR = ""
VALID_GROUPS = {"A", "B", "C", "D"}


def last_sync_error() -> str:
    """返回最近一次同步失败原因，供网页接口展示。"""
    return LAST_SYNC_ERROR


def _as_float(value: Any, default: float = 0.0) -> float:
    """把 Alpaca 返回的字符串/Decimal 安全转成 float。"""
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _position_symbol(pos: Any) -> str:
    """统一清洗 Alpaca symbol。"""
    return str(getattr(pos, "symbol", "") or "").strip().upper()


def _position_qty(pos: Any) -> float:
    """读取真实数量，支持碎股。"""
    return abs(_as_float(getattr(pos, "qty", 0), 0.0))


def _position_avg(pos: Any) -> float:
    """读取平均成本。"""
    return _as_float(getattr(pos, "avg_entry_price", 0), 0.0)


def _position_current(pos: Any) -> float:
    """读取当前价；没有 current_price 时用市值/数量反推。"""
    current = _as_float(getattr(pos, "current_price", 0), 0.0)
    if current > 0:
        return current
    qty = _position_qty(pos)
    market_value = _as_float(getattr(pos, "market_value", 0), 0.0)
    if qty > 0 and market_value > 0:
        return market_value / qty
    return 0.0


def _table_columns(conn, table: str) -> dict[str, str]:
    """读取表字段和类型，后面动态拼 SQL，兼容旧表结构。"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
            """,
            (table,),
        )
        return {str(row["COLUMN_NAME"]): str(row["DATA_TYPE"]).lower() for row in cur.fetchall()}


def _has_single_symbol_unique_key(conn, table: str) -> bool:
    """判断旧表是否仍然是 stock_code 单字段唯一。

    这种表不能同时保存 QQQ/N 和 QQQ/C，遇到非 A/B/C/D 控制行时要保护它。
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT INDEX_NAME, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS cols
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
              AND NON_UNIQUE = 0
            GROUP BY INDEX_NAME
            """,
            (table,),
        )
        for row in cur.fetchall():
            cols = str(row.get("cols") or "").lower()
            if cols == "stock_code":
                return True
    return False


def _normalize_group(value: Any, default: str = "B") -> str:
    """只接受 A/B/C/D，识别不到就给默认 B。"""
    group = str(value or "").strip().upper()
    return group if group in VALID_GROUPS else default


def _resolve_strategy_group(conn, ops_table: str, symbol: str) -> str:
    """同步到 stock_operations 时保留人工维护过的股票类型。

    优先级：
    1. stock_operations 里已有 A/B/C/D 类型；
    2. position_holdings 里已有 A/B/C/D 类型；
    3. 新券商持仓默认归 B。
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT stock_type, strategy_group
            FROM `{ops_table}`
            WHERE stock_code=%s
              AND (stock_type IN ('A','B','C','D') OR strategy_group IN ('A','B','C','D'))
            ORDER BY is_bought DESC,
                     FIELD(COALESCE(NULLIF(stock_type,''), strategy_group), 'A','C','B','D'),
                     stock_code
            LIMIT 1
            """,
            (symbol,),
        )
        row = cur.fetchone()
        if row:
            return _normalize_group(row.get("stock_type") or row.get("strategy_group"))

        cur.execute(
            """
            SELECT strategy_group, stock_type
            FROM position_holdings
            WHERE symbol=%s
              AND (strategy_group IN ('A','B','C','D') OR stock_type IN ('A','B','C','D'))
            ORDER BY FIELD(status, 'open', 'needs_review', 'closed'),
                     FIELD(COALESCE(NULLIF(stock_type,''), strategy_group), 'A','C','B','D'),
                     id DESC
            LIMIT 1
            """,
            (symbol,),
        )
        row = cur.fetchone()
        if row:
            return _normalize_group(row.get("stock_type") or row.get("strategy_group"))

    return "B"


def _stock_operation_qty_value(qty: float, column_type: str | None) -> float | int:
    """按 stock_operations.qty 的字段类型写入。

    旧表如果是 INT，只能保存整数股；新表如果是 DECIMAL/FLOAT，则保留碎股。
    """
    if (column_type or "").lower() in {"tinyint", "smallint", "mediumint", "int", "integer", "bigint"}:
        return int(qty)
    return qty


def _update_ops_row(conn, table: str, columns: dict[str, str], row: dict[str, Any], values: dict[str, Any]) -> None:
    """按存在的字段动态更新 stock_operations。"""
    pairs = []
    args = []
    for key, value in values.items():
        if key not in columns:
            continue
        pairs.append(f"`{key}`=%s")
        args.append(value)
    if "updated_at" in columns:
        pairs.append("`updated_at`=CURRENT_TIMESTAMP")
    if not pairs:
        return
    if "id" in columns and row.get("id") is not None:
        where_sql = "id=%s"
        args.append(row["id"])
    else:
        # 有些老表没有 id，stock_code 是主键。
        where_sql = "stock_code=%s"
        args.append(row["stock_code"])
    with conn.cursor() as cur:
        cur.execute(f"UPDATE `{table}` SET {', '.join(pairs)} WHERE {where_sql}", tuple(args))


def _insert_ops_row(conn, table: str, columns: dict[str, str], values: dict[str, Any]) -> None:
    """按存在的字段动态插入 stock_operations。"""
    keys = [key for key in values if key in columns]
    if not keys:
        return
    placeholders = ", ".join(["%s"] * len(keys))
    fields = ", ".join(f"`{key}`" for key in keys)
    args = tuple(values[key] for key in keys)
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO `{table}` ({fields}) VALUES ({placeholders})", args)


def _sync_stock_operations_from_positions(positions: list[Any]) -> dict[str, int]:
    """把券商真实持仓同步到 stock_operations，供买卖机器人使用。"""
    s = settings()
    table = s.ops_table
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stats = {
        "held": 0,
        "created": 0,
        "updated": 0,
        "flat": 0,
        "default_b": 0,
        "skipped_fractional_int": 0,
        "skipped_symbol_conflict": 0,
    }
    alpaca_symbols: set[str] = set()

    with db_conn(s) as conn:
        columns = _table_columns(conn, table)
        if "stock_code" not in columns or "stock_type" not in columns:
            raise RuntimeError(f"{table} 缺少 stock_code/stock_type 字段，无法同步交易控制表")
        single_symbol_unique = _has_single_symbol_unique_key(conn, table)

        for pos in positions:
            symbol = _position_symbol(pos)
            if not symbol:
                continue
            qty = _position_qty(pos)
            if qty <= 0:
                continue

            alpaca_symbols.add(symbol)
            group = _resolve_strategy_group(conn, table, symbol)
            if group == "B":
                stats["default_b"] += 1

            avg = _position_avg(pos)
            current = _position_current(pos)
            qty_value = _stock_operation_qty_value(qty, columns.get("qty"))
            if float(qty_value or 0) <= 0 and qty > 0:
                # 旧 INT 表无法管理碎股卖出，但仍然保留 position_holdings 展示。
                stats["skipped_fractional_int"] += 1

            with conn.cursor() as cur:
                select_fields = "id, stock_code" if "id" in columns else "stock_code"
                order_sql = "is_bought DESC, id DESC" if "id" in columns else "is_bought DESC, stock_code"
                cur.execute(
                    f"""
                    SELECT {select_fields}
                    FROM `{table}`
                    WHERE stock_code=%s AND stock_type=%s
                    ORDER BY {order_sql}
                    LIMIT 1
                    """,
                    (symbol, group),
                )
                existing = cur.fetchone()

                if not existing and single_symbol_unique:
                    cur.execute(
                        f"""
                        SELECT stock_code, stock_type
                        FROM `{table}`
                        WHERE stock_code=%s
                        LIMIT 1
                        """,
                        (symbol,),
                    )
                    conflict = cur.fetchone()
                    if conflict:
                        stats["skipped_symbol_conflict"] += 1
                        print(
                            f"[OPS SYNC SKIP] symbol={symbol} keep_stock_type={conflict.get('stock_type')} "
                            f"reason=single_stock_code_unique_key",
                            flush=True,
                        )
                        continue

            common_values = {
                "stock_code": symbol,
                "stock_type": group,
                "strategy_group": group,
                "capital_pool": group,
                "margin_used": 1 if group == "D" else 0,
                "qty": qty_value,
                "cost_price": avg,
                "close_price": current,
                "current_price": current,
                "is_bought": 1,
                "can_sell": 1,
                "can_buy": 0,
                "last_order_side": "sync",
                "last_order_intent": f"SYNC_POSITIONS alpaca qty={qty:g}",
                "last_order_time": now_text,
                "last_capital_check_at": now_text,
            }
            if existing:
                _update_ops_row(conn, table, columns, existing, common_values)
                stats["updated"] += 1
            else:
                _insert_ops_row(conn, table, columns, common_values)
                stats["created"] += 1
            stats["held"] += 1

        flat_values = {
            "is_bought": 0,
            "qty": 0,
            "can_sell": 0,
            "last_order_side": "sync_flat",
            "last_order_intent": "SYNC_POSITIONS alpaca_not_held",
            "last_order_time": now_text,
        }
        pairs = []
        args: list[Any] = []
        for key, value in flat_values.items():
            if key in columns:
                pairs.append(f"`{key}`=%s")
                args.append(value)
        if "updated_at" in columns:
            pairs.append("`updated_at`=CURRENT_TIMESTAMP")

        if pairs:
            where_args: list[Any] = []
            if alpaca_symbols:
                placeholders = ", ".join(["%s"] * len(alpaca_symbols))
                where_symbol = f"AND stock_code NOT IN ({placeholders})"
                where_args.extend(sorted(alpaca_symbols))
            else:
                where_symbol = ""
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE `{table}`
                    SET {', '.join(pairs)}
                    WHERE is_bought=1
                      AND stock_type IN ('A','B','C','D')
                      {where_symbol}
                    """,
                    tuple(args + where_args),
                )
                stats["flat"] = int(cur.rowcount or 0)

    print(
        f"[OPS SYNC] held={stats['held']} created={stats['created']} updated={stats['updated']} "
        f"flat={stats['flat']} default_b={stats['default_b']} "
        f"skipped_fractional_int={stats['skipped_fractional_int']} "
        f"skipped_symbol_conflict={stats['skipped_symbol_conflict']}",
        flush=True,
    )
    return stats


def _sync_position_holdings_from_positions(positions: list[Any]) -> dict[str, int]:
    """把券商真实持仓同步到 position_holdings，供网页展示。"""
    symbols = set()
    for pos in positions:
        symbol = _position_symbol(pos)
        if not symbol:
            continue
        symbols.add(symbol)
        sync_open_holding_from_position(pos, "B")
    mark_missing_from_alpaca(symbols)
    counts = summary_counts()
    print(
        f"[HOLDING SYNC] open_count={counts.get('open', 0)} "
        f"closed_count={counts.get('closed', 0)} needs_review={counts.get('needs_review', 0)}",
        flush=True,
    )
    return {
        "open": int(counts.get("open", 0)),
        "closed": int(counts.get("closed", 0)),
        "needs_review": int(counts.get("needs_review", 0)),
    }


def sync_position_holdings() -> bool:
    """同步持仓：Alpaca 有但本地没有就补，本地 open 但 Alpaca 没有就标记复核。"""
    global LAST_SYNC_ERROR
    LAST_SYNC_ERROR = ""
    if not settings().enable_position_holdings:
        print("[POSITION] disabled=1", flush=True)
        return True
    ensure_schema()
    try:
        positions = alpaca_gateway.list_positions()
        _sync_position_holdings_from_positions(positions)
        return True
    except Exception as exc:
        LAST_SYNC_ERROR = str(exc)
        print(f"[POSITION SYNC ERROR] {LAST_SYNC_ERROR}", flush=True)
        return False


def sync_all_positions() -> bool:
    """网页“同步仓位”按钮使用：同时同步展示表和交易控制表。"""
    global LAST_SYNC_ERROR
    LAST_SYNC_ERROR = ""
    if not settings().enable_position_holdings:
        print("[POSITION] disabled=1", flush=True)
        return True
    ensure_schema()
    try:
        positions = alpaca_gateway.list_positions()
        holding_stats = _sync_position_holdings_from_positions(positions)
        ops_stats = _sync_stock_operations_from_positions(positions)
        print(
            f"[POSITION SYNC ALL] holdings={holding_stats} stock_operations={ops_stats}",
            flush=True,
        )
        return True
    except Exception as exc:
        LAST_SYNC_ERROR = str(exc)
        print(f"[POSITION SYNC ALL ERROR] {LAST_SYNC_ERROR}", flush=True)
        return False


if __name__ == "__main__":
    sync_all_positions()
