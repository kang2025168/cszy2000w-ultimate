from __future__ import annotations

"""持仓展示表维护：买入写入/更新，卖出关闭或保留部分仓位。"""

from datetime import datetime

from .config import settings
from .db import db_conn


def _enabled() -> bool:
    return settings().enable_position_holdings


def upsert_buy_holding(
    symbol: str,
    strategy_group: str,
    qty: float,
    avg_entry_price: float,
    *,
    stock_type: str | None = None,
    current_price: float | None = None,
    stop_loss_price: float | None = None,
    take_profit_price: float | None = None,
    b_stage: int | None = None,
    capital_pool: str | None = None,
    margin_used: int = 0,
    last_order_id: str | None = None,
) -> None:
    """买入成交后写入 position_holdings；已有 open 记录则更新。"""
    if not _enabled():
        return
    symbol = symbol.upper()
    group = (strategy_group or stock_type or "UNKNOWN").upper()
    price = float(current_price or avg_entry_price or 0)
    cost_basis = float(qty or 0) * float(avg_entry_price or 0)
    market_value = float(qty or 0) * price
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM position_holdings
                WHERE symbol=%s AND strategy_group=%s AND status='open'
                ORDER BY id DESC LIMIT 1
                """,
                (symbol, group),
            )
            row = cur.fetchone()
            if row:
                cur.execute(
                    """
                    UPDATE position_holdings
                    SET qty=%s, avg_entry_price=%s, cost_basis=%s, market_value=%s,
                        current_price=%s, stop_loss_price=%s, take_profit_price=%s,
                        b_stage=%s, capital_pool=%s, margin_used=%s,
                        last_order_id=%s, last_order_side='buy',
                        last_update_time=NOW()
                    WHERE id=%s
                    """,
                    (
                        qty,
                        avg_entry_price,
                        cost_basis,
                        market_value,
                        price,
                        stop_loss_price,
                        take_profit_price,
                        b_stage,
                        capital_pool or group,
                        margin_used,
                        last_order_id,
                        row["id"],
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO position_holdings (
                        symbol, strategy_group, stock_type, status, qty,
                        avg_entry_price, current_price, market_value, cost_basis,
                        entry_time, stop_loss_price, take_profit_price, b_stage,
                        capital_pool, margin_used, last_order_id, last_order_side,
                        last_update_time
                    ) VALUES (
                        %s,%s,%s,'open',%s,%s,%s,%s,%s,NOW(),%s,%s,%s,%s,%s,%s,'buy',NOW()
                    )
                    """,
                    (
                        symbol,
                        group,
                        stock_type or group,
                        qty,
                        avg_entry_price,
                        price,
                        market_value,
                        cost_basis,
                        stop_loss_price,
                        take_profit_price,
                        b_stage,
                        capital_pool or group,
                        margin_used,
                        last_order_id,
                    ),
                )
    print(f"[HOLDING UPSERT] symbol={symbol} strategy={group} qty={qty} avg={avg_entry_price} status=open", flush=True)


def update_sell_holding(
    symbol: str,
    strategy_group: str,
    sell_qty: float,
    sell_price: float,
    *,
    remaining_qty: float = 0,
    realized_pnl: float | None = None,
    last_order_id: str | None = None,
) -> None:
    """卖出成交后更新展示表；全部卖出标记 closed，部分卖出保持 open。"""
    if not _enabled():
        return
    symbol = symbol.upper()
    group = (strategy_group or "UNKNOWN").upper()
    status = "open" if float(remaining_qty or 0) > 0 else "closed"
    market_value = float(remaining_qty or 0) * float(sell_price or 0)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, avg_entry_price, realized_pnl
                FROM position_holdings
                WHERE symbol=%s AND strategy_group=%s AND status='open'
                ORDER BY id DESC LIMIT 1
                """,
                (symbol, group),
            )
            row = cur.fetchone()
            if not row:
                return
            pnl = realized_pnl
            if pnl is None:
                avg = float(row.get("avg_entry_price") or 0)
                pnl = float(sell_qty or 0) * (float(sell_price or 0) - avg)
            total_pnl = float(row.get("realized_pnl") or 0) + float(pnl or 0)
            cur.execute(
                """
                UPDATE position_holdings
                SET qty=%s, current_price=%s, market_value=%s, realized_pnl=%s,
                    status=%s, exit_time=IF(%s='closed', NOW(), exit_time),
                    last_order_id=%s, last_order_side='sell', last_update_time=NOW()
                WHERE id=%s
                """,
                (remaining_qty, sell_price, market_value, total_pnl, status, status, last_order_id, row["id"]),
            )
    tag = "[HOLDING CLOSED]" if status == "closed" else "[HOLDING SELL]"
    print(f"{tag} symbol={symbol} strategy={group} realized_pnl={float(pnl or 0):.2f} status={status}", flush=True)


def sync_open_holding_from_position(pos, strategy_group: str = "UNKNOWN") -> None:
    """把 Alpaca 当前真实持仓同步到本地展示表。"""
    symbol = str(getattr(pos, "symbol", "")).upper()
    qty = float(getattr(pos, "qty", 0) or 0)
    avg = float(getattr(pos, "avg_entry_price", 0) or 0)
    current = float(getattr(pos, "current_price", 0) or 0)
    market_value = float(getattr(pos, "market_value", 0) or 0)
    unrealized = float(getattr(pos, "unrealized_pl", 0) or 0)
    unrealized_pct = float(getattr(pos, "unrealized_plpc", 0) or 0)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM position_holdings
                WHERE symbol=%s AND status='open'
                ORDER BY FIELD(strategy_group, %s, 'UNKNOWN') DESC, id DESC LIMIT 1
                """,
                (symbol, strategy_group),
            )
            row = cur.fetchone()
            if row:
                cur.execute(
                    """
                    UPDATE position_holdings
                    SET qty=%s, avg_entry_price=%s, current_price=%s, market_value=%s,
                        cost_basis=%s, unrealized_pnl=%s, unrealized_pnl_pct=%s,
                        holding_days=IF(entry_time IS NULL, 0, DATEDIFF(NOW(), entry_time)),
                        last_update_time=NOW()
                    WHERE id=%s
                    """,
                    (qty, avg, current, market_value, qty * avg, unrealized, unrealized_pct, row["id"]),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO position_holdings (
                        symbol, strategy_group, stock_type, status, qty, avg_entry_price,
                        current_price, market_value, cost_basis, unrealized_pnl,
                        unrealized_pnl_pct, entry_time, capital_pool, last_update_time, notes
                    ) VALUES (%s,%s,%s,'open',%s,%s,%s,%s,%s,%s,%s,NOW(),%s,NOW(),%s)
                    """,
                    (symbol, strategy_group, strategy_group, qty, avg, current, market_value, qty * avg, unrealized, unrealized_pct, strategy_group, "auto-created from Alpaca sync"),
                )


def mark_missing_from_alpaca(open_symbols: set[str]) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, symbol FROM position_holdings WHERE status='open'")
            for row in cur.fetchall():
                symbol = str(row["symbol"]).upper()
                if symbol not in open_symbols:
                    cur.execute(
                        "UPDATE position_holdings SET status='needs_review', last_update_time=NOW(), notes=CONCAT(COALESCE(notes,''), ' local_open_but_not_in_alpaca') WHERE id=%s",
                        (row["id"],),
                    )
                    print(f"[POSITION SYNC] symbol={symbol} local_open_but_not_in_alpaca status=needs_review", flush=True)


def summary_counts() -> dict:
    if not _enabled():
        return {"open": 0, "closed": 0, "needs_review": 0}
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) AS n FROM position_holdings GROUP BY status")
            out = {"open": 0, "closed": 0, "needs_review": 0}
            for row in cur.fetchall():
                out[str(row["status"])] = int(row["n"])
            return out
