"""Apply cumulative broker fills and journal progress in one database transaction."""
from __future__ import annotations

import json

from .config import settings
from .db import db_conn, fetch_all
from .order_fills import finite_number, status_text, TERMINAL
from .order_journal import execution_lock
from .position_holdings import upsert_buy_holding


def fill_delta(total_qty, total_avg, accounted_qty, accounted_value):
    qty = finite_number(total_qty)
    avg = finite_number(total_avg)
    delta = qty - finite_number(accounted_qty)
    value = qty * avg - finite_number(accounted_value)
    if delta <= 0 or avg <= 0 or value <= 0:
        return 0.0, 0.0
    return delta, value / delta


def lot_after_fill(old_qty, old_avg, delta, price, side):
    signed = delta if side == "buy" else -delta
    remaining = old_qty + signed
    if not old_qty or old_qty * signed > 0:
        average = (abs(old_qty) * old_avg + delta * price) / abs(remaining)
        return remaining, average, 0.0
    closed = min(abs(old_qty), delta)
    realized = closed * (price - old_avg) * (1 if old_qty > 0 else -1)
    average = price if old_qty * remaining < 0 else old_avg
    return remaining, average, realized


def apply_order(cid: str, order) -> dict:
    # Only orders explicitly recorded by the manual execution service belong here.
    from .sync_positions import _table_columns, _update_ops_row, _insert_ops_row
    from .manual_policy import _manual_stop_policy
    qty = finite_number(getattr(order, "filled_qty", 0))
    avg = finite_number(getattr(order, "filled_avg_price", 0))
    state = status_text(getattr(order, "status", "unknown"))
    oid = str(getattr(order, "id", "") or "")
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM execution_orders WHERE client_order_id=%s FOR UPDATE", (cid,))
            intent = cur.fetchone()
            if not intent:
                raise RuntimeError("Missing durable order intent")
            delta, delta_avg = fill_delta(qty, avg, intent["accounted_qty"], intent["accounted_value"])
            group, symbol, side = intent["pool"], intent["symbol"], intent["side"]
            if delta > 0:
                table = settings().ops_table
                columns = _table_columns(conn, table)
                cur.execute(f"SELECT * FROM `{table}` WHERE stock_code=%s AND stock_type=%s LIMIT 1 FOR UPDATE", (symbol, group))
                existing = cur.fetchone()
                old_qty = finite_number((existing or {}).get("qty"))
                old_avg = finite_number((existing or {}).get("cost_price"))
                new_qty, new_avg, realized = lot_after_fill(old_qty, old_avg, delta, delta_avg, side)
                if side == "sell" and new_qty < -0.000001:
                    raise RuntimeError("Sell fill exceeds strategy lot; reconciliation required")
                stop = _manual_stop_policy(new_avg, group, side)
                values = dict(stock_code=symbol, stock_type=group, strategy_group=group,
                              capital_pool=group, qty=new_qty, base_qty=abs(new_qty),
                              cost_price=new_avg, current_price=avg, close_price=avg,
                              is_bought=int(abs(new_qty) > 0.000001), can_buy=int(abs(new_qty) <= 0.000001),
                              can_sell=int(new_qty > 0.000001), last_order_id=oid,
                              last_order_side="buy" if side == "buy" else "sell",
                              last_order_intent=f"{group}:MANUAL_{side.upper()} reconciled")
                if side == "buy":
                    values["stop_loss_price"] = stop.get("stop_loss_price", 0)
                if existing:
                    _update_ops_row(conn, table, columns, existing, values)
                else:
                    _insert_ops_row(conn, table, columns, values)
                upsert_buy_holding(symbol, group, new_qty, new_avg, current_price=avg,
                    stop_loss_price=stop.get("stop_loss_price", (existing or {}).get("stop_loss_price", 0)),
                    take_profit_price=(existing or {}).get("take_profit_price", 0),
                    b_stage=(existing or {}).get("b_stage", 0), capital_pool=group,
                    last_order_id=oid, connection=conn)
                cur.execute("""UPDATE position_holdings SET
                    realized_pnl=COALESCE(realized_pnl,0)+%s,
                    status=%s, exit_time=IF(%s='closed',NOW(),exit_time),last_order_side=%s
                    WHERE symbol=%s AND strategy_group=%s AND status='open' AND last_order_id=%s""",
                    (realized, "closed" if abs(new_qty) <= 0.000001 else "open",
                     "closed" if abs(new_qty) <= 0.000001 else "open",
                     "buy" if side == "buy" else "sell", symbol, group, oid))
            # Keep the whole reservation while working. This is conservative
            # during partial fills and cannot release capital ahead of booking.
            booked_qty = max(finite_number(intent["accounted_qty"]), qty if delta > 0 else 0)
            booked_value = qty * avg if delta > 0 else finite_number(intent["accounted_value"])
            complete = state in TERMINAL and qty <= booked_qty + 0.000001
            cur.execute("""UPDATE execution_orders SET state=%s,order_id=%s,
                accounted_qty=%s,accounted_value=%s,reserved_notional=%s,last_error=NULL
                WHERE client_order_id=%s""", (state, oid, booked_qty, booked_value,
                0 if complete else intent["reserved_notional"], cid))
    return {"filled_qty": qty, "filled_avg_price": avg, "status": state,
            "stop_loss_added": qty > 0 and side == "buy", "recorded_stock_type": group}


def reconcile_manual_orders(profile: str | None = None) -> None:
    from .alpaca_gateway import trading_client
    sql = """SELECT client_order_id,profile FROM execution_orders
        WHERE client_order_id LIKE 'cszy-manual-%%'
          AND (response_json IS NULL OR reserved_notional>0 OR state NOT IN ('filled','canceled','cancelled','expired','rejected','replaced'))"""
    args = ()
    if profile:
        sql += " AND profile=%s"
        args = (profile,)
    for row in fetch_all(sql, args):
        try:
            with execution_lock(row["profile"]):
                order = trading_client(profile=row["profile"]).get_order_by_client_id(row["client_order_id"])
                result = apply_order(row["client_order_id"], order)
                from .order_journal import get_intent
                from .trade_history import _record_manual_trade
                intent = get_intent(row["client_order_id"])
                preview = json.loads(intent["request_json"])["preview"]
                response = {**preview, **result, "order_id": str(order.id)}
                _record_manual_trade(response)
                from .order_journal import update
                update(row["client_order_id"], response_json=json.dumps(response))
        except Exception as exc:
            print(f"[ORDER RECONCILE] {row['client_order_id']}: {type(exc).__name__}; reservation retained", flush=True)
