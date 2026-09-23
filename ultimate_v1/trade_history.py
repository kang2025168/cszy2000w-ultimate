"""Idempotent manual trade history for dashboard and reconciliation workers."""
from .db import db_conn
from .order_fills import finite_number as _safe_float

def _record_manual_trade(preview: dict) -> None:
    """Persist one manual order event; repeated writes update its broker status."""
    order_id = str(preview.get("order_id") or "").strip()
    if not order_id:
        return
    qty = _safe_float(preview.get("qty"))
    filled_qty = _safe_float(preview.get("filled_qty"))
    price = _safe_float(preview.get("price"))
    filled_avg = _safe_float(preview.get("filled_avg_price"))
    note = (
        f"手动{ {'buy':'买入','sell':'卖出','short':'卖空'}.get(str(preview.get('side') or ''), '交易') }"
        f" · {str(preview.get('order_type') or 'limit').upper()}"
        f" · 资金池 {str(preview.get('pool') or '').upper()}"
    )
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO manual_trade_records (
                    event_time, symbol, side, strategy_group,
                    qty, filled_qty, price, filled_avg_price,
                    order_type, status, note, order_id
                ) VALUES (
                    NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    filled_qty=VALUES(filled_qty),
                    filled_avg_price=VALUES(filled_avg_price),
                    status=VALUES(status),
                    note=VALUES(note),
                    updated_at=CURRENT_TIMESTAMP
                """,
                (
                    str(preview.get("symbol") or "").upper(),
                    str(preview.get("side") or "").upper(),
                    str(preview.get("pool") or "").upper(),
                    qty,
                    filled_qty,
                    price,
                    filled_avg,
                    str(preview.get("order_type") or "limit").lower(),
                    str(preview.get("status") or "submitted")[:32],
                    note[:512],
                    order_id,
                ),
            )


