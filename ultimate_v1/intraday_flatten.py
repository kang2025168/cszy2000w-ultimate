from __future__ import annotations

"""D 类日内交易强制平仓：接近收盘时卖出所有 D 类持仓。"""

from datetime import datetime, time
from zoneinfo import ZoneInfo

from . import alpaca_gateway
from .config import settings
from .db import db_conn
from .position_holdings import update_sell_holding


def _flatten_time() -> time:
    hh, mm = settings().market_close_flatten_time.split(":", 1)
    return time(int(hh), int(mm))


def should_flatten_now(now: datetime | None = None) -> bool:
    """判断当前时间是否已经到达 D 类强平时间。"""
    s = settings()
    now = now or datetime.now(ZoneInfo(s.timezone))
    return now.time() >= _flatten_time()


def flatten_d_positions(force: bool = False) -> int:
    """执行 D 类持仓强平，并同步 stock_operations 和 position_holdings。"""
    s = settings()
    if not s.enable_d_intraday:
        print("[D FLATTEN] disabled=1", flush=True)
        return 0
    if not force and not should_flatten_now():
        print(f"[D FLATTEN] skip before {s.market_close_flatten_time}", flush=True)
        return 0

    flattened = 0
    with db_conn(s) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT stock_code, qty, cost_price, close_price
                FROM `{s.ops_table}`
                WHERE is_bought=1
                  AND UPPER(COALESCE(NULLIF(strategy_group, ''), stock_type))='D'
                """
            )
            rows = list(cur.fetchall())
            for row in rows:
                symbol = str(row["stock_code"]).upper()
                qty = float(row.get("qty") or 0)
                if qty <= 0:
                    continue
                try:
                    order = alpaca_gateway.submit_market_sell(symbol, qty)
                    order_id = str(getattr(order, "id", "") or "")
                    price = float(row.get("close_price") or row.get("cost_price") or 0)
                    cur.execute(
                        f"""
                        UPDATE `{s.ops_table}`
                        SET is_bought=0, can_sell=0, qty=0,
                            last_order_side='sell',
                            last_order_intent='D_FORCE_FLATTEN',
                            last_order_id=%s,
                            last_order_time=NOW()
                        WHERE stock_code=%s
                        """,
                        (order_id, symbol),
                    )
                    update_sell_holding(symbol, "D", qty, price, remaining_qty=0, last_order_id=order_id)
                    flattened += 1
                    print(f"[D FLATTEN] symbol={symbol} qty={qty} order_id={order_id}", flush=True)
                except Exception as exc:
                    print(f"[D FLATTEN ERROR] symbol={symbol} qty={qty} error={exc}", flush=True)
    return flattened


if __name__ == "__main__":
    flatten_d_positions(force=False)
