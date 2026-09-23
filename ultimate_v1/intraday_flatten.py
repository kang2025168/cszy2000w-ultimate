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
    """提交 D 类限价平仓；只在真实成交后更新持仓。"""
    s = settings()
    if not s.enable_d_intraday:
        print("[D FLATTEN] disabled=1", flush=True)
        return 0
    if not force and not should_flatten_now():
        print(f"[D FLATTEN] skip before {s.market_close_flatten_time}", flush=True)
        return 0

    from .schema import ensure_schema
    from .db import fetch_all
    from .manual_execution import _manual_stock_order_payload
    import hashlib
    ensure_schema()
    rows = fetch_all(f"SELECT stock_code,last_order_id FROM `{s.ops_table}` WHERE is_bought=1 AND qty>0 AND stock_type='D'")
    submitted = 0
    today = datetime.now(ZoneInfo(s.timezone)).date().isoformat()
    for row in rows:
        symbol = str(row["stock_code"]).upper()
        intent = f"flatten:{today}:{symbol}:{row.get('last_order_id') or 'initial'}"
        request_id = "flatten-" + hashlib.sha256(intent.encode()).hexdigest()[:40]
        try:
            result = _manual_stock_order_payload(dict(symbol=symbol, pool="D", side="sell",
                size="full", order_type="limit", execute=True, request_id=request_id))
            if result.get("ok") and result.get("order_id"):
                submitted += 1
                print(f"[D FLATTEN] {symbol} submitted; holdings change only on confirmed fills", flush=True)
            else:
                print(f"[D FLATTEN] {symbol}: {result.get('error')}", flush=True)
        except Exception as exc:
            print(f"[D FLATTEN ERROR] {symbol}: {type(exc).__name__}", flush=True)
    return submitted


if __name__ == "__main__":
    flatten_d_positions(force=False)
