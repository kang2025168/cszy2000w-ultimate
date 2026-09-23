"""Manual stock planning, risk checks and durable execution, shared with flattening."""
import hashlib
import json
import re
from . import alpaca_gateway
from .account_config import profile_for_pool
from .config import settings
from .db import fetch_all
from .manual_policy import _manual_stop_policy, _manual_stock_qty
from .order_fills import finite_number as _safe_float
from .trade_history import _record_manual_trade

def _pool_account_buying_power(capital: dict, pool: str) -> float:
    """Return buying power for the broker account that owns this pool."""
    profile = str((capital.get("pool_brokers") or {}).get(pool) or "").strip()
    snapshot = (capital.get("broker_snapshots") or {}).get(profile) or {}
    return max(0.0, _safe_float(snapshot.get("buying_power")))


def _manual_stock_order_payload(payload: dict) -> dict:
    from . import order_journal as journal
    if payload.get("execute") is not True:
        return _plan_manual_stock_order(payload)
    request_id = str(payload.get("request_id") or "")
    if not re.fullmatch(r"[A-Za-z0-9_-]{16,80}", request_id):
        return {"ok": False, "error": "缺少有效订单请求标识，请重新预览"}
    pool = str(payload.get("pool") or "C").upper()
    profile = profile_for_pool(pool)
    cid = f"cszy-manual-{pool}-" + hashlib.sha256(request_id.encode()).hexdigest()[:32]
    original = {k: payload.get(k) for k in ("symbol", "side", "pool", "size", "order_type", "limit_price")}
    with journal.execution_lock(profile):
        existing = journal.get_intent(cid)
        if existing:
            stored = json.loads(existing["request_json"])
            if stored["input"] != original or existing["profile"] != profile:
                return {"ok": False, "error": "请求标识与原订单不符"}
            return _execute_manual_preview(stored["preview"], existing)
        preview = _plan_manual_stock_order({**payload, "execute": False})
        if not preview.get("ok"):
            return preview
        from .trading_gate import can_open_position
        if preview["side"] in {"buy", "short"}:
            allowed, reason = can_open_position(pool, preview["notional"])
            if not allowed:
                return {"ok": False, "error": f"风控阻止下单: {reason}"}
        intent = journal.prepare(cid, profile, pool, preview["symbol"], preview["side"],
            {"input": original, "preview": preview},
            preview["notional"] if preview["side"] in {"buy", "short"} else 0)
        return _execute_manual_preview(preview, intent)


def _execute_manual_preview(preview: dict, intent: dict) -> dict:
    from . import order_journal as journal
    from .manual_ledger import apply_order
    from alpaca.trading.enums import OrderSide, TimeInForce
    from alpaca.trading.requests import LimitOrderRequest, MarketOrderRequest
    preview = dict(preview)
    client = alpaca_gateway.trading_client(profile=intent["profile"])
    kwargs = dict(symbol=preview["symbol"], qty=preview["qty"],
        side=OrderSide.BUY if preview["side"] == "buy" else OrderSide.SELL,
        time_in_force=TimeInForce.DAY, client_order_id=intent["client_order_id"])
    if preview["order_type"] == "market":
        req = MarketOrderRequest(**kwargs)
    else:
        req = LimitOrderRequest(**kwargs, limit_price=alpaca_gateway.stock_limit_price(preview["price"]), extended_hours=True)
    order = journal.submit_prepared(client, req, intent)
    # Read the broker again on retries; never book a saved response twice.
    preview.update(apply_order(intent["client_order_id"], order))
    preview.update(execute=True, order_id=str(order.id), message="订单已提交，成交状态将自动核对")
    _record_manual_trade(preview)
    journal.update(intent["client_order_id"], response_json=json.dumps(preview, default=str))
    return preview


def _plan_manual_stock_order(payload: dict) -> dict:
    """手动股票下单预览/执行。买入/卖空按资金池额度，卖出按当前持仓比例。"""
    symbol = str(payload.get("symbol") or "").strip().upper()
    side = str(payload.get("side") or "").strip().lower()
    pool = str(payload.get("pool") or "C").strip().upper()
    size = str(payload.get("size") or "1/4").strip()
    order_type = str(payload.get("order_type") or "limit").strip().lower()
    execute = bool(payload.get("execute") is True)
    if not symbol or not symbol.replace(".", "").isalpha():
        return {"ok": False, "error": "股票代码无效"}
    if side not in {"buy", "sell", "short"}:
        return {"ok": False, "error": "只支持买入、卖出或卖空"}
    if pool not in {"A", "B", "C", "D"}:
        return {"ok": False, "error": "资金池无效"}
    if side == "short" and pool == "A":
        return {"ok": False, "error": "A 养老金账户不支持卖空"}
    fractions = {"1/4": 0.25, "1/3": 1 / 3, "1/2": 0.5, "1/1": 1.0, "full": 1.0}
    fraction = fractions.get(size)
    if fraction is None:
        return {"ok": False, "error": "额度选项无效"}
    if order_type not in {"limit", "market"}:
        return {"ok": False, "error": "订单类型无效"}

    from .web_app import _stock_quote_payload, _allocation_payload
    quote = _stock_quote_payload(symbol)
    last = _safe_float(quote.get("last"))
    bid = _safe_float(quote.get("bid"))
    ask = _safe_float(quote.get("ask"))
    limit_price = _safe_float(payload.get("limit_price"))
    price = limit_price if order_type == "limit" and limit_price > 0 else last
    if price <= 0:
        return {"ok": False, "error": "暂时没有可用报价"}

    qty = 0.0
    notional = 0.0
    available = 0.0
    held_qty = 0.0
    if side in {"sell", "short"}:
        try:
            for pos in alpaca_gateway.list_positions(pool=pool):
                if str(getattr(pos, "symbol", "") or "").upper() == symbol:
                    held_qty = _safe_float(getattr(pos, "qty", 0))
                    break
        except Exception as exc:
            return {"ok": False, "error": f"读取持仓失败: {str(exc)[:120]}"}
        if side == "short" and held_qty > 0:
            return {"ok": False, "error": f"{pool} 资金池当前持有 {held_qty:.4f} 股，先用卖出平仓后再卖空"}
    if side == "sell":
        lot = fetch_all(f"SELECT qty FROM `{settings().ops_table}` WHERE stock_code=%s AND stock_type=%s", (symbol, pool))
        owned = sum(max(0.0, _safe_float(row.get("qty"))) for row in lot)
        held_qty = min(held_qty, owned)
        pending = fetch_all("SELECT request_json,accounted_qty FROM execution_orders WHERE client_order_id LIKE 'cszy-manual-%%' AND profile=%s AND symbol=%s AND side='sell' AND state NOT IN ('filled','canceled','cancelled','expired','rejected','replaced')", (profile_for_pool(pool), symbol))
        reserved_qty = sum(max(0.0, float(json.loads(row["request_json"])["preview"]["qty"]) - float(row["accounted_qty"])) for row in pending)
        held_qty = max(0.0, held_qty - reserved_qty)
    if side in {"buy", "short"}:
        cap = _allocation_payload()
        if not cap.get("ok"):
            return {"ok": False, "error": cap.get("error") or "资金池不可用"}
        available = _safe_float((cap.get("available") or {}).get(pool))
        buying_power = _pool_account_buying_power(cap, pool)
        notional = max(0.0, min(available * fraction, buying_power))
        qty = _manual_stock_qty(notional / price, price)
        notional = qty * price
    else:
        qty = _manual_stock_qty(max(0.0, held_qty * fraction), price, held_qty if fraction >= 1.0 else None)
        notional = qty * price

    if qty <= 0 or notional <= 0:
        return {"ok": False, "error": "按当前额度/持仓计算后数量为 0，无法下单"}

    preview = {
        "ok": True,
        "execute": execute,
        "symbol": symbol,
        "side": side,
        "pool": pool,
        "size": size,
        "fraction": fraction,
        "order_type": order_type,
        "limit_price": price,
        "price": price,
        "bid": bid,
        "ask": ask,
        "last": last,
        "qty": qty,
        "notional": notional,
        "available": available,
        "held_qty": held_qty,
        "message": "预览完成，未提交订单",
    }
    preview.update(_manual_stop_policy(price, pool, side))
    if not execute:
        return preview

    return preview


