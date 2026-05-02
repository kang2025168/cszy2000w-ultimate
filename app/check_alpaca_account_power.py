# -*- coding: utf-8 -*-
"""
检查 Alpaca 账户资金/购买力字段。

用途：
    排查“页面看起来还有购买力，但期权下不了单”的原因。

这个脚本只读取账户和未成交订单：
    - 不下单
    - 不撤单
    - 不写数据库

用法：
    python app/check_alpaca_account_power.py

环境变量：
    TRADE_ENV=paper/live
    APCA_API_KEY_ID / APCA_API_SECRET_KEY
    或 ALPACA_KEY / ALPACA_SECRET

重点看：
    cash                         现金
    buying_power                 普通购买力，股票可能能用，但期权不一定按这个算
    options_buying_power         如果 Alpaca 返回这个字段，期权更应该看它
    open_orders_notional_est     未成交订单大致占用，可能冻结资金
"""

from __future__ import annotations

import os


def _safe_float(v, default=0.0):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _get_attr(obj, name: str, default=None):
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


def _get_client():
    from alpaca.trading.client import TradingClient

    trade_env = (os.getenv("TRADE_ENV") or os.getenv("ALPACA_MODE") or "paper").strip().lower()
    key = os.getenv("APCA_API_KEY_ID", "") or os.getenv("ALPACA_KEY", "")
    secret = os.getenv("APCA_API_SECRET_KEY", "") or os.getenv("ALPACA_SECRET", "")
    if not key or not secret:
        raise RuntimeError("缺少 Alpaca key: APCA_API_KEY_ID / APCA_API_SECRET_KEY")

    return TradingClient(key, secret, paper=(trade_env != "live")), trade_env


def print_account_fields(acct):
    fields = [
        "status",
        "currency",
        "cash",
        "portfolio_value",
        "equity",
        "last_equity",
        "buying_power",
        "regt_buying_power",
        "daytrading_buying_power",
        "non_marginable_buying_power",
        "options_buying_power",
        "initial_margin",
        "maintenance_margin",
        "last_maintenance_margin",
        "sma",
        "multiplier",
        "pattern_day_trader",
        "trading_blocked",
        "transfers_blocked",
        "account_blocked",
        "trade_suspended_by_user",
        "shorting_enabled",
        "options_approved_level",
        "options_trading_level",
        "crypto_status",
    ]

    print("=" * 72, flush=True)
    print("[ACCOUNT FIELDS]", flush=True)
    for name in fields:
        val = _get_attr(acct, name, "<missing>")
        print(f"{name:32s} = {val}", flush=True)

    print("=" * 72, flush=True)
    print("[INTERPRETATION]", flush=True)
    cash = _safe_float(_get_attr(acct, "cash"))
    bp = _safe_float(_get_attr(acct, "buying_power"))
    opt_bp = _safe_float(_get_attr(acct, "options_buying_power"))
    non_margin_bp = _safe_float(_get_attr(acct, "non_marginable_buying_power"))

    print(f"cash                    = {cash:.2f}", flush=True)
    print(f"buying_power            = {bp:.2f}", flush=True)
    print(f"non_marginable_bp       = {non_margin_bp:.2f}", flush=True)
    if opt_bp > 0:
        print(f"options_buying_power    = {opt_bp:.2f}  <- 期权优先看这个", flush=True)
    else:
        print("options_buying_power    = 未返回或为0，期权可能按 cash/non_marginable_bp 判断", flush=True)

    print(
        "说明：期权通常不能像股票一样使用保证金杠杆；"
        "如果 buying_power 高但 cash/options buying power 低，期权单可能会被拒。",
        flush=True,
    )


def print_open_orders(client):
    print("=" * 72, flush=True)
    print("[OPEN ORDERS]", flush=True)

    try:
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus

        orders = client.get_orders(filter=GetOrdersRequest(status=QueryOrderStatus.OPEN)) or []
    except Exception as e:
        print(f"读取 open orders 失败: {e}", flush=True)
        return

    if not orders:
        print("没有 open orders", flush=True)
        return

    total_est = 0.0
    for o in orders:
        symbol = _get_attr(o, "symbol", "")
        side = _get_attr(o, "side", "")
        qty = _safe_float(_get_attr(o, "qty"))
        limit_price = _safe_float(_get_attr(o, "limit_price"))
        notional = _safe_float(_get_attr(o, "notional"))
        order_class = _get_attr(o, "order_class", "")
        status = _get_attr(o, "status", "")
        order_id = _get_attr(o, "id", "")

        est = notional
        if est <= 0 and qty > 0 and limit_price > 0:
            # 股票是 qty*price；期权合约常见乘数是100。
            # Alpaca order 对多腿/期权可能不适合简单估算，所以这里只做提示。
            est = qty * limit_price
        total_est += max(est, 0.0)

        print(
            f"id={order_id} symbol={symbol} side={side} qty={qty} "
            f"limit={limit_price} class={order_class} status={status} est={est:.2f}",
            flush=True,
        )

    print(f"open_orders_notional_est = {total_est:.2f}", flush=True)
    print("说明：未成交订单可能冻结一部分资金，导致新期权单被拒。", flush=True)


def main():
    client, trade_env = _get_client()
    print(f"[ENV] TRADE_ENV={trade_env}", flush=True)

    acct = client.get_account()
    print_account_fields(acct)
    print_open_orders(client)


if __name__ == "__main__":
    main()

