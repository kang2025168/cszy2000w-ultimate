# -*- coding: utf-8 -*-
"""
一次性扫描 stock_operations 里所有 B 票，实时判断是否触发买入

条件：
1) price > trigger_price
2) up_pct > B_MIN_UP_PCT（默认 5%）
（成交量条件已去掉）

下单：
- 市价 notional（永远优先成交）
- 若 notional 遇到 “not fractionable”，自动降级为 qty 市价单（整数股）
- buying_power >= B_MIN_BUYING_POWER
- notional = min(B_BUY_NOTIONAL_USD, buying_power * B_BP_USE_RATIO)

保护：
- stock_type='B'
- can_buy=1
- is_bought!=1
- 冷却 last_order_time + last_order_side=buy

输出：
- 打印每只票的判定结果（可控）
"""

import os
import time
import traceback
from datetime import datetime, timedelta

import pymysql
import requests

OPS_TABLE = os.getenv("OPS_TABLE", "stock_operations")

B_MIN_UP_PCT = float(os.getenv("B_MIN_UP_PCT", "0.05"))             # 5%
B_MIN_BUYING_POWER = float(os.getenv("B_MIN_BUYING_POWER", "300"))
B_BUY_NOTIONAL_USD = float(os.getenv("B_BUY_NOTIONAL_USD", "300"))

B_COOLDOWN_MINUTES = int(os.getenv("B_COOLDOWN_MINUTES", "30"))
B_BP_USE_RATIO = float(os.getenv("B_BP_USE_RATIO", "0.95"))
B_ALLOW_EXTENDED = int(os.getenv("B_ALLOW_EXTENDED", "0"))

# 先 sip，403 自动降级 iex
PREFERRED_FEED = os.getenv("ALPACA_DATA_FEED", "sip").strip().lower()
FALLBACK_FEED = os.getenv("ALPACA_DATA_FEED_FALLBACK", "iex").strip().lower()
ALPACA_DATA_BASE_URL = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")

TRADE_ENV = os.getenv("TRADE_ENV", os.getenv("ALPACA_MODE", "paper")).strip().lower()
APCA_API_KEY_ID = os.getenv("APCA_API_KEY_ID", "") or os.getenv("ALPACA_KEY", "")
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY", "") or os.getenv("ALPACA_SECRET", "")

DB = dict(
    host=os.getenv("DB_HOST", "mysql"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASS", "mlp009988"),
    database=os.getenv("DB_NAME", "cszy2000"),
    charset="utf8mb4",
    autocommit=True,
    cursorclass=pymysql.cursors.DictCursor,
)

HTTP_TIMEOUT = float(os.getenv("B_HTTP_TIMEOUT", "6"))

# 扫描控制
SCAN_LIMIT = int(os.getenv("B_SCAN_LIMIT", "0"))   # 0=不限制
SLEEP_PER_SYMBOL = float(os.getenv("B_SLEEP_PER_SYMBOL", "0.12"))

# 输出控制
VERBOSE = int(os.getenv("B_SCAN_VERBOSE", "1"))    # 1=打印每只票结果  0=只打印触发/错误


def _log(msg: str):
    print(msg, flush=True)


def _connect():
    return pymysql.connect(**DB)


def _alpaca_headers():
    if not (APCA_API_KEY_ID and APCA_API_SECRET_KEY):
        raise RuntimeError("Alpaca key missing: APCA_API_KEY_ID / APCA_API_SECRET_KEY")
    return {
        "APCA-API-KEY-ID": APCA_API_KEY_ID,
        "APCA-API-SECRET-KEY": APCA_API_SECRET_KEY,
    }


def _is_cooldown(last_order_time, last_order_side) -> bool:
    if not last_order_time or (last_order_side or "").lower() != "buy":
        return False
    try:
        return (datetime.now() - last_order_time) < timedelta(minutes=B_COOLDOWN_MINUTES)
    except Exception:
        return False


def _snapshot_http(code: str, feed: str):
    url = f"{ALPACA_DATA_BASE_URL}/v2/stocks/{code}/snapshot"
    params = {"feed": feed}
    return requests.get(url, headers=_alpaca_headers(), params=params, timeout=HTTP_TIMEOUT)


def _parse_snapshot(js: dict):
    price = None
    lt = js.get("latestTrade") or {}
    if lt.get("p") is not None:
        price = float(lt["p"])
    if price is None:
        lq = js.get("latestQuote") or {}
        bid = float(lq.get("bp") or 0)
        ask = float(lq.get("ap") or 0)
        if bid > 0 and ask > 0:
            price = (bid + ask) / 2.0

    pb = js.get("prevDailyBar") or {}
    prev_close = pb.get("c", None)
    prev_close = float(prev_close) if prev_close is not None else None

    if price is None or prev_close is None:
        raise RuntimeError(f"snapshot missing fields: price={price} prev_close={prev_close}")

    return price, prev_close


def get_snapshot_realtime(code: str):
    r = _snapshot_http(code, PREFERRED_FEED)
    if r.status_code == 200:
        price, prev_close = _parse_snapshot(r.json())
        return price, prev_close, PREFERRED_FEED

    # SIP 权限不够 -> fallback
    if r.status_code == 403 and "SIP" in (r.text or "").upper():
        r2 = _snapshot_http(code, FALLBACK_FEED)
        if r2.status_code == 200:
            price, prev_close = _parse_snapshot(r2.json())
            return price, prev_close, FALLBACK_FEED
        raise RuntimeError(f"snapshot fallback http {r2.status_code}: {r2.text[:200]}")

    raise RuntimeError(f"snapshot http {r.status_code}: {r.text[:200]}")


def _get_trading_client():
    from alpaca.trading.client import TradingClient  # noqa
    paper = True if TRADE_ENV != "live" else False
    return TradingClient(APCA_API_KEY_ID, APCA_API_SECRET_KEY, paper=paper)


def _get_buying_power(trading_client) -> float:
    acct = trading_client.get_account()
    bp = getattr(acct, "buying_power", None)
    if bp is None:
        bp = getattr(acct, "cash", None)
    return float(bp)


def _submit_market_notional(trading_client, code: str, notional: float):
    from alpaca.trading.requests import MarketOrderRequest  # noqa
    from alpaca.trading.enums import OrderSide, TimeInForce  # noqa

    req = MarketOrderRequest(
        symbol=code,
        notional=round(float(notional), 2),
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
        extended_hours=bool(B_ALLOW_EXTENDED),
    )
    return trading_client.submit_order(order_data=req)


def _submit_market_qty(trading_client, code: str, qty: int):
    from alpaca.trading.requests import MarketOrderRequest  # noqa
    from alpaca.trading.enums import OrderSide, TimeInForce  # noqa

    req = MarketOrderRequest(
        symbol=code,
        qty=int(qty),
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
        extended_hours=bool(B_ALLOW_EXTENDED),
    )
    return trading_client.submit_order(order_data=req)


def _is_not_fractionable_err(e: Exception) -> bool:
    s = str(e) or ""
    s_up = s.upper()
    return ("NOT FRACTIONABLE" in s_up) or ("40310000" in s_up)


def main():
    conn = _connect()

    # 拉 B 池
    sql = f"""
    SELECT stock_code, trigger_price, is_bought, can_buy, last_order_time, last_order_side
    FROM `{OPS_TABLE}`
    WHERE stock_type='B'
      AND can_buy=1
      AND (is_bought IS NULL OR is_bought<>1)
      AND trigger_price IS NOT NULL
    ORDER BY updated_at DESC;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    if not rows:
        _log("[WARN] 没有可扫描的 B 票（stock_operations）")
        conn.close()
        return

    if SCAN_LIMIT > 0:
        rows = rows[:SCAN_LIMIT]

    _log(f"[INFO] scan symbols = {len(rows)}  min_up={B_MIN_UP_PCT*100:.2f}%  notional={B_BUY_NOTIONAL_USD}  cooldown={B_COOLDOWN_MINUTES}m")

    trading_client = _get_trading_client()
    buying_power = _get_buying_power(trading_client)
    _log(f"[INFO] buying_power={buying_power:.2f}")

    triggered = 0
    bought = 0
    skipped = 0
    errors = 0

    for i, r in enumerate(rows, 1):
        code = (r.get("stock_code") or "").strip().upper()
        try:
            trigger = float(r.get("trigger_price") or 0)
        except Exception:
            trigger = 0.0

        if not code or trigger <= 0:
            skipped += 1
            continue

        if _is_cooldown(r.get("last_order_time"), r.get("last_order_side")):
            skipped += 1
            if VERBOSE:
                _log(f"[SKIP cooldown] {code}")
            continue

        try:
            price, prev_close, feed = get_snapshot_realtime(code)
            up_pct = (price - prev_close) / prev_close if prev_close and prev_close > 0 else 0.0

            cond_break = price > trigger
            cond_up = up_pct > B_MIN_UP_PCT

            if VERBOSE:
                _log(
                    f"[{i}/{len(rows)}] {code} feed={feed} price={price:.4f} prev={prev_close:.4f} "
                    f"up={up_pct*100:.2f}% trigger={trigger:.4f} break={cond_break} up_ok={cond_up}"
                )

            if not (cond_break and cond_up):
                skipped += 1
                time.sleep(SLEEP_PER_SYMBOL)
                continue

            triggered += 1

            # 动态购买力检查
            buying_power = _get_buying_power(trading_client)
            if buying_power < B_MIN_BUYING_POWER:
                if VERBOSE:
                    _log(f"[SKIP bp] {code} buying_power={buying_power:.2f} < {B_MIN_BUYING_POWER}")
                skipped += 1
                time.sleep(SLEEP_PER_SYMBOL)
                continue

            max_use = buying_power * B_BP_USE_RATIO
            notional = min(B_BUY_NOTIONAL_USD, max_use)
            if notional <= 0:
                skipped += 1
                time.sleep(SLEEP_PER_SYMBOL)
                continue

            # intent 要短：避免 last_order_intent 字段过长
            intent = f"B:SCANBUY up={up_pct*100:.2f}% px={price:.2f} tr={trigger:.2f} nt={notional:.2f} {feed}"

            # 先 notional（优先成交）
            try:
                order = _submit_market_notional(trading_client, code, notional)
                order_id = getattr(order, "id", None) or getattr(order, "order_id", None)
                mode_used = "notional"
                qty_used = None

            except Exception as e:
                # notional 不支持 -> qty fallback
                if _is_not_fractionable_err(e):
                    qty = int(notional // price)
                    if qty < 1:
                        skipped += 1
                        _log(f"[SKIP qty<1] {code} price={price:.2f} notional={notional:.2f}")
                        time.sleep(SLEEP_PER_SYMBOL)
                        continue

                    order = _submit_market_qty(trading_client, code, qty)
                    order_id = getattr(order, "id", None) or getattr(order, "order_id", None)
                    mode_used = "qty"
                    qty_used = qty
                    intent = f"B:SCANBUY qty={qty} up={up_pct*100:.2f}% px={price:.2f} tr={trigger:.2f} {feed}"
                else:
                    raise

            # 写回 last_order 字段
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE `{OPS_TABLE}`
                    SET last_order_side='buy',
                        last_order_intent=%s,
                        last_order_id=%s,
                        last_order_time=NOW(),
                        updated_at=CURRENT_TIMESTAMP
                    WHERE stock_code=%s AND stock_type='B';
                    """,
                    (intent, order_id, code),
                )

            bought += 1
            if mode_used == "notional":
                _log(f"[BUY] {code} ✅ notional={notional:.2f} order_id={order_id}")
            else:
                _log(f"[BUY] {code} ✅ qty={qty_used} order_id={order_id}")

        except Exception as e:
            errors += 1
            _log(f"[ERR] {code} {e}")
            traceback.print_exc()

        time.sleep(SLEEP_PER_SYMBOL)

    _log(f"\n[SUMMARY] total={len(rows)} triggered={triggered} bought={bought} skipped={skipped} errors={errors}")
    conn.close()


if __name__ == "__main__":
    main()