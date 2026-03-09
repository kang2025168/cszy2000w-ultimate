# -*- coding: utf-8 -*-
"""
app/strategy_b.py
策略B（买卖逻辑）——10 阶段结构化退出
"""

import os
import math
import time
import traceback
from datetime import datetime, timedelta

import pymysql
import requests

# =========================
# DB
# =========================
OPS_TABLE = os.getenv("OPS_TABLE", "stock_operations")
PRICES_TABLE = os.getenv("B_PRICES_TABLE", "stock_prices_pool")

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

# =========================
# 参数
# =========================
B_MIN_UP_PCT = float(os.getenv("B_MIN_UP_PCT", "0.05"))
B_MIN_BUYING_POWER = float(os.getenv("B_MIN_BUYING_POWER", "900"))

B_TARGET_NOTIONAL_USD = float(os.getenv("B_TARGET_NOTIONAL_USD", "900"))
B_MAX_NOTIONAL_USD = float(os.getenv("B_MAX_NOTIONAL_USD", "900"))

B_COOLDOWN_MINUTES = int(os.getenv("B_COOLDOWN_MINUTES", "30"))
B_BP_USE_RATIO = float(os.getenv("B_BP_USE_RATIO", "0.95"))
B_ALLOW_EXTENDED = int(os.getenv("B_ALLOW_EXTENDED", "0"))
B_DEBUG = int(os.getenv("B_DEBUG", "0"))
HTTP_TIMEOUT = float(os.getenv("B_HTTP_TIMEOUT", "6"))

B_BP_USE_CASH = int(os.getenv("B_BP_USE_CASH", "0"))  # 0=buying_power,1=cash

# 买入后同步 position
B_POS_WAIT_SEC = int(os.getenv("B_POS_WAIT_SEC", "20"))
B_POS_RETRY = int(os.getenv("B_POS_RETRY", "2"))

ALPACA_DATA_BASE_URL = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")
B_DATA_FEED = os.getenv("B_DATA_FEED", "iex").strip().lower()

TRADE_ENV = (os.getenv("TRADE_ENV") or os.getenv("ALPACA_MODE") or "paper").strip().lower()
APCA_API_KEY_ID = os.getenv("APCA_API_KEY_ID", "") or os.getenv("ALPACA_KEY", "")
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY", "") or os.getenv("ALPACA_SECRET", "")

MAX_INTENT_LEN = int(os.getenv("B_INTENT_MAXLEN", "70"))

SNAPSHOT_MIN_INTERVAL = float(os.getenv("B_SNAPSHOT_MIN_INTERVAL", "0.35"))
SNAPSHOT_CACHE_SEC = int(os.getenv("B_SNAPSHOT_CACHE_SEC", "2"))
_snapshot_last_ts = 0.0
_snapshot_cache = {}  # code -> (ts, price, prev_close, feed)

FILL_POLL_TIMES = int(os.getenv("B_FILL_POLL_TIMES", "5"))
FILL_POLL_SLEEP = float(os.getenv("B_FILL_POLL_SLEEP", "0.4"))


def _d(msg: str):
    if B_DEBUG:
        print(msg, flush=True)


def _connect():
    return pymysql.connect(**DB)


def _intent_short(s: str) -> str:
    s = (s or "").strip()
    if len(s) <= MAX_INTENT_LEN:
        return s
    return s[: MAX_INTENT_LEN - 3] + "..."


def _alpaca_headers():
    if not (APCA_API_KEY_ID and APCA_API_SECRET_KEY):
        raise RuntimeError("Alpaca key missing: APCA_API_KEY_ID / APCA_API_SECRET_KEY")
    return {
        "APCA-API-KEY-ID": APCA_API_KEY_ID,
        "APCA-API-SECRET-KEY": APCA_API_SECRET_KEY,
    }


def _sleep_for_rate_limit():
    global _snapshot_last_ts
    now = time.time()
    gap = now - _snapshot_last_ts
    if gap < SNAPSHOT_MIN_INTERVAL:
        time.sleep(SNAPSHOT_MIN_INTERVAL - gap)
    _snapshot_last_ts = time.time()


def _snapshot_http(code: str, feed: str):
    url = f"{ALPACA_DATA_BASE_URL}/v2/stocks/{code}/snapshot"
    return requests.get(url, headers=_alpaca_headers(), params={"feed": feed}, timeout=HTTP_TIMEOUT)


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
    code = (code or "").strip().upper()
    if not code:
        raise RuntimeError("empty symbol")

    now = time.time()
    cached = _snapshot_cache.get(code)
    if cached:
        ts, price, prev_close, feed = cached
        if (now - ts) <= SNAPSHOT_CACHE_SEC:
            return price, prev_close, feed

    _sleep_for_rate_limit()

    r = _snapshot_http(code, B_DATA_FEED)
    if r.status_code == 200:
        price, prev_close = _parse_snapshot(r.json())
        _snapshot_cache[code] = (time.time(), price, prev_close, B_DATA_FEED)
        return price, prev_close, B_DATA_FEED

    raise RuntimeError(f"snapshot http {r.status_code}: {r.text[:200]}")


def _get_trading_client():
    from alpaca.trading.client import TradingClient
    paper = (TRADE_ENV != "live")
    return TradingClient(APCA_API_KEY_ID, APCA_API_SECRET_KEY, paper=paper)


def _get_buying_power(trading_client) -> float:
    acct = trading_client.get_account()
    if B_BP_USE_CASH == 1:
        v = getattr(acct, "cash", None)
        return float(v or 0.0)
    v = getattr(acct, "buying_power", None)
    if v is None:
        v = getattr(acct, "cash", None)
    return float(v or 0.0)


def _is_cooldown(last_order_time, last_order_side) -> bool:
    if not last_order_time or (last_order_side or "").lower() != "buy":
        return False
    try:
        return (datetime.now() - last_order_time) < timedelta(minutes=B_COOLDOWN_MINUTES)
    except Exception:
        return False


def _submit_market_qty(trading_client, code: str, qty: int, side: str):
    from alpaca.trading.requests import MarketOrderRequest
    from alpaca.trading.enums import OrderSide, TimeInForce

    req = MarketOrderRequest(
        symbol=code,
        qty=int(qty),
        side=(OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL),
        time_in_force=TimeInForce.DAY,
        extended_hours=bool(B_ALLOW_EXTENDED),
    )
    return trading_client.submit_order(order_data=req)


def _poll_filled_avg_price(trading_client, order_id: str):
    if not order_id:
        return None
    for _ in range(max(FILL_POLL_TIMES, 1)):
        try:
            o = trading_client.get_order_by_id(order_id)
            p = getattr(o, "filled_avg_price", None)
            if p is not None and str(p).strip() != "":
                return float(p)
        except Exception:
            pass
        time.sleep(FILL_POLL_SLEEP)
    return None


def _try_get_position_avg_qty(trading_client, code: str):
    try:
        pos = trading_client.get_open_position(code)
        if not pos:
            return None, None
        avg = getattr(pos, "avg_entry_price", None)
        qty = getattr(pos, "qty", None)
        avg_f = float(avg) if avg is not None and str(avg).strip() != "" else None
        qty_i = int(float(qty)) if qty is not None and str(qty).strip() != "" else None
        if qty_i is not None and qty_i <= 0:
            qty_i = None
        return avg_f, qty_i
    except Exception:
        return None, None


def _wait_and_get_position_fill(trading_client, code: str):
    for i in range(max(B_POS_RETRY, 1)):
        time.sleep(B_POS_WAIT_SEC)
        avg, qty = _try_get_position_avg_qty(trading_client, code)
        if avg is not None and qty is not None:
            return float(avg), int(qty)
        _d(f"[DEBUG] {code} position not ready (try={i+1}/{B_POS_RETRY})")
    return None, None


def _load_one_b_row(conn, code: str):
    sql = f"""
    SELECT stock_code, stock_type,
           trigger_price, close_price,
           cost_price, stop_loss_price, take_profit_price,
           qty, is_bought, can_buy, can_sell,
           last_order_time, last_order_side
    FROM `{OPS_TABLE}`
    WHERE stock_code=%s AND stock_type='B'
    LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (code,))
        return cur.fetchone()


def _get_recent_closes(conn, code: str, n: int = 4):
    sql = f"""
    SELECT `close`
    FROM `{PRICES_TABLE}`
    WHERE `symbol`=%s
    ORDER BY `date` DESC
    LIMIT %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (code, int(n)))
        rows = cur.fetchall() or []
    closes = []
    for r in rows:
        try:
            closes.append(float(r.get("close") or 0))
        except Exception:
            closes.append(0.0)
    return closes


def _update_ops_fields(conn, code: str, **kwargs):
    if not kwargs:
        return
    cols = []
    vals = []
    for k, v in kwargs.items():
        cols.append(f"`{k}`=%s")
        vals.append(v)
    sql = f"UPDATE `{OPS_TABLE}` SET {', '.join(cols)} WHERE stock_code=%s AND stock_type='B';"
    vals.append(code)
    with conn.cursor() as cur:
        cur.execute(sql, tuple(vals))


def _sell_qty(conn, code: str, qty: int, reason: str) -> bool:
    qty = int(qty or 0)
    if qty <= 0:
        return False

    tc = _get_trading_client()
    order = _submit_market_qty(tc, code, qty, side="sell")
    order_id = getattr(order, "id", None) or getattr(order, "order_id", None)

    sql = f"""
    UPDATE `{OPS_TABLE}`
    SET
        qty = GREATEST(qty - %s, 0),
        last_order_side='sell',
        last_order_intent=%s,
        last_order_id=%s,
        last_order_time=NOW(),
        is_bought = IF(qty - %s > 0, 1, 0),
        can_sell  = IF(qty - %s > 0, 1, 0),
        can_buy   = IF(qty - %s > 0, 0, 1),
        stop_loss_price = IF(qty - %s > 0, stop_loss_price, NULL),
        take_profit_price = IF(qty - %s > 0, take_profit_price, NULL)
    WHERE stock_code=%s AND stock_type='B';
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                qty,
                _intent_short(reason),
                str(order_id or ""),
                qty, qty, qty, qty, qty,
                code,
            ),
        )

    print(f"[B SELL] {code} ✅ qty={qty} reason={reason} order_id={order_id}", flush=True)
    return True


def _buy_add_qty(conn, code: str, add_qty: int, reason: str, snap_price: float) -> bool:
    add_qty = int(add_qty or 0)
    if add_qty <= 0:
        return False

    tc = _get_trading_client()
    order = _submit_market_qty(tc, code, add_qty, side="buy")
    order_id = getattr(order, "id", None) or getattr(order, "order_id", None)

    filled_avg = _poll_filled_avg_price(tc, str(order_id or ""))
    fill_price = float(filled_avg) if filled_avg else float(snap_price)

    row = _load_one_b_row(conn, code) or {}
    old_qty = int(row.get("qty") or 0)
    old_cost = float(row.get("cost_price") or 0.0)

    new_qty = old_qty + add_qty
    if old_qty > 0 and old_cost > 0:
        new_cost = (old_qty * old_cost + add_qty * fill_price) / float(new_qty)
    else:
        new_cost = fill_price

    sql = f"""
    UPDATE `{OPS_TABLE}`
    SET
        qty=%s,
        cost_price=%s,
        last_order_side='buy',
        last_order_intent=%s,
        last_order_id=%s,
        last_order_time=NOW(),
        is_bought=1,
        can_sell=1,
        can_buy=0
    WHERE stock_code=%s AND stock_type='B';
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                int(new_qty),
                round(float(new_cost), 2),
                _intent_short(reason),
                str(order_id or ""),
                code,
            ),
        )

    print(f"[B ADD] {code} ✅ add_qty={add_qty} fill≈{fill_price:.2f} new_qty={new_qty} order_id={order_id}", flush=True)
    return True


# =========================
# BUY
# =========================
def strategy_B_buy(code: str) -> bool:
    code = (code or "").strip().upper()
    print(f"[B BUY] {code}", flush=True)

    conn = None
    try:
        conn = _connect()
        row = _load_one_b_row(conn, code)
        if not row:
            return False

        if int(row.get("can_buy") or 0) != 1:
            return False
        if int(row.get("is_bought") or 0) == 1:
            return False

        trigger = float(row.get("trigger_price") or 0)
        if trigger <= 0:
            return False

        if _is_cooldown(row.get("last_order_time"), row.get("last_order_side")):
            return False

        price, prev_close, feed = get_snapshot_realtime(code)
        up_pct = (price - prev_close) / prev_close if prev_close and prev_close > 0 else 0.0

        if not (price > trigger):
            return False
        if not (up_pct > B_MIN_UP_PCT):
            return False

        tc = _get_trading_client()
        buying_power = _get_buying_power(tc)

        if buying_power < float(B_TARGET_NOTIONAL_USD):
            return False
        if buying_power < float(B_MIN_BUYING_POWER):
            return False

        max_use = buying_power * B_BP_USE_RATIO
        target = min(float(B_TARGET_NOTIONAL_USD), float(B_MAX_NOTIONAL_USD), float(max_use))
        if target < float(B_TARGET_NOTIONAL_USD):
            return False

        qty = int(math.floor(float(target) / float(price))) if price > 0 else 0
        if qty <= 0:
            return False

        used_notional = float(qty) * float(price)
        intent = f"B:BUY qty={qty} est={used_notional:.2f} rt={price:.2f} trg={trigger:.2f} up={up_pct*100:.2f}% feed={feed}"

        order = _submit_market_qty(tc, code, qty, side="buy")
        order_id = getattr(order, "id", None) or getattr(order, "order_id", None)

        pos_cost, pos_qty = _wait_and_get_position_fill(tc, code)
        filled_avg = _poll_filled_avg_price(tc, str(order_id or ""))

        if pos_cost is not None and pos_qty is not None:
            cost_price = float(pos_cost)
            qty_to_write = int(pos_qty)
        elif filled_avg is not None:
            cost_price = float(filled_avg)
            qty_to_write = int(qty)
        else:
            cost_price = float(price)
            qty_to_write = int(qty)

        init_sl = min(float(trigger), float(cost_price) * 0.95)
        last_stage = 0

        sql = f"""
        UPDATE `{OPS_TABLE}`
        SET
            is_bought=1,
            qty=%s,
            cost_price=%s,
            close_price=%s,
            stop_loss_price=%s,
            take_profit_price=%s,
            can_sell=0,
            can_buy=0,
            last_order_side='buy',
            last_order_intent=%s,
            last_order_id=%s,
            last_order_time=NOW(),
            updated_at=CURRENT_TIMESTAMP
        WHERE stock_code=%s AND stock_type='B';
        """
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    int(qty_to_write),
                    round(float(cost_price), 2),
                    round(float(cost_price), 2),
                    round(float(init_sl), 2),
                    float(last_stage),
                    _intent_short(intent),
                    str(order_id or ""),
                    code,
                ),
            )

        print(
            f"[B BUY] {code} ✅ order_id={order_id} qty={qty_to_write} cost≈{cost_price:.2f} sl={init_sl:.2f} bp={buying_power:.2f}",
            flush=True,
        )
        return True

    except Exception as e:
        print(f"[B BUY] {code} ❌ error: {e}", flush=True)
        traceback.print_exc()
        try:
            if conn:
                _update_ops_fields(
                    conn,
                    code,
                    last_order_side="buy",
                    last_order_intent=_intent_short(f"B:BUY_ERR {code} {str(e)[:80]}"),
                    last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
        except Exception:
            pass
        return False
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


# =========================
# SELL
# =========================
def strategy_B_sell(code: str) -> bool:
    code = (code or "").strip().upper()
    print(f"[B SELL] {code}", flush=True)

    conn = None
    traded = False
    try:
        conn = _connect()
        row = _load_one_b_row(conn, code)
        if not row:
            return False

        if int(row.get("is_bought") or 0) != 1:
            return False
        if int(row.get("can_sell") or 0) != 1:
            return False

        qty = int(row.get("qty") or 0)
        cost = float(row.get("cost_price") or 0.0)
        trigger = float(row.get("trigger_price") or 0.0)
        sl = float(row.get("stop_loss_price") or 0.0)

        try:
            last_stage = int(float(row.get("take_profit_price") or 0))
        except Exception:
            last_stage = 0

        if qty <= 0 or cost <= 0:
            return False

        price, prev_close, feed = get_snapshot_realtime(code)
        up_pct = (price - cost) / cost if cost > 0 else 0.0

        # 1) 止损：全卖
        if sl and sl > 0 and price <= sl:
            reason = f"STOP price={price:.2f} <= sl={sl:.2f}"
            traded = _sell_qty(conn, code, qty, reason) or traded
            return traded

        stage_rules = [
            (1, 0.05, 1.00, None, None),
            (2, 0.10, 1.05, 0.50, None),
            (3, 0.15, 1.10, None, None),
            (4, 0.20, 1.15, 0.50, None),
            (5, 0.25, 1.20, None, None),
            (6, 0.30, 1.25, None, 0.30),
            (7, 0.35, 1.30, None, None),
            (8, 0.40, 1.35, None, 0.40),
        ]

        for stage, pct, sl_mult, add_ratio, sell_ratio in stage_rules:
            if up_pct >= pct and stage > last_stage:
                new_sl = cost * float(sl_mult)
                if new_sl > (sl or 0):
                    sl = new_sl

                _update_ops_fields(
                    conn,
                    code,
                    stop_loss_price=round(float(sl), 2),
                    take_profit_price=float(stage),
                    updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
                last_stage = stage

                if add_ratio is not None:
                    add_qty = int(math.floor(qty * float(add_ratio)))
                    add_qty = max(add_qty, 1)
                    reason = f"STAGE{stage}_ADD{int(add_ratio*100)} price={price:.2f}"
                    traded = _buy_add_qty(conn, code, add_qty, reason, snap_price=price) or traded

                    row2 = _load_one_b_row(conn, code) or {}
                    qty = int(row2.get("qty") or qty)
                    cost = float(row2.get("cost_price") or cost)
                    sl = float(row2.get("stop_loss_price") or sl)

                if sell_ratio is not None:
                    sell_qty = int(math.floor(qty * float(sell_ratio)))
                    sell_qty = max(sell_qty, 1)
                    reason = f"STAGE{stage}_SELL{int(sell_ratio*100)} price={price:.2f}"
                    traded = _sell_qty(conn, code, sell_qty, reason) or traded

                    row3 = _load_one_b_row(conn, code) or {}
                    qty = int(row3.get("qty") or qty)

        # 10) 收盘结构退出：清仓
        closes = _get_recent_closes(conn, code, n=4)
        if len(closes) >= 4:
            c0, c1, c2, c3 = closes[0], closes[1], closes[2], closes[3]
            min3 = min(c1, c2, c3)
            if c0 > 0 and min3 > 0 and c0 < min3 and qty > 0:
                reason = f"STAGE10_EXIT close0={c0:.2f} < min3={min3:.2f}"
                traded = _sell_qty(conn, code, qty, reason) or traded
                return traded

        # 补一次 init SL
        if (sl is None) or (float(sl or 0) <= 0):
            init_sl = min(float(trigger or 0), float(cost) * 0.95) if cost > 0 else 0
            if init_sl > 0:
                _update_ops_fields(conn, code, stop_loss_price=round(float(init_sl), 2))

        return traded

    except Exception as e:
        print(f"[B SELL] {code} ❌ error: {e}", flush=True)
        traceback.print_exc()
        return False
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass