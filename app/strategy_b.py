# -*- coding: utf-8 -*-
"""
app/strategy_b.py

策略B（买卖逻辑）——按你昨晚的 10 阶段结构化退出

✅ BUY：
- snapshot 实时价（默认 IEX）做条件判断
- 下单：优先 notional；不支持分数股则 fallback qty
- 写库：is_bought=1、qty、cost_price（尽量用 filled_avg_price）、stop_loss_price（初始假突破止损）
- take_profit_price 字段用作 “last_stage” 存档（0/1/2/.../10），防止循环重复加仓/卖出

✅ SELL（十阶段）：
1) 建仓后：若跌破 stop_loss_price（min(trigger_price, cost*0.95)）→ 全部止损
2) 涨到 +5%：stop_loss_price = cost
3) 涨到 +10%：stop_loss_price = cost*1.05，同时加仓 50%
4) 涨到 +15%：stop_loss_price = cost*1.10
5) 涨到 +20%：stop_loss_price = cost*1.15，同时加仓 50%
6) 涨到 +25%：stop_loss_price = cost*1.20
7) 涨到 +30%：stop_loss_price = cost*1.25，同时卖出 30%
8) 涨到 +35%：stop_loss_price = cost*1.30
9) 涨到 +40%：stop_loss_price = cost*1.35，同时卖出 40%
10) 当日收盘价 < 前三天收盘价的最低值：清仓剩余全部

依赖表：
- stock_operations（你给的字段齐全）
- stock_prices_pool（历史K线，至少：symbol/date/close）
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
# 策略参数（可 env 配置）
# =========================
B_MIN_UP_PCT = float(os.getenv("B_MIN_UP_PCT", "0.05"))              # 买入触发：当日涨幅>5%
B_MIN_BUYING_POWER = float(os.getenv("B_MIN_BUYING_POWER", "1"))     # 测试用

B_TARGET_NOTIONAL_USD = float(os.getenv("B_TARGET_NOTIONAL_USD", "900"))
B_MAX_NOTIONAL_USD = float(os.getenv("B_MAX_NOTIONAL_USD", "900"))

B_COOLDOWN_MINUTES = int(os.getenv("B_COOLDOWN_MINUTES", "30"))
B_BP_USE_RATIO = float(os.getenv("B_BP_USE_RATIO", "0.95"))
B_ALLOW_EXTENDED = int(os.getenv("B_ALLOW_EXTENDED", "0"))
B_DEBUG = int(os.getenv("B_DEBUG", "0"))

HTTP_TIMEOUT = float(os.getenv("B_HTTP_TIMEOUT", "6"))

# Alpaca snapshot
ALPACA_DATA_BASE_URL = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")
B_DATA_FEED = os.getenv("B_DATA_FEED", "iex").strip().lower()  # 默认 iex

# 交易环境
TRADE_ENV = (os.getenv("TRADE_ENV") or os.getenv("ALPACA_MODE") or "paper").strip().lower()
APCA_API_KEY_ID = os.getenv("APCA_API_KEY_ID", "") or os.getenv("ALPACA_KEY", "")
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY", "") or os.getenv("ALPACA_SECRET", "")

# intent 字段长度保护
MAX_INTENT_LEN = int(os.getenv("B_INTENT_MAXLEN", "70"))

# snapshot 限频
SNAPSHOT_MIN_INTERVAL = float(os.getenv("B_SNAPSHOT_MIN_INTERVAL", "0.35"))
SNAPSHOT_CACHE_SEC = int(os.getenv("B_SNAPSHOT_CACHE_SEC", "2"))
_snapshot_last_ts = 0.0
_snapshot_cache = {}  # code -> (ts, price, prev_close, feed)

# filled_avg_price 轮询（买入/加仓）
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
    bp = getattr(acct, "buying_power", None)
    if bp is None:
        bp = getattr(acct, "cash", None)
    return float(bp or 0.0)


def _is_cooldown(last_order_time, last_order_side) -> bool:
    if not last_order_time or (last_order_side or "").lower() != "buy":
        return False
    try:
        return (datetime.now() - last_order_time) < timedelta(minutes=B_COOLDOWN_MINUTES)
    except Exception:
        return False


def _submit_market_notional(trading_client, code: str, notional: float):
    from alpaca.trading.requests import MarketOrderRequest
    from alpaca.trading.enums import OrderSide, TimeInForce

    req = MarketOrderRequest(
        symbol=code,
        notional=round(float(notional), 2),
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
        extended_hours=bool(B_ALLOW_EXTENDED),
    )
    return trading_client.submit_order(order_data=req)


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
    """
    尝试拿真实成交均价 filled_avg_price（拿不到就返回 None）
    """
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
    """
    返回最近 n 天 close（按 date DESC）
    """
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
    """
    只更新传入字段
    """
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


def _sell_qty(conn, code: str, qty: int, reason: str):
    qty = int(qty or 0)
    if qty <= 0:
        return None

    tc = _get_trading_client()
    order = _submit_market_qty(tc, code, qty, side="sell")
    order_id = getattr(order, "id", None) or getattr(order, "order_id", None)

    # DB：qty 扣减；如果扣完则清空持仓状态
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
        -- 清仓后把止损/阶段归零，避免下次误触
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
    return str(order_id or "")


def _buy_add_qty(conn, code: str, add_qty: int, reason: str, snap_price: float):
    """
    加仓：市价按 qty 买，更新 qty + cost_price(加权均价)
    """
    add_qty = int(add_qty or 0)
    if add_qty <= 0:
        return None

    tc = _get_trading_client()
    order = _submit_market_qty(tc, code, add_qty, side="buy")
    order_id = getattr(order, "id", None) or getattr(order, "order_id", None)

    # 成交均价尽量取 filled_avg_price
    filled_avg = _poll_filled_avg_price(tc, str(order_id or ""))
    fill_price = float(filled_avg) if filled_avg else float(snap_price)

    # 取现有 qty/cost 做加权
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
    return str(order_id or "")


# =========================
# 对外暴露：BUY
# =========================
def strategy_B_buy(code: str):
    code = (code or "").strip().upper()
    print(f"[B BUY] {code}", flush=True)

    conn = None
    try:
        conn = _connect()
        row = _load_one_b_row(conn, code)
        if not row:
            _d(f"[DEBUG] {code} not found")
            return

        if int(row.get("can_buy") or 0) != 1:
            return
        if int(row.get("is_bought") or 0) == 1:
            return

        trigger = float(row.get("trigger_price") or 0)
        if trigger <= 0:
            return

        if _is_cooldown(row.get("last_order_time"), row.get("last_order_side")):
            return

        # 实时价
        price, prev_close, feed = get_snapshot_realtime(code)
        up_pct = (price - prev_close) / prev_close if prev_close and prev_close > 0 else 0.0

        if not (price > trigger):
            return
        if not (up_pct > B_MIN_UP_PCT):
            return

        tc = _get_trading_client()
        buying_power = _get_buying_power(tc)
        if buying_power < B_MIN_BUYING_POWER:
            return

        max_use = buying_power * B_BP_USE_RATIO
        target = min(B_TARGET_NOTIONAL_USD, B_MAX_NOTIONAL_USD, max_use)
        if target <= 0:
            return

        intent = f"B:BUY rt={price:.2f} trg={trigger:.2f} up={up_pct*100:.2f}% feed={feed}"

        order = None
        used_qty = 0
        used_notional = float(target)

        # 先 notional；失败则 fallback qty
        try:
            order = _submit_market_notional(tc, code, target)
            used_qty = 0
        except Exception as e:
            msg = str(e)
            if ("not fractionable" in msg) or ("40310000" in msg):
                qty = int(math.floor(float(target) / float(price))) if price > 0 else 0
                qty = max(qty, 1)
                order = _submit_market_qty(tc, code, qty, side="buy")
                used_qty = qty
                used_notional = float(qty) * float(price)
                intent += f" (qty={qty})"
            else:
                raise

        order_id = getattr(order, "id", None) or getattr(order, "order_id", None)

        # ✅ 成本价：尽量抓 filled_avg_price，抓不到就用 snapshot 价
        filled_avg = _poll_filled_avg_price(tc, str(order_id or ""))
        cost_price = float(filled_avg) if filled_avg else float(price)

        # ✅ 初始止损：min(trigger_price, cost_price*0.95)
        init_sl = min(float(trigger), float(cost_price) * 0.95)

        # ✅ 初始阶段：0（用 take_profit_price 存 last_stage）
        last_stage = 0

        # qty 写库：notional 下单时，我们先写 1（你表默认 qty=1），避免 0；后续你若要严格一致，可再做一次 position sync
        qty_to_write = int(used_qty) if used_qty > 0 else int(row.get("qty") or 1) or 1

        sql = f"""
        UPDATE `{OPS_TABLE}`
        SET
            is_bought=1,
            qty=%s,
            cost_price=%s,
            close_price=%s,
            stop_loss_price=%s,
            take_profit_price=%s,
            can_sell=1,
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

        print(f"[B BUY] {code} ✅ order_id={order_id} cost≈{cost_price:.2f} sl={init_sl:.2f}", flush=True)

    except Exception as e:
        print(f"[B BUY] {code} ❌ error: {e}", flush=True)
        traceback.print_exc()
        # 失败也写一下 last_order，便于排查
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
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


# =========================
# 对外暴露：SELL（按你昨晚 10 阶段）
# =========================
def strategy_B_sell(code: str):
    code = (code or "").strip().upper()
    print(f"[B SELL] {code}", flush=True)

    conn = None
    try:
        conn = _connect()
        row = _load_one_b_row(conn, code)
        if not row:
            return

        if int(row.get("is_bought") or 0) != 1:
            return
        if int(row.get("can_sell") or 0) != 1:
            return

        qty = int(row.get("qty") or 0)
        cost = float(row.get("cost_price") or 0.0)
        trigger = float(row.get("trigger_price") or 0.0)
        sl = float(row.get("stop_loss_price") or 0.0)

        # last_stage 存在 take_profit_price（不新增字段）
        try:
            last_stage = int(float(row.get("take_profit_price") or 0))
        except Exception:
            last_stage = 0

        if qty <= 0 or cost <= 0:
            return

        # 实时价
        price, prev_close, feed = get_snapshot_realtime(code)
        up_pct = (price - cost) / cost if cost > 0 else 0.0

        # ===== 第 1 层：假突破止损 / 任何阶段止损 =====
        if sl and sl > 0 and price <= sl:
            reason = f"STOP price={price:.2f} <= sl={sl:.2f}"
            _sell_qty(conn, code, qty, reason)
            return

        # ===== 阶段表（你的规则）=====
        # stage: 1..9 对应 5%..40%
        # stage 10：收盘反转清仓（最后做）
        stage_rules = [
            (1, 0.05, 1.00, None, None),   # +5%  sl=cost
            (2, 0.10, 1.05, 0.50, None),   # +10% sl=cost*1.05 + 加仓50%
            (3, 0.15, 1.10, None, None),   # +15% sl=cost*1.10
            (4, 0.20, 1.15, 0.50, None),   # +20% sl=cost*1.15 + 加仓50%
            (5, 0.25, 1.20, None, None),   # +25% sl=cost*1.20
            (6, 0.30, 1.25, None, 0.30),   # +30% sl=cost*1.25 + 卖出30%
            (7, 0.35, 1.30, None, None),   # +35% sl=cost*1.30
            (8, 0.40, 1.35, None, 0.40),   # +40% sl=cost*1.35 + 卖出40%
        ]

        # ===== 逐阶段推进（只执行一次：stage > last_stage 才动作）=====
        for stage, pct, sl_mult, add_ratio, sell_ratio in stage_rules:
            if up_pct >= pct and stage > last_stage:
                # 先抬止损（以 cost 为基准）
                new_sl = cost * float(sl_mult)
                if new_sl > (sl or 0):
                    sl = new_sl

                # 写 stop_loss_price + last_stage（take_profit_price）
                _update_ops_fields(
                    conn,
                    code,
                    stop_loss_price=round(float(sl), 2),
                    take_profit_price=float(stage),
                    updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
                last_stage = stage

                # 加仓
                if add_ratio is not None:
                    add_qty = int(math.floor(qty * float(add_ratio)))
                    add_qty = max(add_qty, 1)
                    reason = f"STAGE{stage}_ADD{int(add_ratio*100)} price={price:.2f}"
                    _buy_add_qty(conn, code, add_qty, reason, snap_price=price)

                    # 重新读一次（qty/cost 变了）
                    row2 = _load_one_b_row(conn, code) or {}
                    qty = int(row2.get("qty") or qty)
                    cost = float(row2.get("cost_price") or cost)
                    sl = float(row2.get("stop_loss_price") or sl)

                # 分批止盈卖出
                if sell_ratio is not None:
                    sell_qty = int(math.floor(qty * float(sell_ratio)))
                    sell_qty = max(sell_qty, 1)
                    reason = f"STAGE{stage}_SELL{int(sell_ratio*100)} price={price:.2f}"
                    _sell_qty(conn, code, sell_qty, reason)

                    # 重新读一次（qty 变了）
                    row3 = _load_one_b_row(conn, code) or {}
                    qty = int(row3.get("qty") or qty)

        # ===== 第 10 阶段：结构性退出（收盘反转清仓）=====
        # “当日收盘价低于前三天收盘价中的最低价，清仓剩余部分”
        # 在自动交易里用：最新一根 close（prices_pool 最新日期） vs 前三天 close 最低
        closes = _get_recent_closes(conn, code, n=4)  # [c0, c1, c2, c3]
        if len(closes) >= 4:
            c0, c1, c2, c3 = closes[0], closes[1], closes[2], closes[3]
            min3 = min(c1, c2, c3)
            if c0 > 0 and min3 > 0 and c0 < min3:
                if qty > 0:
                    reason = f"STAGE10_EXIT close0={c0:.2f} < min3={min3:.2f}"
                    _sell_qty(conn, code, qty, reason)
                return

        # 最后再补一层保护：如果 stop_loss_price 为空（异常情况），就按 min(trigger, cost*0.95) 初始化一次
        if (sl is None) or (float(sl or 0) <= 0):
            init_sl = min(float(trigger or 0), float(cost) * 0.95) if cost > 0 else 0
            if init_sl > 0:
                _update_ops_fields(conn, code, stop_loss_price=round(float(init_sl), 2))

    except Exception as e:
        print(f"[B SELL] {code} ❌ error: {e}", flush=True)
        traceback.print_exc()
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass