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
B_MIN_UP_PCT = float(os.getenv("B_MIN_UP_PCT", "0.03"))
B_MAX_BUY_UP_PCT = float(os.getenv("B_MAX_BUY_UP_PCT", "0.10"))
B_MIN_BUYING_POWER = float(os.getenv("B_MIN_BUYING_POWER", "1200"))

B_TARGET_NOTIONAL_USD = float(os.getenv("B_TARGET_NOTIONAL_USD", "1000"))
B_MAX_NOTIONAL_USD = float(os.getenv("B_MAX_NOTIONAL_USD", "1000"))

B_COOLDOWN_MINUTES = int(os.getenv("B_COOLDOWN_MINUTES", "30"))
B_BP_USE_RATIO = float(os.getenv("B_BP_USE_RATIO", "0.98"))
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


# ✅ 优化：加重试逻辑，网络抖动自动重试 3 次
def _snapshot_http(code: str, feed: str, retries: int = 3, backoff: float = 1.5):
    url = f"{ALPACA_DATA_BASE_URL}/v2/stocks/{code}/snapshot"
    last_err = None
    for attempt in range(retries):
        try:
            return requests.get(
                url,
                headers=_alpaca_headers(),
                params={"feed": feed},
                timeout=HTTP_TIMEOUT,
            )
        except (requests.exceptions.ConnectTimeout,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError) as e:
            last_err = e
            if attempt < retries - 1:
                wait = backoff * (attempt + 1)
                print(f"[B SNAP] {code} timeout attempt={attempt+1}/{retries} wait={wait:.1f}s err={e}", flush=True)
                time.sleep(wait)
    raise last_err


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


# ✅ 优化：TradingClient 单例，不再每次新建
_trading_client = None

def _get_trading_client():
    global _trading_client
    if _trading_client is not None:
        return _trading_client
    from alpaca.trading.client import TradingClient
    paper = (TRADE_ENV != "live")
    _trading_client = TradingClient(APCA_API_KEY_ID, APCA_API_SECRET_KEY, paper=paper)
    return _trading_client


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

def _get_real_position_qty(trading_client, code: str):
    try:
        pos = trading_client.get_open_position(code)
        if not pos:
            return 0

        qty = getattr(pos, "qty", None)
        if qty is None or str(qty).strip() == "":
            return 0

        return max(int(float(qty)), 0)

    except Exception as e:
        msg = str(e)

        # ✅ Alpaca: position does not exist，当作真实持仓=0
        if "position does not exist" in msg or "40410000" in msg:
            print(f"[B SELL] {code} real position=0: Alpaca position does not exist", flush=True)
            return 0

        print(f"[B SELL] {code} get_open_position error: {e}", flush=True)
        return None

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
           entry_open, entry_close, entry_date,
           b_stage, base_qty,
           qty, is_bought, can_buy, can_sell,
           last_order_time, last_order_side, last_order_id,
           b_stop_pending_since, b_stop_pending_sl
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

def get_snapshot_quote_realtime(code: str):
    code = (code or "").strip().upper()
    if not code:
        raise RuntimeError("empty symbol")

    _sleep_for_rate_limit()

    r = _snapshot_http(code, B_DATA_FEED)
    if r.status_code != 200:
        raise RuntimeError(f"snapshot http {r.status_code}: {r.text[:200]}")

    js = r.json()

    lt = js.get("latestTrade") or {}
    lq = js.get("latestQuote") or {}
    pb = js.get("prevDailyBar") or {}

    last_price = float(lt["p"]) if lt.get("p") is not None else None
    bid = float(lq["bp"]) if lq.get("bp") is not None else None
    ask = float(lq["ap"]) if lq.get("ap") is not None else None
    prev_close = float(pb["c"]) if pb.get("c") is not None else None

    return {
        "last_price": last_price,
        "bid": bid,
        "ask": ask,
        "prev_close": prev_close,
        "feed": B_DATA_FEED,
    }



def _submit_limit_buy_qty(trading_client, code: str, qty: int, limit_price: float):
    from alpaca.trading.requests import LimitOrderRequest
    from alpaca.trading.enums import OrderSide, TimeInForce

    # 盘内用 IOC（吃不到立刻撤），盘前盘后只能用 DAY
    tif = TimeInForce.DAY if B_ALLOW_EXTENDED else TimeInForce.IOC

    req = LimitOrderRequest(
        symbol=code,
        qty=int(qty),
        side=OrderSide.BUY,
        limit_price=round(float(limit_price), 2),
        time_in_force=tif,
        extended_hours=bool(B_ALLOW_EXTENDED),
    )
    return trading_client.submit_order(order_data=req)

def _cancel_open_buy_orders(tc, code: str) -> int:
    """提交新买单前，取消该 symbol 下所有 open 的 buy 单。返回取消数量。"""
    try:
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus, OrderSide

        open_orders = tc.get_orders(filter=GetOrdersRequest(
            status=QueryOrderStatus.OPEN,
            symbols=[code],
            side=OrderSide.BUY,
        )) or []

        n = 0
        for o in open_orders:
            try:
                tc.cancel_order_by_id(str(o.id))
                n += 1
                print(f"[B BUY] {code} canceled orphan buy {o.id}", flush=True)
            except Exception as e:
                print(f"[B BUY] {code} cancel orphan {o.id} failed: {e}", flush=True)
        if n > 0:
            # 给 Alpaca 一点时间消化 cancel
            import time
            time.sleep(0.4)
        return n
    except Exception as e:
        print(f"[B BUY] {code} list orphan buys failed: {e}", flush=True)
        return 0


def _reconcile_fill(tc, code: str, order_id: str, wait_sec: float = 4.0):
    """
    确认订单成交结果。返回 (filled_qty, filled_avg_price)。

    优先级：Alpaca 真实持仓 > 订单 filled_qty/filled_avg_price。
    一旦订单进入终态（filled/canceled/expired/rejected）立即返回，不死等。
    """
    import time

    deadline = time.time() + max(float(wait_sec), 0.5)
    poll = 0.4

    last_filled_qty = 0
    last_filled_avg = 0.0

    while time.time() < deadline:
        # 优先看真实持仓（最准）
        try:
            pos = tc.get_open_position(code)
            qty = int(float(getattr(pos, "qty", 0) or 0))
            avg = float(getattr(pos, "avg_entry_price", 0) or 0)
            if qty > 0 and avg > 0:
                return qty, avg
        except Exception:
            pass  # position does not exist 是常态,不打印

        # 再看订单状态;终态则立刻返回
        try:
            o = tc.get_order_by_id(str(order_id))
            status = str(getattr(o, "status", "") or "").lower()
            filled_qty = int(float(getattr(o, "filled_qty", 0) or 0))
            filled_avg = float(getattr(o, "filled_avg_price", 0) or 0)

            if filled_qty > 0 and filled_avg > 0:
                last_filled_qty = filled_qty
                last_filled_avg = filled_avg

            if status in ("filled", "canceled", "cancelled", "expired", "rejected"):
                return last_filled_qty, last_filled_avg
        except Exception:
            pass

        time.sleep(poll)

    # 超时:再尝试拿一次最新订单数据
    try:
        o = tc.get_order_by_id(str(order_id))
        return (
            int(float(getattr(o, "filled_qty", 0) or 0)),
            float(getattr(o, "filled_avg_price", 0) or 0),
        )
    except Exception:
        return last_filled_qty, last_filled_avg


def _write_buy_cooldown(conn, code: str, order_id, reason: str):
    """买单未成交，只写 cooldown，不写 is_bought。"""
    from datetime import datetime
    try:
        _update_ops_fields(
            conn,
            code,
            last_order_side="buy",
            last_order_intent=_intent_short(f"B:BUY_{reason}"),
            last_order_id=str(order_id or ""),
            last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
    except Exception as e:
        print(f"[B BUY] {code} write cooldown failed: {e}", flush=True)





def _sell_qty(conn, code: str, qty: int, reason: str) -> bool:
    qty = int(qty or 0)
    if qty <= 0:
        return False

    tc = _get_trading_client()

    # ============================================================
    # 1) 真实持仓校验
    # ============================================================
    real_qty = _get_real_position_qty(tc, code)
    if real_qty is None:
        print(f"[B SELL] {code} skip: failed to query Alpaca real position, reason={reason}", flush=True)
        return False

    if real_qty == 0:
        print(f"[B SELL] {code} skip: no real Alpaca position, db_qty={qty}, reason={reason}", flush=True)
        _update_ops_fields(
            conn, code,
            qty=0, is_bought=0, can_sell=0, can_buy=0,
            stop_loss_price=None, take_profit_price=None,
            b_stage=0, base_qty=0,
            b_stop_pending_since=None, b_stop_pending_sl=None,
            last_order_side="sell",
            last_order_intent=_intent_short(f"B:SELL_SKIP no_real_pos {reason}"),
            last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        return False

    if qty > real_qty:
        print(f"[B SELL] {code} adjust sell qty: req_qty={qty} -> real_qty={real_qty}", flush=True)
        qty = real_qty

    row = _load_one_b_row(conn, code) or {}
    old_sl = row.get("stop_loss_price")
    old_tp = row.get("take_profit_price")
    old_b_stage = int(row.get("b_stage") or 0)
    old_base_qty = int(row.get("base_qty") or 0)

    # ============================================================
    # 2) 提交市价卖单
    # ============================================================
    order = _submit_market_qty(tc, code, qty, side="sell")
    order_id = getattr(order, "id", None) or getattr(order, "order_id", None)
    order_status = str(getattr(order, "status", "") or "")
    print(
        f"[B SELL] {code} order submitted: id={order_id} status={order_status} req_qty={qty}",
        flush=True,
    )

    # ============================================================
    # 3) 立即拒单 → 不动持仓状态,只写 cooldown
    # ============================================================
    if order_status.lower() in ("rejected", "expired"):
        print(f"[B SELL] {code} immediate {order_status}, no position change", flush=True)
        _update_ops_fields(
            conn, code,
            last_order_side="sell",
            last_order_intent=_intent_short(f"B:SELL_REJECT {reason} status={order_status}"),
            last_order_id=str(order_id or ""),
            last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        return False

    # ============================================================
    # 4) 对账,拿真实卖出 qty
    # ============================================================
    sold_qty = _reconcile_sell_fill(tc, code, str(order_id), expected_real_qty=real_qty, wait_sec=4.0)

    if sold_qty <= 0:
        final_status = ""
        try:
            o = tc.get_order_by_id(str(order_id))
            final_status = str(getattr(o, "status", "") or "")
        except Exception:
            pass
        print(
            f"[B SELL] {code} no sell fill: order_id={order_id} status={final_status} reason={reason}",
            flush=True,
        )
        _update_ops_fields(
            conn, code,
            last_order_side="sell",
            last_order_intent=_intent_short(f"B:SELL_NO_FILL {reason} status={final_status}"),
            last_order_id=str(order_id or ""),
            last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        return False

    # 防御:卖出量不可能超过请求量
    if sold_qty > qty:
        sold_qty = qty

    if sold_qty < qty:
        print(f"[B SELL] {code} partial fill: req={qty} sold={sold_qty}", flush=True)

    # ============================================================
    # 5) 按真实成交量更新 DB
    # ============================================================
    remaining_qty = max(real_qty - sold_qty, 0)
    new_is_bought = 1 if remaining_qty > 0 else 0
    new_can_sell = 1 if remaining_qty > 0 else 0
    new_can_buy = 0

    new_stop_loss = old_sl if remaining_qty > 0 else None
    new_take_profit = old_tp if remaining_qty > 0 else None
    new_b_stage = old_b_stage if remaining_qty > 0 else 0
    new_base_qty = old_base_qty if remaining_qty > 0 else 0

    sql = f"""
    UPDATE `{OPS_TABLE}`
    SET
        qty=%s,
        last_order_side='sell',
        last_order_intent=%s,
        last_order_id=%s,
        last_order_time=%s,
        is_bought=%s,
        can_sell=%s,
        can_buy=%s,
        stop_loss_price=%s,
        take_profit_price=%s,
        b_stage=%s,
        base_qty=%s
    WHERE stock_code=%s AND stock_type='B';
    """
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                int(remaining_qty),
                _intent_short(f"{reason} sold={sold_qty}"),
                str(order_id or ""),
                now_str,
                int(new_is_bought),
                int(new_can_sell),
                int(new_can_buy),
                new_stop_loss,
                new_take_profit,
                int(new_b_stage),
                int(new_base_qty),
                code,
            ),
        )

    print(
        f"[B SELL] {code} ✅ sold={sold_qty} (req={qty}) remain={remaining_qty} "
        f"stage={new_b_stage} base_qty={new_base_qty} reason={reason} order_id={order_id}",
        flush=True,
    )
    return True




def _buy_add_qty(conn, code: str, add_qty: int, reason: str, snap_price: float) -> bool:
    """
    加仓。snap_price 现在仅作日志参考,真实成本基从 Alpaca 拿(优先 position avg,
    回退 order filled_avg_price)。
    """
    add_qty = int(add_qty or 0)
    if add_qty <= 0:
        return False

    tc = _get_trading_client()

    # ============================================================
    # 1) 提交市价加仓单
    # ============================================================
    order = _submit_market_qty(tc, code, add_qty, side="buy")
    order_id = getattr(order, "id", None) or getattr(order, "order_id", None)
    order_status = str(getattr(order, "status", "") or "")
    print(
        f"[B ADD] {code} order submitted: id={order_id} status={order_status} req_qty={add_qty}",
        flush=True,
    )

    # ============================================================
    # 2) 立即拒单 → 不改 qty/cost
    # ============================================================
    if order_status.lower() in ("rejected", "expired"):
        print(f"[B ADD] {code} immediate {order_status}, no qty/cost change", flush=True)
        _update_ops_fields(
            conn, code,
            last_order_side="buy",
            last_order_intent=_intent_short(f"B:ADD_REJECT {reason} status={order_status}"),
            last_order_id=str(order_id or ""),
            last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        return False

    # ============================================================
    # 3) 对账拿订单成交量和均价
    # ============================================================
    filled_qty, filled_avg = _reconcile_add_fill(tc, str(order_id), wait_sec=4.0)

    if filled_qty <= 0 or filled_avg <= 0:
        final_status = ""
        try:
            o = tc.get_order_by_id(str(order_id))
            final_status = str(getattr(o, "status", "") or "")
        except Exception:
            pass
        print(
            f"[B ADD] {code} no fill: order_id={order_id} status={final_status} reason={reason}",
            flush=True,
        )
        _update_ops_fields(
            conn, code,
            last_order_side="buy",
            last_order_intent=_intent_short(f"B:ADD_NO_FILL {reason} status={final_status}"),
            last_order_id=str(order_id or ""),
            last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        return False

    if filled_qty < add_qty:
        print(f"[B ADD] {code} partial fill: req={add_qty} filled={filled_qty}", flush=True)

    # ============================================================
    # 4) 优先用 Alpaca position avg(最准),失败再手动算
    # ============================================================
    pos_avg = None
    pos_total_qty = None
    try:
        pos = tc.get_open_position(code)
        pos_avg = float(getattr(pos, "avg_entry_price", 0) or 0)
        pos_total_qty = int(float(getattr(pos, "qty", 0) or 0))
    except Exception:
        pass

    row = _load_one_b_row(conn, code) or {}
    old_qty = int(row.get("qty") or 0)
    old_cost = float(row.get("cost_price") or 0.0)

    if pos_avg and pos_avg > 0 and pos_total_qty and pos_total_qty > 0:
        # ✅ 优先用 Alpaca 的真实持仓均价
        new_qty = pos_total_qty
        new_cost = pos_avg
        cost_source = "alpaca_pos"
    else:
        # 回退:用订单 filled_avg + 旧 cost 加权
        new_qty = old_qty + filled_qty
        if old_qty > 0 and old_cost > 0:
            new_cost = (old_qty * old_cost + filled_qty * filled_avg) / float(new_qty)
        else:
            new_cost = filled_avg
        cost_source = "manual_calc"

    # ============================================================
    # 5) 落库
    # ============================================================
    sql = f"""
    UPDATE `{OPS_TABLE}`
    SET
        qty=%s,
        cost_price=%s,
        last_order_side='buy',
        last_order_intent=%s,
        last_order_id=%s,
        last_order_time=%s,
        is_bought=1,
        can_sell=1,
        can_buy=0
    WHERE stock_code=%s AND stock_type='B';
    """
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                int(new_qty),
                round(float(new_cost), 2),
                _intent_short(f"{reason} filled={filled_qty}@{filled_avg:.2f}"),
                str(order_id or ""),
                now_str,
                code,
            ),
        )

    print(
        f"[B ADD] {code} ✅ filled={filled_qty}@{filled_avg:.2f} "
        f"total_qty={new_qty} new_cost={new_cost:.2f} ({cost_source}) order_id={order_id}",
        flush=True,
    )
    return True






def _reconcile_sell_fill(tc, code: str, order_id: str, expected_real_qty: int, wait_sec: float = 4.0) -> int:
    """
    确认卖单成交结果。返回实际卖出 qty。

    优先级：订单 filled_qty > 真实持仓减少量。
    一旦订单进入终态立即返回。
    """
    import time

    deadline = time.time() + max(float(wait_sec), 0.5)
    poll = 0.4

    last_filled_qty = 0

    while time.time() < deadline:
        # ① 直接看订单 filled_qty(最准)
        try:
            o = tc.get_order_by_id(str(order_id))
            status = str(getattr(o, "status", "") or "").lower()
            filled_qty = int(float(getattr(o, "filled_qty", 0) or 0))

            if filled_qty > 0:
                last_filled_qty = filled_qty

            if status in ("filled", "canceled", "cancelled", "expired", "rejected"):
                return last_filled_qty
        except Exception:
            pass

        # ② 兜底:看 position 减少量
        try:
            pos = tc.get_open_position(code)
            cur_qty = int(float(getattr(pos, "qty", 0) or 0))
            sold = max(expected_real_qty - cur_qty, 0)
            if sold > last_filled_qty:
                last_filled_qty = sold


        except Exception as e:
            msg = str(e)
            if "position does not exist" in msg or "40410000" in msg:
                return max(last_filled_qty, expected_real_qty)
            print(f"[B SELL] {code} position check error during reconcile: {e}", flush=True)

        time.sleep(poll)

    # 超时:再尝试拿一次
    try:
        o = tc.get_order_by_id(str(order_id))
        return int(float(getattr(o, "filled_qty", 0) or 0))
    except Exception:
        return last_filled_qty


def _reconcile_add_fill(tc, order_id: str, wait_sec: float = 4.0):
    """
    确认加仓订单成交结果。返回 (filled_qty, filled_avg_price)。

    专用于加仓：只看订单的 filled_qty / filled_avg_price，不看 position
    （因为 position 的 avg 是合并后的均价，不是本次加仓的成交价）。
    """
    import time

    deadline = time.time() + max(float(wait_sec), 0.5)
    poll = 0.4

    last_filled_qty = 0
    last_filled_avg = 0.0

    while time.time() < deadline:
        try:
            o = tc.get_order_by_id(str(order_id))
            status = str(getattr(o, "status", "") or "").lower()
            filled_qty = int(float(getattr(o, "filled_qty", 0) or 0))
            filled_avg = float(getattr(o, "filled_avg_price", 0) or 0)

            if filled_qty > 0 and filled_avg > 0:
                last_filled_qty = filled_qty
                last_filled_avg = filled_avg

            if status in ("filled", "canceled", "cancelled", "expired", "rejected"):
                return last_filled_qty, last_filled_avg
        except Exception:
            pass

        time.sleep(poll)

    # 超时:再尝试拿一次
    try:
        o = tc.get_order_by_id(str(order_id))
        return (
            int(float(getattr(o, "filled_qty", 0) or 0)),
            float(getattr(o, "filled_avg_price", 0) or 0),
        )
    except Exception:
        return last_filled_qty, last_filled_avg





# =========================
# BUY
# =========================
def _get_prev_close_from_db(conn, code: str):
    sql = f"""
    SELECT `close`
    FROM `{PRICES_TABLE}`
    WHERE `symbol`=%s
    ORDER BY `date` DESC
    LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (code,))
        row = cur.fetchone() or {}
    try:
        return float(row.get("close") or 0.0)
    except Exception:
        return 0.0


def strategy_B_buy(code: str) -> bool:
    import math
    import traceback
    from datetime import datetime

    code = (code or "").strip().upper()
    print(f"[B BUY] {code}", flush=True)

    conn = None
    order_id = None
    try:
        conn = _connect()
        row = _load_one_b_row(conn, code)
        if not row:
            print(f"[B BUY] {code} skip: no B row", flush=True)
            return False

        # ============================================================
        # 1) 前置检查
        # ============================================================
        can_buy = int(row.get("can_buy") or 0)
        is_bought = int(row.get("is_bought") or 0)
        trigger = float(row.get("trigger_price") or 0)
        last_order_time = row.get("last_order_time")
        last_order_side = row.get("last_order_side")

        entry_close = float(row.get("entry_close") or 0)
        if entry_close <= 0:
            entry_close = float(row.get("close_price") or trigger or 0)

        if can_buy != 1:
            print(f"[B BUY] {code} skip: can_buy={can_buy}", flush=True)
            return False
        if is_bought == 1:
            print(f"[B BUY] {code} skip: already bought", flush=True)
            return False
        if trigger <= 0:
            print(f"[B BUY] {code} skip: invalid trigger={trigger:.2f}", flush=True)
            return False
        if entry_close <= 0:
            print(f"[B BUY] {code} skip: invalid entry_close={entry_close:.2f}", flush=True)
            return False
        if _is_cooldown(last_order_time, last_order_side):
            print(
                f"[B BUY] {code} skip: cooldown last_side={last_order_side} last_time={last_order_time}",
                flush=True,
            )
            return False

        # ============================================================
        # 2) 行情 + 信号校验
        # ============================================================
        snap = get_snapshot_quote_realtime(code)
        price = float(snap.get("last_price") or 0.0)
        bid = float(snap.get("bid") or 0.0)
        ask = float(snap.get("ask") or 0.0)
        feed = snap.get("feed")

        prev_close = float(snap.get("prev_close") or 0.0)
        if prev_close <= 0:
            prev_close = _get_prev_close_from_db(conn, code)

        day_up_pct = (price - prev_close) / prev_close if prev_close > 0 else 0.0
        entry_up_pct = (price - entry_close) / entry_close if entry_close > 0 else 0.0

        need_price = prev_close * (1.0 + float(B_MIN_UP_PCT)) if prev_close > 0 else 0.0
        max_buy_price = prev_close * (1.0 + float(B_MAX_BUY_UP_PCT)) if prev_close > 0 else 0.0

        print(
            f"[B BUY] {code} quote bid={bid:.2f} ask={ask:.2f} last={price:.2f} "
            f"prev_close={prev_close:.2f} entry_close={entry_close:.2f} "
            f"trigger={trigger:.2f} day_up={day_up_pct*100:.2f}% "
            f"entry_up={entry_up_pct*100:.2f}% feed={feed}",
            flush=True,
        )

        if price <= 0:
            print(f"[B BUY] {code} skip: invalid price={price:.2f}", flush=True)
            return False
        if prev_close <= 0:
            print(f"[B BUY] {code} skip: invalid prev_close={prev_close:.2f}", flush=True)
            return False
        if not (price > trigger):
            print(f"[B BUY] {code} skip: price={price:.2f} <= trigger={trigger:.2f}", flush=True)
            return False
        if not (day_up_pct > B_MIN_UP_PCT):
            print(
                f"[B BUY] {code} skip: day_up={day_up_pct*100:.2f}% <= min_up={B_MIN_UP_PCT*100:.2f}% "
                f"(need>{need_price:.2f})",
                flush=True,
            )
            return False
        if day_up_pct >= B_MAX_BUY_UP_PCT:
            print(
                f"[B BUY] {code} skip: day_up={day_up_pct*100:.2f}% >= max_buy_up={B_MAX_BUY_UP_PCT*100:.2f}% "
                f"(max_buy_price<={max_buy_price:.2f})",
                flush=True,
            )
            return False

        # spread 保护
        if bid > 0 and ask > 0:
            mid = (bid + ask) / 2.0
            spread_pct = (ask - bid) / mid if mid > 0 else 0.0
            if spread_pct > 0.03:
                print(
                    f"[B BUY] {code} skip: spread too wide bid={bid:.2f} ask={ask:.2f} "
                    f"spread={spread_pct*100:.2f}%",
                    flush=True,
                )
                return False

        # ============================================================
        # 3) 资金 + 计算下单参数
        # ============================================================
        tc = _get_trading_client()
        buying_power = _get_buying_power(tc)

        required_bp = max(
            float(B_MIN_BUYING_POWER),
            float(B_TARGET_NOTIONAL_USD) / max(float(B_BP_USE_RATIO), 0.01),
        )
        if buying_power < required_bp:
            print(
                f"[B BUY] {code} skip: buying_power={buying_power:.2f} < required_bp={required_bp:.2f}",
                flush=True,
            )
            return False

        max_use = float(buying_power) * float(B_BP_USE_RATIO)
        target = min(float(B_TARGET_NOTIONAL_USD), float(B_MAX_NOTIONAL_USD), float(max_use))
        if target < float(B_TARGET_NOTIONAL_USD):
            print(
                f"[B BUY] {code} skip: target={target:.2f} < target_notional={float(B_TARGET_NOTIONAL_USD):.2f}",
                flush=True,
            )
            return False

        qty = int(math.floor(float(target) / float(price))) if price > 0 else 0
        if qty <= 0:
            print(f"[B BUY] {code} skip: qty={qty} target={target:.2f} price={price:.2f}", flush=True)
            return False

        used_notional = float(qty) * float(price)

        # 限价：至少高于 last 0.1%（保证能成交），但不超过 max_buy_price（保证不破上限）
        if ask > 0:
            raw_limit = float(ask) * 1.002
        else:
            raw_limit = float(price) * 1.003
        raw_limit = max(raw_limit, float(price) * 1.001)
        limit_price = min(raw_limit, float(max_buy_price))
        limit_price = round(float(limit_price), 2)

        if limit_price <= 0:
            print(f"[B BUY] {code} skip: invalid limit_price={limit_price:.2f}", flush=True)
            return False
        if limit_price < price:
            print(
                f"[B BUY] {code} skip: limit_price={limit_price:.2f} < last_price={price:.2f}, "
                f"max_buy_price={max_buy_price:.2f}",
                flush=True,
            )
            return False

        # ============================================================
        # 4) 提交订单（先清 orphan）
        # ============================================================
        intent = (
            f"B:BUY qty={qty} est={used_notional:.2f} "
            f"rt={price:.2f} bid={bid:.2f} ask={ask:.2f} "
            f"limit={limit_price:.2f} trg={trigger:.2f} "
            f"day_up={day_up_pct*100:.2f}% entry_up={entry_up_pct*100:.2f}% "
            f"feed={feed} mode={'limit_day_ext' if B_ALLOW_EXTENDED else 'limit_ioc'}"
        )

        print(
            f"[B BUY] {code} submit: qty={qty} est={used_notional:.2f} "
            f"price={price:.2f} limit={limit_price:.2f} "
            f"day_up={day_up_pct*100:.2f}% bp={buying_power:.2f}",
            flush=True,
        )

        # 防御：先取消同 symbol 下任何残留的 open 买单
        _cancel_open_buy_orders(tc, code)

        order = _submit_limit_buy_qty(tc, code, qty, limit_price=limit_price)
        order_id = getattr(order, "id", None) or getattr(order, "order_id", None)
        order_status = str(getattr(order, "status", "") or "")
        print(f"[B BUY] {code} order submitted: id={order_id} status={order_status}", flush=True)

        # ============================================================
        # 5) 对账
        # ============================================================
        # 立即终态拒单 → 快速失败
        if order_status.lower() in ("rejected", "expired"):
            print(f"[B BUY] {code} immediate {order_status}, no wait", flush=True)
            _write_buy_cooldown(conn, code, order_id, f"REJECT_IMMEDIATE status={order_status}")
            return False

        # IOC 单瞬时成交;DAY 单等长一点
        fill_wait_sec = 12.0 if B_ALLOW_EXTENDED else 4.0
        filled_qty, filled_avg = _reconcile_fill(tc, code, str(order_id), wait_sec=fill_wait_sec)

        if filled_qty <= 0 or filled_avg <= 0:
            final_status = ""
            try:
                o = tc.get_order_by_id(str(order_id))
                final_status = str(getattr(o, "status", "") or "")
            except Exception:
                pass
            print(
                f"[B BUY] {code} no fill: order_id={order_id} status={final_status}",
                flush=True,
            )
            _write_buy_cooldown(conn, code, order_id, f"NO_FILL status={final_status}")
            return False

        cost_price = float(filled_avg)
        qty_to_write = int(filled_qty)

        # ============================================================
        # 6) 落库
        # ============================================================
        init_sl = max(
            float(entry_close) if entry_close > 0 else 0,
            float(cost_price) * 0.97,
        )

        last_stage = 0
        base_qty = int(qty_to_write)
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = f"""
        UPDATE `{OPS_TABLE}`
        SET
            is_bought=1,
            qty=%s,
            base_qty=%s,
            cost_price=%s,
            close_price=%s,
            stop_loss_price=%s,
            take_profit_price=%s,
            b_stage=%s,
            b_stop_pending_since=NULL,
            b_stop_pending_sl=NULL,
            can_sell=1,
            can_buy=0,
            last_order_side='buy',
            last_order_intent=%s,
            last_order_id=%s,
            last_order_time=%s,
            updated_at=CURRENT_TIMESTAMP
        WHERE stock_code=%s AND stock_type='B';
        """
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    int(qty_to_write),
                    int(base_qty),
                    round(float(cost_price), 2),
                    round(float(cost_price), 2),
                    round(float(init_sl), 2),
                    float(last_stage),
                    int(last_stage),
                    _intent_short(intent),
                    str(order_id or ""),
                    now_str,
                    code,
                ),
            )

        print(
            f"[B BUY] {code} ✅ bought order_id={order_id} qty={qty_to_write} "
            f"base_qty={base_qty} cost≈{cost_price:.2f} sl={init_sl:.2f} "
            f"prev_close={prev_close:.2f} entry_close={entry_close:.2f} "
            f"day_up={day_up_pct*100:.2f}% limit={limit_price:.2f}",
            flush=True,
        )
        return True

    except Exception as e:
        print(f"[B BUY] {code} ❌ error: {e}", flush=True)
        traceback.print_exc()
        try:
            if conn:
                _write_buy_cooldown(conn, code, order_id, f"ERR {str(e)[:60]}")
        except Exception:
            pass
        return False

    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass# =========================
# SELL
# =========================


def strategy_B_sell(code: str) -> bool:
    """
    策略B：持仓后的动态管理（止损 / 分层加仓 / 分层减仓 / 结构退出）
    """

    import math
    import traceback
    from datetime import datetime

    code = (code or "").strip().upper()
    print(f"[B SELL] {code}", flush=True)

    MAX_TOTAL_MULTIPLIER = 2.5
    MIN_ADD_QTY = 1
    ENABLE_STRUCTURE_EXIT_STAGE = 6

    TRAIL_BACKOFF_PCT = 0.03
    DYNAMIC_TRAIL_START_PCT = 0.08

    BLOCK_SAME_DAY_SELL_AFTER_BUY = True
    SAME_DAY_FORCE_SELL_LOSS_PCT = -0.05
    SAME_DAY_FORCE_SELL_WIN_PCT = 0.30

    STAGE_RULES = [
        (1, 0.03, 1.01, 0.30, None),
        (2, 0.07, 1.03, 0.30, None),
        (3, 0.12, 1.06, 0.20, None),
        (4, 0.18, 1.10, None, 0.10),
        (5, 0.25, 1.16, None, 0.20),
        (6, 0.35, 1.25, None, 0.20),
        (7, 0.45, 1.33, None, 0.10),
        (8, 0.60, 1.45, None, 0.10),
        (9, 0.70, 1.55, None, 0.05),
        (10, 0.95, 1.78, None, 0.05),
    ]

    def _safe_int(v, default=0):
        try:
            return int(float(v))
        except Exception:
            return default

    def _safe_float(v, default=0.0):
        try:
            return float(v)
        except Exception:
            return default

    def _flash_wait_minutes(up_pct_):
        if up_pct_ >= 0.30:
            return 3
        if up_pct_ >= 0.15:
            return 2
        return 0

    def _read_stage_from_row(r: dict) -> int:
        if not r:
            return 0
        if "b_stage" in r and r.get("b_stage") is not None:
            try:
                return int(float(r.get("b_stage") or 0))
            except Exception:
                pass
        try:
            return int(float(r.get("take_profit_price") or 0))
        except Exception:
            return 0

    def _write_stage_and_sl(conn_, code_, stage_, sl_):
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            _update_ops_fields(
                conn_,
                code_,
                b_stage=int(stage_),
                stop_loss_price=round(float(sl_), 2),
                updated_at=now_str,
            )
            return
        except Exception:
            pass

        _update_ops_fields(
            conn_,
            code_,
            take_profit_price=float(stage_),
            stop_loss_price=round(float(sl_), 2),
            updated_at=now_str,
        )

    def _parse_dt(v):
        if not v:
            return None
        if isinstance(v, datetime):
            return v
        s = str(v).strip()
        if not s:
            return None
        try:
            return datetime.fromisoformat(s.replace("Z", ""))
        except Exception:
            pass
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(s[:19], fmt)
            except Exception:
                pass
        return None

    def _is_same_day_buy_lock(row_):
        if not BLOCK_SAME_DAY_SELL_AFTER_BUY:
            return False

        last_side = str(row_.get("last_order_side") or "").strip().lower()
        last_time = _parse_dt(row_.get("last_order_time"))

        if last_side != "buy" or last_time is None:
            return False

        return last_time.date() == datetime.now().date()

    def _allow_sell_even_same_day(row_, up_pct_):
        if not _is_same_day_buy_lock(row_):
            return True

        if up_pct_ <= SAME_DAY_FORCE_SELL_LOSS_PCT:
            print(f"[B SELL] {code} same-day lock overridden by loss {up_pct_:.2%}", flush=True)
            return True

        if up_pct_ >= SAME_DAY_FORCE_SELL_WIN_PCT:
            print(f"[B SELL] {code} same-day lock overridden by big win {up_pct_:.2%}", flush=True)
            return True

        return False

    def _latest_row_for_lock(conn_, fallback_row):
        try:
            return _load_one_b_row(conn_, code) or fallback_row
        except Exception:
            return fallback_row

    def _calc_dynamic_trail_sl(cost_, price_, sl_old_):
        cost_ = _safe_float(cost_, 0.0)
        price_ = _safe_float(price_, 0.0)
        sl_old_ = _safe_float(sl_old_, 0.0)

        if cost_ <= 0 or price_ <= cost_:
            return round(sl_old_, 2)

        up_pct_ = (price_ - cost_) / cost_
        if up_pct_ < DYNAMIC_TRAIL_START_PCT:
            return round(sl_old_, 2)

        trail_sl = price_ - cost_ * float(TRAIL_BACKOFF_PCT)
        return round(max(sl_old_, trail_sl), 2)

    def _get_pending_stop_info(row_):
        pending_since = _parse_dt(row_.get("b_stop_pending_since"))
        pending_sl = _safe_float(row_.get("b_stop_pending_sl"), 0.0)
        return pending_since, pending_sl

    def _set_pending_stop(conn_, code_, sl_):
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            _update_ops_fields(
                conn_,
                code_,
                b_stop_pending_since=now_str,
                b_stop_pending_sl=round(float(sl_), 2),
                updated_at=now_str,
            )
            return True
        except Exception as e:
            print(f"[B SELL] {code_} set pending stop failed: {e}", flush=True)
            return False

    def _clear_pending_stop(conn_, code_):
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            _update_ops_fields(
                conn_,
                code_,
                b_stop_pending_since=None,
                b_stop_pending_sl=None,
                updated_at=now_str,
            )
            return True
        except Exception as e:
            print(f"[B SELL] {code_} clear pending stop failed: {e}", flush=True)
            return False

    def _find_highest_hit_stage(up_pct_):
        hit = None
        for rule in STAGE_RULES:
            if up_pct_ >= rule[1]:
                hit = rule
        return hit

    def _find_rule_by_stage(stage_):
        for rule in STAGE_RULES:
            if rule[0] == stage_:
                return rule
        return None

    conn = None
    traded = False

    try:
        conn = _connect()
        row = _load_one_b_row(conn, code)

        if not row:
            print(f"[B SELL] {code} no row", flush=True)
            return False

        is_bought = _safe_int(row.get("is_bought"), 0)
        can_sell = _safe_int(row.get("can_sell"), 0)

        if is_bought != 1:
            print(f"[B SELL] {code} not bought", flush=True)
            return False

        if can_sell != 1:
            print(f"[B SELL] {code} can_sell != 1", flush=True)
            return False

        qty = _safe_int(row.get("qty"), 0)
        cost = _safe_float(row.get("cost_price"), 0.0)
        trigger = _safe_float(row.get("trigger_price"), 0.0)
        sl = _safe_float(row.get("stop_loss_price"), 0.0)
        last_stage = _read_stage_from_row(row)

        if qty <= 0 or cost <= 0:
            print(f"[B SELL] {code} invalid qty/cost qty={qty} cost={cost}", flush=True)
            return False

        base_qty = _safe_int(row.get("base_qty"), 0)
        if base_qty <= 0:
            base_qty = qty

        max_total_qty = max(base_qty, int(math.floor(base_qty * MAX_TOTAL_MULTIPLIER)))

        price, prev_close, feed = get_snapshot_realtime(code)
        price = _safe_float(price, 0.0)

        if price <= 0:
            print(f"[B SELL] {code} invalid realtime price={price}", flush=True)
            return False

        up_pct = (price - cost) / cost if cost > 0 else 0.0
        flash_wait_minutes = _flash_wait_minutes(up_pct)

        print(
            f"[B SELL] {code} price={price:.2f} cost={cost:.2f} up_pct={up_pct:.2%} "
            f"qty={qty} sl={sl:.2f} stage={last_stage} flash_wait={flash_wait_minutes}m feed={feed}",
            flush=True,
        )

        if sl <= 0:
            init_sl = max(float(trigger or 0), float(cost) * 0.97) if cost > 0 else 0
            if init_sl > 0:
                sl = round(float(init_sl), 2)
                try:
                    _update_ops_fields(conn, code, stop_loss_price=sl)
                    print(f"[B SELL] {code} init_sl={sl:.2f}", flush=True)
                except Exception as e:
                    print(f"[B SELL] {code} init_sl write failed: {e}", flush=True)

        if price > cost:
            dyn_sl = _calc_dynamic_trail_sl(cost, price, sl)
            if dyn_sl > sl + 0.01:
                old_sl = sl
                sl = dyn_sl
                try:
                    _update_ops_fields(conn, code, stop_loss_price=sl)
                    print(f"[B SELL] {code} trail old_sl={old_sl:.2f} new_sl={sl:.2f}", flush=True)
                except Exception as e:
                    print(f"[B SELL] {code} dynamic trail write failed: {e}", flush=True)

        row_now = _latest_row_for_lock(conn, row)
        pending_since, pending_sl = _get_pending_stop_info(row_now)

        if pending_since and pending_sl > 0:
            if price > pending_sl:
                _clear_pending_stop(conn, code)
                print(
                    f"[B SELL] {code} pending stop canceled: price={price:.2f} > pending_sl={pending_sl:.2f}",
                    flush=True,
                )
            else:
                elapsed_sec = (datetime.now() - pending_since).total_seconds()
                wait_sec = flash_wait_minutes * 60

                if flash_wait_minutes > 0 and elapsed_sec < wait_sec:
                    left_sec = int(wait_sec - elapsed_sec)
                    print(
                        f"[B SELL] {code} pending stop waiting: price={price:.2f} <= pending_sl={pending_sl:.2f}, "
                        f"left={left_sec}s wait={flash_wait_minutes}m",
                        flush=True,
                    )
                    return False

                reason = f"PENDING_STOP_TIMEOUT price={price:.2f} <= pending_sl={pending_sl:.2f} waited={flash_wait_minutes}m"
                print(f"[B SELL] {code} pending stop timeout sell qty={qty} reason={reason}", flush=True)
                traded = _sell_qty(conn, code, qty, reason) or traded
                if traded:
                    _clear_pending_stop(conn, code)
                return traded

        if sl > 0 and price <= sl:
            row_now = _latest_row_for_lock(conn, row_now)

            if flash_wait_minutes > 0:
                pending_since2, pending_sl2 = _get_pending_stop_info(row_now)

                if not pending_since2:
                    _set_pending_stop(conn, code, sl)
                    print(
                        f"[B SELL] {code} start pending stop: price={price:.2f} <= sl={sl:.2f}, "
                        f"up_pct={up_pct:.2%}, wait={flash_wait_minutes}m",
                        flush=True,
                    )
                    return False

                elapsed_sec = (datetime.now() - pending_since2).total_seconds()
                wait_sec = flash_wait_minutes * 60

                if price > pending_sl2:
                    _clear_pending_stop(conn, code)
                    print(
                        f"[B SELL] {code} pending recovered: price={price:.2f} > pending_sl={pending_sl2:.2f}",
                        flush=True,
                    )
                    return False

                if elapsed_sec < wait_sec:
                    left_sec = int(wait_sec - elapsed_sec)
                    print(
                        f"[B SELL] {code} still pending stop: left={left_sec}s price={price:.2f} pending_sl={pending_sl2:.2f}",
                        flush=True,
                    )
                    return False

                reason = f"PENDING_STOP_TIMEOUT price={price:.2f} <= pending_sl={pending_sl2:.2f} waited={flash_wait_minutes}m"
                print(f"[B SELL] {code} timeout hard stop sell qty={qty} reason={reason}", flush=True)
                traded = _sell_qty(conn, code, qty, reason) or traded
                if traded:
                    _clear_pending_stop(conn, code)
                return traded

            if not _allow_sell_even_same_day(row_now, up_pct):
                print(
                    f"[B SELL] {code} same-day buy lock: price={price:.2f} <= sl={sl:.2f} up_pct={up_pct:.2%}",
                    flush=True,
                )
                return False

            reason = f"STOP price={price:.2f} <= sl={sl:.2f}"
            print(f"[B SELL] {code} hard stop sell qty={qty} reason={reason}", flush=True)
            traded = _sell_qty(conn, code, qty, reason) or traded
            return traded
        else:
            if pending_since:
                _clear_pending_stop(conn, code)

        highest_rule = _find_highest_hit_stage(up_pct)

        if highest_rule is not None:
            highest_stage, highest_pct, highest_sl_mult, highest_add_ratio, highest_sell_ratio = highest_rule

            if highest_stage > last_stage:
                next_stage = last_stage + 1
                next_rule = _find_rule_by_stage(next_stage)

                if highest_stage > next_stage:
                    sl_old = float(sl or 0)
                    stage_sl = round(float(cost) * float(highest_sl_mult), 2)
                    dyn_sl = _calc_dynamic_trail_sl(cost, price, sl_old)
                    sl = max(sl_old, stage_sl, dyn_sl)

                    if highest_sell_ratio is not None and highest_sell_ratio > 0:
                        row_now = _latest_row_for_lock(conn, row)

                        if _allow_sell_even_same_day(row_now, up_pct):
                            raw_sell_qty = int(math.floor(qty * float(highest_sell_ratio)))
                            sell_qty = max(raw_sell_qty, 1)
                            sell_qty = min(sell_qty, qty)

                            reason = (
                                f"JUMP_STAGE{highest_stage}_SELL{int(highest_sell_ratio * 100)} "
                                f"price={price:.2f} qty={sell_qty} last_stage={last_stage}"
                            )

                            print(f"[B SELL] {code} jump sell qty={sell_qty} reason={reason}", flush=True)

                            sell_ok = _sell_qty(conn, code, sell_qty, reason)
                            traded = sell_ok or traded

                            row_after = _load_one_b_row(conn, code) or {}
                            qty_after = _safe_int(row_after.get("qty"), max(qty - sell_qty, 0))
                            cost_after = _safe_float(row_after.get("cost_price"), cost)
                            sl_old_after = _safe_float(row_after.get("stop_loss_price"), sl)

                            stage_sl = round(float(cost_after) * float(highest_sl_mult), 2)
                            dyn_sl = _calc_dynamic_trail_sl(cost_after, price, sl_old_after)
                            sl = max(sl_old_after, stage_sl, dyn_sl)

                            if qty_after > 0:
                                _write_stage_and_sl(conn, code, highest_stage, sl)

                            print(
                                f"[B SELL] {code} stage jump sell_done: last_stage={last_stage} -> highest_stage={highest_stage} "
                                f"left_qty={qty_after} sl={sl:.2f}",
                                flush=True,
                            )
                            return traded

                        print(
                            f"[B SELL] {code} stage jump sell skipped by same-day lock: "
                            f"last_stage={last_stage} -> highest_stage={highest_stage} "
                            f"up_pct={up_pct:.2%}; stage NOT advanced, only update SL",
                            flush=True,
                        )

                        try:
                            _update_ops_fields(
                                conn,
                                code,
                                stop_loss_price=round(float(sl), 2),
                                updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            )
                        except Exception as e:
                            print(f"[B SELL] {code} jump skip sell update sl failed: {e}", flush=True)

                        return traded

                    _write_stage_and_sl(conn, code, highest_stage, sl)
                    print(
                        f"[B SELL] {code} stage jump: last_stage={last_stage} -> highest_stage={highest_stage} "
                        f"up_pct={up_pct:.2%}; skip missed add, no_jump_sell, new_sl={sl:.2f}",
                        flush=True,
                    )
                    return traded

                if next_rule is not None:
                    stage, pct, sl_mult, add_ratio, sell_ratio = next_rule

                    if up_pct >= pct:
                        print(
                            f"[B SELL] {code} hit stage={stage} threshold={pct:.2%} up_pct={up_pct:.2%}",
                            flush=True,
                        )

                        if add_ratio is not None and add_ratio > 0:
                            raw_add_qty = int(math.floor(qty * float(add_ratio)))
                            raw_add_qty = max(raw_add_qty, MIN_ADD_QTY)

                            allow_add_qty = max(0, max_total_qty - qty)
                            add_qty = min(raw_add_qty, allow_add_qty)

                            if add_qty > 0:
                                try:
                                    tc_check = _get_trading_client()
                                    buying_power = _get_buying_power(tc_check)
                                    est_add_cost = float(add_qty) * float(price)
                                    need_cash = est_add_cost * 1.03

                                    if buying_power < need_cash:
                                        print(
                                            f"[B SELL] {code} skip add: buying_power={buying_power:.2f} "
                                            f"< need≈{need_cash:.2f} add_qty={add_qty} price={price:.2f}",
                                            flush=True,
                                        )
                                        add_qty = 0

                                except Exception as e:
                                    print(
                                        f"[B SELL] {code} skip add: failed to check buying_power err={e}",
                                        flush=True,
                                    )
                                    add_qty = 0

                            if add_qty > 0:
                                reason = f"STAGE{stage}_ADD{int(add_ratio * 100)} price={price:.2f} qty={add_qty}"
                                print(f"[B SELL] {code} add qty={add_qty} reason={reason}", flush=True)

                                buy_ok = _buy_add_qty(conn, code, add_qty, reason, snap_price=price)
                                traded = buy_ok or traded

                                row2 = _load_one_b_row(conn, code) or {}
                                qty = _safe_int(row2.get("qty"), qty)
                                cost = _safe_float(row2.get("cost_price"), cost)
                                sl_old = _safe_float(row2.get("stop_loss_price"), sl)

                                stage_sl = round(float(cost) * float(sl_mult), 2)
                                dyn_sl = _calc_dynamic_trail_sl(cost, price, sl_old)
                                sl = max(sl_old, stage_sl, dyn_sl)

                                _write_stage_and_sl(conn, code, stage, sl)

                                print(f"[B SELL] {code} add_done qty={qty} cost={cost:.2f} sl={sl:.2f}", flush=True)
                                return traded

                            sl_old = float(sl or 0)
                            stage_sl = round(float(cost) * float(sl_mult), 2)
                            dyn_sl = _calc_dynamic_trail_sl(cost, price, sl_old)
                            sl = max(sl_old, stage_sl, dyn_sl)

                            _write_stage_and_sl(conn, code, stage, sl)

                            print(f"[B SELL] {code} skip add, stage={stage}, new_sl={sl:.2f}", flush=True)
                            return traded

                        if sell_ratio is not None and sell_ratio > 0:
                            row_now = _latest_row_for_lock(conn, row)

                            if not _allow_sell_even_same_day(row_now, up_pct):
                                print(
                                    f"[B SELL] {code} same-day buy lock: stage={stage} sell skipped up_pct={up_pct:.2%}",
                                    flush=True,
                                )
                                return False

                            raw_sell_qty = int(math.floor(qty * float(sell_ratio)))
                            sell_qty = max(raw_sell_qty, 1)
                            sell_qty = min(sell_qty, qty)

                            reason = f"STAGE{stage}_SELL{int(sell_ratio * 100)} price={price:.2f} qty={sell_qty}"
                            print(f"[B SELL] {code} sell qty={sell_qty} reason={reason}", flush=True)

                            sell_ok = _sell_qty(conn, code, sell_qty, reason)
                            traded = sell_ok or traded

                            row3 = _load_one_b_row(conn, code) or {}
                            qty = _safe_int(row3.get("qty"), max(qty - sell_qty, 0))
                            cost = _safe_float(row3.get("cost_price"), cost)
                            sl_old = _safe_float(row3.get("stop_loss_price"), sl)

                            stage_sl = round(float(cost) * float(sl_mult), 2)
                            dyn_sl = _calc_dynamic_trail_sl(cost, price, sl_old)
                            sl = max(sl_old, stage_sl, dyn_sl)

                            if qty > 0:
                                _write_stage_and_sl(conn, code, stage, sl)

                            print(f"[B SELL] {code} sell_done left_qty={qty} cost={cost:.2f} sl={sl:.2f}", flush=True)
                            return traded

                        sl_old = float(sl or 0)
                        stage_sl = round(float(cost) * float(sl_mult), 2)
                        dyn_sl = _calc_dynamic_trail_sl(cost, price, sl_old)
                        sl = max(sl_old, stage_sl, dyn_sl)

                        _write_stage_and_sl(conn, code, stage, sl)

                        print(f"[B SELL] {code} hold_only stage={stage} new_sl={sl:.2f}", flush=True)
                        return traded

        if last_stage >= ENABLE_STRUCTURE_EXIT_STAGE:
            closes = _get_recent_closes(conn, code, n=4)
            if len(closes) >= 4:
                c0, c1, c2, c3 = closes[0], closes[1], closes[2], closes[3]
                c0 = _safe_float(c0, 0.0)
                c1 = _safe_float(c1, 0.0)
                c2 = _safe_float(c2, 0.0)
                c3 = _safe_float(c3, 0.0)

                min3 = min(c1, c2, c3)

                if c0 > 0 and min3 > 0 and c0 < min3 and qty > 0:
                    row_now = _latest_row_for_lock(conn, row)

                    if not _allow_sell_even_same_day(row_now, up_pct):
                        print(
                            f"[B SELL] {code} same-day buy lock: structure exit skipped up_pct={up_pct:.2%}",
                            flush=True,
                        )
                        return False

                    reason = f"STRUCT_EXIT close0={c0:.2f} < min3={min3:.2f}"
                    print(f"[B SELL] {code} structure exit qty={qty} reason={reason}", flush=True)
                    traded = _sell_qty(conn, code, qty, reason) or traded
                    return traded

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