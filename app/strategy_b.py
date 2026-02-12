# -*- coding: utf-8 -*-
"""
app/strategy_b.py

策略B：主循环调用的买卖方法（单股票）
- 使用 Alpaca snapshot 获取实时价（sip -> iex fallback）
- 买入：price > trigger_price 且 up_pct > 阈值（默认5%）
- 下单：市价 notional（优先成交），若不可分数则 fallback qty=1
- 写回 stock_operations：last_order_* 等字段
"""

import os
import traceback
from datetime import datetime, timedelta

import pymysql
import requests

# =========================
# DB
# =========================
OPS_TABLE = os.getenv("OPS_TABLE", "stock_operations")

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
B_MIN_UP_PCT = float(os.getenv("B_MIN_UP_PCT", "0.05"))             # 默认 5%
B_MIN_BUYING_POWER = float(os.getenv("B_MIN_BUYING_POWER", "1"))    # 你测试用 1
B_MAX_NOTIONAL_USD = float(os.getenv("B_MAX_NOTIONAL_USD", "900"))  # 你说每只最多 900

B_COOLDOWN_MINUTES = int(os.getenv("B_COOLDOWN_MINUTES", "30"))
B_BP_USE_RATIO = float(os.getenv("B_BP_USE_RATIO", "0.95"))
B_ALLOW_EXTENDED = int(os.getenv("B_ALLOW_EXTENDED", "0"))
B_DEBUG = int(os.getenv("B_DEBUG", "0"))

HTTP_TIMEOUT = float(os.getenv("B_HTTP_TIMEOUT", "6"))

# =========================
# Alpaca Data (snapshot)
# =========================
PREFERRED_FEED = os.getenv("ALPACA_DATA_FEED", "sip").strip().lower()
FALLBACK_FEED = os.getenv("ALPACA_DATA_FEED_FALLBACK", "iex").strip().lower()
ALPACA_DATA_BASE_URL = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")

TRADE_ENV = os.getenv("TRADE_ENV", os.getenv("ALPACA_MODE", "paper")).strip().lower()
APCA_API_KEY_ID = os.getenv("APCA_API_KEY_ID", "") or os.getenv("ALPACA_KEY", "")
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY", "") or os.getenv("ALPACA_SECRET", "")

# intent 字段长度保护（避免 1406 Data too long）
MAX_INTENT_LEN = int(os.getenv("B_INTENT_MAXLEN", "70"))


def _d(msg: str):
    if B_DEBUG:
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
    return requests.get(url, headers=_alpaca_headers(), params={"feed": feed}, timeout=HTTP_TIMEOUT)


def _parse_snapshot(js: dict):
    # price：优先 latestTrade.p，否则用 quote 中间价
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

    # SIP 权限不够 -> 自动降级 IEX
    if r.status_code == 403 and "SIP" in (r.text or "").upper():
        _d(f"[DEBUG] feed {PREFERRED_FEED} blocked, fallback -> {FALLBACK_FEED}")
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


def _intent_short(s: str) -> str:
    s = (s or "").strip()
    if len(s) <= MAX_INTENT_LEN:
        return s
    return s[: MAX_INTENT_LEN - 3] + "..."


def _load_one_b_row(conn, code: str):
    sql = f"""
    SELECT stock_code, stock_type, trigger_price, close_price,
           is_bought, can_buy, can_sell,
           last_order_time, last_order_side
    FROM `{OPS_TABLE}`
    WHERE stock_code=%s AND stock_type='B'
    LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (code,))
        return cur.fetchone()


def _write_last_order(conn, code: str, side: str, intent: str, order_id: str):
    sql = f"""
    UPDATE `{OPS_TABLE}`
    SET last_order_side=%s,
        last_order_intent=%s,
        last_order_id=%s,
        last_order_time=NOW(),
        updated_at=CURRENT_TIMESTAMP
    WHERE stock_code=%s AND stock_type='B';
    """
    with conn.cursor() as cur:
        cur.execute(sql, (side, _intent_short(intent), order_id, code))


# =========================
# 对外暴露：主循环调用
# =========================
def strategy_B_buy(code: str):
    code = (code or "").strip().upper()
    print(f"[B BUY] {code}", flush=True)

    conn = None
    try:
        conn = _connect()
        row = _load_one_b_row(conn, code)
        if not row:
            _d(f"[DEBUG] {code} not found stock_type='B'")
            return

        # 基础保护
        if int(row.get("can_buy") or 0) != 1:
            _d(f"[DEBUG] {code} can_buy!=1 skip")
            return
        if int(row.get("is_bought") or 0) == 1:
            _d(f"[DEBUG] {code} is_bought=1 skip")
            return

        trigger = float(row.get("trigger_price") or 0)
        if trigger <= 0:
            _d(f"[DEBUG] {code} trigger<=0 skip")
            return

        if _is_cooldown(row.get("last_order_time"), row.get("last_order_side")):
            _d(f"[DEBUG] {code} cooldown skip")
            return

        # 实时价
        price, prev_close, feed = get_snapshot_realtime(code)
        up_pct = (price - prev_close) / prev_close if prev_close and prev_close > 0 else 0.0

        cond_break = price > trigger
        cond_up = up_pct > B_MIN_UP_PCT

        _d(f"[DEBUG] {code} feed={feed} price={price:.4f} prev_close={prev_close:.4f} up={up_pct*100:.2f}% trigger={trigger:.4f}")

        if not cond_break:
            _d(f"[DEBUG] {code} price<=trigger skip")
            return
        if not cond_up:
            _d(f"[DEBUG] {code} up_pct<{B_MIN_UP_PCT*100:.2f}% skip")
            return

        # 交易客户端 & 购买力
        trading_client = _get_trading_client()
        buying_power = _get_buying_power(trading_client)

        if buying_power < B_MIN_BUYING_POWER:
            _d(f"[DEBUG] {code} buying_power={buying_power:.2f} < {B_MIN_BUYING_POWER} skip")
            return

        # 每只最多 B_MAX_NOTIONAL_USD，同时不超过 buying_power*ratio
        max_use = buying_power * B_BP_USE_RATIO
        notional = min(B_MAX_NOTIONAL_USD, max_use)
        if notional <= 0:
            _d(f"[DEBUG] {code} notional<=0 skip")
            return

        # intent（短）
        intent = f"B:BUY rt={price:.2f} trg={trigger:.2f} up={up_pct*100:.2f}% feed={feed}"

        # 下单：优先 notional；若不可分数，则 fallback qty=1
        order = None
        try:
            order = _submit_market_notional(trading_client, code, notional)
        except Exception as e:
            msg = str(e)
            if "not fractionable" in msg or "40310000" in msg:
                _d(f"[DEBUG] {code} not fractionable -> fallback qty=1")
                order = _submit_market_qty(trading_client, code, 1)
                intent = intent + " (qty=1)"
            else:
                raise

        order_id = getattr(order, "id", None) or getattr(order, "order_id", None)
        _write_last_order(conn, code, "buy", intent, str(order_id or ""))

        print(f"[B BUY] {code} ✅ order_id={order_id} notional={notional:.2f}", flush=True)

    except Exception as e:
        print(f"[B BUY] {code} ❌ error: {e}", flush=True)
        traceback.print_exc()
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def strategy_B_sell(code: str):
    """
    先给你一个最小可用的 sell 占位：
    - 你后续如果要加入止盈止损/条件卖出，再扩展这里
    """
    code = (code or "").strip().upper()
    print(f"[B SELL] {code}", flush=True)
    # TODO: 你后面定义卖出规则后再完善
    return