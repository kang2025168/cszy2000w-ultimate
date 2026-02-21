# -*- coding: utf-8 -*-
"""
app/strategy_b.py

策略B：主循环调用的买卖方法（单股票）

✅ 当前版本：下单成功即视为买入（不等 filled）
- 实时价：Alpaca snapshot（只用 IEX，避免 SIP 403）
- 买入条件：
  1) price > trigger_price
  2) up_pct > B_MIN_UP_PCT（默认 5%）
  3) buying_power >= B_MIN_BUYING_POWER
  4) 冷却：last_order_time + last_order_side=buy
- 下单方式：
  - 优先按 notional（目标金额），若 not fractionable 则 fallback qty
  - qty fallback：按 B_TARGET_NOTIONAL_USD / price 计算（至少1股）
- 买入后写库：
  - is_bought=1（立刻）
  - qty / cost_price / last_order_*
- 兼容：intent 字段长度截断，避免 1406 Data too long

环境变量（常用）：
- ALPACA_MODE=paper|live
- APCA_API_KEY_ID / APCA_API_SECRET_KEY（或 ALPACA_KEY / ALPACA_SECRET）
- DB_HOST/DB_PORT/DB_USER/DB_PASS/DB_NAME
- B_MIN_UP_PCT=0.05
- B_MIN_BUYING_POWER=1
- B_TARGET_NOTIONAL_USD=900
- B_MAX_NOTIONAL_USD=900
- B_BP_USE_RATIO=0.95
- B_COOLDOWN_MINUTES=30
- B_ALLOW_EXTENDED=0
- B_DEBUG=1
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
B_MIN_UP_PCT = float(os.getenv("B_MIN_UP_PCT", "0.05"))              # 5%
B_MIN_BUYING_POWER = float(os.getenv("B_MIN_BUYING_POWER", "1"))     # 你测试用 1

# 你说“每只最多 900”
B_TARGET_NOTIONAL_USD = float(os.getenv("B_TARGET_NOTIONAL_USD", "900"))  # 目标买入金额
B_MAX_NOTIONAL_USD = float(os.getenv("B_MAX_NOTIONAL_USD", "900"))        # 单票上限（硬上限）

B_COOLDOWN_MINUTES = int(os.getenv("B_COOLDOWN_MINUTES", "30"))
B_BP_USE_RATIO = float(os.getenv("B_BP_USE_RATIO", "0.95"))
B_ALLOW_EXTENDED = int(os.getenv("B_ALLOW_EXTENDED", "0"))
B_DEBUG = int(os.getenv("B_DEBUG", "0"))

HTTP_TIMEOUT = float(os.getenv("B_HTTP_TIMEOUT", "6"))

# Alpaca 数据（snapshot）：✅策略B只用 IEX
ALPACA_DATA_BASE_URL = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")
B_DATA_FEED = os.getenv("B_DATA_FEED", "iex").strip().lower()  # ✅ 默认 iex

# 交易环境
TRADE_ENV = os.getenv("TRADE_ENV", os.getenv("ALPACA_MODE", "paper")).strip().lower()
APCA_API_KEY_ID = os.getenv("APCA_API_KEY_ID", "") or os.getenv("ALPACA_KEY", "")
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY", "") or os.getenv("ALPACA_SECRET", "")

# intent 字段长度保护（避免 1406）
MAX_INTENT_LEN = int(os.getenv("B_INTENT_MAXLEN", "70"))

# snapshot 限频保护（避免 429）
SNAPSHOT_MIN_INTERVAL = float(os.getenv("B_SNAPSHOT_MIN_INTERVAL", "0.35"))  # 最小间隔
SNAPSHOT_CACHE_SEC = int(os.getenv("B_SNAPSHOT_CACHE_SEC", "2"))             # 同一票缓存秒数
_snapshot_last_ts = 0.0
_snapshot_cache = {}  # code -> (ts, price, prev_close, feed)


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


def _intent_short(s: str) -> str:
    s = (s or "").strip()
    if len(s) <= MAX_INTENT_LEN:
        return s
    return s[: MAX_INTENT_LEN - 3] + "..."


def _sleep_for_rate_limit():
    global _snapshot_last_ts
    now = time.time()
    gap = now - _snapshot_last_ts
    if gap < SNAPSHOT_MIN_INTERVAL:
        time.sleep(SNAPSHOT_MIN_INTERVAL - gap)
    _snapshot_last_ts = time.time()


def _snapshot_http(code: str, feed: str):
    url = f"{ALPACA_DATA_BASE_URL}/v2/stocks/{code}/snapshot"
    return requests.get(
        url,
        headers=_alpaca_headers(),
        params={"feed": feed},
        timeout=HTTP_TIMEOUT,
    )


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
    """
    ✅只用 IEX，避免 SIP 403
    ✅加缓存 + 最小间隔，减少 429
    """
    code = (code or "").strip().upper()
    if not code:
        raise RuntimeError("empty symbol")

    # cache
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

    # 429：直接抛出，让上层打印错误即可（你也可以在这里加重试）
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


def _write_buy_success(conn, code: str, intent: str, order_id: str, qty: int, cost_price: float, notional: float):
    """
    ✅下单成功即视为买入：直接 is_bought=1
    """
    sql = f"""
    UPDATE `{OPS_TABLE}`
    SET is_bought=1,
        qty=%s,
        cost_price=%s,
        close_price=%s,
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
                int(qty),
                round(float(cost_price), 2),
                round(float(cost_price), 2),
                _intent_short(intent),
                str(order_id or ""),
                code,
            ),
        )


def _write_last_order_fail(conn, code: str, err_msg: str):
    sql = f"""
    UPDATE `{OPS_TABLE}`
    SET last_order_side='buy',
        last_order_intent=%s,
        last_order_time=NOW(),
        updated_at=CURRENT_TIMESTAMP
    WHERE stock_code=%s AND stock_type='B';
    """
    with conn.cursor() as cur:
        cur.execute(sql, (_intent_short(err_msg), code))


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

        # 实时价（IEX）
        price, prev_close, feed = get_snapshot_realtime(code)
        up_pct = (price - prev_close) / prev_close if prev_close and prev_close > 0 else 0.0

        _d(f"[DEBUG] {code} feed={feed} price={price:.4f} prev_close={prev_close:.4f} up={up_pct*100:.2f}% trigger={trigger:.4f}")

        # 条件
        if not (price > trigger):
            _d(f"[DEBUG] {code} price<=trigger skip")
            return
        if not (up_pct > B_MIN_UP_PCT):
            _d(f"[DEBUG] {code} up_pct<{B_MIN_UP_PCT*100:.2f}% skip")
            return

        # 交易客户端 & 购买力
        trading_client = _get_trading_client()
        buying_power = _get_buying_power(trading_client)
        if buying_power < B_MIN_BUYING_POWER:
            _d(f"[DEBUG] {code} buying_power={buying_power:.2f} < {B_MIN_BUYING_POWER} skip")
            return

        # ✅ 目标买入金额：<=900，同时不超过 buying_power*ratio
        max_use = buying_power * B_BP_USE_RATIO
        target = min(B_TARGET_NOTIONAL_USD, B_MAX_NOTIONAL_USD, max_use)
        if target <= 0:
            _d(f"[DEBUG] {code} target<=0 skip")
            return

        intent = f"B:BUY rt={price:.2f} trg={trigger:.2f} up={up_pct*100:.2f}% feed={feed}"

        # ✅ 下单：优先 notional；不可分数则 fallback qty
        order = None
        used_qty = None
        used_notional = None

        try:
            order = _submit_market_notional(trading_client, code, target)
            used_notional = float(target)
            # notional 下单不一定有 qty，先不强求
            used_qty = 0
        except Exception as e:
            msg = str(e)
            # 不可分数：用 qty = floor(target/price)（至少1）
            if ("not fractionable" in msg) or ("40310000" in msg):
                qty = int(math.floor(float(target) / float(price))) if price > 0 else 0
                qty = max(qty, 1)
                _d(f"[DEBUG] {code} not fractionable -> fallback qty={qty}")
                order = _submit_market_qty(trading_client, code, qty)
                used_qty = qty
                used_notional = float(qty) * float(price)
                intent = intent + f" (qty={qty})"
            else:
                raise

        order_id = getattr(order, "id", None) or getattr(order, "order_id", None)

        # ✅ 下单成功即视为买入：直接 is_bought=1
        qty_to_write = int(used_qty or 1)
        cost_price = float(price)

        _write_buy_success(
            conn,
            code=code,
            intent=intent,
            order_id=str(order_id or ""),
            qty=qty_to_write,
            cost_price=cost_price,
            notional=float(used_notional or target),
        )

        # 输出
        if used_qty and used_qty > 0:
            print(f"[B BUY] {code} ✅ order_id={order_id} qty={used_qty} est_notional={used_notional:.2f}", flush=True)
        else:
            print(f"[B BUY] {code} ✅ order_id={order_id} target_notional={target:.2f}", flush=True)

    except Exception as e:
        err_msg = f"B:BUY_ERR {code} {str(e)[:80]}"
        try:
            if conn:
                _write_last_order_fail(conn, code, err_msg)
        except Exception:
            pass
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
    先保留最小 sell 占位（后续你定止盈止损再扩展）
    """
    code = (code or "").strip().upper()
    print(f"[B SELL] {code}", flush=True)
    return