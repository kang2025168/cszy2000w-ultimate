# -*- coding: utf-8 -*-
"""
app/strategy_b_v2.py
策略B（买卖逻辑）——10 阶段结构化退出（V2 可用版）
"""

import os
import math
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, List

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
B_MIN_BUYING_POWER = float(os.getenv("B_MIN_BUYING_POWER", "2500"))

B_TARGET_NOTIONAL_USD = float(os.getenv("B_TARGET_NOTIONAL_USD", "2500"))
B_MAX_NOTIONAL_USD = float(os.getenv("B_MAX_NOTIONAL_USD", "2500"))

B_COOLDOWN_MINUTES = int(os.getenv("B_COOLDOWN_MINUTES", "30"))
B_BP_USE_RATIO = float(os.getenv("B_BP_USE_RATIO", "0.98"))
B_ALLOW_EXTENDED = int(os.getenv("B_ALLOW_EXTENDED", "0"))
B_DEBUG = int(os.getenv("B_DEBUG", "0"))
HTTP_TIMEOUT = float(os.getenv("B_HTTP_TIMEOUT", "6"))

B_BP_USE_CASH = int(os.getenv("B_BP_USE_CASH", "0"))  # 0=buying_power,1=cash

# 买入后同步 position
B_POS_WAIT_SEC = int(os.getenv("B_POS_WAIT_SEC", "20"))
B_POS_RETRY = int(os.getenv("B_POS_RETRY", "2"))

# 可选：买入时如果点差过大，放弃
B_MAX_SPREAD_PCT = float(os.getenv("B_MAX_SPREAD_PCT", "0.03"))  # 3%

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


# =========================
# 基础工具
# =========================
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

    raise RuntimeError(f"snapshot http {r.status_code}: {r.text[:300]}")


def get_snapshot_quote_realtime(code: str):
    code = (code or "").strip().upper()
    if not code:
        raise RuntimeError("empty symbol")

    _sleep_for_rate_limit()

    r = _snapshot_http(code, B_DATA_FEED)
    if r.status_code != 200:
        raise RuntimeError(f"snapshot http {r.status_code}: {r.text[:300]}")

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


# =========================
# Alpaca
# =========================
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
        print(f"[B POS] {code} get_open_position error: {e}", flush=True)
        return None


def _wait_and_get_position_fill(trading_client, code: str):
    for i in range(max(B_POS_RETRY, 1)):
        time.sleep(B_POS_WAIT_SEC)
        avg, qty = _try_get_position_avg_qty(trading_client, code)
        if avg is not None and qty is not None:
            return float(avg), int(qty)
        _d(f"[DEBUG] {code} position not ready (try={i+1}/{B_POS_RETRY})")
    return None, None


# =========================
# DB helpers
# =========================
def _load_one_b_row(conn, code: str):
    sql = f"""
    SELECT stock_code, stock_type,
           trigger_price, close_price,
           cost_price, stop_loss_price, take_profit_price,
           b_stage, base_qty,
           qty, is_bought, can_buy, can_sell,
           last_order_time, last_order_side,
           last_order_id, last_order_intent,
           updated_at, created_at
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


# =========================
# 配置层
# =========================
@dataclass(frozen=True)
class StageRule:
    stage: int
    profit_pct: float
    sl_mult: float
    add_ratio: Optional[float] = None
    sell_ratio: Optional[float] = None


@dataclass(frozen=True)
class StrategyBConfig:
    max_total_multiplier: float = 1.60
    min_add_qty: int = 1
    enable_structure_exit_stage: int = 6
    allow_jump_stage: bool = False
    use_trailing_stop: bool = False
    trailing_stop_pct: float = 0.08


DEFAULT_STAGE_RULES: List[StageRule] = [
    StageRule(1, 0.03, 1.00, None, None),
    StageRule(2, 0.06, 1.03, 0.15, None),
    StageRule(3, 0.10, 1.06, 0.10, None),
    StageRule(4, 0.15, 1.10, None, None),
    StageRule(5, 0.20, 1.14, 0.10, None),
    StageRule(6, 0.25, 1.18, None, None),
    StageRule(7, 0.35, 1.26, None, 0.20),
    StageRule(8, 0.50, 1.36, None, 0.25),
    StageRule(9, 0.70, 1.50, None, 0.30),
    StageRule(10, 1.00, 1.75, None, 0.20),
]


# =========================
# 数据对象层
# =========================
@dataclass
class PositionState:
    code: str
    qty: int
    cost: float
    trigger: float
    sl: float
    stage: int
    base_qty: int
    is_bought: int
    can_sell: int
    raw_row: dict


@dataclass
class MarketSnapshot:
    price: float
    prev_close: float
    feed: str


@dataclass
class Decision:
    action: str  # HOLD / SELL_ALL / SELL_PART / BUY_ADD / UPDATE_SL
    reason: str
    stage_to_write: Optional[int] = None
    qty: int = 0
    new_sl: Optional[float] = None
    snap_price: Optional[float] = None


# =========================
# 状态 / 规则工具
# =========================
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


def _build_position_state(code: str, row: dict) -> PositionState:
    qty = _safe_int(row.get("qty"), 0)
    base_qty = _safe_int(row.get("base_qty"), 0)
    if base_qty <= 0:
        base_qty = qty

    return PositionState(
        code=(code or "").strip().upper(),
        qty=qty,
        cost=_safe_float(row.get("cost_price"), 0.0),
        trigger=_safe_float(row.get("trigger_price"), 0.0),
        sl=_safe_float(row.get("stop_loss_price"), 0.0),
        stage=_read_stage_from_row(row),
        base_qty=base_qty,
        is_bought=_safe_int(row.get("is_bought"), 0),
        can_sell=_safe_int(row.get("can_sell"), 0),
        raw_row=row,
    )


def _load_snapshot(code: str) -> MarketSnapshot:
    price, prev_close, feed = get_snapshot_realtime(code)
    return MarketSnapshot(
        price=_safe_float(price, 0.0),
        prev_close=_safe_float(prev_close, 0.0),
        feed=str(feed or "")
    )


def _write_stage_and_sl(conn, code: str, stage: int, sl: float):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        _update_ops_fields(
            conn,
            code,
            b_stage=int(stage),
            take_profit_price=float(stage),  # 兼容旧逻辑
            stop_loss_price=round(float(sl), 2),
            updated_at=now_str,
        )
    except Exception as e:
        print(f"[B SELL V2] {code} ⚠️ write stage/sl failed: {e}", flush=True)
        _update_ops_fields(
            conn,
            code,
            take_profit_price=float(stage),
            stop_loss_price=round(float(sl), 2),
            updated_at=now_str,
        )


def _update_only_sl(conn, code: str, sl: float):
    _update_ops_fields(
        conn,
        code,
        stop_loss_price=round(float(sl), 2),
        updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )


def _find_target_rule(stage_rules: List[StageRule], current_stage: int, up_pct: float, allow_jump_stage: bool) -> Optional[StageRule]:
    if allow_jump_stage:
        eligible = [r for r in stage_rules if r.stage > current_stage and up_pct >= r.profit_pct]
        if not eligible:
            return None
        return max(eligible, key=lambda x: x.stage)

    next_stage = current_stage + 1
    for r in stage_rules:
        if r.stage == next_stage:
            return r
    return None


def _calc_initial_sl(pos: PositionState) -> float:
    if pos.cost <= 0:
        return 0.0

    candidates = []
    if pos.trigger > 0:
        candidates.append(pos.trigger)
    candidates.append(pos.cost * 0.98)

    valid = [x for x in candidates if x > 0]
    if not valid:
        return 0.0

    return round(max(valid), 2)


def _calc_stage_sl(cost: float, sl_mult: float, old_sl: float) -> float:
    if cost <= 0:
        return round(old_sl, 2)
    new_sl = round(cost * sl_mult, 2)
    return round(max(old_sl, new_sl), 2)


# =========================
# 下单执行层
# =========================
def _sell_qty(conn, code: str, qty: int, reason: str) -> bool:
    qty = int(qty or 0)
    if qty <= 0:
        return False

    tc = _get_trading_client()

    real_qty = _get_real_position_qty(tc, code)
    if real_qty is None:
        print(f"[B SELL] {code} skip: failed to query Alpaca real position, reason={reason}", flush=True)
        return False

    if real_qty == 0:
        print(f"[B SELL] {code} skip: no real Alpaca position, db_qty={qty}, reason={reason}", flush=True)

        _update_ops_fields(
            conn,
            code,
            qty=0,
            is_bought=0,
            can_sell=0,
            can_buy=0,
            stop_loss_price=None,
            take_profit_price=None,
            b_stage=0,
            last_order_side="sell",
            last_order_intent=_intent_short(f"B:SELL_SKIP no_real_pos {reason}"),
            last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        return False

    if qty > real_qty:
        print(f"[B SELL] {code} adjust sell qty: req_qty={qty} -> real_qty={real_qty}", flush=True)
        qty = real_qty

    row = _load_one_b_row(conn, code) or {}
    old_sl = row.get("stop_loss_price")
    old_tp = row.get("take_profit_price")
    old_stage = _safe_int(row.get("b_stage"), 0)
    base_qty = _safe_int(row.get("base_qty"), 0)

    try:
        order = _submit_market_qty(tc, code, qty, side="sell")
    except Exception as e:
        print(f"[B SELL] {code} ❌ submit sell failed: {e} reason={reason}", flush=True)
        try:
            _update_ops_fields(
                conn,
                code,
                last_order_side="sell",
                last_order_intent=_intent_short(f"B:SELL_ERR {str(e)[:120]}"),
                last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
        except Exception:
            pass
        return False

    order_id = getattr(order, "id", None) or getattr(order, "order_id", None)

    remaining_qty = max(real_qty - qty, 0)
    new_is_bought = 1 if remaining_qty > 0 else 0
    new_can_sell = 1 if remaining_qty > 0 else 0
    new_can_buy = 0
    new_stop_loss = old_sl if remaining_qty > 0 else None
    new_take_profit = old_tp if remaining_qty > 0 else None
    new_stage = old_stage if remaining_qty > 0 else 0
    new_base_qty = base_qty if remaining_qty > 0 else 0

    sql = f"""
    UPDATE `{OPS_TABLE}`
    SET
        qty=%s,
        base_qty=%s,
        b_stage=%s,
        last_order_side='sell',
        last_order_intent=%s,
        last_order_id=%s,
        last_order_time=NOW(),
        is_bought=%s,
        can_sell=%s,
        can_buy=%s,
        stop_loss_price=%s,
        take_profit_price=%s,
        updated_at=CURRENT_TIMESTAMP
    WHERE stock_code=%s AND stock_type='B';
    """

    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                int(remaining_qty),
                int(new_base_qty),
                int(new_stage),
                _intent_short(reason),
                str(order_id or ""),
                int(new_is_bought),
                int(new_can_sell),
                int(new_can_buy),
                new_stop_loss,
                new_take_profit,
                code,
            ),
        )

    print(
        f"[B SELL] {code} ✅ qty={qty} remain={remaining_qty} reason={reason} order_id={order_id}",
        flush=True,
    )
    return True


def _buy_add_qty(conn, code: str, add_qty: int, reason: str, snap_price: float) -> bool:
    add_qty = int(add_qty or 0)
    if add_qty <= 0:
        return False

    tc = _get_trading_client()

    try:
        order = _submit_market_qty(tc, code, add_qty, side="buy")
    except Exception as e:
        print(f"[B ADD] {code} ❌ submit add failed: {e} reason={reason}", flush=True)
        try:
            _update_ops_fields(
                conn,
                code,
                last_order_side="buy",
                last_order_intent=_intent_short(f"B:ADD_ERR {str(e)[:120]}"),
                last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
        except Exception:
            pass
        return False

    order_id = getattr(order, "id", None) or getattr(order, "order_id", None)

    filled_avg = _poll_filled_avg_price(tc, str(order_id or ""))
    fill_price = float(filled_avg) if filled_avg else float(snap_price or 0.0)

    row = _load_one_b_row(conn, code) or {}
    old_qty = _safe_int(row.get("qty"), 0)
    old_cost = _safe_float(row.get("cost_price"), 0.0)
    old_base_qty = _safe_int(row.get("base_qty"), 0)
    old_stage = _safe_int(row.get("b_stage"), _safe_int(row.get("take_profit_price"), 0))

    new_qty = old_qty + add_qty
    if old_qty > 0 and old_cost > 0 and fill_price > 0:
        new_cost = (old_qty * old_cost + add_qty * fill_price) / float(new_qty)
    else:
        new_cost = fill_price if fill_price > 0 else old_cost

    if old_base_qty <= 0:
        old_base_qty = old_qty

    sql = f"""
    UPDATE `{OPS_TABLE}`
    SET
        qty=%s,
        base_qty=%s,
        cost_price=%s,
        b_stage=%s,
        take_profit_price=%s,
        last_order_side='buy',
        last_order_intent=%s,
        last_order_id=%s,
        last_order_time=NOW(),
        is_bought=1,
        can_sell=1,
        can_buy=0,
        updated_at=CURRENT_TIMESTAMP
    WHERE stock_code=%s AND stock_type='B';
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                int(new_qty),
                int(old_base_qty),
                round(float(new_cost), 2),
                int(old_stage),
                float(old_stage),
                _intent_short(reason),
                str(order_id or ""),
                code,
            ),
        )

    print(
        f"[B ADD] {code} ✅ add_qty={add_qty} fill≈{fill_price:.2f} new_qty={new_qty} order_id={order_id}",
        flush=True
    )
    return True


# =========================
# 策略判断层
# =========================
def evaluate_strategy_b(
    pos: PositionState,
    snap: MarketSnapshot,
    stage_rules: List[StageRule],
    config: StrategyBConfig,
    recent_closes: Optional[List[float]] = None,
) -> Decision:
    if pos.is_bought != 1:
        return Decision(action="HOLD", reason="not bought")

    if pos.can_sell != 1:
        return Decision(action="HOLD", reason="can_sell != 1")

    if pos.qty <= 0 or pos.cost <= 0:
        return Decision(action="HOLD", reason=f"invalid qty/cost qty={pos.qty} cost={pos.cost}")

    if snap.price <= 0:
        return Decision(action="HOLD", reason=f"invalid price={snap.price}")

    up_pct = (snap.price - pos.cost) / pos.cost if pos.cost > 0 else 0.0

    # 1) 硬止损
    if pos.sl > 0 and snap.price <= pos.sl:
        return Decision(
            action="SELL_ALL",
            qty=pos.qty,
            reason=f"STOP price={snap.price:.2f} <= sl={pos.sl:.2f}"
        )

    # 2) 初始化止损
    if pos.sl <= 0:
        init_sl = _calc_initial_sl(pos)
        if init_sl > 0:
            return Decision(
                action="UPDATE_SL",
                qty=0,
                new_sl=init_sl,
                reason=f"INIT_SL {init_sl:.2f}"
            )

    # 3) 阶段推进
    rule = _find_target_rule(stage_rules, pos.stage, up_pct, config.allow_jump_stage)
    if rule is not None and up_pct >= rule.profit_pct:
        max_total_qty = max(pos.base_qty, int(math.floor(pos.base_qty * config.max_total_multiplier)))

        # 3.1 加仓
        if rule.add_ratio is not None and rule.add_ratio > 0:
            raw_add_qty = int(math.floor(pos.qty * rule.add_ratio))
            raw_add_qty = max(raw_add_qty, config.min_add_qty)
            allow_add_qty = max(0, max_total_qty - pos.qty)
            add_qty = min(raw_add_qty, allow_add_qty)

            if add_qty > 0:
                return Decision(
                    action="BUY_ADD",
                    qty=add_qty,
                    stage_to_write=rule.stage,
                    snap_price=snap.price,
                    reason=f"STAGE{rule.stage}_ADD{int(rule.add_ratio * 100)} price={snap.price:.2f} qty={add_qty}"
                )

            new_sl = _calc_stage_sl(pos.cost, rule.sl_mult, pos.sl)
            return Decision(
                action="UPDATE_SL",
                qty=0,
                stage_to_write=rule.stage,
                new_sl=new_sl,
                reason=f"STAGE{rule.stage}_SKIP_ADD_MAX_QTY"
            )

        # 3.2 减仓
        if rule.sell_ratio is not None and rule.sell_ratio > 0:
            raw_sell_qty = int(math.floor(pos.qty * rule.sell_ratio))
            sell_qty = max(raw_sell_qty, 1)
            sell_qty = min(sell_qty, pos.qty)

            return Decision(
                action="SELL_PART",
                qty=sell_qty,
                stage_to_write=rule.stage,
                reason=f"STAGE{rule.stage}_SELL{int(rule.sell_ratio * 100)} price={snap.price:.2f} qty={sell_qty}"
            )

        # 3.3 纯抬止损
        new_sl = _calc_stage_sl(pos.cost, rule.sl_mult, pos.sl)
        return Decision(
            action="UPDATE_SL",
            qty=0,
            stage_to_write=rule.stage,
            new_sl=new_sl,
            reason=f"STAGE{rule.stage}_HOLD_ONLY"
        )

    # 4) 结构退出
    if pos.stage >= config.enable_structure_exit_stage and recent_closes and len(recent_closes) >= 4:
        c0, c1, c2, c3 = [_safe_float(x, 0.0) for x in recent_closes[:4]]
        min3 = min(c1, c2, c3)
        if c0 > 0 and min3 > 0 and c0 < min3 and pos.qty > 0:
            return Decision(
                action="SELL_ALL",
                qty=pos.qty,
                reason=f"STRUCT_EXIT close0={c0:.2f} < min3={min3:.2f}"
            )

    return Decision(action="HOLD", reason="no action")


# =========================
# 执行层
# =========================
def execute_strategy_b_decision(conn, code: str, decision: Decision) -> bool:
    if decision.action == "HOLD":
        return False

    if decision.action == "SELL_ALL":
        return _sell_qty(conn, code, decision.qty, decision.reason)

    if decision.action == "SELL_PART":
        return _sell_qty(conn, code, decision.qty, decision.reason)

    if decision.action == "BUY_ADD":
        return _buy_add_qty(conn, code, decision.qty, decision.reason, decision.snap_price or 0.0)

    if decision.action == "UPDATE_SL":
        if decision.new_sl is not None and decision.stage_to_write is not None:
            _write_stage_and_sl(conn, code, decision.stage_to_write, decision.new_sl)
        elif decision.new_sl is not None:
            _update_only_sl(conn, code, decision.new_sl)
        return False

    return False


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
            print(f"[B BUY] {code} no row", flush=True)
            return False

        if _safe_int(row.get("can_buy"), 0) != 1:
            return False
        if _safe_int(row.get("is_bought"), 0) == 1:
            return False

        trigger = _safe_float(row.get("trigger_price"), 0.0)
        if trigger <= 0:
            return False

        if _is_cooldown(row.get("last_order_time"), row.get("last_order_side")):
            print(f"[B BUY] {code} cooldown", flush=True)
            return False

        snap = get_snapshot_quote_realtime(code)
        price = _safe_float(snap["last_price"], 0.0)
        bid = _safe_float(snap["bid"], 0.0)
        ask = _safe_float(snap["ask"], 0.0)
        prev_close = _safe_float(snap["prev_close"], 0.0)
        feed = snap["feed"]

        print(f"[B BUY] {code} bid={bid:.4f} ask={ask:.4f} last={price:.4f} feed={feed}", flush=True)

        if price <= 0:
            return False

        up_pct = (price - prev_close) / prev_close if prev_close and prev_close > 0 else 0.0

        if not (price > trigger):
            return False

        if not (up_pct > B_MIN_UP_PCT):
            return False

        if bid > 0 and ask > 0 and price > 0:
            spread_pct = (ask - bid) / price
            if spread_pct > B_MAX_SPREAD_PCT:
                print(
                    f"[B BUY] {code} spread too wide bid={bid:.2f} ask={ask:.2f} spread_pct={spread_pct:.2%}",
                    flush=True
                )
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
        intent = (
            f"B:BUY qty={qty} est={used_notional:.2f} "
            f"rt={price:.2f} bid={bid:.2f} ask={ask:.2f} "
            f"trg={trigger:.2f} up={up_pct*100:.2f}% feed={feed} mode=market"
        )

        try:
            order = _submit_market_qty(tc, code, qty, side="buy")
        except Exception as e:
            print(f"[B BUY] {code} ❌ submit buy failed: {e}", flush=True)
            try:
                _update_ops_fields(
                    conn,
                    code,
                    last_order_side="buy",
                    last_order_intent=_intent_short(f"B:BUY_ERR {code} {str(e)[:120]}"),
                    last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
            except Exception:
                pass
            return False

        order_id = getattr(order, "id", None) or getattr(order, "order_id", None)

        pos_cost, pos_qty = _wait_and_get_position_fill(tc, code)
        filled_avg = _poll_filled_avg_price(tc, str(order_id or ""))

        if pos_cost is not None and pos_qty is not None and int(pos_qty) > 0:
            cost_price = float(pos_cost)
            qty_to_write = int(pos_qty)
        elif filled_avg is not None:
            cost_price = float(filled_avg)
            qty_to_write = int(qty)
        else:
            cost_price = float(price)
            qty_to_write = int(qty)

        init_sl = max(float(trigger), float(cost_price) * 0.98)
        last_stage = 0
        base_qty = int(qty_to_write)

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
                    int(base_qty),
                    round(float(cost_price), 2),
                    round(float(cost_price), 2),
                    round(float(init_sl), 2),
                    float(last_stage),
                    int(last_stage),
                    _intent_short(intent),
                    str(order_id or ""),
                    code,
                ),
            )

        print(
            f"[B BUY] {code} ✅ order_id={order_id} qty={qty_to_write} "
            f"cost≈{cost_price:.2f} sl={init_sl:.2f} bp={buying_power:.2f}",
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
                    last_order_intent=_intent_short(f"B:BUY_ERR {code} {str(e)[:120]}"),
                    last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
# SELL LEGACY（备份，可删）
# =========================
def strategy_B_sell_legacy(code: str) -> bool:
    code = (code or "").strip().upper()
    print(f"[B SELL LEGACY] {code}", flush=True)

    conn = None
    traded = False
    try:
        conn = _connect()
        row = _load_one_b_row(conn, code)
        if not row:
            return False

        if _safe_int(row.get("is_bought"), 0) != 1:
            return False
        if _safe_int(row.get("can_sell"), 0) != 1:
            return False

        qty = _safe_int(row.get("qty"), 0)
        cost = _safe_float(row.get("cost_price"), 0.0)
        sl = _safe_float(row.get("stop_loss_price"), 0.0)

        try:
            last_stage = int(float(row.get("take_profit_price") or 0))
        except Exception:
            last_stage = 0

        if qty <= 0 or cost <= 0:
            return False

        price, prev_close, feed = get_snapshot_realtime(code)
        up_pct = (price - cost) / cost if cost > 0 else 0.0

        if sl > 0 and price <= sl:
            reason = f"STOP price={price:.2f} <= sl={sl:.2f}"
            traded = _sell_qty(conn, code, qty, reason) or traded
            return traded

        stage_rules = [
            (1, 0.05, 1.03, None, None),
            (2, 0.10, 1.08, 0.20, None),
            (3, 0.15, 1.13, 0.20, None),
            (4, 0.20, 1.18, 0.20, None),
            (5, 0.25, 1.23, 0.20, None),
            (6, 0.30, 1.28, 0.02, None),
            (7, 0.40, 1.35, None, None),
            (8, 0.50, 1.45, None, 0.40),
            (9, 0.70, 1.65, None, 0.30),
            (10, 0.90, 1.85, None, 0.20),
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
                    qty = _safe_int(row2.get("qty"), qty)
                    cost = _safe_float(row2.get("cost_price"), cost)
                    sl = _safe_float(row2.get("stop_loss_price"), sl)

                if sell_ratio is not None:
                    sell_qty = int(math.floor(qty * float(sell_ratio)))
                    sell_qty = max(sell_qty, 1)
                    reason = f"STAGE{stage}_SELL{int(sell_ratio*100)} price={price:.2f}"
                    traded = _sell_qty(conn, code, sell_qty, reason) or traded

                    row3 = _load_one_b_row(conn, code) or {}
                    qty = _safe_int(row3.get("qty"), qty)

        closes = _get_recent_closes(conn, code, n=4)
        if len(closes) >= 4:
            c0, c1, c2, c3 = closes[0], closes[1], closes[2], closes[3]
            min3 = min(c1, c2, c3)
            if c0 > 0 and min3 > 0 and c0 < min3 and qty > 0:
                reason = f"STAGE10_EXIT close0={c0:.2f} < min3={min3:.2f}"
                traded = _sell_qty(conn, code, qty, reason) or traded
                return traded

        return traded

    except Exception as e:
        print(f"[B SELL LEGACY] {code} ❌ error: {e}", flush=True)
        traceback.print_exc()
        return False
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


# =========================
# SELL V2
# =========================
def strategy_B_sell_v2(code: str) -> bool:
    code = (code or "").strip().upper()
    print(f"[B SELL V2] {code}", flush=True)

    config = StrategyBConfig()
    conn = None
    traded = False

    try:
        conn = _connect()
        row = _load_one_b_row(conn, code)
        if not row:
            print(f"[B SELL V2] {code} no row", flush=True)
            return False

        pos = _build_position_state(code, row)
        snap = _load_snapshot(code)

        up_pct = (snap.price - pos.cost) / pos.cost if pos.cost > 0 else 0.0
        print(
            f"[B SELL V2] {code} price={snap.price:.2f} cost={pos.cost:.2f} "
            f"up_pct={up_pct:.2%} qty={pos.qty} sl={pos.sl:.2f} "
            f"stage={pos.stage} feed={snap.feed}",
            flush=True
        )

        recent_closes = None
        if pos.stage >= config.enable_structure_exit_stage:
            recent_closes = _get_recent_closes(conn, code, n=4)

        decision = evaluate_strategy_b(
            pos=pos,
            snap=snap,
            stage_rules=DEFAULT_STAGE_RULES,
            config=config,
            recent_closes=recent_closes,
        )

        print(
            f"[B SELL V2] {code} decision={decision.action} "
            f"qty={decision.qty} stage_to_write={decision.stage_to_write} "
            f"new_sl={decision.new_sl} reason={decision.reason}",
            flush=True
        )

        traded = execute_strategy_b_decision(conn, code, decision) or traded

        if decision.action in ("BUY_ADD", "SELL_PART") and decision.stage_to_write is not None:
            row2 = _load_one_b_row(conn, code) or {}
            pos2 = _build_position_state(code, row2)

            qty_changed_ok = False
            if decision.action == "BUY_ADD":
                qty_changed_ok = (pos2.qty > pos.qty)
            elif decision.action == "SELL_PART":
                qty_changed_ok = (pos2.qty < pos.qty)

            if not qty_changed_ok:
                print(
                    f"[B SELL V2] {code} post-trade qty not changed as expected "
                    f"old_qty={pos.qty} new_qty={pos2.qty}, skip stage/sl update",
                    flush=True
                )
                return traded

            rule = next((r for r in DEFAULT_STAGE_RULES if r.stage == decision.stage_to_write), None)
            if rule is not None:
                new_sl = _calc_stage_sl(pos2.cost, rule.sl_mult, pos2.sl)
                _write_stage_and_sl(conn, code, rule.stage, new_sl)

                print(
                    f"[B SELL V2] {code} post-trade update "
                    f"qty={pos2.qty} cost={pos2.cost:.2f} sl={new_sl:.2f} stage={rule.stage}",
                    flush=True
                )

        return traded

    except Exception as e:
        print(f"[B SELL V2] {code} ❌ error: {e}", flush=True)
        traceback.print_exc()
        return False

    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


# =========================
# 对外统一入口
# =========================
def strategy_B_sell(code: str) -> bool:
    return strategy_B_sell_v2(code)