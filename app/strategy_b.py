# -*- coding: utf-8 -*-
"""
app/strategy_b.py
策略B（买卖逻辑）——10 阶段结构化退出
"""

import os
import math
import re
import time
import traceback
from datetime import datetime, timedelta

import pymysql
import requests
from ultimate_v1.account_config import credentials_for_profile
from ultimate_v1.yahoo_market_data import get_yahoo_intraday_volume, get_yahoo_stock_quote

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# =========================
# DB
# =========================
OPS_TABLE = os.getenv("OPS_TABLE", "stock_operations")
PRICES_TABLE = os.getenv("B_PRICES_TABLE", "stock_prices_pool")
MONSTER_TABLE = os.getenv("MONSTER_TABLE", "monster_watchlist")

DB = dict(
    host=os.getenv("DB_HOST", "mysql"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASS", ""),
    database=os.getenv("DB_NAME", "cszy2000"),
    charset="utf8mb4",
    autocommit=True,
    cursorclass=pymysql.cursors.DictCursor,
)

# =========================
# 参数
# =========================
from app.b_config import *  # noqa: F403; retain legacy parameter API

ALPACA_DATA_BASE_URL = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")
B_DATA_FEED = os.getenv("B_DATA_FEED", "iex").strip().lower()

TRADE_ENV = (os.getenv("TRADE_ENV") or os.getenv("ALPACA_MODE") or "paper").strip().lower()
APCA_API_KEY_ID, APCA_API_SECRET_KEY, _B_PAPER_ACCOUNT = "", "", True

MAX_INTENT_LEN = int(os.getenv("B_INTENT_MAXLEN", "70"))

SNAPSHOT_MIN_INTERVAL = float(os.getenv("B_SNAPSHOT_MIN_INTERVAL", "0.35"))
SNAPSHOT_CACHE_SEC = int(os.getenv("B_SNAPSHOT_CACHE_SEC", "2"))
_snapshot_last_ts = 0.0
_snapshot_cache = {}  # code -> (ts, price, prev_close, feed)

FILL_POLL_TIMES = int(os.getenv("B_FILL_POLL_TIMES", "5"))
FILL_POLL_SLEEP = float(os.getenv("B_FILL_POLL_SLEEP", "0.4"))


def get_strategy_b_runtime_config() -> dict:
    """Return the live Strategy B buy/sell parameters used by the bot."""
    return {
        "candidate": {
            "source": "stock_prices_pool -> stock_operations",
            "lookback_days": B_READY_LOOKBACK_DAYS,
            "avg_volume_days": B_READY_AVG_VOLUME_DAYS,
            "min_price": B_READY_MIN_PRICE,
            "min_avg_volume": B_READY_MIN_VOLUME,
            "min_day_volume": B_READY_MIN_DAY_VOLUME,
            "min_dollar_volume": B_READY_MIN_DOLLAR_VOLUME,
            "min_gain_pct": B_READY_MIN_GAIN_PCT,
            "max_gain_pct": B_READY_MAX_GAIN_PCT,
            "min_up_streak": B_READY_MIN_UP_STREAK,
            "max_up_streak": B_READY_MAX_UP_STREAK,
            "min_close_position": B_READY_MIN_CLOSE_POSITION,
            "min_volume_ratio": B_READY_MIN_VOLUME_RATIO,
            "require_green_day": B_READY_REQUIRE_GREEN,
            "limit": B_READY_LIMIT,
            "window_trading_days": B_READY_WINDOW_TRADING_DAYS,
            "replace_existing": B_READY_REPLACE,
        },
        "score": {
            "table": B_SCORE_TABLE,
            "top_n": B_SCORE_TOP_N,
            "interval_minutes": B_SCORE_INTERVAL_MINUTES,
            "confirmations": B_SCORE_CONFIRMATIONS,
            "lookback_minutes": B_SCORE_LOOKBACK_MINUTES,
            "log_each_candidate": bool(B_SCORE_LOG_EACH_CANDIDATE),
        },
        "market": {
            "enabled": bool(B_MARKET_FILTER_ENABLED),
            "score_min": B_MARKET_SCORE_MIN,
            "max_vix": B_MARKET_MAX_VIX,
            "warn_vix": B_MARKET_WARN_VIX,
            "max_qqq_drop_pct": B_MARKET_MAX_QQQ_DROP_PCT / 100.0,
            "max_downtrend_qqq_drop_pct": B_MARKET_MAX_DOWNTREND_QQQ_DROP_PCT / 100.0,
            "rule": "risk_state score gate: block risk_off, block panic vix, block downtrend with weak QQQ; allow healthy pullbacks",
        },
        "buy": {
            "price_source": f"alpaca:{B_DATA_FEED}",
            "window": f"{B_BUY_WINDOW_START_LA}-{B_BUY_WINDOW_END_LA} LA",
            "min_day_up_pct": B_MIN_UP_PCT,
            "max_buy_day_up_pct": B_MAX_BUY_UP_PCT,
            "max_entry_up_pct": B_MAX_ENTRY_UP_PCT,
            "min_price": B_MIN_PRICE,
            "max_active_positions": B_MAX_ACTIVE_POSITIONS,
            "max_below_open_pct": B_MAX_BELOW_OPEN_PCT,
            "max_pullback_from_high_pct": B_MAX_PULLBACK_FROM_HIGH_PCT,
            "require_intraday_volume": bool(B_REQUIRE_INTRADAY_VOLUME),
            "intraday_volume_note": "disabled: no reliable realtime volume feed" if B_REQUIRE_INTRADAY_VOLUME != 1 else "enabled: realtime intraday volume/RVOL check",
            "min_prev_day_volume": B_MIN_PREV_DAY_VOLUME,
            "min_realtime_volume": B_MIN_REALTIME_VOLUME,
            "min_realtime_dollar_volume": B_MIN_REALTIME_DOLLAR_VOLUME,
            "rvol_early": B_RVOL_EARLY,
            "rvol_mid": B_RVOL_MID,
            "rvol_late": B_RVOL_LATE,
            "min_buying_power": B_MIN_BUYING_POWER,
            "min_open_buying_power": B_MIN_OPEN_BUYING_POWER,
            "target_notional_usd": B_TARGET_NOTIONAL_USD,
            "max_notional_usd": B_MAX_NOTIONAL_USD,
            "dynamic_sizing": bool(B_USE_DYNAMIC_CAPITAL_SIZING),
            "dynamic_min_trade_notional": B_DYNAMIC_MIN_TRADE_NOTIONAL,
            "dynamic_max_trade_notional": B_DYNAMIC_MAX_TRADE_NOTIONAL,
            "bp_use_ratio": B_BP_USE_RATIO,
            "remainder_buy_min_notional": B_REMAINDER_BUY_MIN_NOTIONAL,
            "cooldown_minutes": B_COOLDOWN_MINUTES,
            "limit_mode": "DAY extended" if B_ALLOW_EXTENDED else "IOC regular",
            "notional_rule": "min(target_notional, max_notional, buying_power * bp_use_ratio)",
        },
        "sell": {
            "order_type": "limit_at_realtime_price",
            "initial_stop_pct": B_INITIAL_STOP_MULT - 1.0,
            "early_lock_start_pct": 0.03,
            "early_lock_profit_pct": 0.01,
            "trail_lock_start_pct": B_TRAIL_LOCK_START_PCT,
            "trail_lock_profit_pct": B_TRAIL_LOCK_SL_MULT - 1.0,
            "initial_stop_grace_seconds": B_INITIAL_STOP_GRACE_SECONDS,
            "catastrophic_stop_loss_pct": B_CATASTROPHIC_STOP_LOSS_PCT,
            "flash_wait_rules": [
                {"min_profit_pct": 0.30, "wait_minutes": 3},
                {"min_profit_pct": 0.15, "wait_minutes": 2},
            ],
            "peak_giveback_rules": [
                {"min_peak_gain_pct": min_gain, "giveback_pct": giveback}
                for min_gain, giveback in B_PEAK_GIVEBACK_RULES
            ],
            "stage_sell_rules": [
                {"stage": stage, "profit_pct": profit, "sell_ratio": sell_ratio}
                for stage, profit, _sl_mult, _add_ratio, sell_ratio in B_STAGE_SELL_RULES
            ],
            "structure_exit_stage": 3,
            "same_day_sell_allowed": True,
            "add_position": False,
        },
    }


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


DECISION_LOG_TAG = "[CSZY_DECISION]"


def _decision_log(side: str, symbol: str, event: str, **fields) -> None:
    parts = [
        DECISION_LOG_TAG,
        "strategy=B",
        f"side={str(side or '').strip().lower()}",
        f"symbol={str(symbol or '').strip().upper()}",
        f"event={str(event or '').strip().lower()}",
    ]
    for key, value in fields.items():
        if value is None:
            continue
        safe_key = re.sub(r"[^A-Za-z0-9_]+", "_", str(key).strip()).strip("_").lower()
        if not safe_key:
            continue
        safe_value = str(value).replace("\n", " ").replace("\r", " ").strip()
        parts.append(f"{safe_key}={safe_value}")
    print(" ".join(parts), flush=True)


def _hhmm_to_minutes(s: str, default: str) -> int:
    raw = (s or default or "").strip()
    try:
        hh, mm = raw.split(":", 1)
        return int(hh) * 60 + int(mm)
    except Exception:
        hh, mm = default.split(":", 1)
        return int(hh) * 60 + int(mm)


def _now_la():
    if LA_TZ:
        return datetime.now(LA_TZ)
    return datetime.now()


def _is_b_buy_window_open():
    now_la = _now_la()
    now_min = now_la.hour * 60 + now_la.minute
    start_min = _hhmm_to_minutes(B_BUY_WINDOW_START_LA, "06:50")
    end_min = _hhmm_to_minutes(B_BUY_WINDOW_END_LA, "10:40")
    if start_min <= end_min:
        is_open = start_min <= now_min <= end_min
    else:
        is_open = now_min >= start_min or now_min <= end_min
    return is_open, now_la.strftime("%H:%M"), B_BUY_WINDOW_START_LA, B_BUY_WINDOW_END_LA


def _alpaca_headers():
    global APCA_API_KEY_ID, APCA_API_SECRET_KEY, _B_PAPER_ACCOUNT
    APCA_API_KEY_ID, APCA_API_SECRET_KEY, _B_PAPER_ACCOUNT = credentials_for_profile(pool="B")
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

    resp = _snapshot_http(code, B_DATA_FEED)
    if resp.status_code != 200:
        raise RuntimeError(f"alpaca snapshot http={resp.status_code} body={resp.text[:160]}")
    price, prev_close = _parse_snapshot(resp.json())
    feed = f"alpaca:{B_DATA_FEED}"
    _snapshot_cache[code] = (time.time(), price, prev_close, feed)
    return price, prev_close, feed


# ✅ 优化：TradingClient 单例，不再每次新建
_trading_client = None
_trading_client_signature = None

def _get_trading_client():
    from ultimate_v1.alpaca_gateway import trading_client
    return trading_client(pool="B")


def _is_quote_transient_error(exc: Exception) -> bool:
    text = str(exc or "").lower()
    return any(
        token in text
        for token in (
            "yahoo",
            "too many requests",
            "rate limit",
            "network is unreachable",
            "failed to establish a new connection",
            "max retries exceeded",
        )
    )


def _market_environment_check() -> tuple[bool, dict]:
    """B 开仓环境闸门：过滤明显风险状态，但允许健康回踩中的强势股。"""
    if B_MARKET_FILTER_ENABLED != 1:
        return True, {"score": 100.0, "reason": "market_filter_disabled"}

    try:
        from ultimate_v1.risk_controller import get_risk_state

        state = get_risk_state()
        trend = str(getattr(state, "market_trend", "") or "")
        mode = str(getattr(state, "mode", "") or "")
        reason = str(getattr(state, "reason", "") or "")
        vix = float(getattr(state, "vix", 0.0) or 0.0)
        qqq_change = float(getattr(state, "qqq_change_pct", 0.0) or 0.0)
        risk_multiplier = float(getattr(state, "risk_multiplier", 1.0) or 0.0)
        block_all = bool(getattr(state, "block_all_new", False))
        block_b = bool(getattr(state, "block_b", False))

        notes = []
        score = 100.0

        if block_all or block_b:
            notes.append("risk_state_block")
            score -= 100.0
        if mode == "RISK_OFF":
            notes.append("risk_off")
            score -= 80.0
        elif mode == "SAFE":
            notes.append("safe_mode")
            score -= 18.0

        if trend == "向下":
            notes.append("qqq_downtrend")
            score -= 42.0
        elif trend == "横盘":
            notes.append("sideways")
            score -= 10.0
        elif trend == "向上":
            notes.append("uptrend")
            score += 8.0
        else:
            notes.append("trend_unknown")
            score -= 8.0

        if vix >= B_MARKET_MAX_VIX:
            notes.append(f"vix_panic={vix:.1f}")
            score -= 55.0
        elif vix >= B_MARKET_WARN_VIX:
            notes.append(f"vix_elevated={vix:.1f}")
            score -= 20.0

        if qqq_change <= B_MARKET_MAX_QQQ_DROP_PCT:
            notes.append(f"qqq_drop={qqq_change:.2f}%")
            score -= 35.0
        elif qqq_change < 0:
            notes.append(f"qqq_pullback={qqq_change:.2f}%")
            score -= min(18.0, abs(qqq_change) * 8.0)

        if trend == "向下" and qqq_change <= B_MARKET_MAX_DOWNTREND_QQQ_DROP_PCT:
            notes.append("downtrend_confirmed_by_intraday")
            score -= 25.0

        if risk_multiplier <= 0:
            notes.append("risk_multiplier_zero")
            score -= 80.0
        elif risk_multiplier < 0.5:
            notes.append(f"risk_multiplier_low={risk_multiplier:.2f}")
            score -= 18.0

        allow = score >= B_MARKET_SCORE_MIN
        detail = {
            "allow": allow,
            "score": round(float(score), 2),
            "min_score": float(B_MARKET_SCORE_MIN),
            "trend": trend or "--",
            "mode": mode or "--",
            "vix": round(float(vix), 2),
            "qqq_change_pct": round(float(qqq_change), 2),
            "risk_multiplier": round(float(risk_multiplier), 2),
            "reason": ",".join(notes) or reason or "market_ok",
            "risk_reason": reason,
        }
        return allow, detail
    except Exception as exc:
        return True, {
            "allow": True,
            "score": 0.0,
            "min_score": float(B_MARKET_SCORE_MIN),
            "reason": f"market_filter_unavailable:{str(exc)[:80]}",
        }


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
    from alpaca.trading.requests import LimitOrderRequest
    from alpaca.trading.enums import OrderSide, TimeInForce

    price, _prev_close, _feed = get_snapshot_realtime(code)
    limit_price = round(float(price or 0.0), 2)
    if limit_price <= 0:
        raise RuntimeError(f"invalid realtime limit price for {code}")
    req = LimitOrderRequest(
        symbol=code,
        qty=int(qty),
        side=(OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL),
        limit_price=limit_price,
        time_in_force=TimeInForce.DAY,
    )
    return trading_client.submit_order(order_data=req)


def _submit_limit_sell_qty(trading_client, code: str, qty: int, limit_price: float):
    from alpaca.trading.requests import LimitOrderRequest
    from alpaca.trading.enums import OrderSide, TimeInForce

    price = round(float(limit_price or 0.0), 2)
    if price <= 0:
        raise RuntimeError(f"invalid sell limit price: {limit_price}")
    tif = TimeInForce.DAY if B_ALLOW_EXTENDED else TimeInForce.IOC
    req = LimitOrderRequest(
        symbol=code,
        qty=int(qty),
        side=OrderSide.SELL,
        limit_price=price,
        time_in_force=tif,
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
    SELECT *
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


def _ensure_monster_watchlist_table(conn):
    """确保 B->F 二次启动观察池存在。"""
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{MONSTER_TABLE}` (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        stock_code VARCHAR(20) NOT NULL,
        source_strategy VARCHAR(8) NOT NULL DEFAULT 'B',
        source_reason VARCHAR(255) NULL,
        last_sell_price DOUBLE NULL,
        last_sell_time DATETIME NULL,
        b_peak_price DOUBLE NULL,
        b_peak_profit DOUBLE NULL,
        watch_status VARCHAR(16) NOT NULL DEFAULT 'WATCHING',
        watch_since DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        last_checked_at DATETIME NULL,
        notes VARCHAR(500) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_symbol_source (stock_code, source_strategy),
        KEY idx_status_source (watch_status, source_strategy),
        KEY idx_watch_since (watch_since)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def _peak_giveback_trigger(cost: float, peak: float) -> float | None:
    """Monotone protection across tiers, reconstructed from the stored peak.

    Retain the preceding tier's boundary price when a new tier widens the
    drawdown allowance. No extra state or reset is needed between holdings.
    """
    if cost <= 0 or peak <= 0:
        return None
    rules = sorted(B_PEAK_GIVEBACK_RULES)
    protection = None
    for i, (gain, pullback) in enumerate(rules):
        if peak + 1e-9 < cost * (1 + gain):
            break
        tier_peak = min(peak, cost * (1 + rules[i + 1][0])) if i + 1 < len(rules) else peak
        candidate = tier_peak * (1 - pullback)
        protection = max(protection or 0, candidate)
    return round(protection, 2) if protection is not None else None


def _write_monster_watchlist(conn, code: str, reason: str, sell_price, row: dict):
    """
    把“B 策略最终清仓卖出”的股票放入妖股观察池。

    注意：
    - 这里只记录，不买回，不改变主程序调度。
    - 使用 ON DUPLICATE KEY，避免同一只股票重复插入 WATCHING 记录。
    - 这个函数由卖出成功后调用；失败只打印日志，不影响卖出结果。
    """
    code = (code or "").strip().upper()
    if not code:
        return

    try:
        _ensure_monster_watchlist_table(conn)
        peak_price = row.get("b_peak_price") if row else None
        peak_profit = row.get("b_peak_profit") if row else None
        cost_price = float(row.get("cost_price") or 0.0) if row else 0.0
        peak_price_f = float(peak_price or 0.0)
        peak_gain_pct = (peak_price_f - cost_price) / cost_price if cost_price > 0 and peak_price_f > 0 else 0.0

        # 只把“曾经涨起来过”的 B 放进妖股观察池。
        # 如果买入后没涨过 3% 就止损，大概率只是买错，不值得让 F 二次追踪。
        if not str(reason or "").startswith("PEAK_GIVEBACK") and peak_gain_pct < float(B_MONSTER_MIN_PEAK_GAIN_PCT):
            print(
                f"[B MONSTER] {code} skip watchlist: peak_gain={peak_gain_pct:.2%} "
                f"< min={B_MONSTER_MIN_PEAK_GAIN_PCT:.2%} reason={reason}",
                flush=True,
            )
            return

        notes = "B策略清仓离场，进入妖股二次启动观察池"

        sql = f"""
        INSERT INTO `{MONSTER_TABLE}` (
            stock_code,
            source_strategy,
            source_reason,
            last_sell_price,
            last_sell_time,
            b_peak_price,
            b_peak_profit,
            watch_status,
            watch_since,
            last_checked_at,
            notes
        )
        VALUES (%s, 'B', %s, %s, NOW(), %s, %s, 'WATCHING', NOW(), NULL, %s)
        ON DUPLICATE KEY UPDATE
            watch_status='WATCHING',
            watch_since=NOW(),
            source_reason=VALUES(source_reason),
            last_sell_price=VALUES(last_sell_price),
            last_sell_time=VALUES(last_sell_time),
            b_peak_price=VALUES(b_peak_price),
            b_peak_profit=VALUES(b_peak_profit),
            last_checked_at=NULL,
            notes=VALUES(notes);
        """
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    code,
                    (reason or "")[:255],
                    sell_price,
                    peak_price,
                    peak_profit,
                    notes,
                ),
            )
        print(f"[B MONSTER] {code} added to watchlist sell_price={sell_price} reason={reason}", flush=True)
    except Exception as e:
        print(f"[B MONSTER] {code} write watchlist failed: {e}", flush=True)


def get_snapshot_quote_realtime(code: str):
    code = (code or "").strip().upper()
    if not code:
        raise RuntimeError("empty symbol")

    resp = _snapshot_http(code, B_DATA_FEED, retries=1)
    if resp.status_code != 200:
        raise RuntimeError(f"alpaca quote http={resp.status_code} body={resp.text[:160]}")
    js = resp.json()
    latest_trade = js.get("latestTrade") or {}
    latest_quote = js.get("latestQuote") or {}
    daily = js.get("dailyBar") or {}
    prev = js.get("prevDailyBar") or {}
    bid = float(latest_quote.get("bp") or 0.0)
    ask = float(latest_quote.get("ap") or 0.0)
    last_price = float(latest_trade.get("p") or 0.0)
    if last_price <= 0 and bid > 0 and ask > 0:
        last_price = (bid + ask) / 2.0

    return {
        "last_price": last_price,
        "bid": bid or None,
        "ask": ask or None,
        "day_open": float(daily.get("o") or 0) or None,
        "day_high": float(daily.get("h") or 0) or None,
        "day_volume": int(float(daily.get("v") or 0)),
        "prev_close": float(prev.get("c") or 0) or None,
        "feed": f"alpaca:{B_DATA_FEED}",
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


def _submit_limit_qty_ext(trading_client, code: str, qty: int, side: str, limit_price: float):
    from alpaca.trading.requests import LimitOrderRequest
    from alpaca.trading.enums import OrderSide, TimeInForce

    side = (side or "").strip().lower()
    req = LimitOrderRequest(
        symbol=code,
        qty=int(qty),
        side=(OrderSide.BUY if side == "buy" else OrderSide.SELL),
        limit_price=round(float(limit_price), 2),
        time_in_force=TimeInForce.DAY,
        extended_hours=True,
    )
    return trading_client.submit_order(order_data=req)


def _get_extended_quote_realtime(code: str):
    code = (code or "").strip().upper()
    if not code:
        raise RuntimeError("empty symbol")

    data = get_snapshot_quote_realtime(code)
    last = float(data.get("last_price") or 0.0)
    bid = float(data.get("bid") or 0.0)
    ask = float(data.get("ask") or 0.0)
    regular_close = float(data.get("prev_close") or 0.0)
    prev_close = regular_close
    if last <= 0 or regular_close <= 0:
        raise RuntimeError(f"alpaca extended quote missing fields: last={last} regular_close={regular_close}")

    return {
        "last": last,
        "bid": bid,
        "ask": ask,
        "regular_close": regular_close,
        "prev_close": prev_close,
        "feed": data.get("feed") or f"alpaca:{B_DATA_FEED}",
    }

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
    """Return only this order's fills; existing holdings are not fills."""
    from ultimate_v1.order_fills import wait_for_fill
    qty, price, _status = wait_for_fill(tc, order_id, wait_sec)
    return qty, price


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


def strategy_B_extended_record(code: str, phase: str = "") -> bool:
    code = (code or "").strip().upper()
    conn = None
    try:
        conn = _connect()
        row = _load_one_b_row(conn, code)
        if not row or int(row.get("is_bought") or 0) != 1:
            return False
        q = _get_extended_quote_realtime(code)
        price = float(q["last"])
        regular_close = float(q["prev_close"] or q["regular_close"])
        qty = int(float(row.get("qty") or 0))
        cost = float(row.get("cost_price") or 0.0)
        old_peak = float(row.get("b_peak_price") or cost or regular_close)
        peak_price = max(old_peak, price)
        gain = (price - regular_close) / regular_close if regular_close > 0 else 0.0
        updates = {
            "b_last_profit": round((price - cost) * qty, 4) if cost > 0 and qty > 0 else 0,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        if peak_price > old_peak + 0.005:
            updates["b_peak_price"] = round(peak_price, 4)
            updates["b_peak_profit"] = round(max((peak_price - cost) * qty, 0.0), 4) if cost > 0 and qty > 0 else 0
        _update_ops_fields(conn, code, **updates)
        print(
            f"[B EXT RECORD] {code} phase={phase} price={price:.2f} regular_close={regular_close:.2f} "
            f"ext_gain={gain:.2%} peak={peak_price:.2f}",
            flush=True,
        )
        return False
    except Exception as e:
        print(f"[B EXT RECORD] {code} error: {e}", flush=True)
        return False
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def strategy_B_premarket_manage(code: str) -> bool:
    code = (code or "").strip().upper()
    conn = None
    try:
        conn = _connect()
        row = _load_one_b_row(conn, code)
        if not row or int(row.get("is_bought") or 0) != 1:
            return False
        qty = int(float(row.get("qty") or 0))
        cost = float(row.get("cost_price") or 0.0)
        stage = int(float(row.get("b_stage") or 0))
        if qty <= 0:
            return False

        q = _get_extended_quote_realtime(code)
        price = float(q["last"])
        regular_close = float(q["prev_close"] or q["regular_close"])
        old_peak = float(row.get("b_peak_price") or cost or regular_close)
        peak_price = max(old_peak, price)
        gain = (price - regular_close) / regular_close if regular_close > 0 else 0.0
        peak_gain = (peak_price - regular_close) / regular_close if regular_close > 0 else 0.0
        if peak_price > old_peak + 0.005:
            _update_ops_fields(
                conn, code,
                b_peak_price=round(peak_price, 4),
                b_peak_profit=round(max((peak_price - cost) * qty, 0.0), 4) if cost > 0 else 0,
                updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )

        print(
            f"[B PRE] {code} price={price:.2f} regular_close={regular_close:.2f} "
            f"gain={gain:.2%} peak_gain={peak_gain:.2%} stage={stage}",
            flush=True,
        )

        if peak_gain >= 0.10:
            trigger = round(peak_price * 0.97, 2)
            if price <= trigger:
                reason = f"PREMARKET_GIVEBACK price={price:.2f} <= trigger={trigger:.2f} peak={peak_price:.2f}"
                return _sell_qty_limit_ext(conn, code, qty, limit_price=trigger, reason=reason)

            if stage < 1 and gain >= 0.10:
                sell_qty = max(int(math.floor(qty * 0.20)), 1)
                reason = f"PREMARKET_STAGE1_SELL20 price={price:.2f} gain={gain:.2%}"
                ok = _sell_qty_limit_ext(conn, code, sell_qty, limit_price=round(price * 0.997, 2), reason=reason)
                if ok:
                    _update_ops_fields(conn, code, b_stage=1, take_profit_price=1)
                return ok

        return False
    except Exception as e:
        print(f"[B PRE] {code} error: {e}", flush=True)
        if not _is_quote_transient_error(e):
            traceback.print_exc()
        return False
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def strategy_B_afterhours_add(code: str) -> bool:
    code = (code or "").strip().upper()
    conn = None
    try:
        conn = _connect()
        row = _load_one_b_row(conn, code)
        if not row or int(row.get("is_bought") or 0) != 1:
            return False
        last_intent = str(row.get("last_order_intent") or "")
        last_time = row.get("last_order_time")
        if "AH_ADD" in last_intent and last_time:
            try:
                if (last_time.date() if hasattr(last_time, "date") else datetime.fromisoformat(str(last_time)[:19]).date()) == datetime.now().date():
                    print(f"[B AH ADD] {code} skip: already attempted today", flush=True)
                    return False
            except Exception:
                pass

        tc = _get_trading_client()
        real_qty = _get_real_position_qty(tc, code)
        if real_qty is None or real_qty <= 0:
            return False

        q = _get_extended_quote_realtime(code)
        price = float(q["last"])
        regular_close = float(q["regular_close"])
        after_gain = (price - regular_close) / regular_close if regular_close > 0 else 0.0
        limit_price = round(price, 2)
        add_qty = max(int(math.floor(real_qty * 0.50)), 1)

        print(
            f"[B AH ADD] {code} price={price:.2f} regular_close={regular_close:.2f} "
            f"after_gain={after_gain:.2%} add_qty={add_qty} limit={limit_price:.2f}",
            flush=True,
        )

        if after_gain < 0.05:
            return False

        _cancel_open_buy_orders(tc, code)
        order = _submit_limit_qty_ext(tc, code, add_qty, side="buy", limit_price=limit_price)
        order_id = getattr(order, "id", None) or getattr(order, "order_id", None)
        status = str(getattr(order, "status", "") or "")
        if status.lower() in ("rejected", "expired"):
            _update_ops_fields(
                conn, code,
                last_order_side="buy",
                last_order_intent=_intent_short(f"B:AH_ADD_REJECT status={status} limit={limit_price:.2f}"),
                last_order_id=str(order_id or ""),
                last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            return False

        filled_qty, filled_avg = _reconcile_fill(tc, code, str(order_id), wait_sec=12.0)
        if filled_qty <= 0 or filled_avg <= 0:
            _write_buy_cooldown(conn, code, order_id, f"AH_ADD_NO_FILL limit={limit_price:.2f}")
            return False

        try:
            pos = tc.get_open_position(code)
            new_qty = int(float(getattr(pos, "qty", 0) or 0))
            new_cost = float(getattr(pos, "avg_entry_price", 0) or 0)
        except Exception:
            old_qty = int(float(row.get("qty") or real_qty))
            old_cost = float(row.get("cost_price") or filled_avg)
            new_qty = old_qty + int(filled_qty)
            new_cost = (old_qty * old_cost + int(filled_qty) * float(filled_avg)) / float(new_qty)

        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _update_ops_fields(
            conn, code,
            qty=int(new_qty),
            cost_price=round(float(new_cost), 2),
            base_qty=max(int(row.get("base_qty") or 0), int(new_qty)),
            is_bought=1,
            can_sell=1,
            can_buy=0,
            last_order_side="buy",
            last_order_intent=_intent_short(
                f"B:AH_ADD filled={filled_qty}@{filled_avg:.2f} limit={limit_price:.2f} after_gain={after_gain:.2%}"
            ),
            last_order_id=str(order_id or ""),
            last_order_time=now_str,
            updated_at=now_str,
        )
        print(f"[B AH ADD] {code} ✅ filled={filled_qty}@{filled_avg:.2f} new_qty={new_qty}", flush=True)
        return True
    except Exception as e:
        print(f"[B AH ADD] {code} error: {e}", flush=True)
        if not _is_quote_transient_error(e):
            traceback.print_exc()
        return False
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass





def _sell_qty(conn, code: str, qty: int, reason: str, limit_price: float | None = None) -> bool:
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
        _decision_log("sell", code, "sell_skip", reason="real_position_query_failed", detail=reason)
        return False

    if real_qty == 0:
        print(f"[B SELL] {code} skip: no real Alpaca position, db_qty={qty}, reason={reason}", flush=True)
        _decision_log("sell", code, "sell_skip", reason="no_real_position", db_qty=qty, detail=reason)
        _update_ops_fields(
            conn, code,
            qty=0, is_bought=0, can_sell=0, can_buy=0,
            stop_loss_price=None, take_profit_price=None,
            b_stage=0, base_qty=0,
            b_peak_price=None, b_peak_profit=0, b_last_profit=0,
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
    old_peak_price = float(row.get("b_peak_price") or 0.0)
    old_cost = float(row.get("cost_price") or 0.0)

    # ============================================================
    # 2) 提交实时价限价卖单
    # ============================================================
    if not limit_price or float(limit_price) <= 0:
        try:
            price, _prev_close, _feed = get_snapshot_realtime(code)
            limit_price = float(price or 0.0)
        except Exception:
            limit_price = 0.0
    if not limit_price or float(limit_price) <= 0:
        print(f"[B SELL] {code} skip: invalid realtime sell limit price={limit_price}", flush=True)
        _decision_log("sell", code, "sell_skip", reason="invalid_sell_limit_price", detail=reason)
        return False

    _decision_log("sell", code, "order_submit", qty=qty, reason=reason, real_qty=real_qty, limit=f"{float(limit_price):.2f}")
    order = _submit_limit_sell_qty(tc, code, qty, limit_price=float(limit_price))
    order_id = getattr(order, "id", None) or getattr(order, "order_id", None)
    order_status = str(getattr(order, "status", "") or "")
    print(
        f"[B SELL] {code} limit order submitted: id={order_id} status={order_status} req_qty={qty} limit={float(limit_price):.2f}",
        flush=True,
    )

    # ============================================================
    # 3) 立即拒单 → 不动持仓状态,只写 cooldown
    # ============================================================
    if order_status.lower() in ("rejected", "expired"):
        print(f"[B SELL] {code} immediate {order_status}, no position change", flush=True)
        _decision_log("sell", code, "order_reject", order_id=order_id, status=order_status, reason=reason)
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
        _decision_log("sell", code, "order_no_fill", order_id=order_id, status=final_status, reason=reason)
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

    sell_price = None
    try:
        o = tc.get_order_by_id(str(order_id))
        p = getattr(o, "filled_avg_price", None)
        if p is not None and str(p).strip() != "":
            sell_price = float(p)
    except Exception:
        sell_price = None

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
    new_peak_price = old_peak_price if remaining_qty > 0 else None
    new_peak_profit = max((old_peak_price - old_cost) * remaining_qty, 0.0) if remaining_qty > 0 else 0.0
    new_last_profit = 0.0

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
        base_qty=%s,
        b_peak_price=%s,
        b_peak_profit=%s,
        b_last_profit=%s
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
                new_peak_price,
                float(new_peak_profit),
                float(new_last_profit),
                code,
            ),
        )

    print(
        f"[B SELL] {code} ✅ sold={sold_qty} (req={qty}) remain={remaining_qty} "
        f"stage={new_b_stage} base_qty={new_base_qty} reason={reason} order_id={order_id}",
        flush=True,
    )
    _decision_log(
        "sell",
        code,
        "order_filled",
        order_id=order_id,
        sold=sold_qty,
        req=qty,
        remain=remaining_qty,
        stage=new_b_stage,
        price=f"{sell_price:.2f}" if sell_price else None,
        reason=reason,
    )

    if remaining_qty == 0:
        _write_monster_watchlist(conn, code, reason, sell_price, row)

    return True


def _sell_qty_limit_ext(conn, code: str, qty: int, limit_price: float, reason: str) -> bool:
    qty = int(qty or 0)
    limit_price = float(limit_price or 0.0)
    if qty <= 0 or limit_price <= 0:
        return False

    tc = _get_trading_client()
    real_qty = _get_real_position_qty(tc, code)
    if real_qty is None:
        print(f"[B EXT SELL] {code} skip: failed to query real position reason={reason}", flush=True)
        return False
    if real_qty == 0:
        _update_ops_fields(
            conn, code,
            qty=0, is_bought=0, can_sell=0, can_buy=0,
            stop_loss_price=None, take_profit_price=None,
            b_stage=0, base_qty=0,
            b_peak_price=None, b_peak_profit=0, b_last_profit=0,
            last_order_side="sell",
            last_order_intent=_intent_short(f"B:EXT_SELL_SKIP no_real_pos {reason}"),
            last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        return False
    qty = min(qty, real_qty)

    row = _load_one_b_row(conn, code) or {}
    old_sl = row.get("stop_loss_price")
    old_tp = row.get("take_profit_price")
    old_b_stage = int(row.get("b_stage") or 0)
    old_base_qty = int(row.get("base_qty") or 0)
    old_peak_price = float(row.get("b_peak_price") or 0.0)
    old_cost = float(row.get("cost_price") or 0.0)

    order = _submit_market_qty(tc, code, qty, side="sell")
    order_id = getattr(order, "id", None) or getattr(order, "order_id", None)
    status = str(getattr(order, "status", "") or "")
    print(f"[B EXT SELL] {code} market sell submitted id={order_id} status={status} qty={qty} ref={limit_price:.2f}", flush=True)
    if status.lower() in ("rejected", "expired"):
        _update_ops_fields(
            conn, code,
            last_order_side="sell",
            last_order_intent=_intent_short(f"B:EXT_SELL_REJECT {reason} status={status}"),
            last_order_id=str(order_id or ""),
            last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        return False

    sold_qty = _reconcile_sell_fill(tc, code, str(order_id), expected_real_qty=real_qty, wait_sec=12.0)
    if sold_qty <= 0:
        _update_ops_fields(
            conn, code,
            last_order_side="sell",
            last_order_intent=_intent_short(f"B:EXT_SELL_NO_FILL {reason} status={status}"),
            last_order_id=str(order_id or ""),
            last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        return False

    sold_qty = min(sold_qty, qty)
    remaining_qty = max(real_qty - sold_qty, 0)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _update_ops_fields(
        conn, code,
        qty=int(remaining_qty),
        last_order_side="sell",
        last_order_intent=_intent_short(f"{reason} market ref={limit_price:.2f} sold={sold_qty}"),
        last_order_id=str(order_id or ""),
        last_order_time=now_str,
        is_bought=1 if remaining_qty > 0 else 0,
        can_sell=1 if remaining_qty > 0 else 0,
        can_buy=0,
        stop_loss_price=old_sl if remaining_qty > 0 else None,
        take_profit_price=old_tp if remaining_qty > 0 else None,
        b_stage=old_b_stage if remaining_qty > 0 else 0,
        base_qty=old_base_qty if remaining_qty > 0 else 0,
        b_peak_price=old_peak_price if remaining_qty > 0 else None,
        b_peak_profit=max((old_peak_price - old_cost) * remaining_qty, 0.0) if remaining_qty > 0 else 0,
        b_last_profit=0,
        updated_at=now_str,
    )
    print(f"[B EXT SELL] {code} ✅ sold={sold_qty} remain={remaining_qty} reason={reason}", flush=True)
    if remaining_qty == 0:
        _write_monster_watchlist(conn, code, reason, limit_price, row)
    return True




def _buy_add_qty(conn, code: str, add_qty: int, reason: str, snap_price: float) -> bool:
    """
    加仓。snap_price 用作当前价限价，真实成本基从 Alpaca 拿(优先 position avg,
    回退 order filled_avg_price)。
    """
    add_qty = int(add_qty or 0)
    if add_qty <= 0:
        return False

    tc = _get_trading_client()

    # ============================================================
    # 1) 按当前价提交限价加仓单
    # ============================================================
    limit_price = round(float(snap_price or 0.0), 2)
    if limit_price <= 0:
        return False
    order = _submit_limit_buy_qty(tc, code, add_qty, limit_price=limit_price)
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






def _reconcile_sell_fill(tc, code: str, order_id: str, expected_real_qty: int, wait_sec: float = 4.0) -> float:
    """Count this sell order only, never another strategy's position changes."""
    from ultimate_v1.order_fills import wait_for_fill
    qty, _price, _status = wait_for_fill(tc, order_id, wait_sec)
    return qty


def _reconcile_add_fill(tc, order_id: str, wait_sec: float = 4.0):
    return _reconcile_fill(tc, "", order_id, wait_sec)


def _get_prev_trading_day_volume(conn, code: str) -> tuple[int, str]:
    """读取昨天/前一个已完成交易日成交量，买入前作为硬流动性条件。"""
    today = _now_la().date() if LA_TZ else datetime.now().date()
    sql = f"""
    SELECT `date`, volume
    FROM `{PRICES_TABLE}`
    WHERE `symbol`=%s
      AND volume > 0
      AND `date` < %s
    ORDER BY `date` DESC
    LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (code, today))
        row = cur.fetchone() or {}
    try:
        volume = int(float(row.get("volume") or 0))
    except Exception:
        volume = 0
    return volume, str(row.get("date") or "")


def _prev_day_volume_check(conn, code: str) -> tuple[bool, int, str, str]:
    volume, volume_date = _get_prev_trading_day_volume(conn, code)
    if volume <= 0:
        return False, volume, volume_date, "missing_prev_day_volume"
    if volume <= B_MIN_PREV_DAY_VOLUME:
        return False, volume, volume_date, f"prev_day_volume={volume} <= min={B_MIN_PREV_DAY_VOLUME}"
    return True, volume, volume_date, f"ok prev_day_volume={volume} > {B_MIN_PREV_DAY_VOLUME}"


def _count_active_b_positions(conn) -> int:
    sql = f"""
    SELECT COUNT(*) AS n
    FROM `{OPS_TABLE}`
    WHERE stock_type='B'
      AND is_bought=1
      AND can_sell=1
      AND qty > 0;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone() or {}
    try:
        return int(row.get("n") or 0)
    except Exception:
        return 0


def _dynamic_b_order_notional(base_capital: float) -> float:
    """
    B 单笔金额随 B 资金池规模按阶梯放大。

    默认:
      <10k: 2000
      10k-20k: 2500
      20k-40k: 3000
      40k-80k: 4000
      80k-150k: 5000
      150k-300k: 7500
      >=300k: 10000
    """
    base_capital = max(0.0, float(base_capital or 0.0))
    fallback = max(float(B_DYNAMIC_MIN_TRADE_NOTIONAL), 2000.0)
    for item in str(B_DYNAMIC_ORDER_TIERS or "").split(","):
        item = item.strip()
        if not item or ":" not in item:
            continue
        limit_raw, notional_raw = item.split(":", 1)
        try:
            limit = math.inf if limit_raw.strip().lower() in {"inf", "infinity", "*"} else float(limit_raw)
            notional = float(notional_raw)
        except Exception:
            continue
        if base_capital < limit:
            return max(float(B_DYNAMIC_MIN_TRADE_NOTIONAL), min(float(B_DYNAMIC_ORDER_MAX_NOTIONAL), notional))
        fallback = notional
    return max(float(B_DYNAMIC_MIN_TRADE_NOTIONAL), min(float(B_DYNAMIC_ORDER_MAX_NOTIONAL), fallback))


def _b_remaining_slots_for_available(available: float, order_notional: float) -> int:
    available = max(0.0, float(available or 0.0))
    order_notional = max(float(order_notional or 0.0), 1.0)
    remainder_min = max(float(B_REMAINDER_BUY_MIN_NOTIONAL), float(B_DYNAMIC_MIN_TRADE_NOTIONAL), 1.0)
    if available < remainder_min:
        return 0
    full_slots = int(math.floor(available / order_notional))
    remainder = available - float(full_slots) * order_notional
    if full_slots <= 0:
        return 1
    return full_slots + (1 if remainder >= remainder_min else 0)


def _b_next_trade_notional(available: float, order_notional: float) -> float:
    available = max(0.0, float(available or 0.0))
    order_notional = max(float(order_notional or 0.0), 1.0)
    remainder_min = max(float(B_REMAINDER_BUY_MIN_NOTIONAL), float(B_DYNAMIC_MIN_TRADE_NOTIONAL), 1.0)
    if available < remainder_min:
        return 0.0
    if available < order_notional:
        return available
    return order_notional


def _fallback_b_buy_plan(active_b: int = 0) -> dict:
    max_positions = max(int(B_MAX_ACTIVE_POSITIONS), int(active_b or 0))
    remaining_slots = max(max_positions - int(active_b or 0), 0)
    return {
        "dynamic": False,
        "available": 0.0,
        "active_positions": int(active_b or 0),
        "max_positions": max_positions,
        "remaining_slots": remaining_slots,
        "target_notional": float(B_TARGET_NOTIONAL_USD),
        "reason": "static_env",
    }


def _b_buy_plan(active_b: int = 0) -> dict:
    """
    根据 B 资金池剩余额度动态控制买入节奏。

    默认单笔约 2000；如果最后剩余资金 >=1000 且小于单笔金额，
    就用剩余金额开最后一笔。单笔金额会随 B 资金池规模按平方根慢慢提高。
    """
    active_b = int(active_b or 0)
    if B_USE_DYNAMIC_CAPITAL_SIZING != 1:
        return _fallback_b_buy_plan(active_b)

    try:
        from ultimate_v1.capital_manager import get_capital_allocation

        allocation = get_capital_allocation()
        if allocation is None:
            return _fallback_b_buy_plan(active_b)

        raw_target = max(0.0, float(allocation.target_for("B")))
        raw_used = max(0.0, float(allocation.used.get("B", 0.0)))
        raw_available = max(0.0, float(allocation.available.get("B", 0.0)))
        effective_target = raw_target
        available = max(0.0, effective_target - raw_used)
        order_notional = _dynamic_b_order_notional(raw_target)
        available_slots = _b_remaining_slots_for_available(available, order_notional)
        max_positions = active_b + available_slots
        remaining_slots = max(max_positions - active_b, 0)
        target_notional = _b_next_trade_notional(available, order_notional)

        return {
            "dynamic": True,
            "available": available,
            "raw_target": raw_target,
            "raw_used": raw_used,
            "raw_available": raw_available,
            "effective_target": effective_target,
            "available_multiplier": float(B_AVAILABLE_CAPITAL_MULTIPLIER),
            "order_notional": order_notional,
            "active_positions": active_b,
            "max_positions": max_positions,
            "remaining_slots": remaining_slots,
            "target_notional": target_notional,
            "remainder_buy_min_notional": float(B_REMAINDER_BUY_MIN_NOTIONAL),
            "reason": "capital_available_order_notional",
        }
    except Exception as exc:
        print(f"[B BUY PLAN] dynamic sizing fallback: {exc}", flush=True)
        return _fallback_b_buy_plan(active_b)


def get_b_buy_plan_for_gate() -> dict:
    """供外层资金闸估算 B 单笔金额，避免 gate 和老 B 买入金额不一致。"""
    conn = None
    try:
        conn = _connect()
        active_b = _count_active_b_positions(conn)
        return _b_buy_plan(active_b)
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def _get_avg_volume20(conn, code: str) -> float:
    sql = f"""
    SELECT AVG(volume) AS avg_vol
    FROM (
        SELECT volume
        FROM `{PRICES_TABLE}`
        WHERE symbol=%s
          AND volume > 0
        ORDER BY date DESC
        LIMIT 20
    ) x;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (code,))
        row = cur.fetchone() or {}
    try:
        return float(row.get("avg_vol") or 0.0)
    except Exception:
        return 0.0


def _get_realtime_intraday_volume(code: str) -> int:
    try:
        return int(get_yahoo_intraday_volume(code) or 0)
    except Exception:
        return 0


def _required_intraday_rvol(now_dt=None) -> float:
    now_dt = now_dt or _now_la()
    now_min = now_dt.hour * 60 + now_dt.minute
    t1 = _hhmm_to_minutes(B_VOLUME_T1_LA, "07:30")
    t2 = _hhmm_to_minutes(B_VOLUME_T2_LA, "09:30")
    if now_min < t1:
        return B_RVOL_EARLY
    if now_min < t2:
        return B_RVOL_MID
    return B_RVOL_LATE


def _market_elapsed_ratio(now_dt=None) -> float:
    now_dt = now_dt or _now_la()
    now_min = now_dt.hour * 60 + now_dt.minute
    open_min = _hhmm_to_minutes(B_MARKET_OPEN_LA, "06:30")
    close_min = _hhmm_to_minutes(B_MARKET_CLOSE_LA, "13:00")
    session_minutes = max(close_min - open_min, 1)
    elapsed = min(max(now_min - open_min, 1), session_minutes)
    return elapsed / session_minutes


def _intraday_volume_check(conn, code: str, current_price: float = 0.0):
    if B_REQUIRE_INTRADAY_VOLUME != 1:
        return True, 0, 0.0, 0.0, 0.0, "disabled"

    intraday_volume = _get_realtime_intraday_volume(code)
    avg_volume20 = _get_avg_volume20(conn, code)
    required_rvol = _required_intraday_rvol()
    elapsed_ratio = _market_elapsed_ratio()
    required_ratio = elapsed_ratio * required_rvol
    realtime_dollar_volume = float(intraday_volume) * float(current_price or 0.0)

    if intraday_volume <= 0:
        return False, intraday_volume, avg_volume20, required_ratio, 0.0, "missing_intraday_volume"
    if avg_volume20 <= 0:
        return False, intraday_volume, avg_volume20, required_ratio, 0.0, "missing_avg_volume20"

    volume_ratio = float(intraday_volume) / float(avg_volume20)
    if intraday_volume < B_MIN_REALTIME_VOLUME:
        return (
            False,
            intraday_volume,
            avg_volume20,
            required_ratio,
            volume_ratio,
            f"volume={intraday_volume} < min_volume={B_MIN_REALTIME_VOLUME}",
        )
    if realtime_dollar_volume < B_MIN_REALTIME_DOLLAR_VOLUME:
        return (
            False,
            intraday_volume,
            avg_volume20,
            required_ratio,
            volume_ratio,
            f"dollar_volume={realtime_dollar_volume:.0f} < min_dollar={B_MIN_REALTIME_DOLLAR_VOLUME:.0f}",
        )

    expected_volume_now = float(avg_volume20) * max(elapsed_ratio, 1.0 / 390.0)
    rvol = float(intraday_volume) / expected_volume_now if expected_volume_now > 0 else 0.0
    if rvol < required_rvol:
        return (
            False,
            intraday_volume,
            avg_volume20,
            required_ratio,
            volume_ratio,
            f"rvol={rvol:.2f} < required_rvol={required_rvol:.2f} elapsed={elapsed_ratio:.2%}",
        )

    return (
        True,
        intraday_volume,
        avg_volume20,
        required_ratio,
        volume_ratio,
        f"ok rvol={rvol:.2f}>={required_rvol:.2f} dollar_volume={realtime_dollar_volume:.0f}",
    )


def _intraday_reversal_reject(ref_price: float, day_open: float, day_high: float):
    ref_price = float(ref_price or 0.0)
    day_open = float(day_open or 0.0)
    day_high = float(day_high or 0.0)

    if ref_price <= 0:
        return True, "invalid_ref_price"

    if day_open > 0:
        below_open_pct = (day_open - ref_price) / day_open
        if below_open_pct > B_MAX_BELOW_OPEN_PCT:
            return (
                True,
                f"below_open={below_open_pct:.2%} > max={B_MAX_BELOW_OPEN_PCT:.2%} "
                f"open={day_open:.2f} ref={ref_price:.2f}",
            )

    if day_high > 0:
        pullback_pct = (day_high - ref_price) / day_high
        if pullback_pct > B_MAX_PULLBACK_FROM_HIGH_PCT:
            return (
                True,
                f"pullback_from_high={pullback_pct:.2%} > max={B_MAX_PULLBACK_FROM_HIGH_PCT:.2%} "
                f"high={day_high:.2f} ref={ref_price:.2f}",
            )

    return False, ""


def _b_max_buy_price(prev_close: float) -> float:
    prev_close = float(prev_close or 0.0)
    if prev_close <= 0:
        return 0.0
    return round(prev_close * (1.0 + float(B_MAX_BUY_UP_PCT)), 2)


def _b_entry_limit_price(price: float, ask: float, prev_close: float) -> tuple[float, float, float]:
    price = float(price or 0.0)
    _ = ask
    max_buy_price = _b_max_buy_price(prev_close)
    raw_limit = price
    capped_limit = min(raw_limit, max_buy_price) if max_buy_price > 0 else raw_limit
    return round(float(capped_limit), 2), round(float(raw_limit), 2), max_buy_price


def _ensure_b_score_table(conn):
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{B_SCORE_TABLE}` (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        bucket_time DATETIME NOT NULL,
        symbol VARCHAR(20) NOT NULL,
        rank_no INT NOT NULL,
        score DOUBLE NOT NULL,
        price DOUBLE NULL,
        day_up_pct DOUBLE NULL,
        entry_up_pct DOUBLE NULL,
        spread_pct DOUBLE NULL,
        avg_dollar_vol20 DOUBLE NULL,
        reason VARCHAR(255) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_bucket_symbol (bucket_time, symbol),
        KEY idx_symbol_bucket (symbol, bucket_time),
        KEY idx_bucket_rank (bucket_time, rank_no)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def _score_bucket_time(now_dt=None) -> datetime:
    now_dt = now_dt or datetime.now()
    interval = max(int(B_SCORE_INTERVAL_MINUTES), 1)
    minute = (now_dt.minute // interval) * interval
    return now_dt.replace(minute=minute, second=0, microsecond=0)


def _latest_score_bucket(conn):
    _ensure_b_score_table(conn)
    sql = f"SELECT MAX(bucket_time) AS bucket_time FROM `{B_SCORE_TABLE}`;"
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone() or {}
    return row.get("bucket_time")


def _b_score_log(message: str) -> None:
    if B_SCORE_LOG_EACH_CANDIDATE == 1:
        print(message, flush=True)


def _score_b_reject(code: str, reason: str) -> None:
    _b_score_log(f"[B SCORE CHECK] {code} reject: {reason}")
    _decision_log("buy", code, "score_reject", reason=reason)


def _score_b_candidate(conn, code: str):
    code = (code or "").strip().upper()
    _b_score_log(f"[B SCORE CHECK] {code} check")
    row = _load_one_b_row(conn, code)
    if not row:
        _score_b_reject(code, "no B row")
        return None

    if int(row.get("can_buy") or 0) != 1:
        _score_b_reject(code, f"can_buy={int(row.get('can_buy') or 0)}")
        return None
    if int(row.get("is_bought") or 0) == 1:
        _score_b_reject(code, "already bought")
        return None
    if _is_cooldown(row.get("last_order_time"), row.get("last_order_side")):
        _score_b_reject(
            code,
            f"cooldown last_side={row.get('last_order_side')} last_time={row.get('last_order_time')}",
        )
        return None

    trigger = float(row.get("trigger_price") or 0)
    entry_close = float(row.get("entry_close") or row.get("close_price") or trigger or 0)
    if trigger <= 0:
        _score_b_reject(code, f"invalid trigger={trigger:.2f}")
        return None
    if entry_close <= 0:
        _score_b_reject(code, f"invalid entry_close={entry_close:.2f}")
        return None

    snap = get_snapshot_quote_realtime(code)
    price = float(snap.get("last_price") or 0.0)
    day_open = float(snap.get("day_open") or 0.0)
    day_high = float(snap.get("day_high") or 0.0)
    prev_close = float(snap.get("prev_close") or 0.0)
    if prev_close <= 0:
        prev_close = _get_prev_close_from_db(conn, code)

    if price <= 0:
        _score_b_reject(code, f"invalid price={price:.2f}")
        return None
    if prev_close <= 0:
        _score_b_reject(code, f"invalid prev_close={prev_close:.2f}")
        return None
    if price < B_MIN_PRICE:
        _score_b_reject(code, f"price={price:.2f} < min_price={B_MIN_PRICE:.2f}")
        return None
    if not (price > trigger):
        _score_b_reject(code, f"price={price:.2f} <= trigger={trigger:.2f}")
        return None

    day_up_pct = (price - prev_close) / prev_close if prev_close > 0 else 0.0
    entry_up_pct = (price - entry_close) / entry_close if entry_close > 0 else 0.0
    if not (day_up_pct > B_MIN_UP_PCT):
        need_price = prev_close * (1.0 + float(B_MIN_UP_PCT)) if prev_close > 0 else 0.0
        _score_b_reject(
            code,
            f"day_up={day_up_pct:.2%} <= min_up={B_MIN_UP_PCT:.2%} need>{need_price:.2f}",
        )
        return None
    if day_up_pct >= B_MAX_BUY_UP_PCT:
        max_buy_price = prev_close * (1.0 + float(B_MAX_BUY_UP_PCT)) if prev_close > 0 else 0.0
        _score_b_reject(
            code,
            f"day_up={day_up_pct:.2%} >= max_buy_up={B_MAX_BUY_UP_PCT:.2%} max_buy_price<{max_buy_price:.2f}",
        )
        return None
    if entry_up_pct >= B_MAX_ENTRY_UP_PCT:
        _score_b_reject(
            code,
            f"entry_up={entry_up_pct:.2%} >= max_entry_up={B_MAX_ENTRY_UP_PCT:.2%}",
        )
        return None
    reject_reversal, reversal_reason = _intraday_reversal_reject(price, day_open, day_high)
    if reject_reversal:
        _score_b_reject(code, f"weak intraday price {reversal_reason}")
        return None

    prev_vol_ok, prev_day_volume, prev_volume_date, prev_volume_reason = _prev_day_volume_check(conn, code)
    if not prev_vol_ok:
        _score_b_reject(code, f"{prev_volume_reason} date={prev_volume_date or '--'}")
        return None

    (
        volume_ok,
        intraday_volume,
        avg_volume20,
        required_volume_ratio,
        volume_ratio,
        volume_reason,
    ) = _intraday_volume_check(conn, code, price)
    if not volume_ok:
        _score_b_reject(
            code,
            f"intraday volume {volume_reason} vol={intraday_volume} "
            f"avg20={avg_volume20:.0f} required={required_volume_ratio:.2%}",
        )
        return None

    # 越接近 3%-8% 的强势突破、越接近入选价、成交量越健康，分越高。
    # 注意：不再用 bid/ask 价差和 20 日成交额过滤，避免依赖会员级全市场行情。
    momentum_score = max(0.0, min(day_up_pct, 0.08)) * 1000.0
    pullback_from_high = (day_high - price) / day_high if day_high > 0 else 0.0
    below_open = (day_open - price) / day_open if day_open > 0 else 0.0
    entry_penalty = max(entry_up_pct, 0.0) * 450.0
    reversal_penalty = max(pullback_from_high, 0.0) * 700.0 + max(below_open, 0.0) * 700.0
    volume_liquidity_score = min(avg_volume20 / 1_000_000.0, 120.0) * 0.15
    volume_score = min(volume_ratio / max(required_volume_ratio, 0.01), 2.0) * 18.0
    trigger_score = min(max((price - trigger) / trigger, 0.0), 0.08) * 180.0 if trigger > 0 else 0.0
    score = (
        momentum_score
        + volume_liquidity_score
        + volume_score
        + trigger_score
        - entry_penalty
        - reversal_penalty
    )
    _b_score_log(
        f"[B SCORE CHECK] {code} pass: score={score:.4f} price={price:.2f} "
        f"trigger={trigger:.2f} day_up={day_up_pct:.2%} entry_up={entry_up_pct:.2%} "
        f"pullback={pullback_from_high:.2%} below_open={below_open:.2%} "
        f"volume={intraday_volume}/{avg_volume20:.0f} ratio={volume_ratio:.2%} "
        f"required={required_volume_ratio:.2%}"
    )
    _decision_log(
        "buy",
        code,
        "score_pass",
        score=f"{score:.4f}",
        price=f"{price:.2f}",
        trigger=f"{trigger:.2f}",
        day_up=f"{day_up_pct:.2%}",
        entry_up=f"{entry_up_pct:.2%}",
        prev_day_volume=prev_day_volume,
        prev_volume_date=prev_volume_date,
        volume=f"{intraday_volume}/{avg_volume20:.0f}",
        rvol=f"{volume_ratio:.2%}",
    )

    return {
        "symbol": code,
        "score": round(float(score), 4),
        "price": price,
        "day_up_pct": day_up_pct,
        "entry_up_pct": entry_up_pct,
        # 保留老表字段兼容历史 schema，但不再作为过滤/评分依据。
        "spread_pct": 0.0,
        "avg_dollar_vol20": 0.0,
        "reason": (
            f"day_up={day_up_pct:.2%} entry_up={entry_up_pct:.2%} "
            f"pullback={pullback_from_high:.2%} below_open={below_open:.2%} "
            f"prev_vol={prev_day_volume}({prev_volume_date}) "
            f"vol={intraday_volume}/{avg_volume20:.0f}({volume_ratio:.2%}>={required_volume_ratio:.2%}) "
            f"avg_vol20={avg_volume20:.0f}"
        )[:255],
    }


def strategy_B_rank_and_confirm(codes) -> list[str]:
    """
    B 买入选择器：
    1) 每 5 分钟给全池 can_buy 股票打分。
    2) 只记录 Top3。
    3) 同一只股票在最近窗口里进入 Top3 满 3 次，才交给 strategy_B_buy 下单。
    """
    codes = sorted({(c or "").strip().upper() for c in (codes or []) if (c or "").strip()})
    if not codes:
        return []

    market_ok, market_detail = _market_environment_check()
    if not market_ok:
        print(
            f"[B SCORE] skip: market_filter score={market_detail.get('score')} "
            f"min={market_detail.get('min_score')} trend={market_detail.get('trend')} "
            f"qqq={market_detail.get('qqq_change_pct')}% vix={market_detail.get('vix')} "
            f"reason={market_detail.get('reason')}",
            flush=True,
        )
        return []

    buy_window_open, now_la, window_start, window_end = _is_b_buy_window_open()
    if not buy_window_open:
        print(
            f"[B SCORE] skip: outside LA buy window now={now_la} "
            f"window={window_start}-{window_end}",
            flush=True,
        )
        return []

    conn = None
    try:
        conn = _connect()
        active_b = _count_active_b_positions(conn)
        buy_plan = _b_buy_plan(active_b)
        max_positions = int(buy_plan.get("max_positions") or 0)
        if active_b >= max_positions:
            print(
                f"[B SCORE] skip: active_b_positions={active_b} "
                f">= max_active={max_positions} "
                f"available={float(buy_plan.get('available') or 0):.2f}",
                flush=True,
            )
            return []

        _ensure_b_score_table(conn)
        bucket_time = _score_bucket_time()
        latest_bucket = _latest_score_bucket(conn)
        should_record = latest_bucket is None or latest_bucket < bucket_time

        if should_record:
            scored = []
            for code in codes:
                try:
                    item = _score_b_candidate(conn, code)
                    if item:
                        scored.append(item)
                except Exception as e:
                    print(f"[B SCORE] {code} skip score error: {e}", flush=True)

            scored.sort(key=lambda x: x["score"], reverse=True)
            top = scored[: max(int(B_SCORE_TOP_N), 1)]

            sql = f"""
            INSERT INTO `{B_SCORE_TABLE}` (
                bucket_time, symbol, rank_no, score, price,
                day_up_pct, entry_up_pct, spread_pct, avg_dollar_vol20, reason
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                rank_no=VALUES(rank_no),
                score=VALUES(score),
                price=VALUES(price),
                day_up_pct=VALUES(day_up_pct),
                entry_up_pct=VALUES(entry_up_pct),
                spread_pct=VALUES(spread_pct),
                avg_dollar_vol20=VALUES(avg_dollar_vol20),
                reason=VALUES(reason);
            """
            args = [
                (
                    bucket_time,
                    item["symbol"],
                    idx,
                    item["score"],
                    item["price"],
                    item["day_up_pct"],
                    item["entry_up_pct"],
                    item["spread_pct"],
                    item["avg_dollar_vol20"],
                    item["reason"],
                )
                for idx, item in enumerate(top, start=1)
            ]
            if args:
                with conn.cursor() as cur:
                    cur.executemany(sql, args)
            top_detail = ",".join(
                [
                    f"{idx}:{item['symbol']} score={float(item['score']):.4f} "
                    f"price={float(item['price']):.2f} reason={item['reason']}"
                    for idx, item in enumerate(top, start=1)
                ]
            )
            print(
                f"[B SCORE] bucket={bucket_time} scored={len(scored)} "
                f"top={','.join([x['symbol'] for x in top]) or '-'}",
                flush=True,
            )
            if top_detail:
                print(f"[B SCORE TOP] {top_detail}", flush=True)
        else:
            print(f"[B SCORE] wait next bucket latest={latest_bucket}", flush=True)

        confirm_sql = f"""
        SELECT
            s.symbol,
            COUNT(DISTINCT s.bucket_time) AS hits,
            MAX(CASE WHEN s.bucket_time=(SELECT MAX(bucket_time) FROM `{B_SCORE_TABLE}`) THEN s.rank_no END) AS latest_rank,
            AVG(s.score) AS avg_score
        FROM `{B_SCORE_TABLE}` s
        JOIN (
            SELECT DISTINCT bucket_time
            FROM `{B_SCORE_TABLE}`
            WHERE bucket_time >= DATE_SUB(%s, INTERVAL %s MINUTE)
            ORDER BY bucket_time DESC
            LIMIT %s
        ) b
          ON b.bucket_time = s.bucket_time
        GROUP BY s.symbol
        HAVING hits >= %s
           AND latest_rank IS NOT NULL
        ORDER BY latest_rank ASC, avg_score DESC;
        """
        with conn.cursor() as cur:
            cur.execute(
                confirm_sql,
                (
                    bucket_time,
                    int(B_SCORE_LOOKBACK_MINUTES),
                    int(B_SCORE_CONFIRMATIONS),
                    int(B_SCORE_CONFIRMATIONS),
                ),
            )
            rows = cur.fetchall() or []

        confirmed = [str(r.get("symbol") or "").upper() for r in rows if r.get("symbol")]
        if confirmed:
            print(f"[B SCORE] confirmed={','.join(confirmed)}", flush=True)
            for symbol in confirmed:
                _decision_log(
                    "buy",
                    symbol,
                    "score_confirmed",
                    confirmations=int(B_SCORE_CONFIRMATIONS),
                    lookback_min=int(B_SCORE_LOOKBACK_MINUTES),
                )
        else:
            pending_sql = f"""
            SELECT
                s.symbol,
                COUNT(DISTINCT s.bucket_time) AS hits,
                MAX(CASE WHEN s.bucket_time=(SELECT MAX(bucket_time) FROM `{B_SCORE_TABLE}`) THEN s.rank_no END) AS latest_rank,
                AVG(s.score) AS avg_score
            FROM `{B_SCORE_TABLE}` s
            JOIN (
                SELECT DISTINCT bucket_time
                FROM `{B_SCORE_TABLE}`
                WHERE bucket_time >= DATE_SUB(%s, INTERVAL %s MINUTE)
                ORDER BY bucket_time DESC
                LIMIT %s
            ) b
              ON b.bucket_time = s.bucket_time
            GROUP BY s.symbol
            HAVING latest_rank IS NOT NULL
            ORDER BY latest_rank ASC, hits DESC, avg_score DESC
            LIMIT %s;
            """
            with conn.cursor() as cur:
                cur.execute(
                    pending_sql,
                    (
                        bucket_time,
                        int(B_SCORE_LOOKBACK_MINUTES),
                        int(B_SCORE_CONFIRMATIONS),
                        max(int(B_SCORE_TOP_N), 1),
                    ),
                )
                pending_rows = cur.fetchall() or []
            pending = [
                f"{str(r.get('symbol') or '').upper()}:{int(r.get('hits') or 0)}/{int(B_SCORE_CONFIRMATIONS)}"
                f"#{int(r.get('latest_rank') or 0)}"
                for r in pending_rows
                if r.get("symbol")
            ]
            if pending:
                print(f"[B SCORE] pending={','.join(pending)}", flush=True)
            if pending_rows:
                progress = [
                    f"{str(r.get('symbol') or '').upper()} "
                    f"hits={int(r.get('hits') or 0)}/{int(B_SCORE_CONFIRMATIONS)} "
                    f"latest_rank={int(r.get('latest_rank') or 0)} "
                    f"avg_score={float(r.get('avg_score') or 0.0):.4f}"
                    for r in pending_rows
                    if r.get("symbol")
                ]
                if progress:
                    print(f"[B SCORE PROGRESS] {'; '.join(progress)}", flush=True)
        return confirmed

    except Exception as e:
        print(f"[B SCORE] error: {e}", flush=True)
        traceback.print_exc()
        return []
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def strategy_B_buy(code: str) -> bool:
    import math
    import traceback
    from datetime import datetime

    code = (code or "").strip().upper()
    print(f"[B BUY] {code}", flush=True)

    market_ok, market_detail = _market_environment_check()
    if not market_ok:
        print(
            f"[B BUY] {code} skip: market_filter score={market_detail.get('score')} "
            f"min={market_detail.get('min_score')} trend={market_detail.get('trend')} "
            f"qqq={market_detail.get('qqq_change_pct')}% vix={market_detail.get('vix')} "
            f"reason={market_detail.get('reason')}",
            flush=True,
        )
        _decision_log(
            "buy",
            code,
            "buy_skip",
            reason="market_filter",
            market_score=market_detail.get("score"),
            market_trend=market_detail.get("trend"),
            qqq_change_pct=market_detail.get("qqq_change_pct"),
            vix=market_detail.get("vix"),
            detail=market_detail.get("reason"),
        )
        return False

    buy_window_open, now_la, window_start, window_end = _is_b_buy_window_open()
    if not buy_window_open:
        print(
            f"[B BUY] {code} skip: outside LA buy window now={now_la} "
            f"window={window_start}-{window_end}",
            flush=True,
        )
        return False

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
            _decision_log("buy", code, "buy_skip", reason=f"can_buy={can_buy}")
            return False
        if is_bought == 1:
            print(f"[B BUY] {code} skip: already bought", flush=True)
            _decision_log("buy", code, "buy_skip", reason="already_bought")
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

        active_b = _count_active_b_positions(conn)
        buy_plan = _b_buy_plan(active_b)
        max_positions = int(buy_plan.get("max_positions") or 0)
        if active_b >= max_positions:
            print(
                f"[B BUY] {code} skip: active_b_positions={active_b} "
                f">= max_active={max_positions} "
                f"available={float(buy_plan.get('available') or 0):.2f}",
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
        day_open = float(snap.get("day_open") or 0.0)
        day_high = float(snap.get("day_high") or 0.0)
        feed = snap.get("feed")

        prev_close = float(snap.get("prev_close") or 0.0)
        if prev_close <= 0:
            prev_close = _get_prev_close_from_db(conn, code)

        day_up_pct = (price - prev_close) / prev_close if prev_close > 0 else 0.0
        entry_up_pct = (price - entry_close) / entry_close if entry_close > 0 else 0.0

        need_price = prev_close * (1.0 + float(B_MIN_UP_PCT)) if prev_close > 0 else 0.0

        print(
            f"[B BUY] {code} quote bid={bid:.2f} ask={ask:.2f} last={price:.2f} "
            f"open={day_open:.2f} high={day_high:.2f} "
            f"prev_close={prev_close:.2f} entry_close={entry_close:.2f} "
            f"trigger={trigger:.2f} day_up={day_up_pct*100:.2f}% "
            f"entry_up={entry_up_pct*100:.2f}% feed={feed}",
            flush=True,
        )

        if price <= 0:
            print(f"[B BUY] {code} skip: invalid price={price:.2f}", flush=True)
            return False
        if price < B_MIN_PRICE:
            print(
                f"[B BUY] {code} skip: price={price:.2f} < min_price={B_MIN_PRICE:.2f}",
                flush=True,
            )
            return False
        if prev_close <= 0:
            print(f"[B BUY] {code} skip: invalid prev_close={prev_close:.2f}", flush=True)
            return False
        if not (price > trigger):
            print(f"[B BUY] {code} skip: price={price:.2f} <= trigger={trigger:.2f}", flush=True)
            _decision_log("buy", code, "buy_skip", reason="price_below_trigger", price=f"{price:.2f}", trigger=f"{trigger:.2f}")
            return False
        if not (day_up_pct > B_MIN_UP_PCT):
            print(
                f"[B BUY] {code} skip: day_up={day_up_pct*100:.2f}% <= min_up={B_MIN_UP_PCT*100:.2f}% "
                f"(need>{need_price:.2f})",
                flush=True,
            )
            return False
        if day_up_pct >= B_MAX_BUY_UP_PCT:
            max_buy_price = prev_close * (1.0 + float(B_MAX_BUY_UP_PCT)) if prev_close > 0 else 0.0
            print(
                f"[B BUY] {code} skip: day_up={day_up_pct*100:.2f}% "
                f">= max_buy_up={B_MAX_BUY_UP_PCT*100:.2f}% "
                f"(max_buy_price<{max_buy_price:.2f})",
                flush=True,
            )
            return False
        if entry_up_pct >= B_MAX_ENTRY_UP_PCT:
            print(
                f"[B BUY] {code} skip: entry_up={entry_up_pct*100:.2f}% "
                f">= max_entry_up={B_MAX_ENTRY_UP_PCT*100:.2f}%",
                flush=True,
            )
            return False

        reject_reversal, reversal_reason = _intraday_reversal_reject(price, day_open, day_high)
        if reject_reversal:
            print(f"[B BUY] {code} skip: weak intraday price {reversal_reason}", flush=True)
            return False

        prev_vol_ok, prev_day_volume, prev_volume_date, prev_volume_reason = _prev_day_volume_check(conn, code)
        if not prev_vol_ok:
            print(
                f"[B BUY] {code} skip: {prev_volume_reason} "
                f"date={prev_volume_date or '--'}",
                flush=True,
            )
            _decision_log(
                "buy",
                code,
                "buy_skip",
                reason="prev_day_volume_filter",
                detail=prev_volume_reason,
                prev_day_volume=prev_day_volume,
                prev_volume_date=prev_volume_date,
            )
            return False

        (
            volume_ok,
            intraday_volume,
            avg_volume20,
            required_volume_ratio,
            volume_ratio,
            volume_reason,
        ) = _intraday_volume_check(conn, code, price)
        if not volume_ok:
            print(
                f"[B BUY] {code} skip: intraday volume {volume_reason} "
                f"vol={intraday_volume} avg20={avg_volume20:.0f} "
                f"required={required_volume_ratio:.2%}",
                flush=True,
            )
            _decision_log(
                "buy",
                code,
                "buy_skip",
                reason=f"intraday_volume_{volume_reason}",
                volume=intraday_volume,
                avg20=f"{avg_volume20:.0f}",
                required=f"{required_volume_ratio:.2%}",
            )
            return False
        print(
            f"[B BUY] {code} volume ok: intraday={intraday_volume} "
            f"avg20={avg_volume20:.0f} ratio={volume_ratio:.2%} "
            f"required={required_volume_ratio:.2%}",
            flush=True,
        )

        # ============================================================
        # 3) 资金 + 计算下单参数
        # ============================================================
        tc = _get_trading_client()
        buying_power = _get_buying_power(tc)
        target_notional = float(buy_plan.get("target_notional") or 0.0)
        if target_notional < float(B_DYNAMIC_MIN_TRADE_NOTIONAL):
            print(
                f"[B BUY] {code} skip: target_notional={target_notional:.2f} "
                f"< min_trade_notional={float(B_DYNAMIC_MIN_TRADE_NOTIONAL):.2f} "
                f"available={float(buy_plan.get('available') or 0):.2f} "
                f"remaining_slots={int(buy_plan.get('remaining_slots') or 0)}",
                flush=True,
            )
            return False
        print(
            f"[B BUY PLAN] {code} dynamic={int(bool(buy_plan.get('dynamic')))} "
            f"available={float(buy_plan.get('available') or 0):.2f} "
            f"active={int(buy_plan.get('active_positions') or 0)} "
            f"max_positions={int(buy_plan.get('max_positions') or 0)} "
            f"remaining_slots={int(buy_plan.get('remaining_slots') or 0)} "
            f"target_notional={target_notional:.2f}",
            flush=True,
        )

        required_bp = max(
            float(B_MIN_BUYING_POWER),
            float(B_MIN_OPEN_BUYING_POWER),
        )
        if buying_power < required_bp:
            print(
                f"[B BUY] {code} skip: buying_power={buying_power:.2f} < required_bp={required_bp:.2f}",
                flush=True,
            )
            return False

        max_use = float(buying_power) * float(B_BP_USE_RATIO)
        if bool(buy_plan.get("dynamic")):
            target = min(target_notional, float(B_DYNAMIC_MAX_TRADE_NOTIONAL), float(max_use))
        else:
            target = min(target_notional, float(B_MAX_NOTIONAL_USD), float(max_use))
        if target < target_notional:
            print(
                f"[B BUY] {code} downsize: target={target:.2f} < target_notional={target_notional:.2f} "
                f"buying_power={buying_power:.2f}",
                flush=True,
            )
        if target < float(B_DYNAMIC_MIN_TRADE_NOTIONAL):
            print(
                f"[B BUY] {code} skip: target={target:.2f} "
                f"< min_trade_notional={float(B_DYNAMIC_MIN_TRADE_NOTIONAL):.2f}",
                flush=True,
            )
            return False

        qty = int(math.floor(float(target) / float(price))) if price > 0 else 0
        if qty <= 0:
            print(f"[B BUY] {code} skip: qty={qty} target={target:.2f} price={price:.2f}", flush=True)
            return False

        used_notional = float(qty) * float(price)

        # 限价：至少高于 last 0.1%，提高成交概率；但绝不能超过策略最高买入价。
        limit_price, raw_limit_price, max_buy_price = _b_entry_limit_price(price, ask, prev_close)

        if limit_price <= 0:
            print(f"[B BUY] {code} skip: invalid limit_price={limit_price:.2f}", flush=True)
            return False
        if max_buy_price > 0 and raw_limit_price > max_buy_price:
            print(
                f"[B BUY] {code} cap limit: raw_limit={raw_limit_price:.2f} "
                f"> max_buy_price={max_buy_price:.2f}; capped_limit={limit_price:.2f}",
                flush=True,
            )
        if limit_price < price:
            print(
                f"[B BUY] {code} skip: limit_price={limit_price:.2f} < last_price={price:.2f} "
                f"max_buy_price={max_buy_price:.2f}",
                flush=True,
            )
            return False
        reject_limit_reversal, limit_reversal_reason = _intraday_reversal_reject(limit_price, day_open, day_high)
        if reject_limit_reversal:
            print(
                f"[B BUY] {code} skip: weak intraday limit {limit_reversal_reason}",
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
        _decision_log(
            "buy",
            code,
            "order_submit",
            qty=qty,
            est=f"{used_notional:.2f}",
            price=f"{price:.2f}",
            limit=f"{limit_price:.2f}",
            day_up=f"{day_up_pct:.2%}",
            bp=f"{buying_power:.2f}",
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
            _decision_log("buy", code, "order_reject", order_id=order_id, status=order_status)
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
            _decision_log("buy", code, "order_no_fill", order_id=order_id, status=final_status)
            _write_buy_cooldown(conn, code, order_id, f"NO_FILL status={final_status}")
            return False

        cost_price = float(filled_avg)
        qty_to_write = int(filled_qty)

        # ============================================================
        # 6) 落库
        # ============================================================
        init_sl = float(cost_price) * B_INITIAL_STOP_MULT

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
            b_peak_price=%s,
            b_peak_profit=%s,
            b_last_profit=%s,
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
                    round(float(cost_price), 2),
                    0.0,
                    0.0,
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
        _decision_log(
            "buy",
            code,
            "order_filled",
            order_id=order_id,
            qty=qty_to_write,
            cost=f"{cost_price:.2f}",
            sl=f"{init_sl:.2f}",
            day_up=f"{day_up_pct:.2%}",
            limit=f"{limit_price:.2f}",
        )
        return True

    except Exception as e:
        print(f"[B BUY] {code} ❌ error: {e}", flush=True)
        _decision_log("buy", code, "error", reason=str(e)[:120])
        if not _is_quote_transient_error(e):
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


# def strategy_B_sell(code: str) -> bool:
#     """
#     策略B：持仓后的动态管理（止损 / 分层加仓 / 分层减仓 / 结构退出）
#
#     当前最终版特点：
#     1) 涨3%后，止损抬到保本
#     2) 涨6%后，止损抬到成本+1%
#     3) 涨10%后，进入价格跟踪止盈（现价回撤约4%）
#     4) 同日买入后，只要盈利>=3%，允许触发卖出
#     5) 加仓更保守，避免平均成本被快速抬高
#     6) 所有止损都强制不能高于当前价附近，避免下一轮误触发
#     """
#
#     import math
#     import traceback
#     from datetime import datetime
#
#     code = (code or "").strip().upper()
#     print(f"[B SELL] {code}", flush=True)
#
#     MAX_TOTAL_MULTIPLIER = 1.6
#     MIN_ADD_QTY = 1
#     ENABLE_STRUCTURE_EXIT_STAGE = 6
#
#     TRAIL_BACKOFF_PCT = 0.04
#     DYNAMIC_TRAIL_START_PCT = 0.03
#
#     BLOCK_SAME_DAY_SELL_AFTER_BUY = True
#     SAME_DAY_FORCE_SELL_LOSS_PCT = -0.05
#     SAME_DAY_FORCE_SELL_WIN_PCT = 0.03
#
#     STAGE_RULES = [
#         # stage, profit_pct, sl_mult, add_ratio, sell_ratio
#         (1, 0.06, 1.01, 0.10, None),
#         (2, 0.12, 1.05, 0.10, None),
#         (3, 0.18, 1.10, None, 0.15),
#         (4, 0.25, 1.16, None, 0.20),
#         (5, 0.35, 1.25, None, 0.20),
#         (6, 0.45, 1.34, None, 0.15),
#         (7, 0.60, 1.48, None, 0.10),
#         (8, 0.75, 1.60, None, 0.10),
#         (9, 0.95, 1.78, None, 0.05),
#     ]
#
#     def _safe_int(v, default=0):
#         try:
#             return int(float(v))
#         except Exception:
#             return default
#
#     def _safe_float(v, default=0.0):
#         try:
#             return float(v)
#         except Exception:
#             return default
#
#     def _flash_wait_minutes(up_pct_):
#         if up_pct_ >= 0.30:
#             return 3
#         if up_pct_ >= 0.15:
#             return 2
#         return 0
#
#     def _read_stage_from_row(r: dict) -> int:
#         if not r:
#             return 0
#         if "b_stage" in r and r.get("b_stage") is not None:
#             try:
#                 return int(float(r.get("b_stage") or 0))
#             except Exception:
#                 pass
#         try:
#             return int(float(r.get("take_profit_price") or 0))
#         except Exception:
#             return 0
#
#     def _write_stage_and_sl(conn_, code_, stage_, sl_):
#         now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#         try:
#             _update_ops_fields(
#                 conn_,
#                 code_,
#                 b_stage=int(stage_),
#                 stop_loss_price=round(float(sl_), 2),
#                 updated_at=now_str,
#             )
#             return
#         except Exception:
#             pass
#
#         _update_ops_fields(
#             conn_,
#             code_,
#             take_profit_price=float(stage_),
#             stop_loss_price=round(float(sl_), 2),
#             updated_at=now_str,
#         )
#
#     def _parse_dt(v):
#         if not v:
#             return None
#         if isinstance(v, datetime):
#             return v
#         s = str(v).strip()
#         if not s:
#             return None
#         try:
#             return datetime.fromisoformat(s.replace("Z", ""))
#         except Exception:
#             pass
#         for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
#             try:
#                 return datetime.strptime(s[:19], fmt)
#             except Exception:
#                 pass
#         return None
#
#     def _is_same_day_buy_lock(row_):
#         if not BLOCK_SAME_DAY_SELL_AFTER_BUY:
#             return False
#
#         last_side = str(row_.get("last_order_side") or "").strip().lower()
#         last_time = _parse_dt(row_.get("last_order_time"))
#
#         if last_side != "buy" or last_time is None:
#             return False
#
#         return last_time.date() == datetime.now().date()
#
#     def _allow_sell_even_same_day(row_, up_pct_):
#         if not _is_same_day_buy_lock(row_):
#             return True
#
#         if up_pct_ <= SAME_DAY_FORCE_SELL_LOSS_PCT:
#             print(f"[B SELL] {code} same-day lock overridden by loss {up_pct_:.2%}", flush=True)
#             return True
#
#         if up_pct_ >= SAME_DAY_FORCE_SELL_WIN_PCT:
#             print(f"[B SELL] {code} same-day lock overridden by win {up_pct_:.2%}", flush=True)
#             return True
#
#         return False
#
#     def _latest_row_for_lock(conn_, fallback_row):
#         try:
#             return _load_one_b_row(conn_, code) or fallback_row
#         except Exception:
#             return fallback_row
#
#     def _cap_sl_below_price(sl_, price_):
#         sl_ = _safe_float(sl_, 0.0)
#         price_ = _safe_float(price_, 0.0)
#         if sl_ <= 0 or price_ <= 0:
#             return round(sl_, 2)
#         capped = min(sl_, price_ * 0.995)
#         return round(capped, 2)
#
#     def _calc_dynamic_trail_sl(cost_, price_, sl_old_):
#         """
#         最终版防守型动态止损：
#         1) 盈利<3%：不动
#         2) 盈利>=3%：止损抬到保本
#         3) 盈利>=6%：止损抬到成本+1%
#         4) 盈利>=10%：止损按现价回撤约4%
#         """
#         cost_ = _safe_float(cost_, 0.0)
#         price_ = _safe_float(price_, 0.0)
#         sl_old_ = _safe_float(sl_old_, 0.0)
#
#         if cost_ <= 0 or price_ <= 0:
#             return round(sl_old_, 2)
#
#         up_pct_ = (price_ - cost_) / cost_
#         new_sl = sl_old_
#
#         if up_pct_ < 0.03:
#             return round(new_sl, 2)
#
#         if up_pct_ >= 0.03:
#             new_sl = max(new_sl, cost_ * 1.00)
#
#         if up_pct_ >= 0.06:
#             new_sl = max(new_sl, cost_ * 1.01)
#
#         if up_pct_ >= 0.10:
#             new_sl = max(new_sl, price_ * (1.0 - TRAIL_BACKOFF_PCT))
#
#         new_sl = _cap_sl_below_price(new_sl, price_)
#         return round(new_sl, 2)
#
#     def _get_pending_stop_info(row_):
#         pending_since = _parse_dt(row_.get("b_stop_pending_since"))
#         pending_sl = _safe_float(row_.get("b_stop_pending_sl"), 0.0)
#         return pending_since, pending_sl
#
#     def _set_pending_stop(conn_, code_, sl_):
#         now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#         try:
#             _update_ops_fields(
#                 conn_,
#                 code_,
#                 b_stop_pending_since=now_str,
#                 b_stop_pending_sl=round(float(sl_), 2),
#                 updated_at=now_str,
#             )
#             return True
#         except Exception as e:
#             print(f"[B SELL] {code_} set pending stop failed: {e}", flush=True)
#             return False
#
#     def _clear_pending_stop(conn_, code_):
#         now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#         try:
#             _update_ops_fields(
#                 conn_,
#                 code_,
#                 b_stop_pending_since=None,
#                 b_stop_pending_sl=None,
#                 updated_at=now_str,
#             )
#             return True
#         except Exception as e:
#             print(f"[B SELL] {code_} clear pending stop failed: {e}", flush=True)
#             return False
#
#     def _find_highest_hit_stage(up_pct_):
#         hit = None
#         for rule in STAGE_RULES:
#             if up_pct_ >= rule[1]:
#                 hit = rule
#         return hit
#
#     def _find_rule_by_stage(stage_):
#         for rule in STAGE_RULES:
#             if rule[0] == stage_:
#                 return rule
#         return None
#
#     def _calc_stage_sl(cost_, price_, sl_mult_):
#         cost_ = _safe_float(cost_, 0.0)
#         price_ = _safe_float(price_, 0.0)
#         if cost_ <= 0 or price_ <= 0:
#             return 0.0
#         raw_sl = round(cost_ * float(sl_mult_), 2)
#         return _cap_sl_below_price(raw_sl, price_)
#
#     conn = None
#     traded = False
#
#     try:
#         conn = _connect()
#         row = _load_one_b_row(conn, code)
#
#         if not row:
#             print(f"[B SELL] {code} no row", flush=True)
#             return False
#
#         is_bought = _safe_int(row.get("is_bought"), 0)
#         can_sell = _safe_int(row.get("can_sell"), 0)
#
#         if is_bought != 1:
#             print(f"[B SELL] {code} not bought", flush=True)
#             return False
#
#         if can_sell != 1:
#             print(f"[B SELL] {code} can_sell != 1", flush=True)
#             return False
#
#         qty = _safe_int(row.get("qty"), 0)
#         cost = _safe_float(row.get("cost_price"), 0.0)
#         trigger = _safe_float(row.get("trigger_price"), 0.0)
#         sl = _safe_float(row.get("stop_loss_price"), 0.0)
#         last_stage = _read_stage_from_row(row)
#
#         if qty <= 0 or cost <= 0:
#             print(f"[B SELL] {code} invalid qty/cost qty={qty} cost={cost}", flush=True)
#             return False
#
#         base_qty = _safe_int(row.get("base_qty"), 0)
#         if base_qty <= 0:
#             base_qty = qty
#
#         max_total_qty = max(base_qty, int(math.floor(base_qty * MAX_TOTAL_MULTIPLIER)))
#
#         price, prev_close, feed = get_snapshot_realtime(code)
#         price = _safe_float(price, 0.0)
#
#         if price <= 0:
#             print(f"[B SELL] {code} invalid realtime price={price}", flush=True)
#             return False
#
#         up_pct = (price - cost) / cost if cost > 0 else 0.0
#         flash_wait_minutes = _flash_wait_minutes(up_pct)
#
#         print(
#             f"[B SELL] {code} price={price:.2f} cost={cost:.2f} up_pct={up_pct:.2%} "
#             f"qty={qty} sl={sl:.2f} stage={last_stage} flash_wait={flash_wait_minutes}m feed={feed}",
#             flush=True,
#         )
#
#         if sl <= 0:
#             init_sl = max(float(trigger or 0), float(cost) * 0.97) if cost > 0 else 0
#             if init_sl > 0:
#                 sl = _cap_sl_below_price(init_sl, price)
#                 try:
#                     _update_ops_fields(conn, code, stop_loss_price=sl)
#                     print(f"[B SELL] {code} init_sl={sl:.2f}", flush=True)
#                 except Exception as e:
#                     print(f"[B SELL] {code} init_sl write failed: {e}", flush=True)
#
#         if price > cost:
#             dyn_sl = _calc_dynamic_trail_sl(cost, max(price, peak_price), sl)
#             if dyn_sl > sl + 0.01:
#                 old_sl = sl
#                 sl = dyn_sl
#                 try:
#                     _update_ops_fields(conn, code, stop_loss_price=sl)
#                     print(f"[B SELL] {code} trail old_sl={old_sl:.2f} new_sl={sl:.2f}", flush=True)
#                 except Exception as e:
#                     print(f"[B SELL] {code} dynamic trail write failed: {e}", flush=True)
#
#         row_now = _latest_row_for_lock(conn, row)
#         pending_since, pending_sl = _get_pending_stop_info(row_now)
#
#         if pending_since and pending_sl > 0:
#             if price > pending_sl:
#                 _clear_pending_stop(conn, code)
#                 print(
#                     f"[B SELL] {code} pending stop canceled: price={price:.2f} > pending_sl={pending_sl:.2f}",
#                     flush=True,
#                 )
#             else:
#                 elapsed_sec = (datetime.now() - pending_since).total_seconds()
#                 wait_sec = flash_wait_minutes * 60
#
#                 if flash_wait_minutes > 0 and elapsed_sec < wait_sec:
#                     left_sec = int(wait_sec - elapsed_sec)
#                     print(
#                         f"[B SELL] {code} pending stop waiting: price={price:.2f} <= pending_sl={pending_sl:.2f}, "
#                         f"left={left_sec}s wait={flash_wait_minutes}m",
#                         flush=True,
#                     )
#                     return False
#
#                 reason = f"PENDING_STOP_TIMEOUT price={price:.2f} <= pending_sl={pending_sl:.2f} waited={flash_wait_minutes}m"
#                 print(f"[B SELL] {code} pending stop timeout sell qty={qty} reason={reason}", flush=True)
#                 traded = _sell_qty(conn, code, qty, reason) or traded
#                 if traded:
#                     _clear_pending_stop(conn, code)
#                 return traded
#
#         if sl > 0 and price <= sl:
#             row_now = _latest_row_for_lock(conn, row_now)
#
#             if flash_wait_minutes > 0:
#                 pending_since2, pending_sl2 = _get_pending_stop_info(row_now)
#
#                 if not pending_since2:
#                     _set_pending_stop(conn, code, sl)
#                     print(
#                         f"[B SELL] {code} start pending stop: price={price:.2f} <= sl={sl:.2f}, "
#                         f"up_pct={up_pct:.2%}, wait={flash_wait_minutes}m",
#                         flush=True,
#                     )
#                     return False
#
#                 elapsed_sec = (datetime.now() - pending_since2).total_seconds()
#                 wait_sec = flash_wait_minutes * 60
#
#                 if price > pending_sl2:
#                     _clear_pending_stop(conn, code)
#                     print(
#                         f"[B SELL] {code} pending recovered: price={price:.2f} > pending_sl={pending_sl2:.2f}",
#                         flush=True,
#                     )
#                     return False
#
#                 if elapsed_sec < wait_sec:
#                     left_sec = int(wait_sec - elapsed_sec)
#                     print(
#                         f"[B SELL] {code} still pending stop: left={left_sec}s price={price:.2f} pending_sl={pending_sl2:.2f}",
#                         flush=True,
#                     )
#                     return False
#
#                 reason = f"PENDING_STOP_TIMEOUT price={price:.2f} <= pending_sl={pending_sl2:.2f} waited={flash_wait_minutes}m"
#                 print(f"[B SELL] {code} timeout hard stop sell qty={qty} reason={reason}", flush=True)
#                 traded = _sell_qty(conn, code, qty, reason) or traded
#                 if traded:
#                     _clear_pending_stop(conn, code)
#                 return traded
#
#             if not _allow_sell_even_same_day(row_now, up_pct):
#                 print(
#                     f"[B SELL] {code} same-day buy lock: price={price:.2f} <= sl={sl:.2f} up_pct={up_pct:.2%}",
#                     flush=True,
#                 )
#                 return False
#
#             reason = f"STOP price={price:.2f} <= sl={sl:.2f}"
#             print(f"[B SELL] {code} hard stop sell qty={qty} reason={reason}", flush=True)
#             traded = _sell_qty(conn, code, qty, reason) or traded
#             return traded
#         else:
#             if pending_since:
#                 _clear_pending_stop(conn, code)
#
#         highest_rule = _find_highest_hit_stage(up_pct)
#
#         if highest_rule is not None:
#             highest_stage, highest_pct, highest_sl_mult, highest_add_ratio, highest_sell_ratio = highest_rule
#
#             if highest_stage > last_stage:
#                 next_stage = last_stage + 1
#                 next_rule = _find_rule_by_stage(next_stage)
#
#                 if highest_stage > next_stage:
#                     sl_old = float(sl or 0)
#                     stage_sl = _calc_stage_sl(cost, price, highest_sl_mult)
#                     dyn_sl = _calc_dynamic_trail_sl(cost, price, sl_old)
#                     sl = max(sl_old, stage_sl, dyn_sl)
#                     sl = _cap_sl_below_price(sl, price)
#
#                     if highest_sell_ratio is not None and highest_sell_ratio > 0:
#                         row_now = _latest_row_for_lock(conn, row)
#
#                         if _allow_sell_even_same_day(row_now, up_pct):
#                             raw_sell_qty = int(math.floor(qty * float(highest_sell_ratio)))
#                             sell_qty = max(raw_sell_qty, 1)
#                             sell_qty = min(sell_qty, qty)
#
#                             reason = (
#                                 f"JUMP_STAGE{highest_stage}_SELL{int(highest_sell_ratio * 100)} "
#                                 f"price={price:.2f} qty={sell_qty} last_stage={last_stage}"
#                             )
#
#                             print(f"[B SELL] {code} jump sell qty={sell_qty} reason={reason}", flush=True)
#
#                             sell_ok = _sell_qty(conn, code, sell_qty, reason)
#                             traded = sell_ok or traded
#
#                             row_after = _load_one_b_row(conn, code) or {}
#                             qty_after = _safe_int(row_after.get("qty"), max(qty - sell_qty, 0))
#                             cost_after = _safe_float(row_after.get("cost_price"), cost)
#                             sl_old_after = _safe_float(row_after.get("stop_loss_price"), sl)
#
#                             stage_sl = _calc_stage_sl(cost_after, price, highest_sl_mult)
#                             dyn_sl = _calc_dynamic_trail_sl(cost_after, price, sl_old_after)
#                             sl = max(sl_old_after, stage_sl, dyn_sl)
#                             sl = _cap_sl_below_price(sl, price)
#
#                             if qty_after > 0:
#                                 _write_stage_and_sl(conn, code, highest_stage, sl)
#
#                             print(
#                                 f"[B SELL] {code} stage jump sell_done: last_stage={last_stage} -> highest_stage={highest_stage} "
#                                 f"left_qty={qty_after} sl={sl:.2f}",
#                                 flush=True,
#                             )
#                             return traded
#
#                         print(
#                             f"[B SELL] {code} stage jump sell skipped by same-day lock: "
#                             f"last_stage={last_stage} -> highest_stage={highest_stage} "
#                             f"up_pct={up_pct:.2%}; stage NOT advanced, only update SL",
#                             flush=True,
#                         )
#
#                         try:
#                             _update_ops_fields(
#                                 conn,
#                                 code,
#                                 stop_loss_price=round(float(sl), 2),
#                                 updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#                             )
#                         except Exception as e:
#                             print(f"[B SELL] {code} jump skip sell update sl failed: {e}", flush=True)
#
#                         return traded
#
#                     _write_stage_and_sl(conn, code, highest_stage, sl)
#                     print(
#                         f"[B SELL] {code} stage jump: last_stage={last_stage} -> highest_stage={highest_stage} "
#                         f"up_pct={up_pct:.2%}; skip missed add, no_jump_sell, new_sl={sl:.2f}",
#                         flush=True,
#                     )
#                     return traded
#
#                 if next_rule is not None:
#                     stage, pct, sl_mult, add_ratio, sell_ratio = next_rule
#
#                     if up_pct >= pct:
#                         print(
#                             f"[B SELL] {code} hit stage={stage} threshold={pct:.2%} up_pct={up_pct:.2%}",
#                             flush=True,
#                         )
#
#                         if add_ratio is not None and add_ratio > 0:
#                             raw_add_qty = int(math.floor(qty * float(add_ratio)))
#                             raw_add_qty = max(raw_add_qty, MIN_ADD_QTY)
#
#                             allow_add_qty = max(0, max_total_qty - qty)
#                             add_qty = min(raw_add_qty, allow_add_qty)
#
#                             if add_qty > 0:
#                                 try:
#                                     tc_check = _get_trading_client()
#                                     buying_power = _get_buying_power(tc_check)
#                                     est_add_cost = float(add_qty) * float(price)
#                                     need_cash = est_add_cost * 1.03
#
#                                     if buying_power < need_cash:
#                                         print(
#                                             f"[B SELL] {code} skip add: buying_power={buying_power:.2f} "
#                                             f"< need≈{need_cash:.2f} add_qty={add_qty} price={price:.2f}",
#                                             flush=True,
#                                         )
#                                         add_qty = 0
#
#                                 except Exception as e:
#                                     print(
#                                         f"[B SELL] {code} skip add: failed to check buying_power err={e}",
#                                         flush=True,
#                                     )
#                                     add_qty = 0
#
#                             if add_qty > 0:
#                                 reason = f"STAGE{stage}_ADD{int(add_ratio * 100)} price={price:.2f} qty={add_qty}"
#                                 print(f"[B SELL] {code} add qty={add_qty} reason={reason}", flush=True)
#
#                                 buy_ok = _buy_add_qty(conn, code, add_qty, reason, snap_price=price)
#                                 traded = buy_ok or traded
#
#                                 row2 = _load_one_b_row(conn, code) or {}
#                                 qty = _safe_int(row2.get("qty"), qty)
#                                 cost = _safe_float(row2.get("cost_price"), cost)
#                                 sl_old = _safe_float(row2.get("stop_loss_price"), sl)
#
#                                 stage_sl = _calc_stage_sl(cost, price, sl_mult)
#                                 dyn_sl = _calc_dynamic_trail_sl(cost, price, sl_old)
#                                 sl = max(sl_old, stage_sl, dyn_sl)
#                                 sl = _cap_sl_below_price(sl, price)
#
#                                 _write_stage_and_sl(conn, code, stage, sl)
#
#                                 print(f"[B SELL] {code} add_done qty={qty} cost={cost:.2f} sl={sl:.2f}", flush=True)
#                                 return traded
#
#                             sl_old = float(sl or 0)
#                             stage_sl = _calc_stage_sl(cost, price, sl_mult)
#                             dyn_sl = _calc_dynamic_trail_sl(cost, price, sl_old)
#                             sl = max(sl_old, stage_sl, dyn_sl)
#                             sl = _cap_sl_below_price(sl, price)
#
#                             _write_stage_and_sl(conn, code, stage, sl)
#
#                             print(f"[B SELL] {code} skip add, stage={stage}, new_sl={sl:.2f}", flush=True)
#                             return traded
#
#                         if sell_ratio is not None and sell_ratio > 0:
#                             row_now = _latest_row_for_lock(conn, row)
#
#                             if not _allow_sell_even_same_day(row_now, up_pct):
#                                 print(
#                                     f"[B SELL] {code} same-day buy lock: stage={stage} sell skipped up_pct={up_pct:.2%}",
#                                     flush=True,
#                                 )
#                                 return False
#
#                             raw_sell_qty = int(math.floor(qty * float(sell_ratio)))
#                             sell_qty = max(raw_sell_qty, 1)
#                             sell_qty = min(sell_qty, qty)
#
#                             reason = f"STAGE{stage}_SELL{int(sell_ratio * 100)} price={price:.2f} qty={sell_qty}"
#                             print(f"[B SELL] {code} sell qty={sell_qty} reason={reason}", flush=True)
#
#                             sell_ok = _sell_qty(conn, code, sell_qty, reason)
#                             traded = sell_ok or traded
#
#                             row3 = _load_one_b_row(conn, code) or {}
#                             qty = _safe_int(row3.get("qty"), max(qty - sell_qty, 0))
#                             cost = _safe_float(row3.get("cost_price"), cost)
#                             sl_old = _safe_float(row3.get("stop_loss_price"), sl)
#
#                             stage_sl = _calc_stage_sl(cost, price, sl_mult)
#                             dyn_sl = _calc_dynamic_trail_sl(cost, price, sl_old)
#                             sl = max(sl_old, stage_sl, dyn_sl)
#                             sl = _cap_sl_below_price(sl, price)
#
#                             if qty > 0:
#                                 _write_stage_and_sl(conn, code, stage, sl)
#
#                             print(f"[B SELL] {code} sell_done left_qty={qty} cost={cost:.2f} sl={sl:.2f}", flush=True)
#                             return traded
#
#                         sl_old = float(sl or 0)
#                         stage_sl = _calc_stage_sl(cost, price, sl_mult)
#                         dyn_sl = _calc_dynamic_trail_sl(cost, price, sl_old)
#                         sl = max(sl_old, stage_sl, dyn_sl)
#                         sl = _cap_sl_below_price(sl, price)
#
#                         _write_stage_and_sl(conn, code, stage, sl)
#
#                         print(f"[B SELL] {code} hold_only stage={stage} new_sl={sl:.2f}", flush=True)
#                         return traded
#
#         if last_stage >= ENABLE_STRUCTURE_EXIT_STAGE:
#             closes = _get_recent_closes(conn, code, n=4)
#             if len(closes) >= 4:
#                 c0, c1, c2, c3 = closes[0], closes[1], closes[2], closes[3]
#                 c0 = _safe_float(c0, 0.0)
#                 c1 = _safe_float(c1, 0.0)
#                 c2 = _safe_float(c2, 0.0)
#                 c3 = _safe_float(c3, 0.0)
#
#                 min3 = min(c1, c2, c3)
#
#                 if c0 > 0 and min3 > 0 and c0 < min3 and qty > 0:
#                     row_now = _latest_row_for_lock(conn, row)
#
#                     if not _allow_sell_even_same_day(row_now, up_pct):
#                         print(
#                             f"[B SELL] {code} same-day buy lock: structure exit skipped up_pct={up_pct:.2%}",
#                             flush=True,
#                         )
#                         return False
#
#                     reason = f"STRUCT_EXIT close0={c0:.2f} < min3={min3:.2f}"
#                     print(f"[B SELL] {code} structure exit qty={qty} reason={reason}", flush=True)
#                     traded = _sell_qty(conn, code, qty, reason) or traded
#                     return traded
#
#         return traded
#
#     except Exception as e:
#         print(f"[B SELL] {code} ❌ error: {e}", flush=True)
#         traceback.print_exc()
#         return False
#
#     finally:
#         try:
#             if conn:
#                 conn.close()
#         except Exception:
#             pass


def strategy_B_sell(code: str) -> bool:
    """
    策略B：持仓后的动态管理（无加仓清爽版）

    设计哲学：
    1) 不加仓 —— 初始仓位即最终仓位,简化心智
    2) Stage 仅作"分层落袋的触发器",不再控 SL
    3) 普通B给动量波动空间：初始 -5%，涨 5% 后抬到成本线
    4) 涨 5% 后启用“最高价回撤保护”，防止利润大幅回吐
    5) 保留 Stage 分层止盈 / 闪崩 pending stop / 结构退出；不再限制买入当天卖出

    搭配建议：
    - 把 B_TARGET_NOTIONAL_USD 抬到 1500-2500（用更大基础仓换金字塔效应）
    - _buy_add_qty 不再被调用,可以删,也可以留着不影响
    """

    import math
    import traceback
    from datetime import datetime

    code = (code or "").strip().upper()
    print(f"[B SELL] {code}", flush=True)

    # ============================================================
    # 配置
    # ============================================================
    ENABLE_STRUCTURE_EXIT_STAGE = 3  # +60% 后启用 K 线结构退出

    # 普通B止损参数：
    # - 买入后初始止损由 strategy_B_buy 写入 cost*0.95。
    # - 如果历史记录没有 stop_loss_price，这里也会补成 cost*0.95。
    # - 当前涨幅 >= 5% 后，止损抬到 cost*1.00，先保护本金。
    TRAIL_LOCK_START_PCT = B_TRAIL_LOCK_START_PCT
    TRAIL_LOCK_SL_MULT = B_TRAIL_LOCK_SL_MULT
    INITIAL_STOP_GRACE_SECONDS = B_INITIAL_STOP_GRACE_SECONDS
    CATASTROPHIC_STOP_LOSS_PCT = B_CATASTROPHIC_STOP_LOSS_PCT

    # 最高价回撤保护：
    # 这不是替代分层止盈，而是保护“已经涨起来但又回落”的剩余仓位。
    # 涨幅越大，允许从高点回撤的空间越大，避免妖股后期被太早洗掉。
    PEAK_GIVEBACK_RULES = [
        *B_PEAK_GIVEBACK_RULES,
    ]

    # 现在账户不再受日内交易限制，买入后立刻允许按止损/止盈规则卖出。
    BLOCK_SAME_DAY_SELL_AFTER_BUY = False
    SAME_DAY_FORCE_SELL_LOSS_PCT = -0.05

    # Stage 规则：纯减仓阶梯（add_ratio 永远 None,sl_mult 永远 None）
    # 总落袋: 20+16+10+5+4 ≈ 55%,留 45% 仓位裸奔到天上
    STAGE_RULES = [
        # stage, profit_pct, sl_mult, add_ratio, sell_ratio
        *B_STAGE_SELL_RULES,
    ]

    # ============================================================
    # 内嵌 helper
    # ============================================================
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

    def _read_stage_from_row(r):
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
                conn_, code_,
                b_stage=int(stage_),
                stop_loss_price=round(float(sl_), 2),
                updated_at=now_str,
            )
            return
        except Exception:
            pass
        _update_ops_fields(
            conn_, code_,
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
        return False

    def _initial_stop_grace_active(row_, up_pct_):
        """买入后短暂忽略普通止损，但灾难止损立即生效。"""
        if INITIAL_STOP_GRACE_SECONDS <= 0:
            return False
        if up_pct_ <= CATASTROPHIC_STOP_LOSS_PCT:
            return False
        last_side = str(row_.get("last_order_side") or "").strip().lower()
        last_time = _parse_dt(row_.get("last_order_time"))
        if last_side != "buy" or last_time is None:
            return False
        elapsed = (datetime.now() - last_time).total_seconds()
        return 0 <= elapsed < INITIAL_STOP_GRACE_SECONDS

    def _latest_row_for_lock(conn_, fallback_row):
        try:
            return _load_one_b_row(conn_, code) or fallback_row
        except Exception:
            return fallback_row

    def _cap_sl_below_price(sl_, price_):
        """只在 SL >= 当前价时才动手,避免误压。"""
        sl_ = _safe_float(sl_, 0.0)
        price_ = _safe_float(price_, 0.0)
        if sl_ <= 0 or price_ <= 0:
            return round(sl_, 2)
        if sl_ < price_:
            return round(sl_, 2)
        return round(price_ * 0.99, 2)

    def _calc_dynamic_trail_sl(cost_, price_, sl_old_):
        """
        普通B动态止损：
          1) 初始 SL = cost*0.95，由买入落库；这里兜底补齐。
          2) 涨幅严格超过 3% 后，SL 至少为 cost*1.01；已有止损不下调。
          3) 更高涨幅不在这里继续抬 SL，交给“最高价回撤保护”处理。
        """
        cost_ = _safe_float(cost_, 0.0)
        price_ = _safe_float(price_, 0.0)
        sl_old_ = _safe_float(sl_old_, 0.0)

        if cost_ <= 0 or price_ <= 0:
            return round(sl_old_, 2)

        up_pct_ = (price_ - cost_) / cost_
        new_sl = sl_old_

        # Early profit lock: strictly above +3%, protect +1% of entry cost.
        if price_ > cost_ * 1.03:
            new_sl = max(new_sl, cost_ * 1.01)

        if up_pct_ >= TRAIL_LOCK_START_PCT:
            new_sl = max(new_sl, cost_ * TRAIL_LOCK_SL_MULT)

        return round(new_sl, 2)

    def _giveback_pct_for_peak(peak_gain_pct_):
        """
        根据持仓以来最高涨幅，决定允许从最高价回撤多少。

        返回 None 表示还没涨够，不启用最高价回撤保护。
        """
        for min_gain, giveback_pct in PEAK_GIVEBACK_RULES:
            if peak_gain_pct_ >= min_gain:
                return giveback_pct
        return None

    def _update_peak_tracking(conn_, code_, row_, cost_, qty_, price_):
        """
        记录持仓以来最高价和最高浮盈。

        为了避免每轮都写数据库，只有这些情况才写：
        - 当前价格刷新 b_peak_price
        - 当前浮盈相对 b_last_profit 变化超过 20 美元

        需要字段：
          b_peak_price, b_peak_profit, b_last_profit
        """
        old_peak_price = _safe_float(row_.get("b_peak_price"), 0.0)
        old_last_profit = _safe_float(row_.get("b_last_profit"), 0.0)

        if old_peak_price <= 0 or old_peak_price < cost_ * 0.5:
            old_peak_price = cost_

        peak_price = max(old_peak_price, price_)
        profit_now = (price_ - cost_) * qty_
        peak_profit = max(_safe_float(row_.get("b_peak_profit"), 0.0), (peak_price - cost_) * qty_)

        updates = {}
        if peak_price > old_peak_price + 0.005:
            updates["b_peak_price"] = round(float(peak_price), 4)
            updates["b_peak_profit"] = round(float(peak_profit), 4)

        if abs(profit_now - old_last_profit) >= 20:
            updates["b_last_profit"] = round(float(profit_now), 4)

        if updates:
            try:
                _update_ops_fields(conn_, code_, **updates)
            except Exception as e:
                print(f"[B SELL] {code_} peak tracking write failed: {e}", flush=True)

        return peak_price, profit_now, peak_profit

    def _get_pending_stop_info(row_):
        pending_since = _parse_dt(row_.get("b_stop_pending_since"))
        pending_sl = _safe_float(row_.get("b_stop_pending_sl"), 0.0)
        return pending_since, pending_sl

    def _set_pending_stop(conn_, code_, sl_):
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            _update_ops_fields(
                conn_, code_,
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
                conn_, code_,
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

    # ============================================================
    # 主流程
    # ============================================================
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

        # ----- 0) 更新最高价/最高浮盈追踪 -----
        # 这个数据用于后面的“最高价回撤保护”：
        # 比如最高涨到 +5% 后，如果从最高价回撤 2%，就保护利润离场。
        peak_price, profit_now, peak_profit = _update_peak_tracking(conn, code, row, cost, qty, price)
        peak_gain_pct = (peak_price - cost) / cost if cost > 0 else 0.0
        print(
            f"[B SELL] {code} peak_price={peak_price:.2f} peak_gain={peak_gain_pct:.2%} "
            f"profit_now={profit_now:.2f} peak_profit={peak_profit:.2f}",
            flush=True,
        )

        # ----- 1) 补初始 SL -----
        if sl <= 0:
            # 普通B初始止损统一用 cost*0.95，给动量股正常震荡空间。
            init_sl = float(cost) * B_INITIAL_STOP_MULT if cost > 0 else 0
            if init_sl > 0:
                sl = _cap_sl_below_price(init_sl, price)
                try:
                    _update_ops_fields(conn, code, stop_loss_price=sl)
                    print(f"[B SELL] {code} init_sl={sl:.2f}", flush=True)
                except Exception as e:
                    print(f"[B SELL] {code} init_sl write failed: {e}", flush=True)

        # ----- 2) 动态拖移 SL -----
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

        # ----- 3) 最高价回撤保护 -----
        # 这条规则专门解决“最高赚很多，最后吐回很多”的问题。
        # 它不取消分层止盈；如果价格一路涨，不触发回撤，后面仍然会执行 Stage 分层卖出。
        giveback_pct = _giveback_pct_for_peak(peak_gain_pct)
        if giveback_pct is not None:
            giveback_trigger = _peak_giveback_trigger(cost, peak_price)
            print(
                f"[B SELL] {code} giveback watch: peak={peak_price:.2f} "
                f"allow_pullback={giveback_pct:.2%} trigger={giveback_trigger:.2f}",
                flush=True,
            )

            if price <= giveback_trigger:
                row_now = _latest_row_for_lock(conn, row)
                if not _allow_sell_even_same_day(row_now, up_pct):
                    print(
                        f"[B SELL] {code} giveback blocked by same-day lock: "
                        f"price={price:.2f} trigger={giveback_trigger:.2f} up_pct={up_pct:.2%}",
                        flush=True,
                    )
                    return False

                reason = (
                    f"PEAK_GIVEBACK price={price:.2f} <= trigger={giveback_trigger:.2f} "
                    f"peak={peak_price:.2f} peak_gain={peak_gain_pct:.2%} "
                    f"pullback={giveback_pct:.2%} profit_now={profit_now:.2f} peak_profit={peak_profit:.2f}"
                )
                print(f"[B SELL] {code} peak giveback sell qty={qty} reason={reason}", flush=True)
                traded = _sell_qty(conn, code, qty, reason, limit_price=price) or traded
                return traded

        # ----- 3) 已存在的 pending stop -----
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
                traded = _sell_qty(conn, code, qty, reason, limit_price=price) or traded
                if traded:
                    _clear_pending_stop(conn, code)
                return traded

        # ----- 4) 硬止损 -----
        if sl > 0 and price <= sl:
            row_now = _latest_row_for_lock(conn, row_now)

            if _initial_stop_grace_active(row_now, up_pct):
                last_time = _parse_dt(row_now.get("last_order_time"))
                elapsed = int((datetime.now() - last_time).total_seconds()) if last_time else 0
                left = max(INITIAL_STOP_GRACE_SECONDS - elapsed, 0)
                print(
                    f"[B SELL] {code} initial stop grace: price={price:.2f} <= sl={sl:.2f} "
                    f"up_pct={up_pct:.2%}, left={left}s, catastrophic={CATASTROPHIC_STOP_LOSS_PCT:.2%}",
                    flush=True,
                )
                return False

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
                traded = _sell_qty(conn, code, qty, reason, limit_price=price) or traded
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
            traded = _sell_qty(conn, code, qty, reason, limit_price=price) or traded
            return traded
        else:
            if pending_since:
                _clear_pending_stop(conn, code)

        # ----- 5) Stage 推进（含跳级,纯减仓）-----
        highest_rule = _find_highest_hit_stage(up_pct)

        if highest_rule is not None:
            highest_stage, highest_pct, _, _, highest_sell_ratio = highest_rule

            if highest_stage > last_stage:
                next_stage = last_stage + 1
                next_rule = _find_rule_by_stage(next_stage)

                # 跳级
                if highest_stage > next_stage:
                    # 跳级时 SL 已经被 dynamic trail 抬上去了,这里只需推进 stage
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
                            sell_ok = _sell_qty(conn, code, sell_qty, reason, limit_price=price)
                            traded = sell_ok or traded

                            row_after = _load_one_b_row(conn, code) or {}
                            qty_after = _safe_int(row_after.get("qty"), max(qty - sell_qty, 0))
                            sl_old_after = _safe_float(row_after.get("stop_loss_price"), sl)
                            cost_after = _safe_float(row_after.get("cost_price"), cost)

                            dyn_sl = _calc_dynamic_trail_sl(cost_after, price, sl_old_after)
                            sl = max(sl_old_after, dyn_sl)
                            sl = _cap_sl_below_price(sl, price)

                            if qty_after > 0:
                                _write_stage_and_sl(conn, code, highest_stage, sl)

                            print(
                                f"[B SELL] {code} stage jump sell_done: last_stage={last_stage} -> {highest_stage} "
                                f"left_qty={qty_after} sl={sl:.2f}",
                                flush=True,
                            )
                            return traded

                        # same-day lock 阻挡:不推进 stage
                        print(
                            f"[B SELL] {code} stage jump sell skipped by same-day lock: "
                            f"last_stage={last_stage} -> {highest_stage} up_pct={up_pct:.2%}",
                            flush=True,
                        )
                        return traded

                    # 跳级到没 sell 的档位:推进 stage
                    _write_stage_and_sl(conn, code, highest_stage, sl)
                    print(
                        f"[B SELL] {code} stage jump (no sell): last_stage={last_stage} -> {highest_stage} sl={sl:.2f}",
                        flush=True,
                    )
                    return traded

                # 正常推进下一档
                if next_rule is not None:
                    stage, pct, _, _, sell_ratio = next_rule
                    if up_pct >= pct:
                        print(
                            f"[B SELL] {code} hit stage={stage} threshold={pct:.2%} up_pct={up_pct:.2%}",
                            flush=True,
                        )

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
                            sell_ok = _sell_qty(conn, code, sell_qty, reason, limit_price=price)
                            traded = sell_ok or traded

                            row3 = _load_one_b_row(conn, code) or {}
                            qty = _safe_int(row3.get("qty"), max(qty - sell_qty, 0))
                            cost = _safe_float(row3.get("cost_price"), cost)
                            sl_old = _safe_float(row3.get("stop_loss_price"), sl)

                            dyn_sl = _calc_dynamic_trail_sl(cost, price, sl_old)
                            sl = max(sl_old, dyn_sl)
                            sl = _cap_sl_below_price(sl, price)

                            if qty > 0:
                                _write_stage_and_sl(conn, code, stage, sl)
                            print(f"[B SELL] {code} sell_done left_qty={qty} cost={cost:.2f} sl={sl:.2f}", flush=True)
                            return traded

                        # 该档没有 sell:推进 stage
                        _write_stage_and_sl(conn, code, stage, sl)
                        print(f"[B SELL] {code} stage_marker stage={stage} sl={sl:.2f}", flush=True)
                        return traded

        # ----- 6) 结构退出（stage >= 3 即 +60% 之后）-----
        if last_stage >= ENABLE_STRUCTURE_EXIT_STAGE:
            closes = _get_recent_closes(conn, code, n=4)
            if len(closes) >= 4:
                c0 = _safe_float(closes[0], 0.0)
                c1 = _safe_float(closes[1], 0.0)
                c2 = _safe_float(closes[2], 0.0)
                c3 = _safe_float(closes[3], 0.0)
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
                    traded = _sell_qty(conn, code, qty, reason, limit_price=price) or traded
                    return traded

        return traded

    except Exception as e:
        print(f"[B SELL] {code} ❌ error: {e}", flush=True)
        if not _is_quote_transient_error(e):
            traceback.print_exc()
        return False

    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
