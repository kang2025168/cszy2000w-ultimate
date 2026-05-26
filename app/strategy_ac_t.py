from __future__ import annotations

"""A/C 长期核心仓日内做T策略。

A = ETF / 指数基金长期核心仓。
C = 长期成长股核心仓。

本模块替代旧 strategy_a.py。它不是清仓策略，而是围绕长期核心仓做日内T：
- 上涨/低开反弹：买入新增仓，之后只卖新增仓，不动核心仓。
- 下跌/高开回撤：可临时卖出核心仓，但 12:55 必须恢复，避免隔夜丢失核心仓。
"""

import argparse
import math
import time as sleep_time
import traceback
from dataclasses import dataclass
from datetime import datetime, time
from decimal import Decimal, ROUND_HALF_UP

from zoneinfo import ZoneInfo

from ultimate_v1.alpaca_gateway import get_latest_stock_price, trading_client
from ultimate_v1.config import env_bool, env_float, env_str, settings
from ultimate_v1.db import db_conn
from ultimate_v1.schema import ensure_schema


TABLE = settings().ops_table
PRICES_TABLE = env_str("AC_T_PRICES_TABLE", env_str("B_PRICES_TABLE", "stock_prices_pool"))
LA_TZ = ZoneInfo(settings().timezone or "America/Los_Angeles")

MARKET_OPEN = time(6, 30)        # 06:30-06:40 只观察开盘，不交易
TRADE_START = time(6, 40)        # 06:40 后才允许新开做T动作
FORCE_RECOVER_TIME = time(12, 55)  # 12:55 后只恢复核心仓，不再新开做T
MARKET_CLOSE = time(13, 0)

STATE_IDLE = "IDLE"
STATE_UP_HOLDING = "UP_T_HOLDING"
STATE_UP_WAIT_COST = "UP_T_WAIT_SELL_AT_COST"
STATE_DOWN_WAIT_BUY = "DOWN_T_WAIT_BUYBACK"
STATE_DOWN_WAIT_SELL_PRICE = "DOWN_T_WAIT_BUYBACK_AT_SELL_PRICE"
STATE_GAP_UP_WAIT_PULLBACK_SELL = "GAP_UP_WAIT_PULLBACK_SELL"
STATE_GAP_UP_WAIT_BUYBACK = "GAP_UP_WAIT_BUYBACK"
STATE_GAP_DOWN_WAIT_REBOUND_BUY = "GAP_DOWN_WAIT_REBOUND_BUY"
STATE_GAP_DOWN_HOLDING = "GAP_DOWN_HOLDING"

DOWN_STATES = {STATE_DOWN_WAIT_BUY, STATE_DOWN_WAIT_SELL_PRICE, STATE_GAP_UP_WAIT_BUYBACK}
UP_STATES = {STATE_UP_HOLDING, STATE_UP_WAIT_COST, STATE_GAP_DOWN_HOLDING}

AC_T_PARAMS = {
    "A": {
        "up_trigger_pct": 0.005,
        "up_take_profit_pct": 0.01,
        "up_pullback_pct": 0.0025,
        "down_trigger_pct": 0.005,
        "down_continue_pct": 0.01,
        "down_rebound_pct": 0.0025,
        "gap_pct": 0.005,
        "gap_pullback_pct": 0.0025,
        "gap_rebound_pct": 0.0025,
        "gap_buy_take_profit_pct": 0.005,
        "gap_sell_continue_pct": 0.005,
    },
    "C": {
        "up_trigger_pct": 0.01,
        "up_take_profit_pct": None,
        "up_pullback_pct": 0.01,
        "down_trigger_pct": 0.01,
        "down_continue_pct": None,
        "down_rebound_pct": 0.01,
        "gap_pct": 0.01,
        "gap_pullback_pct": 0.01,
        "gap_rebound_pct": 0.01,
        "gap_buy_take_profit_pct": 0.01,
        "gap_sell_continue_pct": 0.01,
    },
}

MIN_BUYING_POWER = env_float("AC_T_MIN_BUYING_POWER", 100.0)
BUY_LIMIT_BUFFER_PCT = env_float("AC_T_BUY_LIMIT_BUFFER_PCT", 0.002)
SELL_LIMIT_BUFFER_PCT = env_float("AC_T_SELL_LIMIT_BUFFER_PCT", 0.002)
FILL_WAIT_SEC = env_float("AC_T_FILL_WAIT_SEC", 4.0)
DRY_RUN = env_bool("AC_T_DRY_RUN", False)
FORCE_CLOSE_UP_T = env_bool("AC_T_FORCE_CLOSE_UP_T", True)
MIN_LEG_HOLD_MINUTES = env_float("AC_T_MIN_LEG_HOLD_MINUTES", 30.0)

ACTIVE_ORDER_STATUSES = {
    "new",
    "accepted",
    "pending_new",
    "accepted_for_bidding",
    "partially_filled",
    "calculated",
    "held",
    "pending_replace",
    "pending_cancel",
}
TERMINAL_ORDER_STATUSES = {"filled", "canceled", "cancelled", "expired", "rejected", "done_for_day"}


@dataclass
class FillResult:
    submitted: bool
    order_id: str = ""
    status: str = ""
    filled_qty: int = 0
    filled_avg_price: float = 0.0
    error: str = ""


def _now_la() -> datetime:
    return datetime.now(LA_TZ)


def _today_la():
    return _now_la().date()


def _money(price: float) -> float:
    q = Decimal(str(price or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return max(float(q), 0.01)


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except Exception:
        return default


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(float(value or default))
    except Exception:
        return default


def _row_key(row: dict) -> tuple[str, tuple]:
    if row.get("id") is not None:
        return "id=%s", (row["id"],)
    return "stock_code=%s AND stock_type=%s", (row["stock_code"], row.get("stock_type") or row.get("ac_t_type"))


def _set_row(conn, row: dict, fields: dict) -> None:
    if not fields:
        return
    where_sql, where_args = _row_key(row)
    cols = [f"`{k}`=%s" for k in fields]
    args = list(fields.values()) + list(where_args)
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE `{TABLE}` SET {', '.join(cols)}, updated_at=NOW() WHERE {where_sql}",
            tuple(args),
        )


def _reset_to_idle_fields(extra: dict | None = None) -> dict:
    """完成一轮做T后，清空临时做T状态，只保留核心仓字段。"""
    fields = {
        "ac_t_state": STATE_IDLE,
        "ac_t_qty": 0,
        "ac_t_buy_price": None,
        "ac_t_sell_price": None,
        "ac_t_high_price": None,
        "ac_t_low_price": None,
        "ac_t_entry_time": None,
        "ac_t_trade_high_price": None,
        "ac_t_trade_low_price": None,
        "ac_t_extreme_confirmed": 0,
        "ac_t_temporarily_out": 0,
        "ac_t_force_recover_deadline": None,
    }
    if extra:
        fields.update(extra)
    return fields


def load_ac_t_rows(conn, symbol: str | None = None) -> list[dict]:
    args: list = []
    symbol_filter = ""
    if symbol:
        symbol_filter = "AND stock_code=%s"
        args.append(symbol.upper())
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT *
            FROM `{TABLE}`
            WHERE COALESCE(ac_t_enabled, 1)=1
              -- 必须显式标记 ac_t_type，避免误扫旧 C 策略或普通 A/C 记录。
              AND UPPER(COALESCE(NULLIF(ac_t_type, ''), '')) IN ('A','C')
              {symbol_filter}
            ORDER BY stock_code
            """,
            tuple(args),
        )
        return list(cur.fetchall())


def _get_position_qty(client, symbol: str) -> int:
    """读取券商真实持仓，做卖核心仓前必须以真实仓位为准。"""
    try:
        pos = client.get_open_position(symbol)
        return int(math.floor(abs(float(getattr(pos, "qty", 0) or 0))))
    except Exception:
        return 0


def _has_buying_power(client, qty: int, price: float) -> bool:
    try:
        acct = client.get_account()
        buying_power = float(getattr(acct, "buying_power", 0) or 0)
    except Exception as exc:
        print(f"[AC_T] account read failed: {exc}", flush=True)
        return False
    need = max(float(qty) * float(price), MIN_BUYING_POWER)
    if buying_power < need:
        print(f"[AC_T] skip buy: buying_power={buying_power:.2f} need={need:.2f}", flush=True)
        return False
    return True


def _submit_limit_and_wait(client, symbol: str, qty: int, side: str, price: float) -> FillResult:
    from alpaca.trading.enums import OrderSide, TimeInForce
    from alpaca.trading.requests import LimitOrderRequest

    if qty <= 0 or price <= 0:
        return FillResult(False, error="invalid qty/price")
    side_enum = OrderSide.BUY if side.upper() == "BUY" else OrderSide.SELL
    # 用“市价化限价单”提高成交概率，同时避免直接 market order 在特殊时段表现不可控。
    limit_price = _money(price * (1 + BUY_LIMIT_BUFFER_PCT)) if side_enum == OrderSide.BUY else _money(price * (1 - SELL_LIMIT_BUFFER_PCT))
    if DRY_RUN:
        print(f"[AC_T DRY] {side} {symbol} qty={qty} limit={limit_price:.2f}", flush=True)
        return FillResult(True, order_id="DRY_RUN", status="filled", filled_qty=qty, filled_avg_price=_money(price))

    try:
        order = client.submit_order(
            order_data=LimitOrderRequest(
                symbol=symbol,
                qty=str(int(qty)),
                side=side_enum,
                time_in_force=TimeInForce.DAY,
                limit_price=limit_price,
            )
        )
    except Exception as exc:
        return FillResult(False, error=str(exc))

    order_id = str(getattr(order, "id", "") or "")
    result = FillResult(True, order_id=order_id, status=str(getattr(order, "status", "") or ""))
    deadline = sleep_time.time() + max(float(FILL_WAIT_SEC), 0.5)
    while sleep_time.time() < deadline:
        try:
            fresh = client.get_order_by_id(order_id)
            result.status = str(getattr(fresh, "status", "") or "").lower()
            result.filled_qty = int(float(getattr(fresh, "filled_qty", 0) or 0))
            result.filled_avg_price = float(getattr(fresh, "filled_avg_price", 0) or 0)
            if result.filled_qty > 0 and result.filled_avg_price > 0:
                if result.status in {"filled", "canceled", "cancelled", "expired", "rejected"}:
                    return result
            if result.status in {"canceled", "cancelled", "expired", "rejected"}:
                return result
        except Exception:
            pass
        sleep_time.sleep(0.35)

    try:
        fresh = client.get_order_by_id(order_id)
        result.status = str(getattr(fresh, "status", "") or "").lower()
        result.filled_qty = int(float(getattr(fresh, "filled_qty", 0) or 0))
        result.filled_avg_price = float(getattr(fresh, "filled_avg_price", 0) or 0)
    except Exception as exc:
        result.error = str(exc)
    if result.status in ACTIVE_ORDER_STATUSES:
        try:
            client.cancel_order_by_id(order_id)
            cancel_deadline = sleep_time.time() + 1.5
            while True:
                sleep_time.sleep(0.25)
                fresh = client.get_order_by_id(order_id)
                result.status = str(getattr(fresh, "status", "") or "").lower()
                result.filled_qty = int(float(getattr(fresh, "filled_qty", 0) or 0))
                result.filled_avg_price = float(getattr(fresh, "filled_avg_price", 0) or 0)
                if result.status not in ACTIVE_ORDER_STATUSES or sleep_time.time() >= cancel_deadline:
                    break
            if result.filled_qty > 0:
                print(
                    f"[AC_T] partial fill then cancel: symbol={symbol} side={side} "
                    f"filled_qty={result.filled_qty} status={result.status}",
                    flush=True,
                )
        except Exception as exc:
            result.error = f"{result.error}; cancel_active_order_failed={exc}" if result.error else f"cancel_active_order_failed={exc}"
    return result


def _write_last_order(conn, row: dict, fill: FillResult, side: str, intent: str) -> None:
    _set_row(
        conn,
        row,
        {
            "last_order_id": fill.order_id or None,
            "last_order_side": side,
            "last_order_intent": intent[:80],
            "last_order_time": _now_la().replace(tzinfo=None),
        },
    )


def _intent(action: str, row: dict) -> str:
    return f"AC_T:{_today_la()}:{_ac_type(row)}:{row['stock_code']}:{action}"[:80]


def _latest_order_status(client, order_id: str) -> tuple[str, int]:
    if not order_id:
        return "", 0
    try:
        fresh = client.get_order_by_id(order_id)
        status = str(getattr(fresh, "status", "") or "").lower()
        filled_qty = int(float(getattr(fresh, "filled_qty", 0) or 0))
        return status, filled_qty
    except Exception as exc:
        print(f"[AC_T] lock status check failed order_id={order_id}: {exc}", flush=True)
        return "unknown", 0


def _same_intent_can_retry(client, row: dict, intent: str) -> tuple[bool, str]:
    """同一个 intent 不再直接拦截；只有最近订单仍活跃时才挡住。"""
    last_intent = str(row.get("last_order_intent") or "")
    if last_intent != intent:
        return True, "new_intent"
    order_id = str(row.get("last_order_id") or "")
    if not order_id:
        return True, "same_intent_no_order_id"
    if order_id == "DRY_RUN":
        return True, "same_intent_dry_run"
    status, filled_qty = _latest_order_status(client, order_id)
    if status in ACTIVE_ORDER_STATUSES:
        return False, f"active_order_still_open status={status} filled_qty={filled_qty}"
    if status == "filled" and filled_qty > 0:
        return False, f"same_intent_already_filled status={status} filled_qty={filled_qty}"
    if status in TERMINAL_ORDER_STATUSES:
        return True, f"retry_after_terminal_order status={status} filled_qty={filled_qty}"
    if status == "unknown":
        return False, "active_order_status_unknown"
    return True, f"retry_after_non_active_status status={status or 'empty'} filled_qty={filled_qty}"


def _acquire_intent_lock(conn, client, row: dict, intent: str, side: str) -> bool:
    """轻量 DB 锁：防并发重复提交，但不阻止失败/未成交后的重试。"""
    can_retry, reason = _same_intent_can_retry(client, row, intent)
    if not can_retry:
        print(f"[AC_T] lock block {row.get('stock_code')} intent={intent} reason={reason}", flush=True)
        return False
    if reason != "new_intent":
        print(f"[AC_T] lock retry allowed {row.get('stock_code')} intent={intent} reason={reason}", flush=True)
    where_sql, where_args = _row_key(row)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE `{TABLE}`
            SET last_order_intent=%s,
                last_order_side=%s,
                last_order_time=NOW()
            WHERE {where_sql}
              AND (
                last_order_intent IS NULL
                OR last_order_intent <> %s
                OR last_order_id IS NULL
                OR last_order_id = ''
                OR last_order_id = 'DRY_RUN'
                OR last_order_time < (NOW() - INTERVAL 15 SECOND)
              )
            """,
            (intent, side, *where_args, intent),
        )
        ok = cur.rowcount == 1
    if ok:
        conn.commit()
    else:
        print(f"[AC_T] lock block {row.get('stock_code')} intent={intent} reason=duplicate_protection", flush=True)
    return ok


def _base_price(conn, row: dict, current_price: float) -> float:
    """当天基准价：优先用昨日收盘价，拿不到才退回首次价格。"""
    base = _safe_float(row.get("ac_t_base_price"))
    base_date = row.get("ac_t_base_date")
    if hasattr(base_date, "date"):
        base_date = base_date.date()
    if base > 0 and str(base_date or "") == str(_today_la()):
        return base
    prev_close = _prev_close_from_db(conn, row["stock_code"])
    next_base = _money(prev_close if prev_close > 0 else current_price)
    _set_row(conn, row, {"ac_t_base_price": next_base, "ac_t_base_date": _today_la()})
    row["ac_t_base_price"] = next_base
    row["ac_t_base_date"] = _today_la()
    return next_base


def _prev_close_from_db(conn, symbol: str) -> float:
    """从日线表读取今天以前最近一个收盘价，用于判断当日涨跌幅和开盘缺口。"""
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT `close`
                FROM `{PRICES_TABLE}`
                WHERE `symbol`=%s
                  AND `date` < %s
                  AND `close` IS NOT NULL
                  AND `close` > 0
                ORDER BY `date` DESC
                LIMIT 1
                """,
                (symbol, _today_la()),
            )
            row = cur.fetchone() or {}
        return _safe_float(row.get("close"))
    except Exception as exc:
        print(f"[AC_T] prev close unavailable for {symbol}: {exc}", flush=True)
        return 0.0


def _open_mode(conn, row: dict, current_price: float, params: dict) -> str:
    """记录当天开盘模式：NORMAL / GAP_UP / GAP_DOWN。

    ac_t_open_price 是机器人首次观察价，不保证等于交易所真实 open。
    后续可以接 Alpaca dailyBar/open 或 1min bar open 优化；拿不到真实 open 时继续 fallback 当前价。
    """
    open_date = row.get("ac_t_open_date")
    if hasattr(open_date, "date"):
        open_date = open_date.date()
    if str(open_date or "") == str(_today_la()):
        mode = str(row.get("ac_t_open_mode") or "NORMAL").strip().upper()
        return mode if mode in {"NORMAL", "GAP_UP", "GAP_DOWN"} else "NORMAL"

    base = _base_price(conn, row, current_price)
    gap_pct = float(params["gap_pct"])
    if current_price >= base * (1 + gap_pct):
        mode = "GAP_UP"
    elif current_price <= base * (1 - gap_pct):
        mode = "GAP_DOWN"
    else:
        mode = "NORMAL"
    fields = {
        "ac_t_open_price": _money(current_price),
        "ac_t_open_date": _today_la(),
        "ac_t_open_mode": mode,
        "ac_t_trade_high_price": None,
        "ac_t_trade_low_price": None,
        "ac_t_extreme_confirmed": 0,
    }
    _set_row(conn, row, fields)
    row.update(fields)
    return mode


def _is_observation_window() -> bool:
    """06:30-06:40 为开盘观察窗口，只记录模式，不交易。"""
    now = _now_la().time()
    return MARKET_OPEN <= now < TRADE_START


def _is_trade_window() -> bool:
    """06:40-12:55 才允许新开一轮做T。"""
    now = _now_la().time()
    return TRADE_START <= now < FORCE_RECOVER_TIME


def _core_qty(conn, row: dict, real_qty: int) -> int:
    core = _safe_int(row.get("ac_t_core_qty"))
    if core > 0:
        return core
    fallback = _safe_int(row.get("qty"))
    core = max(real_qty, fallback)
    if core > 0:
        _set_row(conn, row, {"ac_t_core_qty": core})
        row["ac_t_core_qty"] = core
    return core


def _state(row: dict) -> str:
    return str(row.get("ac_t_state") or STATE_IDLE).strip().upper()


def _ac_type(row: dict) -> str:
    return str(row.get("ac_t_type") or row.get("stock_type") or "").strip().upper()


def _same_day(value) -> bool:
    if not value:
        return False
    if hasattr(value, "date"):
        value = value.date()
    return str(value) == str(_today_la())


def _entry_age_minutes(row: dict) -> float | None:
    entry_time = row.get("ac_t_entry_time")
    if not entry_time:
        return None
    if isinstance(entry_time, str):
        try:
            entry_time = datetime.fromisoformat(entry_time.replace("Z", ""))
        except Exception:
            return None
    if getattr(entry_time, "tzinfo", None) is not None:
        entry_time = entry_time.astimezone(LA_TZ).replace(tzinfo=None)
    try:
        return max(0.0, (_now_la().replace(tzinfo=None) - entry_time).total_seconds() / 60.0)
    except Exception:
        return None


def _min_leg_hold_ok(row: dict, force: bool = False) -> tuple[bool, str]:
    """第一腿成交后至少等待一段时间再做第二腿；强制收尾/恢复除外。"""
    if force or MIN_LEG_HOLD_MINUTES <= 0:
        return True, "force_or_disabled"
    age = _entry_age_minutes(row)
    if age is None:
        return True, "no_entry_time"
    if age < MIN_LEG_HOLD_MINUTES:
        return False, f"min_leg_hold age={age:.1f}m need={MIN_LEG_HOLD_MINUTES:.1f}m"
    return True, f"min_leg_hold_ok age={age:.1f}m"


def should_force_recover_now(state: str) -> bool:
    """12:55 后，只要核心仓处于临时卖出状态，就强制恢复。"""
    now = _now_la().time()
    return state in DOWN_STATES and FORCE_RECOVER_TIME <= now < MARKET_CLOSE


def should_force_close_up_t_now(state: str) -> bool:
    """12:55 后，按配置强制卖出上涨做T新增仓，避免新增仓隔夜。"""
    now = _now_la().time()
    return FORCE_CLOSE_UP_T and state in UP_STATES and FORCE_RECOVER_TIME <= now < MARKET_CLOSE


def _buy_t_qty(conn, client, row: dict, qty: int, current_price: float, intent: str) -> FillResult:
    if not _has_buying_power(client, qty, current_price):
        return FillResult(False, error="buying_power")
    if not _acquire_intent_lock(conn, client, row, intent, "buy"):
        return FillResult(False, error="intent_lock_busy")
    fill = _submit_limit_and_wait(client, row["stock_code"], qty, "BUY", current_price)
    _write_last_order(conn, row, fill, "buy", intent)
    return fill


def _sell_t_qty(conn, client, row: dict, qty: int, current_price: float, intent: str) -> FillResult:
    if not _acquire_intent_lock(conn, client, row, intent, "sell"):
        return FillResult(False, error="intent_lock_busy")
    fill = _submit_limit_and_wait(client, row["stock_code"], qty, "SELL", current_price)
    _write_last_order(conn, row, fill, "sell", intent)
    return fill


def handle_idle(conn, client, row: dict, current_price: float, params: dict) -> str:
    """空闲状态：先处理开盘缺口分流，再按普通 AC 做T逻辑触发。"""
    symbol = row["stock_code"]
    ac_type = _ac_type(row)
    real_qty = _get_position_qty(client, symbol)
    core_qty = _core_qty(conn, row, real_qty)
    if core_qty <= 0:
        return "skip:no_core_qty"

    mode = _open_mode(conn, row, current_price, params)
    if _is_observation_window() and not DRY_RUN:
        return f"observe:{mode}"
    if not _is_trade_window() and not DRY_RUN:
        return f"skip:no_new_t_window:{mode}"
    if mode == "GAP_UP":
        # 高开不追买。B 方案：06:40 首次观察价直接作为临时高点，
        # 后续从这个采样高点回撤即可触发卖核心仓。
        _set_row(
            conn,
            row,
            {
                "ac_t_state": STATE_GAP_UP_WAIT_PULLBACK_SELL,
                "ac_t_trade_high_price": _money(current_price),
                "ac_t_trade_low_price": None,
                "ac_t_extreme_confirmed": 1,
            },
        )
        return "gap_up_wait_pullback_sell:init"
    if mode == "GAP_DOWN":
        # 低开不杀跌。B 方案：06:40 首次观察价直接作为临时低点，
        # 后续从这个采样低点反弹即可触发买新增仓。
        _set_row(
            conn,
            row,
            {
                "ac_t_state": STATE_GAP_DOWN_WAIT_REBOUND_BUY,
                "ac_t_trade_high_price": None,
                "ac_t_trade_low_price": _money(current_price),
                "ac_t_extreme_confirmed": 1,
            },
        )
        return "gap_down_wait_rebound_buy:init"

    base = _base_price(conn, row, current_price)
    today = _today_la()
    force_deadline = _now_la().replace(hour=12, minute=55, second=0, microsecond=0).replace(tzinfo=None)

    if current_price >= base * (1 + params["up_trigger_pct"]):
        if _same_day(row.get("ac_t_last_up_date")):
            return "skip:up_done_today"
        fill = _buy_t_qty(conn, client, row, core_qty, current_price, _intent("UP_BUY", row))
        if fill.filled_qty <= 0 or fill.filled_avg_price <= 0:
            return f"no_fill:up_buy status={fill.status} err={fill.error}"
        _set_row(
            conn,
            row,
            {
                "ac_t_state": STATE_UP_HOLDING,
                "ac_t_qty": fill.filled_qty,
                "ac_t_buy_price": _money(fill.filled_avg_price),
                "ac_t_high_price": _money(fill.filled_avg_price),
                "ac_t_last_action_date": today,
                "ac_t_last_action_side": "UP_BUY",
                "ac_t_last_up_date": today,
                "ac_t_entry_time": _now_la().replace(tzinfo=None),
                "qty": core_qty + fill.filled_qty,
                "is_bought": 1,
            },
        )
        return f"up_buy:{fill.filled_qty}@{fill.filled_avg_price:.2f}"

    if current_price <= base * (1 - params["down_trigger_pct"]):
        if _same_day(row.get("ac_t_last_down_date")):
            return "skip:down_done_today"
        sell_qty = min(core_qty, real_qty)
        if sell_qty <= 0:
            return "skip:no_real_qty_to_sell"
        fill = _sell_t_qty(conn, client, row, sell_qty, current_price, _intent("DOWN_SELL", row))
        if fill.filled_qty <= 0 or fill.filled_avg_price <= 0:
            return f"no_fill:down_sell status={fill.status} err={fill.error}"
        _set_row(
            conn,
            row,
            {
                "ac_t_state": STATE_DOWN_WAIT_BUY,
                "ac_t_qty": fill.filled_qty,
                "ac_t_sell_price": _money(fill.filled_avg_price),
                "ac_t_low_price": _money(fill.filled_avg_price),
                "ac_t_last_action_date": today,
                "ac_t_last_action_side": "DOWN_SELL",
                "ac_t_last_down_date": today,
                "ac_t_entry_time": _now_la().replace(tzinfo=None),
                "ac_t_temporarily_out": 1,
                "ac_t_force_recover_deadline": force_deadline,
                "qty": max(core_qty - fill.filled_qty, 0),
                "is_bought": 1 if core_qty - fill.filled_qty > 0 else 0,
            },
        )
        return f"down_sell:{fill.filled_qty}@{fill.filled_avg_price:.2f}"

    return "idle:no_signal"


def _finish_up_sell(conn, client, row: dict, current_price: float, reason: str, force: bool = False) -> str:
    """卖出上涨/低开反弹买入的新增仓；这里不卖核心仓。"""
    t_qty = _safe_int(row.get("ac_t_qty"))
    if t_qty <= 0:
        _set_row(conn, row, _reset_to_idle_fields())
        return "up_reset:no_qty"
    hold_ok, hold_reason = _min_leg_hold_ok(row, force=force)
    if not hold_ok:
        print(f"[AC_T] hold block {row.get('stock_code')} reason={reason} {hold_reason}", flush=True)
        return f"skip:{hold_reason}"
    fill = _sell_t_qty(conn, client, row, t_qty, current_price, _intent(reason, row))
    if fill.filled_qty <= 0:
        return f"no_fill:up_sell status={fill.status} err={fill.error}"
    remaining = max(t_qty - fill.filled_qty, 0)
    core_qty = _safe_int(row.get("ac_t_core_qty"), _safe_int(row.get("qty")))
    if remaining > 0:
        print(
            f"[AC_T] up sell partial {row.get('stock_code')} reason={reason} "
            f"filled_qty={fill.filled_qty} remaining_qty={remaining}",
            flush=True,
        )
        _set_row(
            conn,
            row,
            {
                "ac_t_qty": remaining,
                "ac_t_last_action_date": _today_la(),
                "ac_t_last_action_side": "UP_SELL_PART",
                "qty": core_qty + remaining,
            },
        )
        return f"up_sell_partial:{fill.filled_qty}"
    _set_row(
        conn,
        row,
        _reset_to_idle_fields(
            {
                "ac_t_last_action_date": _today_la(),
                "ac_t_last_action_side": "UP_SELL",
                "qty": core_qty,
                "is_bought": 1 if core_qty > 0 else 0,
            }
        ),
    )
    return f"up_sell:{fill.filled_qty}@{fill.filled_avg_price:.2f}"


def handle_up_t_holding(conn, client, row: dict, current_price: float, params: dict) -> str:
    """普通上涨做T：买入新增仓后，按止盈或高点回撤卖出。"""
    buy_price = _safe_float(row.get("ac_t_buy_price"))
    high_price = max(_safe_float(row.get("ac_t_high_price"), buy_price), current_price)
    if high_price != _safe_float(row.get("ac_t_high_price")):
        # 这里记录的是机器人轮询采样高点，不是交易所完整日内最高价。
        _set_row(conn, row, {"ac_t_high_price": _money(high_price)})
        row["ac_t_high_price"] = _money(high_price)

    take_profit = params.get("up_take_profit_pct")
    if take_profit is not None and buy_price > 0 and current_price >= buy_price * (1 + take_profit):
        return _finish_up_sell(conn, client, row, current_price, "UP_TAKE_PROFIT")

    if high_price > 0 and current_price <= high_price * (1 - params["up_pullback_pct"]):
        if buy_price > 0 and current_price >= buy_price:
            return _finish_up_sell(conn, client, row, current_price, "UP_PULLBACK")
        # 回撤触发但低于买入价，不卖，避免把成本做高。
        _set_row(conn, row, {"ac_t_state": STATE_UP_WAIT_COST})
        return "up_wait_cost"

    return "up_holding"


def handle_up_t_wait_sell_at_cost(conn, client, row: dict, current_price: float) -> str:
    """新增仓低于买入价时等待回本，回到买入价以上再卖。"""
    buy_price = _safe_float(row.get("ac_t_buy_price"))
    if buy_price > 0 and current_price >= buy_price:
        return _finish_up_sell(conn, client, row, current_price, "UP_SELL_AT_COST")
    return "up_wait_cost"


def _finish_down_buy(conn, client, row: dict, current_price: float, reason: str, force: bool = False) -> str:
    """买回下跌/高开回撤中临时卖出的核心仓。"""
    t_qty = _safe_int(row.get("ac_t_qty"))
    if t_qty <= 0:
        _set_row(conn, row, _reset_to_idle_fields())
        return "down_reset:no_qty"
    hold_ok, hold_reason = _min_leg_hold_ok(row, force=force)
    if not hold_ok:
        print(f"[AC_T] hold block {row.get('stock_code')} reason={reason} {hold_reason}", flush=True)
        return f"skip:{hold_reason}"
    fill = _buy_t_qty(conn, client, row, t_qty, current_price, _intent(reason, row))
    if fill.filled_qty <= 0:
        return f"no_fill:down_buy status={fill.status} err={fill.error}"
    remaining = max(t_qty - fill.filled_qty, 0)
    core_qty = _safe_int(row.get("ac_t_core_qty"), _safe_int(row.get("qty")))
    if remaining > 0:
        print(
            f"[AC_T] down buy partial {row.get('stock_code')} reason={reason} "
            f"filled_qty={fill.filled_qty} remaining_qty={remaining}",
            flush=True,
        )
        _set_row(
            conn,
            row,
            {
                "ac_t_qty": remaining,
                "ac_t_state": STATE_DOWN_WAIT_BUY,
                "ac_t_last_action_date": _today_la(),
                "ac_t_last_action_side": "FORCE_PART" if force else "DOWN_BUY_PART",
                "qty": max(core_qty - remaining, 0),
                "is_bought": 1,
            },
        )
        return f"down_buy_partial:{fill.filled_qty}"
    _set_row(
        conn,
        row,
        _reset_to_idle_fields(
            {
                "ac_t_last_action_date": _today_la(),
                "ac_t_last_action_side": "FORCE_RECOVER" if force else "DOWN_BUY",
                "qty": core_qty,
                "is_bought": 1 if core_qty > 0 else 0,
            }
        ),
    )
    return f"{'force_' if force else ''}down_buy:{fill.filled_qty}@{fill.filled_avg_price:.2f}"


def handle_down_t_wait_buyback(conn, client, row: dict, current_price: float, params: dict) -> str:
    """普通下跌做T：临时卖出核心仓后，等待低位买回。"""
    sell_price = _safe_float(row.get("ac_t_sell_price"))
    low_price = min(_safe_float(row.get("ac_t_low_price"), sell_price), current_price)
    if low_price != _safe_float(row.get("ac_t_low_price")):
        # 这里记录的是机器人轮询采样低点，不是交易所完整日内最低价。
        _set_row(conn, row, {"ac_t_low_price": _money(low_price)})
        row["ac_t_low_price"] = _money(low_price)

    continue_pct = params.get("down_continue_pct")
    if continue_pct is not None and sell_price > 0 and current_price <= sell_price * (1 - continue_pct):
        return _finish_down_buy(conn, client, row, current_price, "DOWN_CONTINUE")

    if low_price > 0 and current_price >= low_price * (1 + params["down_rebound_pct"]):
        if sell_price > 0 and current_price <= sell_price:
            return _finish_down_buy(conn, client, row, current_price, "DOWN_REBOUND")
        # 低点反弹了，但价格已经高于卖出价，不追高买回。
        _set_row(conn, row, {"ac_t_state": STATE_DOWN_WAIT_SELL_PRICE})
        return "down_wait_sell_price"

    return "down_wait_buyback"


def handle_down_t_wait_buyback_at_sell_price(conn, client, row: dict, current_price: float) -> str:
    """反弹价高于卖出价时，等待价格回到卖出价以下再买回。"""
    sell_price = _safe_float(row.get("ac_t_sell_price"))
    if sell_price > 0 and current_price <= sell_price:
        return _finish_down_buy(conn, client, row, current_price, "DOWN_BUY_AT_SELL")
    return "down_wait_sell_price"


def handle_gap_up_wait_pullback_sell(conn, client, row: dict, current_price: float, params: dict) -> str:
    """高开模式：6:40 首次观察价可作为临时高点，从采样高点回撤后卖核心仓。"""
    if _now_la().time() >= FORCE_RECOVER_TIME and not DRY_RUN:
        return "skip:no_gap_up_core_sell_after_1255"
    trade_high = _safe_float(row.get("ac_t_trade_high_price"))
    if trade_high <= 0:
        _set_row(conn, row, {"ac_t_trade_high_price": _money(current_price), "ac_t_extreme_confirmed": 1})
        return "gap_up_track_high:init"
    if current_price > trade_high:
        # 这里记录的是机器人轮询采样高点，不是交易所完整日内最高价。
        _set_row(conn, row, {"ac_t_trade_high_price": _money(current_price), "ac_t_extreme_confirmed": 1})
        return "gap_up_track_high:new_high"
    if current_price > trade_high * (1 - params["gap_pullback_pct"]):
        return "gap_up_wait_pullback"

    symbol = row["stock_code"]
    real_qty = _get_position_qty(client, symbol)
    core_qty = _core_qty(conn, row, real_qty)
    sell_qty = min(core_qty, real_qty)
    if sell_qty <= 0:
        return "skip:no_real_qty_to_gap_sell"
    fill = _sell_t_qty(conn, client, row, sell_qty, current_price, _intent("GAP_UP_SELL", row))
    if fill.filled_qty <= 0 or fill.filled_avg_price <= 0:
        return f"no_fill:gap_up_sell status={fill.status} err={fill.error}"
    force_deadline = _now_la().replace(hour=12, minute=55, second=0, microsecond=0).replace(tzinfo=None)
    _set_row(
        conn,
        row,
        {
            "ac_t_state": STATE_GAP_UP_WAIT_BUYBACK,
            "ac_t_qty": fill.filled_qty,
            "ac_t_sell_price": _money(fill.filled_avg_price),
            "ac_t_low_price": _money(fill.filled_avg_price),
            "ac_t_last_action_date": _today_la(),
            "ac_t_last_action_side": "GAP_UP_SELL",
            "ac_t_last_down_date": _today_la(),
            "ac_t_entry_time": _now_la().replace(tzinfo=None),
            "ac_t_temporarily_out": 1,
            "ac_t_force_recover_deadline": force_deadline,
            "qty": max(core_qty - fill.filled_qty, 0),
            "is_bought": 1 if core_qty - fill.filled_qty > 0 else 0,
        },
    )
    return f"gap_up_sell:{fill.filled_qty}@{fill.filled_avg_price:.2f}"


def handle_gap_up_wait_buyback(conn, client, row: dict, current_price: float, params: dict) -> str:
    """高开回撤卖出核心仓后，等待不高于卖出价买回；12:55 兜底恢复。"""
    sell_price = _safe_float(row.get("ac_t_sell_price"))
    low_price = min(_safe_float(row.get("ac_t_low_price"), sell_price), current_price)
    if low_price != _safe_float(row.get("ac_t_low_price")):
        # 这里记录的是机器人轮询采样低点，不是交易所完整日内最低价。
        _set_row(conn, row, {"ac_t_low_price": _money(low_price)})
        row["ac_t_low_price"] = _money(low_price)
    if sell_price > 0 and current_price <= sell_price:
        return _finish_down_buy(conn, client, row, current_price, "GAP_UP_BUYBACK")
    if sell_price > 0 and current_price <= sell_price * (1 - params["gap_sell_continue_pct"]):
        return _finish_down_buy(conn, client, row, current_price, "GAP_UP_CONTINUE_DOWN")
    return "gap_up_wait_buyback"


def handle_gap_down_wait_rebound_buy(conn, client, row: dict, current_price: float, params: dict) -> str:
    """低开模式：6:40 首次观察价可作为临时低点，从采样低点反弹后买新增仓。"""
    if _now_la().time() >= FORCE_RECOVER_TIME and not DRY_RUN:
        return "skip:no_gap_down_buy_after_1255"
    trade_low = _safe_float(row.get("ac_t_trade_low_price"))
    if trade_low <= 0:
        _set_row(conn, row, {"ac_t_trade_low_price": _money(current_price), "ac_t_extreme_confirmed": 1})
        return "gap_down_track_low:init"
    if current_price < trade_low:
        # 这里记录的是机器人轮询采样低点，不是交易所完整日内最低价。
        _set_row(conn, row, {"ac_t_trade_low_price": _money(current_price), "ac_t_extreme_confirmed": 1})
        return "gap_down_track_low:new_low"
    if current_price < trade_low * (1 + params["gap_rebound_pct"]):
        return "gap_down_wait_rebound"

    symbol = row["stock_code"]
    real_qty = _get_position_qty(client, symbol)
    core_qty = _core_qty(conn, row, real_qty)
    if core_qty <= 0:
        return "skip:no_core_qty"
    fill = _buy_t_qty(conn, client, row, core_qty, current_price, _intent("GAP_DOWN_BUY", row))
    if fill.filled_qty <= 0 or fill.filled_avg_price <= 0:
        return f"no_fill:gap_down_buy status={fill.status} err={fill.error}"
    _set_row(
        conn,
        row,
        {
            "ac_t_state": STATE_GAP_DOWN_HOLDING,
            "ac_t_qty": fill.filled_qty,
            "ac_t_buy_price": _money(fill.filled_avg_price),
            "ac_t_high_price": _money(fill.filled_avg_price),
            "ac_t_last_action_date": _today_la(),
            "ac_t_last_action_side": "GAP_DOWN_BUY",
            "ac_t_last_up_date": _today_la(),
            "ac_t_entry_time": _now_la().replace(tzinfo=None),
            "qty": real_qty + fill.filled_qty,
            "is_bought": 1,
        },
    )
    return f"gap_down_buy:{fill.filled_qty}@{fill.filled_avg_price:.2f}"


def handle_gap_down_holding(conn, client, row: dict, current_price: float, params: dict) -> str:
    """低开反弹买入新增仓后，按小止盈或高点回撤卖出新增仓。"""
    buy_price = _safe_float(row.get("ac_t_buy_price"))
    high_price = max(_safe_float(row.get("ac_t_high_price"), buy_price), current_price)
    if high_price != _safe_float(row.get("ac_t_high_price")):
        # 这里记录的是机器人轮询采样高点，不是交易所完整日内最高价。
        _set_row(conn, row, {"ac_t_high_price": _money(high_price)})
        row["ac_t_high_price"] = _money(high_price)

    if buy_price > 0 and current_price >= buy_price * (1 + params["gap_buy_take_profit_pct"]):
        return _finish_up_sell(conn, client, row, current_price, "GAP_DOWN_TAKE_PROFIT")
    if high_price > 0 and current_price <= high_price * (1 - params["gap_pullback_pct"]):
        if buy_price > 0 and current_price >= buy_price:
            return _finish_up_sell(conn, client, row, current_price, "GAP_DOWN_PULLBACK")
        # 回撤时如果低于本次买入价，不卖，等回本价。
        _set_row(conn, row, {"ac_t_state": STATE_UP_WAIT_COST})
        return "gap_down_wait_cost"
    return "gap_down_holding"


def force_buyback_core(conn, client, row: dict, current_price: float) -> str:
    """12:55 强制恢复核心仓：恢复长期仓优先于单次做T盈亏。"""
    print(
        f"[AC_T] FORCE_RECOVER {row.get('stock_code')} "
        f"qty={_safe_int(row.get('ac_t_qty'))} price={current_price:.2f}",
        flush=True,
    )
    return _finish_down_buy(conn, client, row, current_price, "FORCE_RECOVER_1255", force=True)


def force_close_up_t(conn, client, row: dict, current_price: float) -> str:
    """12:55 强制卖出上涨做T新增仓：只处理 ac_t_qty，不动核心仓。"""
    print(
        f"[AC_T] FORCE_CLOSE_UP_T {row.get('stock_code')} "
        f"qty={_safe_int(row.get('ac_t_qty'))} price={current_price:.2f}",
        flush=True,
    )
    return _finish_up_sell(conn, client, row, current_price, "FORCE_CLOSE_UP_T", force=True)


def process_ac_t_symbol(conn, client, row: dict) -> str:
    """推进单只股票的 AC 做T状态机。"""
    symbol = str(row.get("stock_code") or "").strip().upper()
    if not symbol:
        return "skip:empty_symbol"
    row["stock_code"] = symbol
    ac_type = _ac_type(row)
    params = AC_T_PARAMS.get(ac_type)
    if not params:
        return "skip:bad_ac_type"

    raw_price = float(get_latest_stock_price(symbol) or 0)
    if raw_price <= 0:
        return "skip:no_price"
    current_price = _money(raw_price)

    state = _state(row)
    if should_force_recover_now(state):
        # 强制恢复优先级最高，即使已经过了新开做T窗口也要执行。
        return force_buyback_core(conn, client, row, current_price)
    if should_force_close_up_t_now(state):
        # 只卖新增做T仓 ac_t_qty，不动长期核心仓。
        return force_close_up_t(conn, client, row, current_price)
    if not (MARKET_OPEN <= _now_la().time() < MARKET_CLOSE) and not DRY_RUN:
        return "skip:not_regular_hours"

    if state == STATE_IDLE:
        return handle_idle(conn, client, row, current_price, params)
    if state == STATE_UP_HOLDING:
        return handle_up_t_holding(conn, client, row, current_price, params)
    if state == STATE_UP_WAIT_COST:
        return handle_up_t_wait_sell_at_cost(conn, client, row, current_price)
    if state == STATE_DOWN_WAIT_BUY:
        return handle_down_t_wait_buyback(conn, client, row, current_price, params)
    if state == STATE_DOWN_WAIT_SELL_PRICE:
        return handle_down_t_wait_buyback_at_sell_price(conn, client, row, current_price)
    if state == STATE_GAP_UP_WAIT_PULLBACK_SELL:
        return handle_gap_up_wait_pullback_sell(conn, client, row, current_price, params)
    if state == STATE_GAP_UP_WAIT_BUYBACK:
        return handle_gap_up_wait_buyback(conn, client, row, current_price, params)
    if state == STATE_GAP_DOWN_WAIT_REBOUND_BUY:
        return handle_gap_down_wait_rebound_buy(conn, client, row, current_price, params)
    if state == STATE_GAP_DOWN_HOLDING:
        return handle_gap_down_holding(conn, client, row, current_price, params)

    _set_row(conn, row, {"ac_t_state": STATE_IDLE})
    return f"reset:unknown_state:{state}"


def run_strategy_ac_t_once(symbol: str | None = None) -> list[dict]:
    ensure_schema()
    client = trading_client()
    results: list[dict] = []
    with db_conn() as conn:
        rows = load_ac_t_rows(conn, symbol=symbol)
        for row in rows:
            sym = str(row.get("stock_code") or "").strip().upper()
            try:
                result = process_ac_t_symbol(conn, client, row)
            except Exception as exc:
                result = f"error:{exc}"
                traceback.print_exc()
            print(f"[AC_T] {sym} {result}", flush=True)
            results.append({"symbol": sym, "result": result})
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="A/C intraday T strategy")
    parser.add_argument("--symbol")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=int, default=30)
    args = parser.parse_args()
    if args.loop:
        while True:
            run_strategy_ac_t_once(symbol=args.symbol)
            sleep_time.sleep(max(args.interval, 1))
    else:
        print(run_strategy_ac_t_once(symbol=args.symbol), flush=True)


if __name__ == "__main__":
    main()
