# -*- coding: utf-8 -*-
"""
策略 C：期权价差四模式计划器。

第一版故意只做“计划”，不做真实下单：
1) 从 MySQL 读取标的 OHLCV 日线数据。
2) 判断行情状态：上涨 / 下跌 / 横盘，并给出偏多或偏空。
3) 按行情选择四种价差模式之一：
   - Bull Call：上涨趋势，买权借方价差，必须涨才容易赚钱。
   - Bear Put：下跌趋势，卖权借方价差，必须跌才容易赚钱。
   - Bull Put：横盘偏多，卖权信用价差，不跌破支撑就容易赚钱。
   - Bear Call：横盘偏空，买权信用价差，不突破压力就容易赚钱。
4) 生成两条腿的期权组合计划。
5) 可选写入 option_spreads / option_spread_legs，方便后面人工确认或继续接下单。

真实期权链筛选和真实下单先不接，等计划输出稳定后再做。
"""

from __future__ import annotations

import os
import traceback
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

import pymysql


# =========================
# DB
# =========================
PRICES_TABLE = os.getenv("C_PRICES_TABLE", os.getenv("B_PRICES_TABLE", "stock_prices_pool"))
SPREADS_TABLE = os.getenv("C_SPREADS_TABLE", "option_spreads")
LEGS_TABLE = os.getenv("C_LEGS_TABLE", "option_spread_legs")

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
# 策略 C 参数
# =========================
C_LOOKBACK_DAYS = int(os.getenv("C_LOOKBACK_DAYS", "90"))
C_MIN_BARS = int(os.getenv("C_MIN_BARS", "60"))

C_UP_RET3 = float(os.getenv("C_UP_RET3", "0.015"))
C_DOWN_RET3 = float(os.getenv("C_DOWN_RET3", "-0.015"))
C_NEAR_HIGH20 = float(os.getenv("C_NEAR_HIGH20", "0.03"))
C_NEAR_LOW20 = float(os.getenv("C_NEAR_LOW20", "0.03"))
C_SIDEWAYS_MA20_BAND = float(os.getenv("C_SIDEWAYS_MA20_BAND", "0.025"))
C_SIDEWAYS_RANGE20_MAX = float(os.getenv("C_SIDEWAYS_RANGE20_MAX", "0.10"))
C_SIDEWAYS_RET10_MAX = float(os.getenv("C_SIDEWAYS_RET10_MAX", "0.04"))

C_STRIKE_STEP = float(os.getenv("C_STRIKE_STEP", "5"))
C_SPREAD_WIDTH = float(os.getenv("C_SPREAD_WIDTH", "10"))
C_CREDIT_SHORT_OTM_PCT = float(os.getenv("C_CREDIT_SHORT_OTM_PCT", "0.02"))
C_EXPIRY_DAYS_MIN = int(os.getenv("C_EXPIRY_DAYS_MIN", "21"))
C_TAKE_PROFIT_PCT = float(os.getenv("C_TAKE_PROFIT_PCT", "0.60"))

# 1 = 如果表存在，就把生成的计划写入数据库；0 = 只打印，不写库。
C_RECORD_PLAN = int(os.getenv("C_RECORD_PLAN", "1"))

# 真实下单开关。当前即使设成 1，也不会下单，因为真实执行还没有实现。
C_ENABLE_REAL_ORDER = int(os.getenv("C_ENABLE_REAL_ORDER", "0"))

MODE_NO_TRADE = "NO_TRADE"
MODE_BULL_CALL = "BULL_CALL"
MODE_BEAR_PUT = "BEAR_PUT"
MODE_BULL_PUT = "BULL_PUT"
MODE_BEAR_CALL = "BEAR_CALL"
DEBIT_MODES = {MODE_BULL_CALL, MODE_BEAR_PUT}
CREDIT_MODES = {MODE_BULL_PUT, MODE_BEAR_CALL}


@dataclass
class OptionLeg:
    side: str       # BUY / SELL，买入或卖出这一条腿
    cp: str         # C / P，Call 或 Put
    strike: float
    qty: int = 1
    option_symbol: Optional[str] = None


@dataclass
class SpreadPlan:
    underlying: str
    mode: str
    expiry: date
    underlying_price: float
    width: float
    legs: list[OptionLeg]
    signal_score: float
    signal_reason: str
    max_profit: Optional[float] = None
    max_loss: Optional[float] = None
    status: str = "PLANNED"


def _connect():
    return pymysql.connect(**DB)


def _safe_float(v, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _round_to_step(price: float, step: float = C_STRIKE_STEP) -> float:
    if step <= 0:
        return round(price, 2)
    return round(round(float(price) / step) * step, 2)


def _next_friday_after(min_days: int) -> date:
    d = datetime.now().date() + timedelta(days=max(int(min_days), 1))
    while d.weekday() != 4:  # 4 = 周五，期权常用周五到期
        d += timedelta(days=1)
    return d


def _load_bars(conn, symbol: str, limit: int = C_LOOKBACK_DAYS) -> list[dict]:
    sql = f"""
    SELECT DATE(`date`) AS d, `open`, `high`, `low`, `close`, `volume`
    FROM `{PRICES_TABLE}`
    WHERE UPPER(TRIM(symbol))=%s
      AND `date` IS NOT NULL
      AND `close` IS NOT NULL
    ORDER BY `date` DESC
    LIMIT %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (symbol.upper(), int(limit)))
        rows = cur.fetchall() or []
    return list(reversed(rows))


def _mean(xs: list[float]) -> float:
    return sum(xs) / float(len(xs)) if xs else 0.0


def analyze_market(symbol: str) -> dict:
    """
    只使用本地 OHLCV 日线数据判断行情。

    返回：
      trend: up / down / sideways / unknown
      bias: up / down / neutral
      score: 类似信心分数，方便日志排序和人工判断
      reason: 触发原因说明
    """
    symbol = (symbol or "").strip().upper()
    conn = _connect()
    try:
        bars = _load_bars(conn, symbol)
    finally:
        conn.close()

    if len(bars) < C_MIN_BARS:
        return {
            "trend": "unknown",
            "bias": "neutral",
            "score": 0.0,
            "reason": f"not enough bars: {len(bars)} < {C_MIN_BARS}",
            "price": 0.0,
        }

    closes = [_safe_float(r.get("close")) for r in bars]
    highs = [_safe_float(r.get("high")) for r in bars]
    lows = [_safe_float(r.get("low")) for r in bars]
    vols = [_safe_float(r.get("volume")) for r in bars]

    close = closes[-1]
    if close <= 0:
        return {"trend": "unknown", "bias": "neutral", "score": 0.0, "reason": "invalid close", "price": 0.0}

    ma5 = _mean(closes[-5:])
    ma10 = _mean(closes[-10:])
    ma20 = _mean(closes[-20:])
    ma50 = _mean(closes[-50:])

    ret3 = close / closes[-4] - 1 if len(closes) >= 4 and closes[-4] > 0 else 0.0
    ret5 = close / closes[-6] - 1 if len(closes) >= 6 and closes[-6] > 0 else 0.0
    ret10 = close / closes[-11] - 1 if len(closes) >= 11 and closes[-11] > 0 else 0.0

    high20 = max(highs[-20:])
    low20 = min(lows[-20:])
    range20_pct = (high20 - low20) / close if close > 0 else 0.0
    dist_high20 = close / high20 - 1 if high20 > 0 else 0.0
    dist_low20 = close / low20 - 1 if low20 > 0 else 0.0
    vol20 = _mean(vols[-20:])
    vol_ratio = vols[-1] / vol20 if vol20 > 0 else 0.0

    strong_up = (
        close > ma5 > ma10 > ma20
        and ma20 >= ma50
        and ret3 >= C_UP_RET3
        and dist_high20 >= -C_NEAR_HIGH20
    )
    strong_down = (
        close < ma5 < ma10 < ma20
        and ma20 <= ma50
        and ret3 <= C_DOWN_RET3
        and dist_low20 <= C_NEAR_LOW20
    )
    sideways = (
        abs(close / ma20 - 1) <= C_SIDEWAYS_MA20_BAND
        and range20_pct <= C_SIDEWAYS_RANGE20_MAX
        and abs(ret10) <= C_SIDEWAYS_RET10_MAX
    )

    if strong_up:
        trend = "up"
        bias = "up"
        score = 80 + min(ret5 * 100, 20) + min(vol_ratio, 3) * 3
        reason = (
            f"strong_up close>MA5>MA10>MA20, ret3={ret3:.2%}, "
            f"dist_high20={dist_high20:.2%}, volx={vol_ratio:.2f}"
        )
    elif strong_down:
        trend = "down"
        bias = "down"
        score = 80 + min(abs(ret5) * 100, 20) + min(vol_ratio, 3) * 3
        reason = (
            f"strong_down close<MA5<MA10<MA20, ret3={ret3:.2%}, "
            f"dist_low20={dist_low20:.2%}, volx={vol_ratio:.2f}"
        )
    elif sideways:
        trend = "sideways"
        if close >= ma20 and ma5 >= ma10:
            bias = "up"
        elif close <= ma20 and ma5 <= ma10:
            bias = "down"
        else:
            bias = "neutral"
        score = 55 + max(0.0, 1.0 - range20_pct / max(C_SIDEWAYS_RANGE20_MAX, 0.01)) * 20
        reason = (
            f"sideways close/MA20={close / ma20 - 1:.2%}, "
            f"range20={range20_pct:.2%}, ret10={ret10:.2%}, bias={bias}"
        )
    else:
        trend = "unknown"
        bias = "neutral"
        score = 0.0
        reason = (
            f"no_trade close={close:.2f}, MA5={ma5:.2f}, MA10={ma10:.2f}, "
            f"MA20={ma20:.2f}, ret3={ret3:.2%}, ret10={ret10:.2%}, range20={range20_pct:.2%}"
        )

    return {
        "trend": trend,
        "bias": bias,
        "score": round(float(score), 2),
        "reason": reason,
        "price": close,
        "ma5": ma5,
        "ma10": ma10,
        "ma20": ma20,
        "ma50": ma50,
        "ret3": ret3,
        "ret5": ret5,
        "ret10": ret10,
        "high20": high20,
        "low20": low20,
        "range20_pct": range20_pct,
        "dist_high20": dist_high20,
        "dist_low20": dist_low20,
        "vol_ratio": vol_ratio,
    }


def select_mode(market: dict) -> str:
    trend = market.get("trend")
    bias = market.get("bias", "neutral")

    if trend == "up":
        return MODE_BULL_CALL
    if trend == "down":
        return MODE_BEAR_PUT
    if trend == "sideways" and bias == "up":
        return MODE_BULL_PUT
    if trend == "sideways" and bias == "down":
        return MODE_BEAR_CALL
    return MODE_NO_TRADE


def build_spread_plan(symbol: str, mode: str, price: float, market: dict) -> Optional[SpreadPlan]:
    symbol = (symbol or "").strip().upper()
    price = float(price or 0)
    if price <= 0 or mode == MODE_NO_TRADE:
        return None

    expiry = _next_friday_after(C_EXPIRY_DAYS_MIN)
    width = float(C_SPREAD_WIDTH)
    atm = _round_to_step(price)

    if mode == MODE_BULL_CALL:
        # Bull Call：买 ATM Call，卖更高行权价 Call。
        # 适合强上涨；最大风险是净支出，最大收益受宽度限制。
        buy_strike = atm
        sell_strike = _round_to_step(buy_strike + width)
        legs = [
            OptionLeg("BUY", "C", buy_strike),
            OptionLeg("SELL", "C", sell_strike),
        ]
        max_loss = None
        max_profit = width

    elif mode == MODE_BEAR_PUT:
        # Bear Put：买 ATM Put，卖更低行权价 Put。
        # 适合强下跌；最大风险是净支出，最大收益受宽度限制。
        buy_strike = atm
        sell_strike = _round_to_step(buy_strike - width)
        legs = [
            OptionLeg("BUY", "P", buy_strike),
            OptionLeg("SELL", "P", sell_strike),
        ]
        max_loss = None
        max_profit = width

    elif mode == MODE_BULL_PUT:
        # Bull Put：卖下方 OTM Put，买更低行权价 Put 保护。
        # 适合横盘偏多；只要不跌破 short put 附近，就偏向收租。
        sell_strike = _round_to_step(price * (1.0 - C_CREDIT_SHORT_OTM_PCT))
        buy_strike = _round_to_step(sell_strike - width)
        legs = [
            OptionLeg("SELL", "P", sell_strike),
            OptionLeg("BUY", "P", buy_strike),
        ]
        max_loss = width
        max_profit = None

    elif mode == MODE_BEAR_CALL:
        # Bear Call：卖上方 OTM Call，买更高行权价 Call 保护。
        # 适合横盘偏空；只要不突破 short call 附近，就偏向收租。
        sell_strike = _round_to_step(price * (1.0 + C_CREDIT_SHORT_OTM_PCT))
        buy_strike = _round_to_step(sell_strike + width)
        legs = [
            OptionLeg("SELL", "C", sell_strike),
            OptionLeg("BUY", "C", buy_strike),
        ]
        max_loss = width
        max_profit = None
    else:
        return None

    return SpreadPlan(
        underlying=symbol,
        mode=mode,
        expiry=expiry,
        underlying_price=round(price, 2),
        width=abs(float(legs[0].strike) - float(legs[1].strike)),
        legs=legs,
        signal_score=float(market.get("score") or 0.0),
        signal_reason=str(market.get("reason") or ""),
        max_profit=max_profit,
        max_loss=max_loss,
    )


def _has_active_plan(conn, symbol: str) -> bool:
    sql = f"""
    SELECT id
    FROM `{SPREADS_TABLE}`
    WHERE underlying=%s
      AND status IN ('PLANNED','OPEN','SUBMITTED')
    ORDER BY id DESC
    LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (symbol,))
        return cur.fetchone() is not None


def record_spread_plan(plan: SpreadPlan) -> Optional[int]:
    conn = _connect()
    try:
        if _has_active_plan(conn, plan.underlying):
            print(f"[C] skip record: active plan already exists for {plan.underlying}", flush=True)
            return None

        spread_sql = f"""
        INSERT INTO `{SPREADS_TABLE}` (
            underlying, mode, expiry, status,
            underlying_price, width, signal_score, signal_reason,
            max_profit, max_loss, take_profit_pct, created_at, updated_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW());
        """
        with conn.cursor() as cur:
            cur.execute(
                spread_sql,
                (
                    plan.underlying,
                    plan.mode,
                    plan.expiry,
                    plan.status,
                    plan.underlying_price,
                    plan.width,
                    plan.signal_score,
                    plan.signal_reason[:500],
                    plan.max_profit,
                    plan.max_loss,
                    C_TAKE_PROFIT_PCT,
                ),
            )
            spread_id = int(cur.lastrowid)

            leg_sql = f"""
            INSERT INTO `{LEGS_TABLE}` (
                spread_id, leg_no, side, cp, strike, qty, option_symbol,
                created_at, updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,NOW(),NOW());
            """
            for i, leg in enumerate(plan.legs, start=1):
                cur.execute(
                    leg_sql,
                    (
                        spread_id,
                        i,
                        leg.side,
                        leg.cp,
                        leg.strike,
                        leg.qty,
                        leg.option_symbol,
                    ),
                )
        return spread_id
    finally:
        conn.close()


def load_open_spreads(conn, underlying: str) -> list[dict]:
    """
    读取某个标的当前 OPEN 的期权价差。

    注意：
    - PLANNED 只是计划，不参与平仓判断。
    - 只有真实开仓后把 status 改成 OPEN，才会进入这里。
    """
    sql = f"""
    SELECT *
    FROM `{SPREADS_TABLE}`
    WHERE underlying=%s
      AND status='OPEN'
    ORDER BY id ASC;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (underlying,))
        return cur.fetchall() or []


def load_spread_legs(conn, spread_id: int) -> list[dict]:
    """读取某个价差组合的两条腿。"""
    sql = f"""
    SELECT *
    FROM `{LEGS_TABLE}`
    WHERE spread_id=%s
    ORDER BY leg_no ASC;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (int(spread_id),))
        return cur.fetchall() or []


def get_spread_current_value(spread: dict, legs: list[dict]) -> Optional[float]:
    """
    获取当前价差价值。

    当前版本还没有接真实期权链，所以先从 option_spreads.current_value 读取。
    你可以手工或用其它脚本更新这个字段：

    - 借方价差 Bull Call / Bear Put：
      current_value = 当前平仓卖出该 spread 大约能收回多少钱。

    - 信用价差 Bull Put / Bear Call：
      current_value = 当前买回该 spread 大约要花多少钱。

    后面接 Alpaca 期权行情时，只需要把这里替换成：
    用每条腿 bid/ask 估算整组 close value。
    """
    _ = legs  # 预留给后续真实取价时使用
    current_value = _safe_float(spread.get("current_value"), 0.0)
    if current_value <= 0:
        return None
    return current_value


def calc_spread_profit(spread: dict, current_value: float) -> dict:
    """
    计算价差收益率。

    借方价差：
      entry_price 是开仓成本 debit。
      profit = current_value - entry_price
      profit_pct = profit / entry_price

    信用价差：
      entry_price 是开仓收到的 credit。
      profit = entry_price - current_value
      profit_pct = profit / entry_price

    这样四种模式都可以用同一个 C_TAKE_PROFIT_PCT 判断是否止盈。
    """
    mode = str(spread.get("mode") or "").upper()
    entry_price = _safe_float(spread.get("entry_price"), 0.0)

    if entry_price <= 0:
        return {
            "ok": False,
            "profit": 0.0,
            "profit_pct": 0.0,
            "reason": "missing entry_price",
        }

    if mode in DEBIT_MODES:
        profit = float(current_value) - entry_price
        profit_pct = profit / entry_price
        label = "debit"
    elif mode in CREDIT_MODES:
        profit = entry_price - float(current_value)
        profit_pct = profit / entry_price
        label = "credit"
    else:
        return {
            "ok": False,
            "profit": 0.0,
            "profit_pct": 0.0,
            "reason": f"unknown mode={mode}",
        }

    return {
        "ok": True,
        "profit": round(float(profit), 4),
        "profit_pct": round(float(profit_pct), 6),
        "reason": f"{label} profit={profit:.2f} profit_pct={profit_pct:.2%}",
    }


def should_close_spread(spread: dict, current_value: float) -> tuple[bool, str, dict]:
    """
    判断是否达到止盈平仓条件。

    默认目标是 C_TAKE_PROFIT_PCT=0.60，也就是 60%。
    如果某一笔 spread 自己有 take_profit_pct 字段，就优先用该字段。
    """
    metric = calc_spread_profit(spread, current_value)
    if not metric.get("ok"):
        return False, metric.get("reason", "metric error"), metric

    target = _safe_float(spread.get("take_profit_pct"), C_TAKE_PROFIT_PCT)
    if target <= 0:
        target = C_TAKE_PROFIT_PCT

    profit_pct = float(metric.get("profit_pct") or 0.0)
    if profit_pct >= target:
        return True, f"TAKE_PROFIT profit_pct={profit_pct:.2%} >= target={target:.2%}", metric

    return False, f"HOLD profit_pct={profit_pct:.2%} < target={target:.2%}", metric


def build_close_legs(open_legs: list[dict]) -> list[dict]:
    """
    平仓腿就是开仓腿反过来：
    - 原来 BUY，平仓 SELL
    - 原来 SELL，平仓 BUY

    四种模式都适用：
    Bull Call / Bear Put / Bull Put / Bear Call 都是两腿价差。
    """
    close_legs = []
    for leg in open_legs:
        side = str(leg.get("side") or "").upper()
        close_side = "SELL" if side == "BUY" else "BUY"
        close_legs.append({
            "side": close_side,
            "cp": str(leg.get("cp") or "").upper(),
            "strike": _safe_float(leg.get("strike")),
            "qty": int(_safe_float(leg.get("qty"), 1)),
            "option_symbol": leg.get("option_symbol"),
        })
    return close_legs


def print_close_plan(spread: dict, close_legs: list[dict], reason: str, metric: dict, current_value: float) -> None:
    """打印平仓计划；当前版本只打印，不真实下单。"""
    print(
        f"[C CLOSE PLAN] spread_id={spread.get('id')} underlying={spread.get('underlying')} "
        f"mode={spread.get('mode')} current_value={current_value:.2f} "
        f"profit={float(metric.get('profit') or 0):.2f} "
        f"profit_pct={float(metric.get('profit_pct') or 0):.2%}",
        flush=True,
    )
    print(f"[C CLOSE PLAN] reason={reason}", flush=True)
    for i, leg in enumerate(close_legs, start=1):
        print(
            f"[C CLOSE PLAN] leg{i}: {leg['side']} {leg['cp']} "
            f"strike={leg['strike']:.2f} qty={leg['qty']} symbol={leg.get('option_symbol')}",
            flush=True,
        )


def mark_spread_close_planned(conn, spread_id: int, reason: str, current_value: float, metric: dict) -> None:
    """
    把 OPEN 组合标记为 CLOSE_PLANNED。

    这样不会真实平仓，但数据库里能看到：这笔已经达到止盈条件，
    下一步可以人工平仓，或者以后接真实下单。
    """
    sql = f"""
    UPDATE `{SPREADS_TABLE}`
    SET
        status='CLOSE_PLANNED',
        exit_price=%s,
        close_reason=%s,
        profit=%s,
        profit_pct=%s,
        updated_at=NOW()
    WHERE id=%s
      AND status='OPEN';
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                float(current_value),
                str(reason)[:500],
                float(metric.get("profit") or 0.0),
                float(metric.get("profit_pct") or 0.0),
                int(spread_id),
            ),
        )


def print_plan(plan: SpreadPlan) -> None:
    print(
        f"[C PLAN] {plan.underlying} mode={plan.mode} expiry={plan.expiry} "
        f"underlying={plan.underlying_price:.2f} width={plan.width:.2f} "
        f"score={plan.signal_score:.2f}",
        flush=True,
    )
    print(f"[C PLAN] reason={plan.signal_reason}", flush=True)
    for i, leg in enumerate(plan.legs, start=1):
        print(
            f"[C PLAN] leg{i}: {leg.side} {leg.cp} strike={leg.strike:.2f} qty={leg.qty}",
            flush=True,
        )


def strategy_C_buy(code: str) -> bool:
    """
    主机器人入口。

    默认返回 False，因为当前只生成计划，不真实下单。
    后面如果实现 C_ENABLE_REAL_ORDER=1，只有券商确认下单后才应该返回 True。
    """
    code = (code or "").strip().upper()
    print(f"[C] strategy start {code}", flush=True)

    try:
        market = analyze_market(code)
        mode = select_mode(market)
        price = _safe_float(market.get("price"))

        print(
            f"[C] market {code}: trend={market.get('trend')} bias={market.get('bias')} "
            f"score={market.get('score')} mode={mode} price={price:.2f}",
            flush=True,
        )
        print(f"[C] reason: {market.get('reason')}", flush=True)

        if mode == MODE_NO_TRADE:
            return False

        plan = build_spread_plan(code, mode, price, market)
        if plan is None:
            print(f"[C] no plan built for {code}", flush=True)
            return False

        print_plan(plan)

        if C_RECORD_PLAN == 1:
            try:
                spread_id = record_spread_plan(plan)
                if spread_id:
                    print(f"[C] recorded spread_id={spread_id}", flush=True)
            except Exception as e:
                print(f"[C] record plan failed: {e}", flush=True)

        if C_ENABLE_REAL_ORDER != 1:
            print("[C] dry-run planner only: real option order disabled", flush=True)
            return False

        # 后续真实执行要补在这里：
        # - 获取真实期权链
        # - 校验 bid/ask、价差、成交量、open interest
        # - 提交多腿期权订单
        # - 更新 option_spreads.status='OPEN'
        print("[C] real order execution not implemented yet", flush=True)
        return False

    except Exception as e:
        print(f"[C ERROR] {code}: {e}", flush=True)
        traceback.print_exc()
        return False


def strategy_C_sell(code: str) -> bool:
    """
    期权价差退出管理。

    当前只实现“达到收益百分比 -> 生成平仓计划”：
    1) 读取 option_spreads 中 status='OPEN' 的组合。
    2) 读取每个组合的两条腿。
    3) 用 current_value 和 entry_price 计算收益百分比。
    4) 如果收益率 >= take_profit_pct，打印反向平仓腿。
    5) 可选把状态改成 CLOSE_PLANNED。

    注意：当前还不接真实期权行情，也不真实下单。
    """
    code = (code or "").strip().upper()
    print(f"[C SELL] {code} spread exit manager start", flush=True)

    conn = None
    planned_any = False
    try:
        conn = _connect()
        spreads = load_open_spreads(conn, code)
        if not spreads:
            print(f"[C SELL] {code} no OPEN spreads", flush=True)
            return False

        for spread in spreads:
            spread_id = int(spread.get("id") or 0)
            legs = load_spread_legs(conn, spread_id)
            if not legs:
                print(f"[C SELL] spread_id={spread_id} skip: no legs", flush=True)
                continue

            current_value = get_spread_current_value(spread, legs)
            if current_value is None:
                print(
                    f"[C SELL] spread_id={spread_id} skip: missing current_value. "
                    f"Update {SPREADS_TABLE}.current_value first.",
                    flush=True,
                )
                continue

            should_close, reason, metric = should_close_spread(spread, current_value)
            print(
                f"[C SELL] spread_id={spread_id} mode={spread.get('mode')} "
                f"entry={_safe_float(spread.get('entry_price')):.2f} "
                f"current={current_value:.2f} {reason}",
                flush=True,
            )

            if not should_close:
                continue

            close_legs = build_close_legs(legs)
            print_close_plan(spread, close_legs, reason, metric, current_value)

            if C_RECORD_PLAN == 1:
                try:
                    mark_spread_close_planned(conn, spread_id, reason, current_value, metric)
                    print(f"[C SELL] spread_id={spread_id} marked CLOSE_PLANNED", flush=True)
                except Exception as e:
                    print(f"[C SELL] spread_id={spread_id} mark close planned failed: {e}", flush=True)

            planned_any = True

        if C_ENABLE_REAL_ORDER != 1:
            print("[C SELL] dry-run close planner only: real close order disabled", flush=True)
            return False

        print("[C SELL] real close execution not implemented yet", flush=True)
        return False

    except Exception as e:
        print(f"[C SELL ERROR] {code}: {e}", flush=True)
        traceback.print_exc()
        return False
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    strategy_C_buy(os.getenv("C_TEST_SYMBOL", "QQQ"))
