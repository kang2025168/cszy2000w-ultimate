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
CANDIDATES_TABLE = os.getenv("C_CANDIDATES_TABLE", "strategy_c_candidates")
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
C_EXPIRY_DAYS_MIN = int(os.getenv("C_EXPIRY_DAYS_MIN", "30"))
C_EXPIRY_DAYS_MAX = int(os.getenv("C_EXPIRY_DAYS_MAX", "45"))

# 借方价差：Bull Call / Bear Put。
# 例如花 2.00 买入，涨到 3.20 是盈利 60%，跌到 1.20 是亏损 40%。
C_DEBIT_TAKE_PROFIT_PCT = float(os.getenv("C_DEBIT_TAKE_PROFIT_PCT", "0.60"))
C_DEBIT_STOP_LOSS_PCT = float(os.getenv("C_DEBIT_STOP_LOSS_PCT", "0.40"))

# 信用价差：Bull Put / Bear Call。
# 例如收 1.00 credit，买回价跌到 0.45 是盈利 55%；涨到 2.00 是亏损扩大，需要止损。
C_CREDIT_TAKE_PROFIT_PCT = float(os.getenv("C_CREDIT_TAKE_PROFIT_PCT", "0.55"))
C_CREDIT_STOP_MULT = float(os.getenv("C_CREDIT_STOP_MULT", "2.0"))

# 到期前剩余天数太短，Gamma 风险会变大，统一退出。
C_DTE_EXIT_DAYS = int(os.getenv("C_DTE_EXIT_DAYS", "10"))

# 仓位控制：C 是期权策略，宁可少做，不要铺太多。
C_MAX_OPEN_SPREADS = int(os.getenv("C_MAX_OPEN_SPREADS", "3"))
C_MAX_RISK_PER_TRADE = float(os.getenv("C_MAX_RISK_PER_TRADE", "300"))
C_BP_USE_RATIO = float(os.getenv("C_BP_USE_RATIO", "0.95"))
C_MIN_CANDIDATE_SCORE = float(os.getenv("C_MIN_CANDIDATE_SCORE", "60"))
C_REFRESH_LIMIT = int(os.getenv("C_REFRESH_LIMIT", "5"))
C_CANDIDATE_MAX_AGE_DAYS = int(os.getenv("C_CANDIDATE_MAX_AGE_DAYS", "3"))
C_SAME_SYMBOL_COOLDOWN_DAYS = int(os.getenv("C_SAME_SYMBOL_COOLDOWN_DAYS", "2"))

# 兼容旧字段/旧表：如果 option_spreads.take_profit_pct 已经有值，仍优先读表里的值。
C_TAKE_PROFIT_PCT = float(os.getenv("C_TAKE_PROFIT_PCT", str(C_DEBIT_TAKE_PROFIT_PCT)))

# =========================
# 期权流动性过滤
# =========================
# 说明：
# - 这些参数用于真实接入期权链后，过滤不好买卖的合约。
# - 当前 strategy_C_buy 仍是计划器，不拉真实期权链，所以先提供统一判断函数。
C_OPTION_MIN_OPEN_INTEREST = int(os.getenv("C_OPTION_MIN_OPEN_INTEREST", "500"))
C_OPTION_MIN_VOLUME = int(os.getenv("C_OPTION_MIN_VOLUME", "100"))
C_OPTION_MAX_SPREAD_PCT = float(os.getenv("C_OPTION_MAX_SPREAD_PCT", "0.15"))
C_OPTION_MAX_SPREAD_ABS = float(os.getenv("C_OPTION_MAX_SPREAD_ABS", "0.10"))
C_OPTION_MIN_BID = float(os.getenv("C_OPTION_MIN_BID", "0.01"))
C_OPTION_MIN_MID = float(os.getenv("C_OPTION_MIN_MID", "0.05"))
C_OPTION_DATA_FEED = os.getenv("C_OPTION_DATA_FEED", "indicative").strip().lower()
C_OPTION_ENTRY_PRICE_BUFFER_PCT = float(os.getenv("C_OPTION_ENTRY_PRICE_BUFFER_PCT", "0.03"))

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
class OptionQuote:
    """
    单个期权合约的行情快照。

    后面接 Alpaca/券商期权链时，把每个合约转成这个结构，
    再用 is_option_quote_liquid() 做统一流动性过滤。
    """
    option_symbol: str
    bid: float
    ask: float
    volume: int = 0
    open_interest: int = 0


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


@dataclass
class SpreadPricing:
    """
    真实开仓前的价格/风险测算。

    entry_price 是每 1 组价差的价格：
    - 借方价差：正数，表示每组合约要付出的 debit。
    - 信用价差：正数，表示每组合约能收到的 credit。

    alpaca_limit_price 是提交 mleg 限价单用的价格：
    - 借方价差：正数 debit。
    - 信用价差：负数 credit。
    """
    entry_price: float
    alpaca_limit_price: float
    max_loss_per_spread: float
    qty: int
    buying_power: float
    reason: str


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


def _intent_short(s: str, max_len: int = 80) -> str:
    s = (s or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


def _parse_date(v) -> Optional[date]:
    if not v:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        return datetime.fromisoformat(str(v)[:10]).date()
    except Exception:
        return None


def _update_ops_fields(conn, code: str, **kwargs) -> None:
    if not kwargs:
        return
    cols = []
    vals = []
    for k, v in kwargs.items():
        cols.append(f"`{k}`=%s")
        vals.append(v)
    vals.append(code)
    sql = f"UPDATE `{OPS_TABLE}` SET {', '.join(cols)} WHERE stock_code=%s AND stock_type='C';"
    with conn.cursor() as cur:
        cur.execute(sql, tuple(vals))


def _load_c_ops_row(conn, code: str) -> Optional[dict]:
    sql = f"""
    SELECT *
    FROM `{OPS_TABLE}`
    WHERE stock_code=%s AND stock_type='C'
    LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (code,))
        return cur.fetchone()


_trading_client = None


def _get_trading_client():
    """Alpaca 交易客户端；只在需要查资金或真实下单时创建。"""
    global _trading_client
    if _trading_client is not None:
        return _trading_client

    from alpaca.trading.client import TradingClient

    trade_env = (os.getenv("TRADE_ENV") or os.getenv("ALPACA_MODE") or "paper").strip().lower()
    key = os.getenv("APCA_API_KEY_ID", "") or os.getenv("ALPACA_KEY", "")
    secret = os.getenv("APCA_API_SECRET_KEY", "") or os.getenv("ALPACA_SECRET", "")
    if not key or not secret:
        raise RuntimeError("Alpaca key missing: APCA_API_KEY_ID / APCA_API_SECRET_KEY")

    _trading_client = TradingClient(key, secret, paper=(trade_env != "live"))
    return _trading_client


def _get_buying_power() -> float:
    tc = _get_trading_client()
    acct = tc.get_account()
    bp = getattr(acct, "buying_power", None)
    if bp is None:
        bp = getattr(acct, "cash", None)
    return float(bp or 0.0)


def is_option_quote_liquid(q: OptionQuote) -> tuple[bool, str]:
    """
    判断单个期权合约是否足够好买卖。

    一般高流动性期权的特征：
    - bid/ask 都有效，不能 bid=0。
    - bid/ask 价差不能太大。
    - open interest 足够高，说明市场里有存量仓位。
    - 当天 volume 足够高，说明今天也有人在交易。

    默认阈值偏保守：
    - OI >= 500
    - volume >= 100
    - spread <= 0.10 美元 或 spread/mid <= 15%

    重要：
    - OPRA feed 下会严格检查 volume/open_interest。
    - indicative feed 经常拿不到 volume/open_interest，会显示 0；
      这种情况下只把 bid/ask/spread 作为硬过滤，否则所有合约都会被挡住。
    """
    bid = _safe_float(q.bid)
    ask = _safe_float(q.ask)
    volume = int(_safe_float(q.volume))
    open_interest = int(_safe_float(q.open_interest))
    strict_volume_oi = (C_OPTION_DATA_FEED == "opra")

    if bid < C_OPTION_MIN_BID or ask <= 0 or ask <= bid:
        return False, f"bad bid/ask bid={bid:.2f} ask={ask:.2f}"

    mid = (bid + ask) / 2.0
    if mid < C_OPTION_MIN_MID:
        return False, f"mid too small mid={mid:.2f}"

    spread = ask - bid
    spread_pct = spread / mid if mid > 0 else 999.0
    if spread > C_OPTION_MAX_SPREAD_ABS and spread_pct > C_OPTION_MAX_SPREAD_PCT:
        return False, f"spread too wide spread={spread:.2f} spread_pct={spread_pct:.2%}"

    if strict_volume_oi and open_interest < C_OPTION_MIN_OPEN_INTEREST:
        return False, f"open_interest too low oi={open_interest} min={C_OPTION_MIN_OPEN_INTEREST}"

    if strict_volume_oi and volume < C_OPTION_MIN_VOLUME:
        return False, f"volume too low volume={volume} min={C_OPTION_MIN_VOLUME}"

    data_note = "strict_liquidity" if strict_volume_oi else "indicative_skip_vol_oi"
    return True, (
        f"liquid bid={bid:.2f} ask={ask:.2f} spread={spread:.2f} "
        f"spread_pct={spread_pct:.2%} volume={volume} oi={open_interest} {data_note}"
    )


def is_spread_quotes_liquid(quotes: list[OptionQuote]) -> tuple[bool, str]:
    """
    两腿价差必须两条腿都流动性合格。

    注意：信用价差/借方价差都一样，任何一条腿流动性差，
    平仓时都可能滑点很大，所以整组直接过滤。
    """
    if len(quotes) < 2:
        return False, "missing option quotes"

    reasons = []
    for q in quotes:
        ok, reason = is_option_quote_liquid(q)
        reasons.append(f"{q.option_symbol}: {reason}")
        if not ok:
            return False, "; ".join(reasons)

    return True, "; ".join(reasons)


def _occ_option_symbol(underlying: str, expiry: date, cp: str, strike: float) -> str:
    """
    生成标准 OCC 期权代码。

    例：AAPL 2026-06-19 200C -> AAPL260619C00200000
    Alpaca 的期权 symbol 使用这种格式。
    """
    root = (underlying or "").strip().upper()
    yymmdd = expiry.strftime("%y%m%d")
    cp = (cp or "").strip().upper()[0]
    strike_int = int(round(float(strike) * 1000))
    return f"{root}{yymmdd}{cp}{strike_int:08d}"


def _attach_option_symbols(plan: SpreadPlan) -> SpreadPlan:
    for leg in plan.legs:
        if not leg.option_symbol:
            leg.option_symbol = _occ_option_symbol(plan.underlying, plan.expiry, leg.cp, leg.strike)
    return plan


def _get_option_quotes(option_symbols: list[str]) -> dict[str, OptionQuote]:
    """
    拉取期权最新报价。

    注意：
    - 默认 C_OPTION_DATA_FEED=indicative，避免没有 OPRA 订阅时报错。
    - volume/open_interest 在不同数据权限下可能拿不到；拿不到会按 0 处理，
      从而无法通过流动性过滤。这是刻意的：真实下单宁可保守。
    """
    if not option_symbols:
        return {}

    from alpaca.data import OptionHistoricalDataClient
    from alpaca.data.requests import OptionSnapshotRequest

    try:
        from alpaca.data.enums import OptionsFeed
        feed = OptionsFeed.OPRA if C_OPTION_DATA_FEED == "opra" else OptionsFeed.INDICATIVE
    except Exception:
        feed = None

    key = os.getenv("APCA_API_KEY_ID", "") or os.getenv("ALPACA_KEY", "")
    secret = os.getenv("APCA_API_SECRET_KEY", "") or os.getenv("ALPACA_SECRET", "")
    client = OptionHistoricalDataClient(key, secret)

    req_kwargs = {"symbol_or_symbols": option_symbols}
    if feed is not None:
        req_kwargs["feed"] = feed
    snapshots = client.get_option_snapshot(OptionSnapshotRequest(**req_kwargs))

    out = {}
    for sym in option_symbols:
        snap = snapshots.get(sym) if isinstance(snapshots, dict) else getattr(snapshots, sym, None)
        if not snap:
            continue

        latest_quote = getattr(snap, "latest_quote", None) or getattr(snap, "latestQuote", None)
        bid = _safe_float(getattr(latest_quote, "bid_price", None), None)
        ask = _safe_float(getattr(latest_quote, "ask_price", None), None)
        if bid is None:
            bid = _safe_float(getattr(latest_quote, "bp", None), 0.0)
        if ask is None:
            ask = _safe_float(getattr(latest_quote, "ap", None), 0.0)

        daily_bar = getattr(snap, "daily_bar", None) or getattr(snap, "dailyBar", None)
        volume = int(_safe_float(getattr(daily_bar, "volume", None), 0.0))

        # 不同 SDK/数据源字段名可能不同，这里做宽松兼容。
        open_interest = int(_safe_float(
            getattr(snap, "open_interest", None)
            or getattr(snap, "openInterest", None)
            or getattr(snap, "oi", None),
            0.0,
        ))

        out[sym] = OptionQuote(
            option_symbol=sym,
            bid=float(bid or 0.0),
            ask=float(ask or 0.0),
            volume=volume,
            open_interest=open_interest,
        )
    return out


def _price_spread_from_quotes(plan: SpreadPlan, quote_map: dict[str, OptionQuote]) -> Optional[SpreadPricing]:
    """
    用 bid/ask 测算开仓价格、最大亏损和可买数量。

    开仓估算：
    - BUY 腿按 ask 买。
    - SELL 腿按 bid 卖。
    这是保守估算，避免低估成本/风险。
    """
    plan = _attach_option_symbols(plan)
    quotes = []
    for leg in plan.legs:
        q = quote_map.get(str(leg.option_symbol))
        if not q:
            return None
        quotes.append(q)

    liquid_ok, liquid_reason = is_spread_quotes_liquid(quotes)
    if not liquid_ok:
        raise RuntimeError(f"option not liquid: {liquid_reason}")

    net = 0.0
    for leg in plan.legs:
        q = quote_map[str(leg.option_symbol)]
        side = leg.side.upper()
        if side == "BUY":
            net += q.ask
        elif side == "SELL":
            net -= q.bid

    width = abs(float(plan.legs[0].strike) - float(plan.legs[1].strike))
    buying_power = _get_buying_power()
    usable_bp = max(buying_power * C_BP_USE_RATIO, 0.0)

    if plan.mode in DEBIT_MODES:
        debit = max(float(net), 0.01)
        entry_price = round(debit * (1.0 + C_OPTION_ENTRY_PRICE_BUFFER_PCT), 2)
        max_loss_per_spread = entry_price * 100.0
        alpaca_limit_price = entry_price
    elif plan.mode in CREDIT_MODES:
        credit = abs(min(float(net), -0.01))
        entry_price = round(credit * (1.0 - C_OPTION_ENTRY_PRICE_BUFFER_PCT), 2)
        if entry_price <= 0:
            raise RuntimeError(f"invalid credit entry_price={entry_price}")
        max_loss_per_spread = max((width - entry_price) * 100.0, 0.0)
        alpaca_limit_price = -entry_price
    else:
        raise RuntimeError(f"unknown mode={plan.mode}")

    if max_loss_per_spread <= 0:
        raise RuntimeError(f"invalid max_loss_per_spread={max_loss_per_spread}")

    max_risk_budget = min(C_MAX_RISK_PER_TRADE, usable_bp)
    qty = int(max_risk_budget // max_loss_per_spread)
    if qty <= 0:
        raise RuntimeError(
            f"not enough buying_power: bp={buying_power:.2f} usable={usable_bp:.2f} "
            f"risk_per_spread={max_loss_per_spread:.2f} max_risk={C_MAX_RISK_PER_TRADE:.2f}"
        )

    return SpreadPricing(
        entry_price=round(entry_price, 2),
        alpaca_limit_price=round(alpaca_limit_price, 2),
        max_loss_per_spread=round(max_loss_per_spread, 2),
        qty=qty,
        buying_power=round(buying_power, 2),
        reason=f"{liquid_reason}; max_loss_per_spread={max_loss_per_spread:.2f} qty={qty}",
    )


def _next_friday_after(min_days: int) -> date:
    d = datetime.now().date() + timedelta(days=max(int(min_days), 1))
    while d.weekday() != 4:  # 4 = 周五，期权常用周五到期
        d += timedelta(days=1)
    return d


def _select_expiry() -> date:
    """
    选择 30-45 DTE 附近的周五到期。

    第一版不做复杂期限优化，只取 >= C_EXPIRY_DAYS_MIN 的最近周五。
    通常会落在 30-36 天；如果你后面想更久，可以调 C_EXPIRY_DAYS_MIN。
    """
    expiry = _next_friday_after(C_EXPIRY_DAYS_MIN)
    max_expiry = datetime.now().date() + timedelta(days=max(C_EXPIRY_DAYS_MAX, C_EXPIRY_DAYS_MIN))
    if expiry <= max_expiry:
        return expiry
    return _next_friday_after(C_EXPIRY_DAYS_MAX)


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

    expiry = _select_expiry()
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
      AND status IN ('PLANNED','SUBMITTED','OPEN','CLOSE_PLANNED')
    ORDER BY id DESC
    LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (symbol,))
        return cur.fetchone() is not None


def _take_profit_for_mode(mode: str) -> float:
    mode = (mode or "").strip().upper()
    if mode in CREDIT_MODES:
        return C_CREDIT_TAKE_PROFIT_PCT
    return C_DEBIT_TAKE_PROFIT_PCT


def _table_columns(conn, table_name: str) -> set[str]:
    sql = f"SHOW COLUMNS FROM `{table_name}`;"
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall() or []
    return {str(r.get("Field") or "") for r in rows}


def _update_spread_existing_fields(conn, spread_id: int, **kwargs) -> None:
    """
    只更新表里真实存在的字段。

    你不同阶段建过的 option_spreads 字段可能不完全一致；
    这样可以避免因为缺少 order_id / entry_price 等字段导致策略中断。
    """
    cols_available = _table_columns(conn, SPREADS_TABLE)
    clean = {k: v for k, v in kwargs.items() if k in cols_available}
    if not clean:
        return

    parts = []
    vals = []
    for k, v in clean.items():
        parts.append(f"`{k}`=%s")
        vals.append(v)
    vals.append(int(spread_id))
    sql = f"UPDATE `{SPREADS_TABLE}` SET {', '.join(parts)}, updated_at=NOW() WHERE id=%s;"
    with conn.cursor() as cur:
        cur.execute(sql, tuple(vals))


def submit_open_spread_order(plan: SpreadPlan, pricing: SpreadPricing):
    """
    提交真实多腿期权开仓单。

    Alpaca 官方 mleg 规则：
    - order_class=MLEG
    - legs 使用 OptionLegRequest
    - limit_price 正数是 debit，负数是 credit
    - 每条腿用 position_intent 表示 BTO/STO
    """
    from alpaca.trading.enums import OrderClass, PositionIntent, TimeInForce
    from alpaca.trading.requests import LimitOrderRequest, OptionLegRequest

    legs = []
    for leg in _attach_option_symbols(plan).legs:
        side = leg.side.upper()
        if side == "BUY":
            intent = PositionIntent.BUY_TO_OPEN
        elif side == "SELL":
            intent = PositionIntent.SELL_TO_OPEN
        else:
            raise RuntimeError(f"unknown option leg side={leg.side}")

        legs.append(OptionLegRequest(
            symbol=str(leg.option_symbol),
            ratio_qty=1.0,
            position_intent=intent,
        ))

    req = LimitOrderRequest(
        qty=int(pricing.qty),
        order_class=OrderClass.MLEG,
        time_in_force=TimeInForce.DAY,
        limit_price=float(pricing.alpaca_limit_price),
        legs=legs,
    )
    return _get_trading_client().submit_order(order_data=req)


def submit_close_spread_order(spread: dict, open_legs: list[dict], current_value: float):
    """
    提交真实多腿期权平仓单。

    平仓方向：
    - 原 BUY 腿 -> SELL_TO_CLOSE
    - 原 SELL 腿 -> BUY_TO_CLOSE

    mleg limit_price：
    - 平掉借方价差通常是收回 credit，所以用负数。
    - 平掉信用价差通常是支付 debit，所以用正数。
    """
    from alpaca.trading.enums import OrderClass, PositionIntent, TimeInForce
    from alpaca.trading.requests import LimitOrderRequest, OptionLegRequest

    mode = str(spread.get("mode") or "").upper()
    qty = int(_safe_float(spread.get("qty"), 0))
    if qty <= 0:
        qty = max([int(_safe_float(leg.get("qty"), 1)) for leg in open_legs] or [1])
    if qty <= 0:
        qty = 1

    legs = []
    for leg in open_legs:
        old_side = str(leg.get("side") or "").upper()
        opt_symbol = str(leg.get("option_symbol") or "").strip()
        if not opt_symbol:
            raise RuntimeError("missing option_symbol for close order")

        if old_side == "BUY":
            intent = PositionIntent.SELL_TO_CLOSE
        elif old_side == "SELL":
            intent = PositionIntent.BUY_TO_CLOSE
        else:
            raise RuntimeError(f"unknown open leg side={old_side}")

        legs.append(OptionLegRequest(
            symbol=opt_symbol,
            ratio_qty=1.0,
            position_intent=intent,
        ))

    if mode in DEBIT_MODES:
        limit_price = -abs(float(current_value))
    elif mode in CREDIT_MODES:
        limit_price = abs(float(current_value))
    else:
        raise RuntimeError(f"unknown mode={mode}")

    req = LimitOrderRequest(
        qty=qty,
        order_class=OrderClass.MLEG,
        time_in_force=TimeInForce.DAY,
        limit_price=round(limit_price, 2),
        legs=legs,
    )
    return _get_trading_client().submit_order(order_data=req)


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
                    _take_profit_for_mode(plan.mode),
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


def _count_active_spreads(conn) -> int:
    """统计当前 C 策略已有多少个活跃组合，用来限制同时持仓数量。"""
    sql = f"""
    SELECT COUNT(*) AS n
    FROM `{SPREADS_TABLE}`
    WHERE status IN ('PLANNED','SUBMITTED','OPEN','CLOSE_PLANNED');
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone() or {}
    return int(row.get("n") or 0)


def _has_recent_closed_spread(conn, symbol: str) -> bool:
    """同一标的刚退出后冷却几天，避免刚卖完又马上追进去。"""
    if C_SAME_SYMBOL_COOLDOWN_DAYS <= 0:
        return False
    sql = f"""
    SELECT id
    FROM `{SPREADS_TABLE}`
    WHERE underlying=%s
      AND status IN ('CLOSE_PLANNED','CLOSED','CANCELED','FAILED')
      AND updated_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
    ORDER BY id DESC
    LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (symbol, int(C_SAME_SYMBOL_COOLDOWN_DAYS)))
        return cur.fetchone() is not None


def _load_latest_c_candidates(conn, limit: int = C_REFRESH_LIMIT) -> list[dict]:
    """
    从 strategy_c_candidates 取最新交易日的高分候选。

    注意：扫描器只负责分类入表；这里才把少数候选送进 stock_operations，
    让主程序 BUY PHASE 像 B/F 一样统一调度。
    """
    sql = f"""
    SELECT c.*
    FROM `{CANDIDATES_TABLE}` c
    JOIN (
        SELECT MAX(as_of) AS as_of
        FROM `{CANDIDATES_TABLE}`
    ) x ON x.as_of = c.as_of
    WHERE c.option_mode <> %s
      AND c.score >= %s
      AND c.as_of >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
    ORDER BY c.score DESC, c.symbol ASC
    LIMIT %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (MODE_NO_TRADE, float(C_MIN_CANDIDATE_SCORE), int(C_CANDIDATE_MAX_AGE_DAYS), int(limit)))
        return cur.fetchall() or []


def _upsert_c_ops_candidate(conn, row: dict) -> bool:
    """
    把 C 候选写入 stock_operations(stock_type='C', can_buy=1)。

    保护规则：
    - 已有其它策略持仓，不抢。
    - 已有其它策略候选，不强行覆盖。
    - 同一个标的已有 C 活跃组合，不重复开。
    """
    code = (row.get("symbol") or "").strip().upper()
    if not code:
        return False

    if _has_active_plan(conn, code):
        print(f"[C READY] {code} skip: active spread exists", flush=True)
        return False
    if _has_recent_closed_spread(conn, code):
        print(f"[C READY] {code} skip: cooldown after recent close", flush=True)
        return False

    sql = f"SELECT * FROM `{OPS_TABLE}` WHERE stock_code=%s LIMIT 1;"
    with conn.cursor() as cur:
        cur.execute(sql, (code,))
        existing = cur.fetchone()

    close_price = _safe_float(row.get("close_price"))
    intent = _intent_short(
        f"C:READY {row.get('option_mode')} score={_safe_float(row.get('score')):.2f} "
        f"as_of={row.get('as_of')}"
    )

    if existing:
        old_type = str(existing.get("stock_type") or "").strip().upper()
        is_bought = int(existing.get("is_bought") or 0)
        if is_bought == 1:
            print(f"[C READY] {code} skip: already bought stock_type={old_type}", flush=True)
            return False
        if old_type != "C":
            print(f"[C READY] {code} skip: protected old stock_type={old_type}", flush=True)
            return False

        sql = f"""
        UPDATE `{OPS_TABLE}`
        SET can_buy=1,
            can_sell=0,
            is_bought=0,
            trigger_price=%s,
            close_price=%s,
            last_order_side=NULL,
            last_order_intent=%s,
            updated_at=CURRENT_TIMESTAMP
        WHERE stock_code=%s AND stock_type='C';
        """
        with conn.cursor() as cur:
            cur.execute(sql, (close_price, close_price, intent, code))
        return True

    sql = f"""
    INSERT INTO `{OPS_TABLE}` (
        stock_code, stock_type, is_bought, can_buy, can_sell,
        trigger_price, close_price, last_order_intent,
        created_at, updated_at
    )
    VALUES (%s, 'C', 0, 1, 0, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
    """
    with conn.cursor() as cur:
        cur.execute(sql, (code, close_price, close_price, intent))
    return True


def strategy_C_refresh_candidates() -> int:
    """
    给主程序调用：从 C 候选表挑少数高分票，写入 stock_operations 等待买入。

    这一步不下单，只是把“今天最值得看的 C 候选”放进主机器人队列。
    主程序仍然会先 SELL PHASE，再 BUY PHASE。
    """
    conn = _connect()
    ready = 0
    try:
        active_n = _count_active_spreads(conn)
        if active_n >= C_MAX_OPEN_SPREADS:
            print(f"[C READY] skip: active_spreads={active_n} >= max={C_MAX_OPEN_SPREADS}", flush=True)
            return 0

        room = max(C_MAX_OPEN_SPREADS - active_n, 0)
        rows = _load_latest_c_candidates(conn, limit=min(C_REFRESH_LIMIT, room))
        print(f"[C READY] candidates={len(rows)} active={active_n} room={room}", flush=True)

        for row in rows:
            try:
                if _upsert_c_ops_candidate(conn, row):
                    ready += 1
                    print(
                        f"[C READY] {row.get('symbol')} mode={row.get('option_mode')} "
                        f"score={_safe_float(row.get('score')):.2f}",
                        flush=True,
                    )
            except Exception as e:
                print(f"[C READY] {row.get('symbol')} failed: {e}", flush=True)
                traceback.print_exc()
        return ready
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


def _days_to_expiry(spread: dict) -> Optional[int]:
    expiry = _parse_date(spread.get("expiry"))
    if not expiry:
        return None
    return (expiry - datetime.now().date()).days


def _short_leg_strike(legs: list[dict], cp: str) -> Optional[float]:
    cp = (cp or "").strip().upper()
    for leg in legs:
        if str(leg.get("side") or "").upper() == "SELL" and str(leg.get("cp") or "").upper() == cp:
            return _safe_float(leg.get("strike"))
    return None


def _trend_exit_reason(spread: dict, legs: list[dict]) -> Optional[str]:
    """
    用标的趋势/关键价位做保护退出。

    四种模式的退出含义：
    - Bull Call：上涨趋势失效，或者价格跌回 MA10 下方。
    - Bear Put：下跌趋势失效，或者价格站回 MA10 上方。
    - Bull Put：标的跌破 short put，说明信用价差被威胁。
    - Bear Call：标的突破 short call，说明信用价差被威胁。
    """
    underlying = str(spread.get("underlying") or "").strip().upper()
    mode = str(spread.get("mode") or "").strip().upper()
    if not underlying:
        return None

    market = analyze_market(underlying)
    price = _safe_float(market.get("price"))
    ma10 = _safe_float(market.get("ma10"))
    selected_mode = select_mode(market)

    if price <= 0:
        return None

    if mode == MODE_BULL_CALL:
        if ma10 > 0 and price < ma10:
            return f"TREND_FAIL Bull Call price={price:.2f} < MA10={ma10:.2f}"
        if selected_mode not in (MODE_BULL_CALL, MODE_BULL_PUT):
            return f"TREND_FAIL Bull Call selected_mode={selected_mode}"

    elif mode == MODE_BEAR_PUT:
        if ma10 > 0 and price > ma10:
            return f"TREND_FAIL Bear Put price={price:.2f} > MA10={ma10:.2f}"
        if selected_mode not in (MODE_BEAR_PUT, MODE_BEAR_CALL):
            return f"TREND_FAIL Bear Put selected_mode={selected_mode}"

    elif mode == MODE_BULL_PUT:
        short_put = _short_leg_strike(legs, "P")
        if short_put and price <= short_put:
            return f"SHORT_STRIKE_THREAT Bull Put price={price:.2f} <= short_put={short_put:.2f}"
        if selected_mode == MODE_BEAR_PUT:
            return f"TREND_FAIL Bull Put selected_mode={selected_mode}"

    elif mode == MODE_BEAR_CALL:
        short_call = _short_leg_strike(legs, "C")
        if short_call and price >= short_call:
            return f"SHORT_STRIKE_THREAT Bear Call price={price:.2f} >= short_call={short_call:.2f}"
        if selected_mode == MODE_BULL_CALL:
            return f"TREND_FAIL Bear Call selected_mode={selected_mode}"

    return None


def should_close_spread(spread: dict, current_value: float, legs: Optional[list[dict]] = None) -> tuple[bool, str, dict]:
    """
    判断是否应该平仓。

    第一版规则：
    1) 到期剩余 <= C_DTE_EXIT_DAYS：退出。
    2) 借方价差盈利 >= 60%：止盈；亏损 >= 40%：止损。
    3) 信用价差盈利 >= 55%：止盈；买回成本 >= 收款 2 倍：止损。
    4) 趋势失效 / short strike 被威胁：退出。
    """
    legs = legs or []
    metric = calc_spread_profit(spread, current_value)
    if not metric.get("ok"):
        return False, metric.get("reason", "metric error"), metric

    mode = str(spread.get("mode") or "").upper()
    profit_pct = float(metric.get("profit_pct") or 0.0)

    dte = _days_to_expiry(spread)
    if dte is not None and dte <= C_DTE_EXIT_DAYS:
        return True, f"DTE_EXIT dte={dte} <= {C_DTE_EXIT_DAYS}", metric

    trend_reason = _trend_exit_reason(spread, legs)
    if trend_reason:
        return True, trend_reason, metric

    if mode in DEBIT_MODES:
        target = _safe_float(spread.get("take_profit_pct"), C_DEBIT_TAKE_PROFIT_PCT)
        if target <= 0:
            target = C_DEBIT_TAKE_PROFIT_PCT
        if profit_pct >= target:
            return True, f"TAKE_PROFIT debit profit_pct={profit_pct:.2%} >= target={target:.2%}", metric
        if profit_pct <= -abs(C_DEBIT_STOP_LOSS_PCT):
            return True, f"STOP_LOSS debit profit_pct={profit_pct:.2%} <= -{C_DEBIT_STOP_LOSS_PCT:.2%}", metric

    elif mode in CREDIT_MODES:
        target = _safe_float(spread.get("take_profit_pct"), C_CREDIT_TAKE_PROFIT_PCT)
        if target <= 0:
            target = C_CREDIT_TAKE_PROFIT_PCT
        entry_price = _safe_float(spread.get("entry_price"), 0.0)
        if profit_pct >= target:
            return True, f"TAKE_PROFIT credit profit_pct={profit_pct:.2%} >= target={target:.2%}", metric
        if entry_price > 0 and float(current_value) >= entry_price * C_CREDIT_STOP_MULT:
            return True, (
                f"STOP_LOSS credit current={float(current_value):.2f} "
                f">= entry*{C_CREDIT_STOP_MULT:.2f} ({entry_price * C_CREDIT_STOP_MULT:.2f})"
            ), metric

    return False, f"HOLD mode={mode} profit_pct={profit_pct:.2%}", metric


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


def _load_latest_candidate_for_symbol(conn, code: str) -> Optional[dict]:
    sql = f"""
    SELECT *
    FROM `{CANDIDATES_TABLE}`
    WHERE symbol=%s
      AND option_mode <> %s
      AND score >= %s
      AND as_of >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
    ORDER BY as_of DESC, score DESC
    LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (code, MODE_NO_TRADE, float(C_MIN_CANDIDATE_SCORE), int(C_CANDIDATE_MAX_AGE_DAYS)))
        return cur.fetchone()


def strategy_C_buy(code: str) -> bool:
    """
    主机器人入口。

    默认返回 False，因为当前只生成计划，不真实下单。
    后面如果实现 C_ENABLE_REAL_ORDER=1，只有券商确认下单后才应该返回 True。
    """
    code = (code or "").strip().upper()
    print(f"[C] strategy start {code}", flush=True)

    conn = None
    try:
        conn = _connect()

        row = _load_c_ops_row(conn, code)
        if not row:
            print(f"[C] {code} skip: no C row in stock_operations", flush=True)
            return False
        if int(row.get("can_buy") or 0) != 1:
            print(f"[C] {code} skip: can_buy={row.get('can_buy')}", flush=True)
            return False

        active_n = _count_active_spreads(conn)
        if active_n >= C_MAX_OPEN_SPREADS:
            print(f"[C] {code} skip: active_spreads={active_n} >= max={C_MAX_OPEN_SPREADS}", flush=True)
            return False
        if _has_active_plan(conn, code):
            print(f"[C] {code} skip: active plan already exists", flush=True)
            _update_ops_fields(
                conn,
                code,
                can_buy=0,
                last_order_side="buy",
                last_order_intent=_intent_short("C:SKIP active_plan_exists"),
                last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            return False

        candidate = _load_latest_candidate_for_symbol(conn, code)
        market = analyze_market(code)
        mode = str((candidate or {}).get("option_mode") or select_mode(market)).strip().upper()
        price = _safe_float((candidate or {}).get("close_price"), _safe_float(market.get("price")))

        print(
            f"[C] market {code}: trend={market.get('trend')} bias={market.get('bias')} "
            f"score={market.get('score')} mode={mode} price={price:.2f}",
            flush=True,
        )
        print(f"[C] reason: {market.get('reason')}", flush=True)
        if candidate:
            print(
                f"[C] candidate {code}: as_of={candidate.get('as_of')} "
                f"mode={candidate.get('option_mode')} score={_safe_float(candidate.get('score')):.2f} "
                f"reason={candidate.get('reason')}",
                flush=True,
            )

        if mode == MODE_NO_TRADE:
            _update_ops_fields(
                conn,
                code,
                can_buy=0,
                last_order_side="buy",
                last_order_intent=_intent_short("C:NO_TRADE signal faded"),
                last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            return False

        plan = build_spread_plan(code, mode, price, market)
        if plan is None:
            print(f"[C] no plan built for {code}", flush=True)
            return False

        _attach_option_symbols(plan)
        print_plan(plan)

        pricing = None
        if C_ENABLE_REAL_ORDER == 1:
            try:
                option_symbols = [str(leg.option_symbol) for leg in plan.legs if leg.option_symbol]
                quote_map = _get_option_quotes(option_symbols)
                pricing = _price_spread_from_quotes(plan, quote_map)

                for leg in plan.legs:
                    leg.qty = int(pricing.qty)
                plan.status = "SUBMITTED"
                plan.max_loss = float(pricing.max_loss_per_spread) * int(pricing.qty)
                plan.signal_reason = (
                    f"{plan.signal_reason}; entry={pricing.entry_price:.2f} "
                    f"limit={pricing.alpaca_limit_price:.2f} bp={pricing.buying_power:.2f} "
                    f"{pricing.reason}"
                )
                print(
                    f"[C] pricing {code}: entry={pricing.entry_price:.2f} "
                    f"limit={pricing.alpaca_limit_price:.2f} "
                    f"risk_per_spread={pricing.max_loss_per_spread:.2f} "
                    f"qty={pricing.qty} bp={pricing.buying_power:.2f}",
                    flush=True,
                )
            except Exception as e:
                print(f"[C] {code} real order blocked before submit: {e}", flush=True)
                _update_ops_fields(
                    conn,
                    code,
                    can_buy=0,
                    last_order_side="buy",
                    last_order_intent=_intent_short(f"C:BLOCK {str(e)[:60]}"),
                    last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
                return False

        spread_id = None
        if C_RECORD_PLAN == 1:
            try:
                spread_id = record_spread_plan(plan)
                if spread_id:
                    print(f"[C] recorded spread_id={spread_id}", flush=True)
                    if pricing is not None:
                        _update_spread_existing_fields(
                            conn,
                            spread_id,
                            entry_price=float(pricing.entry_price),
                            qty=int(pricing.qty),
                            max_loss=float(pricing.max_loss_per_spread) * int(pricing.qty),
                            status="SUBMITTED",
                        )
                    _update_ops_fields(
                        conn,
                        code,
                        can_buy=0,
                        can_sell=0,
                        is_bought=0,
                        last_order_side="buy",
                        last_order_intent=_intent_short(f"C:PLAN spread_id={spread_id} {mode}"),
                        last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    )
            except Exception as e:
                print(f"[C] record plan failed: {e}", flush=True)
                return False

        if C_ENABLE_REAL_ORDER != 1:
            print("[C] dry-run planner only: real option order disabled", flush=True)
            return False

        try:
            order = submit_open_spread_order(plan, pricing)
            order_id = getattr(order, "id", None) or getattr(order, "order_id", None)
            order_status = str(getattr(order, "status", "") or "")
            print(
                f"[C] real order submitted {code}: id={order_id} status={order_status} "
                f"mode={mode} qty={pricing.qty} limit={pricing.alpaca_limit_price:.2f}",
                flush=True,
            )

            if spread_id:
                _update_spread_existing_fields(
                    conn,
                    spread_id,
                    status="SUBMITTED",
                    order_id=str(order_id or ""),
                    entry_price=float(pricing.entry_price),
                    current_value=float(pricing.entry_price),
                    qty=int(pricing.qty),
                    max_loss=float(pricing.max_loss_per_spread) * int(pricing.qty),
                )
            _update_ops_fields(
                conn,
                code,
                can_buy=0,
                can_sell=1,
                is_bought=1,
                qty=int(pricing.qty),
                cost_price=float(pricing.entry_price),
                last_order_side="buy",
                last_order_id=str(order_id or ""),
                last_order_intent=_intent_short(f"C:OPEN {mode} qty={pricing.qty} limit={pricing.alpaca_limit_price:.2f}"),
                last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            return True

        except Exception as e:
            print(f"[C] real order submit failed {code}: {e}", flush=True)
            if spread_id:
                _update_spread_existing_fields(conn, spread_id, status="FAILED", close_reason=str(e)[:500])
            _update_ops_fields(
                conn,
                code,
                can_buy=0,
                last_order_side="buy",
                last_order_intent=_intent_short(f"C:ORDER_FAIL {str(e)[:60]}"),
                last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            return False

    except Exception as e:
        print(f"[C ERROR] {code}: {e}", flush=True)
        traceback.print_exc()
        return False
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


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

            should_close, reason, metric = should_close_spread(spread, current_value, legs)
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

            if C_ENABLE_REAL_ORDER == 1:
                try:
                    order = submit_close_spread_order(spread, legs, current_value)
                    order_id = getattr(order, "id", None) or getattr(order, "order_id", None)
                    order_status = str(getattr(order, "status", "") or "")
                    print(
                        f"[C SELL] real close submitted spread_id={spread_id} "
                        f"order_id={order_id} status={order_status}",
                        flush=True,
                    )
                    _update_spread_existing_fields(
                        conn,
                        spread_id,
                        status="CLOSE_SUBMITTED",
                        close_order_id=str(order_id or ""),
                        exit_price=float(current_value),
                        close_reason=str(reason)[:500],
                        profit=float(metric.get("profit") or 0.0),
                        profit_pct=float(metric.get("profit_pct") or 0.0),
                    )
                    planned_any = True
                    continue
                except Exception as e:
                    print(f"[C SELL] real close submit failed spread_id={spread_id}: {e}", flush=True)
                    _update_spread_existing_fields(
                        conn,
                        spread_id,
                        close_reason=f"CLOSE_ORDER_FAIL {str(e)[:450]}",
                    )
                    continue

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

        return planned_any

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
