from __future__ import annotations

"""
动态风控控制器 v2。

A/C/B 使用本金池并归一化到 100%：
- A = 大盘指数类
- C = 成长股
- B = 策略B

D 是日内测试策略，单独使用保证金额度池，不参与 A/C/B 本金归一化。
"""

import json
import os
from dataclasses import dataclass
from datetime import date, datetime
from urllib.parse import quote
from urllib.request import Request, urlopen

from .config import env_bool, env_float, env_str, settings


@dataclass
class RiskState:
    enabled: bool
    mode: str

    daily_pnl_pct: float
    loss_days: int
    max_drawdown: float

    risk_multiplier: float

    block_all_new: bool = False
    block_a: bool = False
    block_b: bool = False
    block_c: bool = False
    block_d: bool = False

    suggest_mode: str | None = None
    reason: str | None = None

    market_trend: str = "横盘"
    market_reason: str = ""
    qqq_change_pct: float = 0.0
    vix: float = 18.0

    risk_preference: str = "中性"
    allocation_mode: str = "动态分仓"

    recommended_exposure: float = 0.6
    recommended_weights: dict[str, float] | None = None

    account_metrics_source: str = "未知"
    vix_source: str = "未知"


BASE_BUCKET_WEIGHTS = {
    "A": 0.35,
    "C": 0.35,
    "B": 0.30,
    "D": 0.30,
}

CAPITAL_MODE_LABELS = {
    "NORMAL": "正常",
    "SAFE": "保守",
    "ATTACK": "进攻",
    "RISK_OFF": "避险",
}


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _risk_setting(name: str, default: str = "") -> str:
    """读取网页写入的风险设置；环境变量仍然拥有最高优先级。"""
    env_value = env_str(name, "")
    if env_value:
        return env_value
    try:
        from .db import fetch_one

        row = fetch_one("SELECT setting_value FROM app_settings WHERE setting_key=%s LIMIT 1", (name,))
        if row and row.get("setting_value") is not None:
            return str(row.get("setting_value") or "").strip()
    except Exception:
        pass
    return default


def _env_float_if_set(name: str) -> float | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    """A/C/B 归一化成本金池 100%，D 独立表示保证金额度最多使用比例。"""
    principal_groups = ("A", "C", "B")
    cleaned = {group: max(0.0, float(weights.get(group, 0.0))) for group in principal_groups}
    total = sum(cleaned.values())
    if total <= 0:
        return {
            "A": 0.35,
            "C": 0.35,
            "B": 0.30,
            "D": max(0.0, float(weights.get("D", 0.30))),
        }
    result = {group: cleaned[group] / total for group in principal_groups}
    result["D"] = max(0.0, float(weights.get("D", 0.30)))
    return result


def _calc_market_trend_from_daily(qqq_close_list: list[float]) -> tuple[str, str, float]:
    closes = []
    for value in qqq_close_list:
        try:
            close = float(value)
            if close > 0:
                closes.append(close)
        except Exception:
            continue

    if len(closes) < 20:
        return "横盘", "QQQ 日线数据不足 20 天，默认横盘", 0.0

    last = closes[-1]
    close_20_ago = closes[-20]
    ma5 = sum(closes[-5:]) / 5
    ma10 = sum(closes[-10:]) / 10
    ma20 = sum(closes[-20:]) / 20
    ret20 = (last - close_20_ago) / close_20_ago * 100.0

    if last > ma20 and ma5 > ma10 > ma20 and ret20 >= 3.0:
        return (
            "向上",
            f"QQQ 日线上涨：close={last:.2f} > MA20={ma20:.2f}, MA5={ma5:.2f} > MA10={ma10:.2f} > MA20={ma20:.2f}, ret20={ret20:.2f}%",
            ret20,
        )

    if last < ma20 and ma5 < ma10 < ma20 and ret20 <= -3.0:
        return (
            "向下",
            f"QQQ 日线下跌：close={last:.2f} < MA20={ma20:.2f}, MA5={ma5:.2f} < MA10={ma10:.2f} < MA20={ma20:.2f}, ret20={ret20:.2f}%",
            ret20,
        )

    return (
        "横盘",
        f"QQQ 日线横盘：close={last:.2f}, MA5={ma5:.2f}, MA10={ma10:.2f}, MA20={ma20:.2f}, ret20={ret20:.2f}%",
        ret20,
    )


def _fetch_vix_value() -> tuple[float, str]:
    """优先拉实时/延迟行情 VIX，失败后才使用 RISK_VIX。"""
    env_vix = _env_float_if_set("RISK_VIX")
    if env_str("RISK_VIX_SOURCE", "").lower() == "env" and env_vix is not None:
        return env_vix, "环境变量"

    if env_bool("RISK_USE_YAHOO_VIX", True):
        try:
            symbol = quote("^VIX", safe="")
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=5d&interval=1d"
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=5) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            result = ((payload.get("chart") or {}).get("result") or [None])[0] or {}
            meta = result.get("meta") or {}
            value = _safe_float(meta.get("regularMarketPrice"), 0.0)
            if value <= 0:
                closes = (((result.get("indicators") or {}).get("quote") or [{}])[0] or {}).get("close") or []
                valid = [_safe_float(x, 0.0) for x in closes if _safe_float(x, 0.0) > 0]
                value = valid[-1] if valid else 0.0
            if value > 0:
                return value, "Yahoo实时/延迟"
        except Exception as exc:
            print(f"[RISK VIX] Yahoo unavailable, fallback: {exc}", flush=True)

    if env_bool("RISK_USE_YFINANCE_VIX", False):
        try:
            import yfinance as yf

            vix_hist = yf.Ticker("^VIX").history(period="5d")["Close"]
            if len(vix_hist) > 0:
                return float(vix_hist.iloc[-1]), "yfinance"
        except Exception as exc:
            print(f"[RISK VIX] yfinance unavailable, fallback: {exc}", flush=True)

    return env_float("RISK_VIX", 18.0), "环境变量兜底"


def _fetch_market_inputs() -> tuple[str, str, float, float, str]:
    qqq_change_pct = env_float("RISK_QQQ_CHANGE_PCT", 0.0)
    vix, vix_source = _fetch_vix_value()

    manual_trend = _risk_setting("RISK_MARKET_TREND", "")
    if manual_trend in {"向上", "横盘", "向下"}:
        return manual_trend, f"使用手动市场趋势 RISK_MARKET_TREND={manual_trend}", qqq_change_pct, vix, vix_source

    if env_bool("RISK_USE_ALPACA_MARKET_DATA", True):
        try:
            from . import alpaca_gateway

            closes = alpaca_gateway.get_daily_closes("QQQ", days=60, feed=env_str("RISK_ALPACA_DATA_FEED", "iex"))
            trend, reason, _ret20 = _calc_market_trend_from_daily(closes)
            if len(closes) >= 2 and closes[-2] > 0:
                qqq_change_pct = (closes[-1] - closes[-2]) / closes[-2] * 100.0
            return trend, reason, qqq_change_pct, vix, vix_source
        except Exception as exc:
            print(f"[RISK MARKET] Alpaca market data unavailable, fallback: {exc}", flush=True)

    if not env_bool("RISK_USE_YFINANCE", True):
        if qqq_change_pct >= 3.0:
            trend = "向上"
        elif qqq_change_pct <= -3.0:
            trend = "向下"
        else:
            trend = "横盘"
        return trend, f"未启用 yfinance，使用 RISK_QQQ_CHANGE_PCT={qqq_change_pct:.2f}% 判断 trend={trend}", qqq_change_pct, vix, vix_source

    try:
        import yfinance as yf

        qqq_hist = yf.Ticker("QQQ").history(period="60d")["Close"]
        closes = [float(x) for x in qqq_hist.tolist()]
        trend, reason, _ret20 = _calc_market_trend_from_daily(closes)
        if len(closes) >= 2 and closes[-2] > 0:
            qqq_change_pct = (closes[-1] - closes[-2]) / closes[-2] * 100.0

        print(f"[RISK MARKET TREND] {reason}", flush=True)
        return trend, reason, qqq_change_pct, vix, vix_source
    except Exception as exc:
        print(f"[RISK MARKET] yfinance unavailable, fallback env/defaults: {exc}", flush=True)
        if qqq_change_pct >= 3.0:
            trend = "向上"
        elif qqq_change_pct <= -3.0:
            trend = "向下"
        else:
            trend = "横盘"
        return trend, f"yfinance 失败，使用环境变量判断：RISK_QQQ_CHANGE_PCT={qqq_change_pct:.2f}%, trend={trend}", qqq_change_pct, vix, vix_source


def _daily_equity_rows() -> list[tuple[date, float]]:
    """读取每天最后一条账户快照，用于日亏、连亏、回撤。"""
    try:
        from .db import fetch_all

        rows = fetch_all(
            """
            SELECT DATE(s.created_at) AS snapshot_date, s.equity
            FROM account_equity_snapshots s
            JOIN (
                SELECT DATE(created_at) AS d, MAX(created_at) AS max_created_at
                FROM account_equity_snapshots
                GROUP BY DATE(created_at)
            ) latest
              ON DATE(s.created_at)=latest.d AND s.created_at=latest.max_created_at
            ORDER BY s.created_at
            LIMIT 5000
            """
        )
    except Exception as exc:
        print(f"[RISK ACCOUNT] cannot read equity snapshots: {exc}", flush=True)
        return []

    result: list[tuple[date, float]] = []
    for row in rows:
        raw_day = row.get("snapshot_date")
        if isinstance(raw_day, datetime):
            day = raw_day.date()
        elif isinstance(raw_day, date):
            day = raw_day
        else:
            try:
                day = datetime.fromisoformat(str(raw_day)).date()
            except Exception:
                continue
        equity = _safe_float(row.get("equity"), 0.0)
        if equity > 0:
            result.append((day, equity))
    return result


def _latest_snapshot_equity() -> float | None:
    try:
        from .db import fetch_one

        row = fetch_one("SELECT equity FROM account_equity_snapshots ORDER BY created_at DESC LIMIT 1")
        if row:
            equity = _safe_float(row.get("equity"), 0.0)
            if equity > 0:
                return equity
    except Exception as exc:
        print(f"[RISK ACCOUNT] cannot read latest equity snapshot: {exc}", flush=True)
    return None


def _current_account_equity() -> tuple[float | None, str]:
    if env_bool("RISK_USE_LIVE_ACCOUNT_METRICS", True):
        try:
            from . import alpaca_gateway

            snap = alpaca_gateway.get_account_snapshot()
            if snap and snap.equity > 0:
                return snap.equity, "Alpaca实时账户"
        except Exception as exc:
            print(f"[RISK ACCOUNT] live account unavailable: {exc}", flush=True)

    latest = _latest_snapshot_equity()
    if latest is not None:
        return latest, "数据库最新快照"
    return None, "环境变量兜底"


def _fetch_account_risk_metrics() -> tuple[float, int, float, str]:
    """自动计算日亏、连续亏损天数、最大回撤。"""
    if env_str("RISK_ACCOUNT_METRICS_SOURCE", "").lower() == "env":
        from .config import env_int

        return (
            env_float("RISK_DAILY_PNL_PCT", 0.0),
            env_int("RISK_LOSS_DAYS", 0),
            env_float("RISK_MAX_DRAWDOWN_PCT", 0.0),
            "环境变量",
        )

    current_equity, source = _current_account_equity()
    rows = _daily_equity_rows()
    if current_equity is None:
        from .config import env_int

        return (
            env_float("RISK_DAILY_PNL_PCT", 0.0),
            env_int("RISK_LOSS_DAYS", 0),
            env_float("RISK_MAX_DRAWDOWN_PCT", 0.0),
            source,
        )

    today = date.today()
    if rows:
        if rows[-1][0] == today:
            rows[-1] = (today, current_equity)
        else:
            rows.append((today, current_equity))
    else:
        rows = [(today, current_equity)]

    previous_equity = None
    for day, equity in reversed(rows[:-1]):
        if day < rows[-1][0] and equity > 0:
            previous_equity = equity
            break

    daily_pnl_pct = 0.0
    if previous_equity and previous_equity > 0:
        daily_pnl_pct = (current_equity - previous_equity) / previous_equity

    loss_days = 0
    for idx in range(len(rows) - 1, 0, -1):
        if rows[idx][1] < rows[idx - 1][1]:
            loss_days += 1
        else:
            break

    peak_equity = max((equity for _day, equity in rows), default=current_equity)
    max_drawdown = (peak_equity - current_equity) / peak_equity if peak_equity > 0 else 0.0
    return daily_pnl_pct, loss_days, max(0.0, max_drawdown), source


def _base_exposure_by_trend(trend: str) -> float:
    if trend == "向上":
        return 0.90
    if trend == "向下":
        return 0.30
    return 0.60


def _dynamic_weights_from_base(trend: str, vix: float) -> dict[str, float]:
    weights = BASE_BUCKET_WEIGHTS.copy()
    if trend == "向上" and vix < 20:
        weights.update({"A": 0.30, "C": 0.40, "B": 0.30, "D": 0.30})
    elif trend == "横盘" or 20 <= vix <= 28:
        weights.update({"A": 0.40, "C": 0.35, "B": 0.25, "D": 0.20})
    elif trend == "向下" or vix > 28:
        weights.update({"A": 0.60, "C": 0.25, "B": 0.15, "D": 0.00})
    return _normalize_weights(weights)


def _tilt_by_risk(weights: dict[str, float], risk_preference: str) -> dict[str, float]:
    weights = weights.copy()
    if risk_preference == "保守":
        weights["A"] += 0.10
        weights["C"] -= 0.05
        weights["B"] -= 0.05
        weights["D"] = min(weights.get("D", 0.30), 0.15)
    elif risk_preference == "激进":
        weights["A"] -= 0.10
        weights["C"] += 0.05
        weights["B"] += 0.05
        weights["D"] = max(weights.get("D", 0.30), 0.30)
    return _normalize_weights(weights)


def recommend_position(
    trend: str,
    qqq_change_pct: float,
    vix: float,
    risk_preference: str,
    allocation_mode: str,
) -> tuple[float, dict[str, float]]:
    exposure = _base_exposure_by_trend(trend)
    if vix <= 14:
        exposure += 0.05
    elif vix <= 20:
        exposure += 0.0
    elif vix <= 28:
        exposure -= 0.10
    else:
        exposure -= 0.20

    if risk_preference == "保守":
        exposure -= 0.10
    elif risk_preference == "激进":
        exposure += 0.10

    exposure = _clamp(exposure, 0.0, 1.0)
    if allocation_mode == "固定分仓":
        weights = BASE_BUCKET_WEIGHTS.copy()
    else:
        weights = _dynamic_weights_from_base(trend, vix)
    return exposure, _tilt_by_risk(weights, risk_preference)


def _realtime_risk_multiplier(
    market_trend: str,
    vix: float,
    daily_pnl_pct: float,
    loss_days: int,
    max_drawdown: float,
) -> float:
    """根据实时风控输入自动调整风险倍率。"""
    s = settings()
    multiplier = _clamp(s.default_risk_multiplier, s.min_risk_multiplier, s.max_risk_multiplier)

    if daily_pnl_pct <= -abs(s.daily_loss_limit_pct):
        return 0.0
    if max_drawdown >= abs(s.max_drawdown_pct):
        return 0.0
    if market_trend == "向下" and vix > 28:
        return 0.0

    if loss_days >= s.max_loss_days:
        multiplier = min(multiplier, 0.50)
    elif loss_days > 0:
        multiplier = min(multiplier, 0.75)

    if max_drawdown >= abs(s.max_drawdown_pct) * 0.75:
        multiplier = min(multiplier, 0.50)
    elif max_drawdown >= abs(s.max_drawdown_pct) * 0.50:
        multiplier = min(multiplier, 0.70)

    if daily_pnl_pct < 0:
        multiplier = min(multiplier, 0.80)

    if vix > 28:
        multiplier = min(multiplier, 0.30)
    elif vix >= 25:
        multiplier = min(multiplier, 0.60)
    elif vix >= 20:
        multiplier = min(multiplier, 0.80)

    if market_trend == "向下":
        multiplier = min(multiplier, 0.50)
    elif market_trend == "横盘":
        multiplier = min(multiplier, 0.85)

    return _clamp(multiplier, s.min_risk_multiplier, s.max_risk_multiplier)


def _auto_capital_mode(
    market_trend: str,
    vix: float,
    daily_pnl_pct: float,
    loss_days: int,
    max_drawdown: float,
    risk_multiplier: float,
) -> tuple[str, str]:
    """自动判定当前有效资金模式。"""
    s = settings()
    if daily_pnl_pct <= -abs(s.daily_loss_limit_pct):
        return "RISK_OFF", "当日亏损达到限制"
    if max_drawdown >= abs(s.max_drawdown_pct):
        return "RISK_OFF", "最大回撤达到限制"
    if market_trend == "向下" and vix > 28:
        return "RISK_OFF", "QQQ 向下且 VIX 高于 28"
    if loss_days >= s.max_loss_days:
        return "SAFE", "连续亏损天数达到限制"
    if market_trend == "横盘" and vix >= 20:
        return "SAFE", "横盘且 VIX 偏高"
    if risk_multiplier >= 1.0 and market_trend == "向上" and vix < 20 and daily_pnl_pct >= 0 and max_drawdown < abs(s.max_drawdown_pct) * 0.5:
        return "ATTACK", "QQQ 向上、VIX 低且风控正常"
    return "NORMAL", "默认正常模式"


def get_risk_state() -> RiskState:
    s = settings()

    daily_pnl_pct, loss_days, max_drawdown, account_metrics_source = _fetch_account_risk_metrics()
    market_trend, market_reason, qqq_change_pct, vix, vix_source = _fetch_market_inputs()
    multiplier = _realtime_risk_multiplier(market_trend, vix, daily_pnl_pct, loss_days, max_drawdown)
    risk_preference = _risk_setting("RISK_PREFERENCE", "中性")
    if risk_preference not in {"保守", "中性", "激进"}:
        risk_preference = "中性"
    allocation_mode = _risk_setting("RISK_ALLOCATION_MODE", "动态分仓")
    if allocation_mode not in {"动态分仓", "固定分仓"}:
        allocation_mode = "动态分仓"

    recommended_exposure, recommended_weights = recommend_position(
        market_trend,
        qqq_change_pct,
        vix,
        risk_preference,
        allocation_mode,
    )
    auto_mode, auto_mode_reason = _auto_capital_mode(
        market_trend,
        vix,
        daily_pnl_pct,
        loss_days,
        max_drawdown,
        multiplier,
    )

    state = RiskState(
        enabled=s.enable_risk_controller,
        mode=auto_mode,
        daily_pnl_pct=daily_pnl_pct,
        loss_days=loss_days,
        max_drawdown=max_drawdown,
        risk_multiplier=multiplier,
        market_trend=market_trend,
        market_reason=market_reason,
        qqq_change_pct=qqq_change_pct,
        vix=vix,
        risk_preference=risk_preference,
        allocation_mode=allocation_mode,
        recommended_exposure=recommended_exposure,
        recommended_weights=recommended_weights,
        account_metrics_source=account_metrics_source,
        vix_source=vix_source,
    )

    if not state.enabled:
        return state

    if daily_pnl_pct <= -abs(s.daily_loss_limit_pct):
        state.block_all_new = True
        state.block_a = True
        state.block_b = True
        state.block_c = True
        state.block_d = True
        state.risk_multiplier = 0.0
        state.reason = "daily_loss_limit"
    elif max_drawdown >= abs(s.max_drawdown_pct):
        state.block_b = True
        state.block_d = True
        state.risk_multiplier = 0.0
        state.suggest_mode = "RISK_OFF"
        state.reason = "max_drawdown"
    elif loss_days >= s.max_loss_days:
        state.block_d = True
        state.risk_multiplier = min(state.risk_multiplier, 0.5)
        state.suggest_mode = "SAFE"
        state.reason = "loss_days"

    if state.mode == "SAFE":
        state.block_d = True
    if state.mode == "RISK_OFF":
        state.block_b = True
        state.block_d = True
        state.risk_multiplier = 0.0

    if not state.reason:
        state.reason = auto_mode_reason

    return state


def can_open(strategy_group: str) -> tuple[bool, str]:
    s = settings()
    if not s.enable_risk_controller:
        return True, "risk_controller_disabled"

    try:
        state = get_risk_state()
        group = (strategy_group or "").upper().strip()
        if state.block_all_new:
            print(f"[RISK BLOCK] reason={state.reason} strategy={group}", flush=True)
            return False, state.reason or "block_all_new"
        if group == "A" and state.block_a:
            print(f"[RISK BLOCK] reason={state.reason or 'block_a'} strategy=A", flush=True)
            return False, state.reason or "block_a"
        if group == "B" and state.block_b:
            print(f"[RISK BLOCK] reason={state.reason or 'block_b'} strategy=B", flush=True)
            return False, state.reason or "block_b"
        if group == "C" and state.block_c:
            print(f"[RISK BLOCK] reason={state.reason or 'block_c'} strategy=C", flush=True)
            return False, state.reason or "block_c"
        if group == "D" and state.block_d:
            print(f"[RISK BLOCK] reason={state.reason or 'block_d'} strategy=D", flush=True)
            return False, state.reason or "block_d"
        return True, "allow"
    except Exception as exc:
        print(f"[RISK ERROR] cannot calculate risk: {exc}", flush=True)
        return False, "risk_calc_failed"


def log_risk_state() -> RiskState:
    state = get_risk_state()
    print(f"[RISK] enabled={1 if state.enabled else 0}", flush=True)
    print(f"[RISK] mode={state.mode}", flush=True)
    print(f"[RISK] mode_label={CAPITAL_MODE_LABELS.get(state.mode, state.mode)} reason={state.reason}", flush=True)
    print(f"[RISK] daily_pnl_pct={state.daily_pnl_pct:.2%}", flush=True)
    print(f"[RISK] loss_days={state.loss_days}", flush=True)
    print(f"[RISK] max_drawdown={state.max_drawdown:.2%}", flush=True)
    print(f"[RISK] account_metrics_source={state.account_metrics_source}", flush=True)
    print(f"[RISK] risk_multiplier={state.risk_multiplier:.2f}", flush=True)
    print(
        f"[RISK MARKET] trend={state.market_trend} qqq_day={state.qqq_change_pct:.2f}% "
        f"vix={state.vix:.2f} vix_source={state.vix_source} principal_exposure={state.recommended_exposure:.2%}",
        flush=True,
    )
    if state.market_reason:
        print(f"[RISK MARKET REASON] {state.market_reason}", flush=True)
    if state.recommended_weights:
        print(
            "[RISK WEIGHTS] "
            f"A指数={state.recommended_weights.get('A', 0.0):.2%} "
            f"C成长={state.recommended_weights.get('C', 0.0):.2%} "
            f"B策略B={state.recommended_weights.get('B', 0.0):.2%} "
            f"D日内保证金={state.recommended_weights.get('D', 0.0):.2%}",
            flush=True,
        )
    print(
        f"[RISK BLOCKS] all={int(state.block_all_new)} A={int(state.block_a)} "
        f"C={int(state.block_c)} B={int(state.block_b)} D={int(state.block_d)}",
        flush=True,
    )
    if state.suggest_mode:
        print(f"[RISK MODE] suggest={state.suggest_mode} reason={state.reason}", flush=True)
    return state
