from __future__ import annotations

"""动态风控控制器：根据亏损、连亏、回撤、市场状态计算开仓风险和仓位建议。"""

from dataclasses import dataclass

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
    block_b: bool = False
    block_d: bool = False
    suggest_mode: str | None = None
    reason: str | None = None
    market_trend: str = "横盘"
    qqq_change_pct: float = 0.0
    vix: float = 18.0
    risk_preference: str = "中性"
    allocation_mode: str = "动态分仓"
    recommended_exposure: float = 0.5
    recommended_weights: dict[str, float] | None = None


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


BASE_BUCKET_WEIGHTS = {
    "D": 0.10,  # 对冲
    "A": 0.20,  # 优选股
    "B": 0.30,  # 策略B
    "C": 0.40,  # 成长型
}


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    cleaned = {group: max(0.0, float(weights.get(group, 0.0))) for group in ("A", "B", "C", "D")}
    total = sum(cleaned.values())
    if total <= 0:
        return BASE_BUCKET_WEIGHTS.copy()
    return {group: value / total for group, value in cleaned.items()}


def _base_exposure_by_trend(trend: str) -> float:
    if trend == "向上":
        return 0.8
    if trend == "向下":
        return 0.2
    return 0.5


def _dynamic_weights_from_base(trend: str, vix: float) -> dict[str, float]:
    """把旧仓位助手的动态分仓规则映射成 A/B/C/D 权重。"""
    weights = BASE_BUCKET_WEIGHTS.copy()
    if vix > 28 or trend == "向下":
        weights["C"] -= 0.10
        weights["B"] -= 0.05
        weights["A"] -= 0.10
        weights["D"] += 0.25
    elif trend == "横盘" or 18 <= vix <= 25:
        weights["A"] -= 0.05
        weights["D"] += 0.05
    elif trend == "向上" and vix < 18:
        weights["C"] += 0.05
        weights["D"] -= 0.05
    return _normalize_weights(weights)


def _tilt_by_risk(weights: dict[str, float], risk_preference: str) -> dict[str, float]:
    """风险偏好微调：保守把优选股让给成长型，激进把成长型让给优选股。"""
    weights = weights.copy()
    shift = 0.05
    if risk_preference == "保守":
        delta = min(shift, weights.get("A", 0.0))
        weights["A"] -= delta
        weights["C"] += delta
    elif risk_preference == "激进":
        delta = min(shift, weights.get("C", 0.0))
        weights["C"] -= delta
        weights["A"] += delta
    return _normalize_weights(weights)


def recommend_position(trend: str, qqq_change_pct: float, vix: float, risk_preference: str, allocation_mode: str) -> tuple[float, dict[str, float]]:
    """返回建议总仓位和 A/B/C/D 建议占比。"""
    exposure = _base_exposure_by_trend(trend)
    exposure += _clamp(qqq_change_pct / 20.0, -0.1, 0.1)
    if vix <= 14:
        exposure += 0.1
    elif vix <= 20:
        exposure += 0.0
    elif vix <= 28:
        exposure -= 0.1
    else:
        exposure -= 0.2

    if risk_preference == "保守":
        exposure -= 0.1
    elif risk_preference == "激进":
        exposure += 0.1
    exposure = _clamp(exposure, 0.0, 1.0)

    if allocation_mode == "平均分仓":
        weights = BASE_BUCKET_WEIGHTS.copy()
    else:
        weights = _dynamic_weights_from_base(trend, vix)
    return exposure, _tilt_by_risk(weights, risk_preference)


def _fetch_market_inputs() -> tuple[str, float, float]:
    """读取市场输入；环境变量优先，yfinance 失败时用稳健默认值。"""
    qqq_change_pct = env_float("RISK_QQQ_CHANGE_PCT", 0.0)
    vix = env_float("RISK_VIX", 18.0)
    if not env_bool("RISK_USE_YFINANCE", True):
        trend = env_str("RISK_MARKET_TREND", "")
        if trend not in {"向上", "横盘", "向下"}:
            if qqq_change_pct > 0.3:
                trend = "向上"
            elif qqq_change_pct < -0.3:
                trend = "向下"
            else:
                trend = "横盘"
        return trend, qqq_change_pct, vix

    try:
        import yfinance as yf

        qqq_hist = yf.Ticker("QQQ").history(period="2d")["Close"]
        if len(qqq_hist) >= 2:
            prev = float(qqq_hist.iloc[0])
            last = float(qqq_hist.iloc[-1])
            if prev > 0:
                qqq_change_pct = (last - prev) / prev * 100.0
        vix_hist = yf.Ticker("^VIX").history(period="1d")["Close"]
        if len(vix_hist) > 0:
            vix = float(vix_hist.iloc[-1])
    except Exception as exc:
        print(f"[RISK MARKET] yfinance unavailable, fallback env/defaults: {exc}", flush=True)

    trend = env_str("RISK_MARKET_TREND", "")
    if trend not in {"向上", "横盘", "向下"}:
        if qqq_change_pct > 0.3:
            trend = "向上"
        elif qqq_change_pct < -0.3:
            trend = "向下"
        else:
            trend = "横盘"
    return trend, qqq_change_pct, vix


def get_risk_state() -> RiskState:
    """计算当前风险状态、市场风险输入和仓位建议。"""
    s = settings()
    enabled = s.enable_risk_controller
    daily_pnl_pct = 0.0
    loss_days = 0
    max_drawdown = 0.0
    multiplier = _clamp(s.default_risk_multiplier, s.min_risk_multiplier, s.max_risk_multiplier)

    from .config import env_int

    daily_pnl_pct = env_float("RISK_DAILY_PNL_PCT", 0.0)
    loss_days = env_int("RISK_LOSS_DAYS", 0)
    max_drawdown = env_float("RISK_MAX_DRAWDOWN_PCT", 0.0)
    market_trend, qqq_change_pct, vix = _fetch_market_inputs()
    risk_preference = env_str("RISK_PREFERENCE", "中性")
    if risk_preference not in {"保守", "中性", "激进"}:
        risk_preference = "中性"
    allocation_mode = env_str("RISK_ALLOCATION_MODE", "动态分仓")
    if allocation_mode not in {"动态分仓", "平均分仓"}:
        allocation_mode = "动态分仓"
    recommended_exposure, recommended_weights = recommend_position(
        market_trend,
        qqq_change_pct,
        vix,
        risk_preference,
        allocation_mode,
    )

    state = RiskState(
        enabled=enabled,
        mode=s.capital_mode,
        daily_pnl_pct=daily_pnl_pct,
        loss_days=loss_days,
        max_drawdown=max_drawdown,
        risk_multiplier=multiplier,
        market_trend=market_trend,
        qqq_change_pct=qqq_change_pct,
        vix=vix,
        risk_preference=risk_preference,
        allocation_mode=allocation_mode,
        recommended_exposure=recommended_exposure,
        recommended_weights=recommended_weights,
    )
    if not enabled:
        return state

    if daily_pnl_pct <= -abs(s.daily_loss_limit_pct):
        state.block_all_new = True
        state.block_b = True
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

    if s.capital_mode == "SAFE":
        state.block_d = True
    if s.capital_mode == "RISK_OFF":
        state.block_b = True
        state.block_d = True
        state.risk_multiplier = 0.0
    return state


def can_open(strategy_group: str) -> tuple[bool, str]:
    """下单前风控检查：触发保护时返回 False 和原因。"""
    s = settings()
    if not s.enable_risk_controller:
        return True, "risk_controller_disabled"
    try:
        state = get_risk_state()
        group = (strategy_group or "").upper()
        if state.block_all_new:
            print(f"[RISK BLOCK] reason={state.reason} strategy={group}", flush=True)
            return False, state.reason or "block_all_new"
        if group == "B" and state.block_b:
            print(f"[RISK BLOCK] reason={state.reason or 'block_b'} strategy=B", flush=True)
            return False, state.reason or "block_b"
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
    print(f"[RISK] daily_pnl_pct={state.daily_pnl_pct:.2%}", flush=True)
    print(f"[RISK] loss_days={state.loss_days}", flush=True)
    print(f"[RISK] max_drawdown={state.max_drawdown:.2%}", flush=True)
    print(f"[RISK] risk_multiplier={state.risk_multiplier:.2f}", flush=True)
    print(
        f"[RISK MARKET] trend={state.market_trend} qqq={state.qqq_change_pct:.2f}% "
        f"vix={state.vix:.2f} exposure={state.recommended_exposure:.2%}",
        flush=True,
    )
    print(f"[RISK] mode={state.mode}", flush=True)
    if state.suggest_mode:
        print(f"[RISK MODE] suggest={state.suggest_mode} reason={state.reason}", flush=True)
    return state
