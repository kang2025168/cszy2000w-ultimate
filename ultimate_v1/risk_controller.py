from __future__ import annotations

"""动态风控控制器：根据亏损、连亏、回撤等条件限制新开仓。"""

from dataclasses import dataclass

from .config import settings


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


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def get_risk_state() -> RiskState:
    """计算当前风险状态；第一阶段主要限制 B/D 和所有新开仓。"""
    s = settings()
    enabled = s.enable_risk_controller
    daily_pnl_pct = 0.0
    loss_days = 0
    max_drawdown = 0.0
    multiplier = _clamp(s.default_risk_multiplier, s.min_risk_multiplier, s.max_risk_multiplier)

    # V1 第一阶段先从环境变量读取风险指标；后续可以接账户历史、VIX、SPY/QQQ 趋势等数据源。
    from .config import env_float, env_int

    daily_pnl_pct = env_float("RISK_DAILY_PNL_PCT", 0.0)
    loss_days = env_int("RISK_LOSS_DAYS", 0)
    max_drawdown = env_float("RISK_MAX_DRAWDOWN_PCT", 0.0)

    state = RiskState(
        enabled=enabled,
        mode=s.capital_mode,
        daily_pnl_pct=daily_pnl_pct,
        loss_days=loss_days,
        max_drawdown=max_drawdown,
        risk_multiplier=multiplier,
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
    print(f"[RISK] mode={state.mode}", flush=True)
    if state.suggest_mode:
        print(f"[RISK MODE] suggest={state.suggest_mode} reason={state.reason}", flush=True)
    return state
