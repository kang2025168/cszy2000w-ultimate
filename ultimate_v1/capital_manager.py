from __future__ import annotations

"""多策略资金池管理：计算 A/B/C/D 目标资金、已用资金和开仓许可。"""

import os
from dataclasses import dataclass
from datetime import date

from . import alpaca_gateway
from .account_config import load_account_config, profile_for_pool
from .config import env_float, settings
from .db import db_conn, fetch_all
from .risk_controller import get_risk_state
from .state_store import get_app_setting


@dataclass
class CapitalAllocation:
    mode: str
    allocation_month: date
    equity: float
    buying_power: float
    cash: float
    portfolio_value: float
    trading_blocked: bool
    account_blocked: bool
    trade_suspended_by_user: bool
    A_target: float
    B_target: float
    C_target: float
    D_target: float
    base_targets: dict[str, float]
    base_percents: dict[str, float]
    total_risk_percent: float
    pool_risk_percents: dict[str, float]
    pool_enabled: dict[str, bool]
    margin_usage_mode: str
    margin_usage_percent: float
    margin_usage_reason: str
    used: dict[str, float]
    available: dict[str, float]
    pool_brokers: dict[str, str]
    broker_snapshots: dict[str, dict]

    def target_for(self, strategy_group: str) -> float:
        return float(getattr(self, f"{strategy_group.upper()}_target", 0.0))


def get_account_snapshot():
    return alpaca_gateway.get_account_snapshot(profile=profile_for_pool("B"))


POOL_BROKERS = {
    "A": "retirement",
    "B": "trading",
    "C": "trading",
    "D": "trading",
}
POOL_GROUPS = ("A", "B", "C", "D")
BASE_POOL_WEIGHTS = {"A": 1.00, "B": 0.40, "C": 0.40, "D": 0.20}
DISABLED_POOL_TRANSFER = {"C": "B", "D": "C"}


def _manual_account_snapshot(prefix: str) -> alpaca_gateway.AccountSnapshot:
    """读取非 Alpaca 账户的手工资金快照。

    Fidelity / Webull 当前没有接入真实网关，先用环境变量隔离资金池。
    例如 FIDELITY_EQUITY、FIDELITY_BUYING_POWER、FIDELITY_CASH。
    """
    name = prefix.upper()
    equity = env_float(f"{name}_EQUITY", 0.0)
    portfolio_value = env_float(f"{name}_PORTFOLIO_VALUE", equity)
    cash = env_float(f"{name}_CASH", equity)
    buying_power = env_float(f"{name}_BUYING_POWER", cash)
    return alpaca_gateway.AccountSnapshot(
        equity=max(0.0, equity),
        buying_power=max(0.0, buying_power),
        cash=cash,
        portfolio_value=max(0.0, portfolio_value),
    )


def get_broker_account_snapshots() -> dict[str, alpaca_gateway.AccountSnapshot]:
    """按账户 profile 读取资金快照；A/B/C/D 不互相借用余额。"""
    config = load_account_config()
    names = set((config.get("pool_profiles") or {}).values()) | {profile_for_pool("B", config)}
    snaps: dict[str, alpaca_gateway.AccountSnapshot] = {}
    for name in sorted(n for n in names if n):
        snap = alpaca_gateway.get_account_snapshot(profile=name)
        if snap is None:
            snap = alpaca_gateway.AccountSnapshot(0.0, 0.0, 0.0, 0.0, account_blocked=True, trading_blocked=True)
        snaps[name] = snap
    return snaps


def _aggregate_account_snapshot(snaps: dict[str, alpaca_gateway.AccountSnapshot]) -> alpaca_gateway.AccountSnapshot:
    """合并展示用账户快照，交易阻断状态只继承 Alpaca 实盘通道。"""
    alpaca = snaps.get(profile_for_pool("B")) or alpaca_gateway.AccountSnapshot(0.0, 0.0, 0.0, 0.0)
    equity = sum(float(s.equity or 0) for s in snaps.values())
    buying_power = sum(float(s.buying_power or 0) for s in snaps.values())
    cash = sum(float(s.cash or 0) for s in snaps.values())
    portfolio_value = sum(float(s.portfolio_value or 0) for s in snaps.values())
    return alpaca_gateway.AccountSnapshot(
        equity=equity,
        buying_power=buying_power,
        cash=cash,
        portfolio_value=portfolio_value,
        trading_blocked=alpaca.trading_blocked,
        account_blocked=alpaca.account_blocked,
        trade_suspended_by_user=alpaca.trade_suspended_by_user,
        pattern_day_trader=alpaca.pattern_day_trader,
        daytrade_count=alpaca.daytrade_count,
    )


def _snapshot_payload(snap: alpaca_gateway.AccountSnapshot) -> dict:
    return {
        "equity": snap.equity,
        "buying_power": snap.buying_power,
        "cash": snap.cash,
        "portfolio_value": snap.portfolio_value,
        "trading_blocked": snap.trading_blocked,
        "account_blocked": snap.account_blocked,
        "trade_suspended_by_user": snap.trade_suspended_by_user,
    }


def _month_start(today: date | None = None) -> date:
    """资金池按月管理，每月第一天作为分配月份。"""
    today = today or date.today()
    return date(today.year, today.month, 1)


def _setting_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None:
        raw = get_app_setting(key, str(default))
    try:
        value = float(raw)
        if value > 10:
            value = value / 100.0
        return value
    except Exception:
        return default


def _setting_bool(key: str, default: bool = True) -> bool:
    raw = os.getenv(key)
    if raw is None:
        raw = get_app_setting(key, "1" if default else "0")
    return str(raw).strip().lower() in {"1", "true", "yes", "on", "y"}


def pool_enabled_settings() -> dict[str, bool]:
    enabled = {group: _setting_bool(f"RISK_{group}_POOL_ENABLED", True) for group in POOL_GROUPS}
    if not any(enabled.values()):
        enabled["B"] = True
    return enabled


def _auto_margin_usage_pct(risk) -> tuple[float, str]:
    """按市场环境给出 100%-150% 的自动保证金额度。"""
    trend = str(getattr(risk, "market_trend", "") or "")
    vix = float(getattr(risk, "vix", 0.0) or 0.0)
    qqq_change = float(getattr(risk, "qqq_change_pct", 0.0) or 0.0)
    loss_days = int(getattr(risk, "loss_days", 0) or 0)
    max_drawdown = float(getattr(risk, "max_drawdown", 0.0) or 0.0)
    block_all = bool(getattr(risk, "block_all", False))
    risk_preference = str(getattr(risk, "risk_preference", "") or "中性")

    if block_all or trend == "向下" or vix >= 28 or loss_days >= 2 or max_drawdown >= 0.10:
        value, reason = 1.0, "防守：向下/VIX高/连续亏损/回撤扩大，额度 100%"
    elif trend == "向上" and vix < 16 and qqq_change >= 0:
        value, reason = 1.5, "进攻：向上且 VIX<16，额度 150%"
    elif trend == "向上" and vix < 20:
        value, reason = 1.4, "偏强：向上且 VIX<20，额度 140%"
    elif trend == "横盘" and vix < 18:
        value, reason = 1.2, "中性偏强：横盘低波动，额度 120%"
    elif vix < 24:
        value, reason = 1.1, "中性：波动可控，额度 110%"
    else:
        value, reason = 1.0, "谨慎：波动升高，额度 100%"

    if risk_preference == "保守":
        capped = min(value, 1.1)
        if capped < value:
            return capped, f"{reason}；保守模式上限 110%"
        return capped, reason
    if risk_preference == "中性":
        capped = min(value, 1.3)
        if capped < value:
            return capped, f"{reason}；中性模式上限 130%"
        return capped, reason
    return value, reason


def resolve_margin_usage_pct(risk=None) -> tuple[str, float, str]:
    mode = str(get_app_setting("RISK_MARGIN_MODE", "AUTO") or "AUTO").upper()
    if mode == "AUTO":
        risk = risk or get_risk_state()
        value, reason = _auto_margin_usage_pct(risk)
        return "AUTO", max(1.0, min(1.5, value)), reason
    value = _setting_float("RISK_TOTAL_CAPITAL_PCT", 1.0)
    return "MANUAL", max(1.0, min(1.5, value)), "手动固定额度"


def _mode_weights(mode: str) -> tuple[dict[str, float], bool]:
    """读取当前资金模式的基础比例；A 独立，B/C/D 在原保证金账户内分配。"""
    try:
        risk = get_risk_state()
        if risk.recommended_weights:
            weights = {group: max(0.0, float(risk.recommended_weights.get(group, 0.0))) for group in ("A", "B", "C", "D")}
            principal_total = weights["A"] + weights["B"] + weights["C"]
            if principal_total > 0:
                weights["A"] /= principal_total
                weights["B"] /= principal_total
                weights["C"] /= principal_total
                return weights, weights.get("D", 0.0) > 0
    except Exception as exc:
        print(f"[CAPITAL WARN] dynamic weights unavailable, fallback mode weights: {exc}", flush=True)

    a, b, c, d = {
        "NORMAL": (1.00, 0.40, 0.40, 0.20),
        "SAFE": (1.00, 0.40, 0.40, 0.20),
        "ATTACK": (1.00, 0.60, 0.25, 0.15),
        "RISK_OFF": (1.00, 0.00, 0.80, 0.20),
    }.get(mode, (1.00, 0.40, 0.40, 0.20))
    allow_d = d > 0
    return {"A": a, "B": b, "C": c, "D": d}, allow_d


def _margin_usage_pct() -> float:
    """读取保证金总额度上限：100%-150%。"""
    return resolve_margin_usage_pct()[1]


def _market_exposure_pct(risk) -> float:
    """读取市场环境目标仓位：向上/VIX低 90%，横盘 80%，向下 35%。"""
    recommended = float(getattr(risk, "recommended_exposure", 0.0) or 0.0)
    if recommended > 0:
        return max(0.0, min(1.0, recommended))
    if risk.market_trend == "向上" and risk.vix < env_float("REBALANCE_LOW_VIX", 20.0):
        return env_float("REBALANCE_TARGET_UP", 0.90)
    if risk.market_trend == "向下":
        return env_float("REBALANCE_TARGET_DOWN", 0.35)
    return env_float("REBALANCE_TARGET_SIDEWAYS", 0.80)


def _risk_percents() -> tuple[float, dict[str, float]]:
    """计算 B/C/D 有效保证金额度；A 养老金现金账户不使用总杠杆。"""
    risk = get_risk_state()
    total_pct = resolve_margin_usage_pct(risk)[1] * _market_exposure_pct(risk)
    enabled = pool_enabled_settings()
    pool_pct = {
        group: 0.0 if not enabled[group] else max(0.0, min(1.0, _setting_float(f"RISK_{group}_POOL_PCT", 1.0)))
        for group in POOL_GROUPS
    }
    return total_pct, pool_pct


def _risk_target_for_group(group: str, base_target: float, total_pct: float, pool_pct: dict[str, float]) -> float:
    """A 独立且无杠杆；B/C/D 同源于保证金账户并使用相同风险系数。"""
    group = (group or "").upper()
    if group == "A":
        return base_target * pool_pct[group]
    return base_target * total_pct * pool_pct[group]


def _pool_base_percents() -> dict[str, float]:
    raw = {
        "A": _setting_float("A_ACCOUNT_CAPITAL_PCT", BASE_POOL_WEIGHTS["A"]),
        "B": _setting_float("B_ACCOUNT_CAPITAL_PCT", BASE_POOL_WEIGHTS["B"]),
        "C": _setting_float("C_ACCOUNT_CAPITAL_PCT", BASE_POOL_WEIGHTS["C"]),
        "D": _setting_float("D_ACCOUNT_CAPITAL_PCT", BASE_POOL_WEIGHTS["D"]),
    }
    enabled = pool_enabled_settings()
    adjusted = {"A": max(0.0, raw["A"]) if enabled.get("A") else 0.0, "B": 0.0, "C": 0.0, "D": 0.0}
    active = [group for group in ("B", "C", "D") if enabled[group]]
    if not active:
        adjusted["B"] = 1.0
        return adjusted

    def destination(group: str) -> str:
        seen = set()
        current = group
        while current not in seen:
            seen.add(current)
            if enabled.get(current):
                return current
            next_group = DISABLED_POOL_TRANSFER.get(current)
            if not next_group:
                break
            current = next_group
        active_total = sum(max(0.0, raw[g]) for g in active)
        if active_total <= 0:
            return active[0]
        return max(active, key=lambda g: raw[g])

    for group in ("B", "C", "D"):
        weight = raw[group]
        adjusted[destination(group)] += max(0.0, weight)
    total = sum(adjusted[group] for group in ("B", "C", "D"))
    if total > 0:
        for group in ("B", "C", "D"):
            adjusted[group] = adjusted[group] / total
    return adjusted


def _ensure_monthly_capital_pools(mode: str, snap, broker_snaps: dict[str, alpaca_gateway.AccountSnapshot]) -> date:
    """当月没有资金池记录时，按当月账户资金和模式比例写入一次。"""
    month = _month_start()
    weights, _allow_d = _mode_weights(mode)
    pool_base_percents = _pool_base_percents()

    def pool_snapshot(group: str) -> alpaca_gateway.AccountSnapshot:
        profile = profile_for_pool(group)
        return broker_snaps.get(profile) or broker_snaps.get(POOL_BROKERS[group]) or snap

    base_targets = {
        "A": pool_snapshot("A").equity * pool_base_percents["A"],
        "B": pool_snapshot("B").equity * pool_base_percents["B"],
        "C": pool_snapshot("C").equity * pool_base_percents["C"],
        "D": pool_snapshot("D").equity * pool_base_percents["D"],
    }
    total_pct, pool_pct = _risk_percents()
    with db_conn() as conn:
        with conn.cursor() as cur:
            for group in ("A", "B", "C", "D"):
                group_snap = pool_snapshot(group)
                risk_target = _risk_target_for_group(group, base_targets[group], total_pct, pool_pct)
                cur.execute(
                    """
                    INSERT IGNORE INTO capital_pools (
                        allocation_month, strategy_group, mode, base_percent,
                        base_target_capital, total_risk_percent, pool_risk_percent,
                        risk_target_capital, used_capital, available_capital,
                        used_percent, source_equity, source_buying_power, notes
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,0,%s,0,%s,%s,%s)
                    """,
                    (
                        month,
                        group,
                        mode,
                        pool_base_percents[group],
                        base_targets[group],
                        total_pct,
                        pool_pct[group],
                        risk_target,
                        risk_target,
                        group_snap.equity,
                        group_snap.buying_power,
                        f"monthly allocation auto-created broker={profile_for_pool(group)}",
                    ),
                )
                cur.execute(
                    """
                    UPDATE capital_pools
                    SET mode=%s, base_percent=%s, base_target_capital=%s,
                        total_risk_percent=%s, pool_risk_percent=%s,
                        risk_target_capital=%s,
                        source_equity=%s, source_buying_power=%s,
                        notes='realtime allocation refreshed',
                        updated_at=NOW()
                    WHERE allocation_month=%s
                      AND strategy_group=%s
                    """,
                    (
                        mode,
                        pool_base_percents[group],
                        base_targets[group],
                        total_pct,
                        pool_pct[group],
                        risk_target,
                        group_snap.equity,
                        group_snap.buying_power,
                        month,
                        group,
                    ),
                )
    return month


def _capital_pool_rows(month: date) -> list[dict]:
    """读取当月资金池表。"""
    return fetch_all(
        """
        SELECT *
        FROM capital_pools
        WHERE allocation_month=%s
        ORDER BY FIELD(strategy_group, 'A','B','C','D')
        """,
        (month,),
    )


def refresh_capital_pool_usage(month: date | None = None) -> list[dict]:
    """把 position_holdings 汇总出的真实占用金额回写到 capital_pools。"""
    month = month or _month_start()
    total_pct, pool_pct = _risk_percents()
    used = {group: get_strategy_used_capital(group) for group in ("A", "B", "C", "D")}
    with db_conn() as conn:
        with conn.cursor() as cur:
            for group in ("A", "B", "C", "D"):
                cur.execute(
                    """
                    SELECT base_target_capital
                    FROM capital_pools
                    WHERE allocation_month=%s AND strategy_group=%s
                    """,
                    (month, group),
                )
                row = cur.fetchone()
                if not row:
                    continue
                base_target = float(row.get("base_target_capital") or 0)
                risk_target = _risk_target_for_group(group, base_target, total_pct, pool_pct)
                used_capital = used[group]
                from .order_journal import reserved_for_pool
                available = max(0.0, risk_target - used_capital - reserved_for_pool(group))
                used_percent = used_capital / risk_target if risk_target > 0 else 0.0
                cur.execute(
                    """
                    UPDATE capital_pools
                    SET total_risk_percent=%s, pool_risk_percent=%s,
                        risk_target_capital=%s, used_capital=%s,
                        available_capital=%s, used_percent=%s,
                        updated_at=NOW()
                    WHERE allocation_month=%s AND strategy_group=%s
                    """,
                    (total_pct, pool_pct[group], risk_target, used_capital, available, used_percent, month, group),
                )
    return _capital_pool_rows(month)


def get_capital_allocation(mode: str | None = None) -> CapitalAllocation | None:
    """读取月度资金池表，并用真实持仓金额刷新已用资金。"""
    s = settings()
    broker_snaps = get_broker_account_snapshots()
    snap = _aggregate_account_snapshot(broker_snaps)
    if snap is None or all(float(b.equity or 0) <= 0 for b in broker_snaps.values()):
        return None
    if mode is None:
        mode = get_risk_state().mode
    mode = (mode or s.capital_mode or "NORMAL").upper()
    month = _ensure_monthly_capital_pools(mode, snap, broker_snaps)
    rows = refresh_capital_pool_usage(month)
    by_group = {str(row["strategy_group"]).upper(): row for row in rows}
    targets = {group: float((by_group.get(group) or {}).get("risk_target_capital") or 0) for group in ("A", "B", "C", "D")}
    base_targets = {group: float((by_group.get(group) or {}).get("base_target_capital") or 0) for group in ("A", "B", "C", "D")}
    base_percents = {group: float((by_group.get(group) or {}).get("base_percent") or 0) for group in ("A", "B", "C", "D")}
    pool_risk_percents = {group: float((by_group.get(group) or {}).get("pool_risk_percent") or 0) for group in ("A", "B", "C", "D")}
    pool_enabled = pool_enabled_settings()
    margin_mode, margin_usage, margin_reason = resolve_margin_usage_pct()
    used = {group: float((by_group.get(group) or {}).get("used_capital") or 0) for group in ("A", "B", "C", "D")}
    available = {group: float((by_group.get(group) or {}).get("available_capital") or 0) for group in ("A", "B", "C", "D")}
    total_risk_percent = max((float(row.get("total_risk_percent") or 0) for row in rows), default=0.0)
    return CapitalAllocation(
        mode=mode,
        allocation_month=month,
        equity=snap.equity,
        buying_power=snap.buying_power,
        cash=snap.cash,
        portfolio_value=snap.portfolio_value,
        trading_blocked=snap.trading_blocked,
        account_blocked=snap.account_blocked,
        trade_suspended_by_user=snap.trade_suspended_by_user,
        A_target=targets["A"],
        B_target=targets["B"],
        C_target=targets["C"],
        D_target=targets["D"],
        base_targets=base_targets,
        base_percents=base_percents,
        total_risk_percent=total_risk_percent,
        pool_risk_percents=pool_risk_percents,
        pool_enabled=pool_enabled,
        margin_usage_mode=margin_mode,
        margin_usage_percent=margin_usage,
        margin_usage_reason=margin_reason,
        used=used,
        available=available,
        pool_brokers={group: profile_for_pool(group) for group in ("A", "B", "C", "D")},
        broker_snapshots={name: _snapshot_payload(value) for name, value in broker_snaps.items()},
    )


def _get_strategy_used_capital_from_operations(strategy_group: str) -> float:
    """兜底逻辑：从旧交易控制表读取某个策略组当前持仓占用资金。"""
    s = settings()
    group = (strategy_group or "").upper()
    with db_conn(s) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT qty, current_price, close_price, cost_price
                FROM `{s.ops_table}`
                WHERE is_bought = 1
                  AND UPPER(COALESCE(NULLIF(strategy_group, ''), stock_type)) = %s
                """,
                (group,),
            )
            total = 0.0
            for row in cur.fetchall():
                qty = float(row.get("qty") or 0)
                price = row.get("current_price")
                if price is None:
                    price = row.get("close_price")
                if price is None:
                    price = row.get("cost_price")
                total += qty * float(price or 0)
            return total


def _get_d_options_used_capital(cur) -> float:
    """按最大亏损统计 Q 页面手动期权组合占用的 D 资金。"""
    try:
        cur.execute(
            """
            SELECT COALESCE(SUM(COALESCE(max_loss, 0)), 0) AS total
            FROM option_spreads
            WHERE status IN ('PLANNED','SUBMITTED','OPEN','CLOSE_PLANNED','CLOSE_SUBMITTED')
              AND (signal_reason LIKE 'Q_MANUAL_OPTION%%'
                   OR signal_reason LIKE 'D_MANUAL_OPTION%%')
            """
        )
        return abs(float((cur.fetchone() or {}).get("total") or 0.0))
    except Exception:
        # 兼容尚未创建期权表或旧表缺少 signal_reason 的环境。
        return 0.0


def get_strategy_used_capital(strategy_group: str) -> float:
    """从真实持仓展示表 position_holdings 读取某个策略组当前占用资金。

    stock_operations 是交易控制表，不再作为资金池展示的主要来源。
    只有关闭 ENABLE_POSITION_HOLDINGS 时，才退回旧表兜底。
    """
    s = settings()
    group = (strategy_group or "").upper()
    if not s.enable_position_holdings:
        total = _get_strategy_used_capital_from_operations(group)
        if group != "D":
            return total
        with db_conn(s) as conn:
            with conn.cursor() as cur:
                return total + _get_d_options_used_capital(cur)
    with db_conn(s) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT qty, avg_entry_price, current_price, market_value, cost_basis
                FROM position_holdings
                WHERE status = 'open'
                  AND UPPER(
                    CASE
                      WHEN strategy_group IN ('A','B','C','D') THEN strategy_group
                      WHEN stock_type IN ('A','B','C','D') THEN stock_type
                      ELSE strategy_group
                    END
                  ) = %s
                """,
                (group,),
            )
            total = 0.0
            for row in cur.fetchall():
                market_value = row.get("market_value")
                if market_value is not None:
                    total += abs(float(market_value or 0))
                    continue
                qty = float(row.get("qty") or 0)
                price = row.get("current_price")
                if price is None:
                    price = row.get("avg_entry_price")
                if price is not None:
                    total += abs(qty * float(price or 0))
                    continue
                total += abs(float(row.get("cost_basis") or 0))
            if group == "D":
                # Q 页面手动建立的期权价差归 D 资金池。期权持仓未必能稳定映射到
                # position_holdings，因此按组合最大亏损计入占用，避免股票与期权
                # 同时重复使用同一份 D 额度。
                total += _get_d_options_used_capital(cur)
            return total


def get_available_capital(strategy_group: str) -> float:
    allocation = get_capital_allocation()
    if allocation is None:
        raise RuntimeError("资金池计算失败")
    target = allocation.target_for(strategy_group)
    used = allocation.used.get((strategy_group or "").upper(), get_strategy_used_capital(strategy_group))
    return max(0.0, target - used)


def can_open_new_position(strategy_group: str, estimated_notional: float) -> tuple[bool, str]:
    """下单前资金池检查：超过本组资金池就拒绝开仓。"""
    s = settings()
    group = (strategy_group or "").upper()
    if not s.enable_capital_manager:
        return True, "capital_manager_disabled"
    try:
        allocation = get_capital_allocation()
        if allocation is None:
            return False, "account_snapshot_failed"
        broker = allocation.pool_brokers.get(group, "alpaca")
        broker_snapshot = allocation.broker_snapshots.get(broker, {})
        if broker_snapshot.get("account_blocked"):
            return False, "account_blocked"
        if broker_snapshot.get("trading_blocked"):
            return False, "trading_blocked"
        if broker_snapshot.get("trade_suspended_by_user"):
            return False, "trade_suspended_by_user"
        target = allocation.target_for(group)
        used = allocation.used[group] if group in allocation.used else get_strategy_used_capital(group)
        available = allocation.available.get(group, max(0.0, target - used))
        allow = float(estimated_notional or 0) <= available
        if allow:
            print(
                f"[CAPITAL CHECK] strategy={group} target={target:.2f} used={used:.2f} "
                f"available={available:.2f} request={estimated_notional:.2f} allow=True",
                flush=True,
            )
            return True, "allow"
        print(
            f"[CAPITAL BLOCK] strategy={group} target={target:.2f} used={used:.2f} "
            f"available={available:.2f} request={estimated_notional:.2f} "
            "allow=False reason=exceed_pool_limit",
            flush=True,
        )
        return False, "exceed_pool_limit"
    except Exception as exc:
        print(f"[CAPITAL ERROR] strategy={group} error={exc}", flush=True)
        return False, "capital_calc_failed"


def log_capital_startup() -> CapitalAllocation | None:
    allocation = get_capital_allocation()
    print(f"[CAPITAL MODE] {allocation.mode if allocation else get_risk_state().mode}", flush=True)
    if allocation is None:
        print("[CAPITAL ERROR] startup account snapshot failed; new positions disabled", flush=True)
        return None
    print(f"[EQUITY] {allocation.equity:.2f}", flush=True)
    print(f"[BUYING POWER] {allocation.buying_power:.2f}", flush=True)
    print(f"[A TARGET] {allocation.A_target:.2f}", flush=True)
    print(f"[B TARGET] {allocation.B_target:.2f}", flush=True)
    print(f"[C TARGET] {allocation.C_target:.2f}", flush=True)
    print(f"[D TARGET] {allocation.D_target:.2f}", flush=True)
    return allocation
