from __future__ import annotations

"""多策略资金池管理：计算 A/B/C/D 目标资金、已用资金和开仓许可。"""

import os
from dataclasses import dataclass
from datetime import date

from . import alpaca_gateway
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
    A_target: float
    B_target: float
    C_target: float
    D_target: float
    base_targets: dict[str, float]
    base_percents: dict[str, float]
    total_risk_percent: float
    pool_risk_percents: dict[str, float]
    used: dict[str, float]
    available: dict[str, float]

    def target_for(self, strategy_group: str) -> float:
        return float(getattr(self, f"{strategy_group.upper()}_target", 0.0))


def get_account_snapshot():
    return alpaca_gateway.get_account_snapshot()


def _month_start(today: date | None = None) -> date:
    """资金池按月管理，每月第一天作为分配月份。"""
    today = today or date.today()
    return date(today.year, today.month, 1)


def _mode_weights(mode: str) -> tuple[dict[str, float], bool]:
    """读取当前资金模式的基础比例；A/B/C 是本金池比例，D 是独立保证金额度比例。"""
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

    a, b, c, allow_d = {
        "NORMAL": (0.30, 0.40, 0.30, True),
        "SAFE": (0.40, 0.25, 0.35, False),
        "ATTACK": (0.25, 0.45, 0.30, True),
        "RISK_OFF": (0.70, 0.00, 0.30, False),
    }.get(mode, (0.30, 0.40, 0.30, True))
    d = 0.30 if allow_d else 0.0
    return {"A": a, "B": b, "C": c, "D": d}, allow_d


def _margin_usage_pct() -> float:
    """读取保证金总额度上限：100%-150%。"""
    raw_total = os.getenv("RISK_TOTAL_CAPITAL_PCT") or get_app_setting("RISK_TOTAL_CAPITAL_PCT", "1.0")
    try:
        margin_pct = float(raw_total)
        if margin_pct > 10:
            margin_pct = margin_pct / 100.0
    except Exception:
        margin_pct = 1.0
    return max(1.0, min(1.5, margin_pct))


def _market_exposure_pct(risk) -> float:
    """读取市场环境目标仓位：向上/VIX低 85%，横盘 55%，向下 25%。"""
    if risk.market_trend == "向上" and risk.vix < env_float("REBALANCE_LOW_VIX", 20.0):
        return env_float("REBALANCE_TARGET_UP", 0.85)
    if risk.market_trend == "向下":
        return env_float("REBALANCE_TARGET_DOWN", 0.25)
    return env_float("REBALANCE_TARGET_SIDEWAYS", 0.55)


def _risk_percents() -> tuple[float, dict[str, float]]:
    """计算 A/B/C 有效可用额度：保证金上限 × 市场仓位目标。"""
    risk = get_risk_state()
    total_pct = _margin_usage_pct() * _market_exposure_pct(risk)
    pool_pct = {
        "A": max(0.0, min(1.0, env_float("RISK_A_POOL_PCT", 1.0))),
        "B": max(0.0, min(1.0, env_float("RISK_B_POOL_PCT", 1.0))),
        "C": max(0.0, min(1.0, env_float("RISK_C_POOL_PCT", 1.0))),
        "D": max(0.0, min(1.0, env_float("RISK_D_POOL_PCT", risk.risk_multiplier))),
    }
    if risk.block_all_new:
        pool_pct["D"] = 0.0
    if risk.block_a:
        pool_pct["A"] = 0.0
    if risk.block_c:
        pool_pct["C"] = 0.0
    if risk.block_d:
        pool_pct["D"] = 0.0
    return total_pct, pool_pct


def _ensure_monthly_capital_pools(mode: str, snap) -> date:
    """当月没有资金池记录时，按当月账户资金和模式比例写入一次。"""
    month = _month_start()
    weights, _allow_d = _mode_weights(mode)
    base_targets = {
        "A": snap.equity * weights["A"],
        "B": snap.equity * weights["B"],
        "C": snap.equity * weights["C"],
        "D": snap.buying_power * weights["D"],
    }
    total_pct, pool_pct = _risk_percents()
    with db_conn() as conn:
        with conn.cursor() as cur:
            for group in ("A", "B", "C", "D"):
                risk_target = base_targets[group] * pool_pct[group] if group == "D" else base_targets[group] * total_pct * pool_pct[group]
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
                        weights[group],
                        base_targets[group],
                        total_pct,
                        pool_pct[group],
                        risk_target,
                        risk_target,
                        snap.equity,
                        snap.buying_power,
                        "monthly allocation auto-created",
                    ),
                )
                # 如果本月资金池已经存在，但策略比例规则升级了，只修正比例变化的行。
                # 这样可以把旧的 D=0 自动升级成 D=30%，同时不因为账户 equity 波动重写整月资金池。
                cur.execute(
                    """
                    UPDATE capital_pools
                    SET mode=%s, base_percent=%s, base_target_capital=%s,
                        notes='monthly allocation policy upgraded',
                        updated_at=NOW()
                    WHERE allocation_month=%s
                      AND strategy_group=%s
                      AND (mode<>%s OR ABS(base_percent - %s) > 0.0001)
                    """,
                    (mode, weights[group], base_targets[group], month, group, mode, weights[group]),
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
                risk_target = base_target * pool_pct[group] if group == "D" else base_target * total_pct * pool_pct[group]
                used_capital = used[group]
                available = max(0.0, risk_target - used_capital)
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
    snap = get_account_snapshot()
    if snap is None:
        return None
    if mode is None:
        mode = get_risk_state().mode
    mode = (mode or s.capital_mode or "NORMAL").upper()
    month = _ensure_monthly_capital_pools(mode, snap)
    rows = refresh_capital_pool_usage(month)
    by_group = {str(row["strategy_group"]).upper(): row for row in rows}
    targets = {group: float((by_group.get(group) or {}).get("risk_target_capital") or 0) for group in ("A", "B", "C", "D")}
    base_targets = {group: float((by_group.get(group) or {}).get("base_target_capital") or 0) for group in ("A", "B", "C", "D")}
    base_percents = {group: float((by_group.get(group) or {}).get("base_percent") or 0) for group in ("A", "B", "C", "D")}
    pool_risk_percents = {group: float((by_group.get(group) or {}).get("pool_risk_percent") or 0) for group in ("A", "B", "C", "D")}
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
        A_target=targets["A"],
        B_target=targets["B"],
        C_target=targets["C"],
        D_target=targets["D"],
        base_targets=base_targets,
        base_percents=base_percents,
        total_risk_percent=total_risk_percent,
        pool_risk_percents=pool_risk_percents,
        used=used,
        available=available,
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


def get_strategy_used_capital(strategy_group: str) -> float:
    """从真实持仓展示表 position_holdings 读取某个策略组当前占用资金。

    stock_operations 是交易控制表，不再作为资金池展示的主要来源。
    只有关闭 ENABLE_POSITION_HOLDINGS 时，才退回旧表兜底。
    """
    s = settings()
    group = (strategy_group or "").upper()
    if not s.enable_position_holdings:
        return _get_strategy_used_capital_from_operations(group)
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
        mode = get_risk_state().mode
        if mode == "SAFE" and group == "D":
            print(f"[CAPITAL BLOCK] strategy=D allow=False reason=safe_mode_blocks_d", flush=True)
            return False, "safe_mode_blocks_d"
        if mode == "RISK_OFF" and group in {"B", "D"}:
            print(f"[CAPITAL BLOCK] strategy={group} allow=False reason=risk_off_blocks_attack", flush=True)
            return False, "risk_off_blocks_attack"
        allocation = get_capital_allocation()
        if allocation is None:
            return False, "account_snapshot_failed"
        target = allocation.target_for(group)
        used = allocation.used.get(group, get_strategy_used_capital(group))
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
