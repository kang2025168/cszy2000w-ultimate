from __future__ import annotations

"""多策略资金池管理：计算 A/B/C/D 目标资金、已用资金和开仓许可。"""

from dataclasses import dataclass

from . import alpaca_gateway
from .config import settings
from .db import db_conn
from .risk_controller import get_risk_state


@dataclass
class CapitalAllocation:
    mode: str
    equity: float
    buying_power: float
    cash: float
    portfolio_value: float
    A_target: float
    B_target: float
    C_target: float
    D_target: float

    def target_for(self, strategy_group: str) -> float:
        return float(getattr(self, f"{strategy_group.upper()}_target", 0.0))


def get_account_snapshot():
    return alpaca_gateway.get_account_snapshot()


def get_capital_allocation(mode: str | None = None) -> CapitalAllocation | None:
    """按 CAPITAL_MODE 计算各策略资金池，B/D 会受 risk_multiplier 影响。"""
    s = settings()
    snap = get_account_snapshot()
    if snap is None:
        return None
    mode = (mode or s.capital_mode or "NORMAL").upper()
    weights = {
        "NORMAL": (0.35, 0.30, 0.35, True),
        "SAFE": (0.40, 0.20, 0.40, False),
        "ATTACK": (0.25, 0.45, 0.30, True),
        "RISK_OFF": (0.70, 0.00, 0.30, False),
    }.get(mode, (0.35, 0.30, 0.35, True))
    a, b, c, allow_d = weights
    d_target = max(0.0, snap.buying_power - snap.equity) if allow_d else 0.0
    risk = get_risk_state()
    return CapitalAllocation(
        mode=mode,
        equity=snap.equity,
        buying_power=snap.buying_power,
        cash=snap.cash,
        portfolio_value=snap.portfolio_value,
        A_target=snap.equity * a,
        B_target=snap.equity * b * risk.risk_multiplier,
        C_target=snap.equity * c,
        D_target=d_target * risk.risk_multiplier,
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
    used = get_strategy_used_capital(strategy_group)
    return max(0.0, target - used)


def can_open_new_position(strategy_group: str, estimated_notional: float) -> tuple[bool, str]:
    """下单前资金池检查：超过本组资金池就拒绝开仓。"""
    s = settings()
    group = (strategy_group or "").upper()
    if not s.enable_capital_manager:
        return True, "capital_manager_disabled"
    try:
        if s.capital_mode == "SAFE" and group == "D":
            print(f"[CAPITAL BLOCK] strategy=D allow=False reason=safe_mode_blocks_d", flush=True)
            return False, "safe_mode_blocks_d"
        if s.capital_mode == "RISK_OFF" and group in {"B", "D"}:
            print(f"[CAPITAL BLOCK] strategy={group} allow=False reason=risk_off_blocks_attack", flush=True)
            return False, "risk_off_blocks_attack"
        allocation = get_capital_allocation()
        if allocation is None:
            return False, "account_snapshot_failed"
        target = allocation.target_for(group)
        used = get_strategy_used_capital(group)
        available = max(0.0, target - used)
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
    print(f"[CAPITAL MODE] {settings().capital_mode}", flush=True)
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
