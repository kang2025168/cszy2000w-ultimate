from __future__ import annotations

"""策略公共工具：统一结果格式、估算下单金额、读取 stock_operations。"""

from dataclasses import dataclass

from ..config import env_float, settings
from ..db import db_conn


@dataclass
class StrategyResult:
    """策略执行结果，方便调度器和日志统一处理。"""

    ok: bool
    strategy_group: str
    symbol: str
    action: str
    reason: str = ""


def default_notional(strategy_group: str) -> float:
    """按策略组读取默认单笔金额，用来做开仓前资金检查。"""
    group = (strategy_group or "").upper()
    return env_float(f"{group}_TARGET_NOTIONAL_USD", env_float("DEFAULT_TARGET_NOTIONAL_USD", 500.0))


def load_operation(symbol: str, strategy_group: str) -> dict | None:
    """从 stock_operations 读取一只股票在某策略组的控制记录。"""
    s = settings()
    with db_conn(s) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT *
                FROM `{s.ops_table}`
                WHERE stock_code=%s
                  AND UPPER(COALESCE(NULLIF(strategy_group, ''), stock_type))=%s
                ORDER BY id DESC
                LIMIT 1
                """,
                (symbol.upper(), strategy_group.upper()),
            )
            return cur.fetchone()


def mark_strategy_group(symbol: str, strategy_group: str, *, capital_pool: str | None = None, margin_used: int = 0) -> None:
    """把旧表记录补上 strategy_group/capital_pool，方便资金池统计。"""
    s = settings()
    with db_conn(s) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE `{s.ops_table}`
                SET strategy_group=%s,
                    capital_pool=%s,
                    margin_used=%s,
                    last_capital_check_at=NOW()
                WHERE stock_code=%s
                  AND UPPER(COALESCE(NULLIF(strategy_group, ''), stock_type))=%s
                """,
                (strategy_group.upper(), capital_pool or strategy_group.upper(), margin_used, symbol.upper(), strategy_group.upper()),
            )

