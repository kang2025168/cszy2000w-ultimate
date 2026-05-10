from __future__ import annotations

"""中央状态读写：风险状态、资金状态、机器人心跳、机器人命令。"""

import json
from calendar import monthrange
from datetime import date, datetime, timedelta

from .db import db_conn, fetch_all, fetch_one
from .risk_controller import RiskState


def heartbeat(bot_name: str, status: str = "running", message: str = "") -> None:
    """机器人心跳：每个机器人定期写入，方便网页判断谁还活着。"""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bot_heartbeats (bot_name, status, last_seen_at, last_message)
                VALUES (%s, %s, NOW(), %s)
                ON DUPLICATE KEY UPDATE
                  status=VALUES(status),
                  last_seen_at=VALUES(last_seen_at),
                  last_message=VALUES(last_message)
                """,
                (bot_name, status, message[:255]),
            )


def write_risk_state(state: RiskState) -> None:
    """把风险机器人计算结果写入 risk_state。"""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO risk_state (
                    mode, risk_multiplier, daily_pnl_pct, loss_days, max_drawdown_pct,
                    block_all_new, block_b_buy, block_d_buy, suggest_capital_mode,
                    reason, market_trend, qqq_change_pct, vix, risk_preference,
                    allocation_mode, recommended_exposure, recommended_weights, updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                """,
                (
                    state.mode,
                    state.risk_multiplier,
                    state.daily_pnl_pct,
                    state.loss_days,
                    state.max_drawdown,
                    1 if state.block_all_new else 0,
                    1 if state.block_b else 0,
                    1 if state.block_d else 0,
                    state.suggest_mode,
                    state.reason,
                    state.market_trend,
                    state.qqq_change_pct,
                    state.vix,
                    state.risk_preference,
                    state.allocation_mode,
                    state.recommended_exposure,
                    json.dumps(state.recommended_weights or {}, ensure_ascii=False),
                ),
            )


def latest_risk_state() -> dict | None:
    """读取最新风险状态。"""
    return fetch_one("SELECT * FROM risk_state ORDER BY id DESC LIMIT 1")


def replace_capital_state(rows: list[dict]) -> None:
    """刷新各策略组资金状态；保留历史写入时间，但每组只保留最新一行。"""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM capital_state")
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO capital_state (
                        strategy_group, target_capital, used_capital, available_capital,
                        risk_adjusted_target, can_open_new, reason, updated_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
                    """,
                    (
                        row["strategy_group"],
                        row["target_capital"],
                        row["used_capital"],
                        row["available_capital"],
                        row["risk_adjusted_target"],
                        1 if row.get("can_open_new", True) else 0,
                        row.get("reason"),
                    ),
                )


def capital_state_rows() -> list[dict]:
    """读取当前资金状态。"""
    return fetch_all("SELECT * FROM capital_state ORDER BY strategy_group")


def bot_heartbeats() -> list[dict]:
    """读取所有机器人心跳。"""
    return fetch_all("SELECT * FROM bot_heartbeats ORDER BY bot_name")


def bot_controls() -> list[dict]:
    """读取买卖机器人的开关状态。"""
    return fetch_all("SELECT * FROM bot_controls ORDER BY bot_name")


def is_bot_enabled(bot_name: str) -> bool:
    """判断某个机器人是否允许运行。"""
    row = fetch_one("SELECT enabled FROM bot_controls WHERE bot_name=%s", (bot_name,))
    return bool(row is None or int(row.get("enabled") or 0) == 1)


def set_bot_enabled(bot_name: str, enabled: bool) -> None:
    """设置机器人开关。"""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bot_controls (bot_name, enabled)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE enabled=VALUES(enabled)
                """,
                (bot_name, 1 if enabled else 0),
            )


def pending_commands(bot_name: str) -> list[dict]:
    """读取某个机器人的待执行命令。"""
    return fetch_all(
        """
        SELECT *
        FROM bot_commands
        WHERE bot_name=%s AND status='pending'
        ORDER BY id
        """,
        (bot_name,),
    )


def complete_command(command_id: int, status: str, result: str = "") -> None:
    """标记机器人命令执行结果。"""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE bot_commands
                SET status=%s, result=%s, executed_at=NOW()
                WHERE id=%s
                """,
                (status, result[:255], command_id),
            )


def add_command(bot_name: str, command: str, payload: dict | None = None) -> None:
    """给某个机器人写一条命令，后续可由网页按钮调用。"""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bot_commands (bot_name, command, payload)
                VALUES (%s, %s, %s)
                """,
                (bot_name, command, json.dumps(payload or {}, ensure_ascii=False)),
            )


def write_account_snapshot(equity: float, buying_power: float, cash: float, portfolio_value: float) -> None:
    """保存账户资金快照，用于网页收益曲线。"""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO account_equity_snapshots (equity, buying_power, cash, portfolio_value)
                VALUES (%s, %s, %s, %s)
                """,
                (equity, buying_power, cash, portfolio_value),
            )


def equity_curve_bounds(period: str = "week") -> tuple[date | None, date | None]:
    """计算收益曲线固定窗口：周一到周五、月初到月末、年初到 12 月 30 日。"""
    today = date.today()
    period = (period or "week").lower()
    if period == "week":
        start = today - timedelta(days=today.weekday())
        return start, start + timedelta(days=4)
    if period == "month":
        return today.replace(day=1), today.replace(day=monthrange(today.year, today.month)[1])
    if period == "year":
        return date(today.year, 1, 1), date(today.year, 12, 30)
    return None, None


def equity_curve(period: str = "week") -> dict:
    """读取收益曲线数据：每个交易日只取当天最后一条账户快照。"""
    period = (period or "week").lower()
    start, end = equity_curve_bounds(period)
    if period == "all":
        rows = fetch_all(
            """
            SELECT DATE(s.created_at) AS snapshot_date,
                   s.equity, s.buying_power, s.cash, s.portfolio_value, s.created_at
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
        return {"period": period, "start_date": None, "end_date": None, "rows": rows}
    rows = fetch_all(
        """
        SELECT DATE(s.created_at) AS snapshot_date,
               s.equity, s.buying_power, s.cash, s.portfolio_value, s.created_at
        FROM account_equity_snapshots s
        JOIN (
            SELECT DATE(created_at) AS d, MAX(created_at) AS max_created_at
            FROM account_equity_snapshots
            WHERE DATE(created_at) BETWEEN %s AND %s
            GROUP BY DATE(created_at)
        ) latest
          ON DATE(s.created_at)=latest.d AND s.created_at=latest.max_created_at
        ORDER BY s.created_at
        LIMIT 2000
        """,
        (start, end),
    )
    return {"period": period, "start_date": start.isoformat(), "end_date": end.isoformat(), "rows": rows}
