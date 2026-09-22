"""Trading performance statistics used by the dashboard analytics view."""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import date, timedelta

from .db import fetch_all


def _number(value, default: float = 0.0) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return default


def _safe_fetch(sql: str, args: tuple = (), *, quiet: bool = False) -> list[dict]:
    try:
        return fetch_all(sql, args)
    except Exception as exc:
        if not quiet:
            print(f"[PERFORMANCE] query unavailable: {exc}", flush=True)
        return []


def _period_start(period: str) -> date | None:
    today = date.today()
    key = (period or "90d").lower()
    if key == "30d":
        return today - timedelta(days=30)
    if key == "year":
        return today.replace(month=1, day=1)
    if key == "all":
        return None
    return today - timedelta(days=90)


def equity_metrics(rows: list[dict]) -> dict:
    raw_points = []
    for row in rows:
        equity = _number(row.get("equity") or row.get("portfolio_value"))
        if equity > 0:
            raw_points.append({"date": str(row.get("snapshot_date") or ""), "equity": round(equity, 2)})

    # Deposits, withdrawals and account-profile switches are not investment returns.
    # Keep the latest continuous segment instead of presenting a cash injection as profit.
    reset_indexes = []
    for index in range(1, len(raw_points)):
        previous_equity = raw_points[index - 1]["equity"]
        if previous_equity > 0 and abs(raw_points[index]["equity"] / previous_equity - 1.0) > 0.20:
            reset_indexes.append(index)
    segment_start = reset_indexes[-1] if reset_indexes else 0
    source_points = raw_points[segment_start:]

    points = []
    peak = 0.0
    max_drawdown = 0.0
    returns = []
    previous = 0.0
    for row in source_points:
        equity = row["equity"]
        peak = max(peak, equity)
        drawdown = (equity - peak) / peak if peak > 0 else 0.0
        max_drawdown = min(max_drawdown, drawdown)
        if previous > 0:
            returns.append(equity / previous - 1.0)
        previous = equity
        points.append({"date": row["date"], "equity": round(equity, 2), "drawdown": round(drawdown, 6)})
    start = points[0]["equity"] if points else 0.0
    end = points[-1]["equity"] if points else 0.0
    total_return = end / start - 1.0 if start > 0 else 0.0
    volatility = 0.0
    if len(returns) >= 2:
        mean = sum(returns) / len(returns)
        variance = sum((item - mean) ** 2 for item in returns) / (len(returns) - 1)
        volatility = math.sqrt(variance) * math.sqrt(252)
    return {
        "start_equity": round(start, 2),
        "end_equity": round(end, 2),
        "net_change": round(end - start, 2),
        "total_return": round(total_return, 6),
        "max_drawdown": round(max_drawdown, 6),
        "annualized_volatility": round(volatility, 6),
        "sample_days": len(points),
        "account_resets": len(reset_indexes),
        "ignored_days": segment_start,
        "points": points,
    }


def strategy_metrics(rows: list[dict]) -> list[dict]:
    groups: dict[str, dict] = defaultdict(lambda: {
        "market_value": 0.0, "cost_basis": 0.0, "unrealized_pnl": 0.0,
        "realized_pnl": 0.0, "open_positions": 0, "closed_trades": 0,
        "wins": [], "losses": [],
    })
    for row in rows:
        group = str(row.get("strategy_group") or row.get("stock_type") or "MANUAL").upper()
        if group not in {"A", "B", "C", "D", "Q", "MANUAL"}:
            group = "MANUAL"
        item = groups[group]
        status = str(row.get("status") or "").lower()
        realized = _number(row.get("realized_pnl"))
        item["realized_pnl"] += realized
        if status == "open":
            item["open_positions"] += 1
            item["market_value"] += _number(row.get("market_value"))
            item["cost_basis"] += _number(row.get("cost_basis"))
            item["unrealized_pnl"] += _number(row.get("unrealized_pnl"))
        elif realized != 0:
            item["closed_trades"] += 1
            (item["wins"] if realized > 0 else item["losses"]).append(realized)

    result = []
    for group in ("A", "B", "C", "D", "Q", "MANUAL"):
        item = groups[group]
        wins = item.pop("wins")
        losses = item.pop("losses")
        closed = int(item["closed_trades"])
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0
        payoff = avg_win / abs(avg_loss) if avg_win > 0 and avg_loss < 0 else 0.0
        expectancy = (sum(wins) + sum(losses)) / closed if closed else 0.0
        capital_return = (item["realized_pnl"] + item["unrealized_pnl"]) / item["cost_basis"] if item["cost_basis"] > 0 else 0.0
        result.append({
            "strategy": group,
            **{key: round(value, 2) if isinstance(value, float) else value for key, value in item.items()},
            "win_rate": round(len(wins) / closed, 6) if closed else 0.0,
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "payoff_ratio": round(payoff, 3),
            "expectancy": round(expectancy, 2),
            "capital_return": round(capital_return, 6),
        })
    return result


def _insights(equity: dict, strategies: list[dict], execution: dict) -> list[dict]:
    notes = []
    days = int(equity.get("sample_days") or 0)
    if days < 20:
        notes.append({"tone": "warn", "title": "净值样本不足", "detail": f"当前只有 {days} 个日终样本，暂时不能判断长期稳定性。"})
    resets = int(equity.get("account_resets") or 0)
    if resets:
        notes.append({"tone": "warn", "title": "已排除资金基线跳变", "detail": f"发现 {resets} 次超过 20% 的账户变化，收益和回撤从最近一次变化后重新计算。"})
    drawdown = _number(equity.get("max_drawdown"))
    if drawdown <= -0.10:
        notes.append({"tone": "danger", "title": "回撤需要控制", "detail": f"区间最大回撤为 {drawdown:.1%}，应优先检查仓位和止损执行。"})
    elif days >= 5:
        notes.append({"tone": "ok", "title": "回撤仍在可观察范围", "detail": f"区间最大回撤为 {drawdown:.1%}，继续关注放大后的表现。"})
    closed = sum(int(row.get("closed_trades") or 0) for row in strategies)
    if closed < 30:
        notes.append({"tone": "warn", "title": "闭环交易样本偏少", "detail": f"只有 {closed} 笔可计算盈亏的平仓记录，胜率与盈亏比仅供参考。"})
    failed = int(execution.get("failed_orders") or 0)
    if failed:
        notes.append({"tone": "danger", "title": "存在失败订单", "detail": f"区间内有 {failed} 笔拒绝或错误订单，建议先处理执行质量。"})
    ranked = [row for row in strategies if int(row.get("closed_trades") or 0) >= 5]
    if ranked:
        best = max(ranked, key=lambda row: _number(row.get("expectancy")))
        notes.append({"tone": "ok" if _number(best.get("expectancy")) > 0 else "warn", "title": f"{best['strategy']} 当前期望值最高", "detail": f"每笔已平仓交易平均 {best['expectancy']:+.2f} 美元，样本 {best['closed_trades']} 笔。"})
    if not notes:
        notes.append({"tone": "warn", "title": "等待更多数据", "detail": "系统已开始积累统计样本，先保持规则一致，不要频繁修改策略。"})
    return notes[:5]


def performance_payload(period: str = "90d") -> dict:
    start = _period_start(period)
    equity_args = () if start is None else (start,)
    equity_where = "" if start is None else "AND DATE(s.created_at) >= %s"
    equity_rows = _safe_fetch(
        f"""
        SELECT DATE(s.created_at) AS snapshot_date, s.equity, s.portfolio_value
        FROM account_equity_snapshots s
        JOIN (
            SELECT DATE(created_at) AS d, MAX(created_at) AS max_created_at
            FROM account_equity_snapshots
            WHERE broker_profile IN ('legacy','combined')
            GROUP BY DATE(created_at)
        ) latest ON DATE(s.created_at)=latest.d AND s.created_at=latest.max_created_at
        WHERE s.broker_profile IN ('legacy','combined') {equity_where}
        ORDER BY s.created_at
        """,
        equity_args,
    )
    holdings_where = "" if start is None else "WHERE status='open' OR DATE(COALESCE(exit_time,last_update_time)) >= %s"
    holding_args = () if start is None else (start,)
    holdings = _safe_fetch(
        f"""
        SELECT strategy_group, stock_type, status, market_value, cost_basis,
               unrealized_pnl, realized_pnl
        FROM position_holdings
        {holdings_where}
        """,
        holding_args,
    )
    time_filter = "" if start is None else "WHERE event_time >= %s"
    order_args = () if start is None else (start,)
    order_rows = _safe_fetch(
        f"""
        SELECT status, COALESCE(NULLIF(filled_qty,0),qty) AS qty
        FROM manual_trade_records
        {time_filter}
        """,
        order_args,
    )
    orders_time_filter = "" if start is None else "WHERE created_at >= %s"
    robot_orders = _safe_fetch(
        f"""
        SELECT status, qty
        FROM orders
        {orders_time_filter}
        """,
        order_args,
        quiet=True,
    )
    order_rows.extend(robot_orders)
    filled = sum(1 for row in order_rows if _number(row.get("qty")) > 0 and "FILL" in str(row.get("status") or "").upper())
    failed = sum(1 for row in order_rows if any(word in str(row.get("status") or "").upper() for word in ("ERROR", "REJECT", "FAILED")))
    canceled = sum(1 for row in order_rows if "CANCEL" in str(row.get("status") or "").upper())
    execution = {"orders": len(order_rows), "filled_orders": filled, "failed_orders": failed, "canceled_orders": canceled}
    execution["fill_rate"] = round(filled / len(order_rows), 6) if order_rows else 0.0
    equity = equity_metrics(equity_rows)
    strategies = strategy_metrics(holdings)
    return {
        "ok": True,
        "period": period,
        "start_date": start.isoformat() if start else None,
        "equity": equity,
        "strategies": strategies,
        "execution": execution,
        "insights": _insights(equity, strategies, execution),
        "methodology": "收益与回撤使用账户日终净值；策略盈亏使用 position_holdings 明确记录；失败或未成交订单不计为亏损。",
    }
