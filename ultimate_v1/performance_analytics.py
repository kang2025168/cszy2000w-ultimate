"""Trading performance statistics used by the dashboard analytics view."""

from __future__ import annotations

import math
import re
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


def _b_closed_trades(start: date | None) -> list[dict]:
    """Rebuild B round trips from filled broker/manual records using FIFO average cost."""
    rows = _safe_fetch(
        """
        SELECT event_time,symbol,UPPER(side) AS side,
               COALESCE(NULLIF(filled_qty,0),qty) AS qty,
               COALESCE(NULLIF(filled_avg_price,0),price) AS price,
               order_id
        FROM manual_trade_records
        WHERE UPPER(strategy_group)='B'
          AND UPPER(status) LIKE '%FILL%'
        ORDER BY event_time,id
        """,
        quiet=True,
    )
    positions: dict[str, dict] = defaultdict(lambda: {"qty": 0.0, "cost": 0.0})
    trades = []
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        side = str(row.get("side") or "").upper()
        qty, price = _number(row.get("qty")), _number(row.get("price"))
        if not symbol or qty <= 0 or price <= 0:
            continue
        position = positions[symbol]
        if side == "BUY":
            position["cost"] += qty * price
            position["qty"] += qty
            continue
        if side != "SELL" or position["qty"] <= 0:
            continue
        closed_qty = min(qty, position["qty"])
        entry_price = position["cost"] / position["qty"] if position["qty"] else 0.0
        pnl = (price - entry_price) * closed_qty
        position["qty"] -= closed_qty
        position["cost"] = max(0.0, position["cost"] - entry_price * closed_qty)
        if start is not None and str(row.get("event_time") or "")[:10] < start.isoformat():
            continue
        trades.append({
            "completed_at": row.get("event_time"), "strategy_group": "B", "symbol": symbol,
            "direction": "BUY_THEN_SELL", "qty": closed_qty, "entry_price": entry_price,
            "exit_price": price, "realized_pnl": pnl,
            "return_pct": pnl / (entry_price * closed_qty) if entry_price > 0 else 0.0,
            "cost_effect": "PROFIT" if pnl > 0 else "LOSS" if pnl < 0 else "FLAT",
            "exit_reason": "B 平仓成交",
            "exit_order_id": row.get("order_id"),
            "price_note": "首笔价格为历史持仓平均成本，可能包含多笔买入。",
        })
    return trades


def _d_closed_trades(start: date | None) -> list[dict]:
    where = "" if start is None else "AND e.created_at >= %s"
    args = () if start is None else (start,)
    rows = _safe_fetch(
        f"""
        SELECT e.created_at,e.symbol,e.cycle_no,e.qty,e.price,e.message,e.order_id,
               b.order_id AS entry_order_id, b.created_at AS entry_submitted_at
        FROM d_grid_events e
        LEFT JOIN d_grid_events b ON b.id = (
            SELECT MAX(buy.id) FROM d_grid_events buy
            WHERE buy.symbol=e.symbol AND buy.cycle_no=e.cycle_no
              AND buy.event_type='BUY_SUBMITTED'
              AND buy.id < e.id AND buy.created_at <= e.created_at
        )
        WHERE e.event_type='CYCLE_FILLED' {where}
        ORDER BY e.created_at DESC,e.id DESC
        """,
        args,
        quiet=True,
    )
    trades = []
    for row in rows:
        qty, exit_price = _number(row.get("qty")), _number(row.get("price"))
        match = re.search(r"gross_pnl=(-?[0-9]+(?:\.[0-9]+)?)", str(row.get("message") or ""))
        pnl = _number(match.group(1)) if match else 0.0
        entry_price = exit_price - pnl / qty if qty > 0 else 0.0
        trades.append({
            "completed_at": row.get("created_at"), "strategy_group": "D",
            "symbol": str(row.get("symbol") or ""), "direction": "GRID_CYCLE", "qty": qty,
            "entry_price": entry_price, "exit_price": exit_price, "realized_pnl": pnl,
            "return_pct": pnl / (entry_price * qty) if entry_price > 0 and qty > 0 else 0.0,
            "cost_effect": "PROFIT" if pnl > 0 else "LOSS" if pnl < 0 else "FLAT",
            "exit_reason": f"D 循环 #{int(_number(row.get('cycle_no')))}" + (" · 5%止损" if "STOP_LOSS_5PCT" in str(row.get("message") or "") else ""),
            "exit_order_id": row.get("order_id"),
            "entry_order_id": row.get("entry_order_id"),
            "entry_submitted_at": row.get("entry_submitted_at"),
            "price_note": "首笔价格由循环成交收益反算；买入时间为委托提交时间，卖出时间为闭环确认时间，均非券商逐笔成交时间。",
        })
    return trades


def _q_closed_trades(start: date | None) -> list[dict]:
    where = "" if start is None else "AND updated_at >= %s"
    args = () if start is None else (start,)
    rows = _safe_fetch(
        f"""
        SELECT updated_at,underlying,mode,qty,entry_price,exit_price,profit,profit_pct,close_reason
        FROM option_spreads
        WHERE status='CLOSED' {where}
        ORDER BY updated_at DESC,id DESC
        """,
        args,
        quiet=True,
    )
    result = []
    for row in rows:
        qty = _number(row.get("qty"))
        unit_profit = _number(row.get("profit"))
        result.append({
        "completed_at": row.get("updated_at"), "strategy_group": "Q",
        "symbol": str(row.get("underlying") or ""), "direction": str(row.get("mode") or "OPTION"),
        "qty": qty, "entry_price": _number(row.get("entry_price")),
        "exit_price": _number(row.get("exit_price")), "realized_pnl": unit_profit * qty * 100,
        "return_pct": _number(row.get("profit_pct")),
        "cost_effect": "PROFIT" if unit_profit > 0 else "LOSS" if unit_profit < 0 else "FLAT",
        "exit_reason": str(row.get("close_reason") or "Q 组合平仓"),
        })
    return result


def _closed_trade_summary(rows: list[dict]) -> list[dict]:
    grouped: dict[str, dict] = defaultdict(lambda: {
        "cycles": 0, "wins": 0, "losses": 0, "realized_pnl": 0.0,
        "lowered": 0, "raised": 0,
    })
    for row in rows:
        item = grouped[str(row.get("strategy_group") or "--").upper()]
        pnl = _number(row.get("realized_pnl"))
        item["cycles"] += 1
        item["realized_pnl"] += pnl
        item["wins"] += int(pnl > 0)
        item["losses"] += int(pnl < 0)
        item["lowered"] += int(str(row.get("cost_effect")) == "LOWERED")
        item["raised"] += int(str(row.get("cost_effect")) == "RAISED")
    result = []
    for group in ("A", "B", "C", "D", "Q"):
        item = grouped[group]
        cycles = item["cycles"]
        result.append({
            "strategy": group, **item, "realized_pnl": round(item["realized_pnl"], 2),
            "win_rate": round(item["wins"] / cycles, 6) if cycles else 0.0,
        })
    return result


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
    t_where = "" if start is None else "WHERE completed_at >= %s"
    t_args = () if start is None else (start,)
    t_rows = _safe_fetch(
        f"""
        SELECT strategy_group,symbol,direction,entry_side,exit_side,started_at,exit_order_id,qty,entry_price,exit_price,
               realized_pnl,return_pct,cost_effect,exit_reason,completed_at
        FROM ac_t_cycle_results
        {t_where}
        ORDER BY completed_at DESC, id DESC
        LIMIT 200
        """,
        t_args,
        quiet=True,
    )
    closed_trades = t_rows + _b_closed_trades(start) + _d_closed_trades(start) + _q_closed_trades(start)
    closed_trades.sort(key=lambda row: str(row.get("completed_at") or ""), reverse=True)
    closed_trades = closed_trades[:200]
    t_summary = _closed_trade_summary(closed_trades)
    equity = equity_metrics(equity_rows)
    strategies = strategy_metrics(holdings)
    return {
        "ok": True,
        "period": period,
        "start_date": start.isoformat() if start else None,
        "equity": equity,
        "strategies": strategies,
        "execution": execution,
        "ac_t": {"summary": t_summary, "rows": closed_trades},
        "insights": _insights(equity, strategies, execution),
        "methodology": "收益与回撤使用账户日终净值；A/C 按做T第二腿成交、B 按股票买卖闭环、D 按循环成交、Q 按期权组合平仓统计；失败或未成交订单不计盈亏。",
    }
