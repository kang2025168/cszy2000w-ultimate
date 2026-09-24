from __future__ import annotations

"""轻量网页看板：展示资金池、风控状态和 position_holdings 持仓。"""

import json
import hashlib
import hmac
import contextlib
import csv
import importlib.util
import io
import re
import time
from datetime import date, datetime, time as dt_time, timedelta
from decimal import Decimal
from http.server import BaseHTTPRequestHandler
from .http_server import DashboardServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from . import alpaca_gateway
from .account_config import load_account_config, public_account_config, save_account_config, profile_for_pool
from .bot_supervisor import (
    managed_bot_names,
    process_status,
    set_bot_runtime,
    shutdown_supervisor,
    start_watchdog,
    sync_from_controls,
)
from .capital_manager import get_capital_allocation, get_strategy_used_capital, resolve_margin_usage_pct
from .config import env_bool, env_float, env_int, env_str, settings
from .db import db_conn, fetch_all
from .d_tactical import d_tactical_payload, option_preview, submit_option_combo
from .d_grid import config_payload as d_grid_config_payload, save_config as save_d_grid_config
from .exposure_manager import latest_exposure_state, latest_rebalance_actions, refresh_exposure_plan
from .monthly_investment import load_monthly_invest_config, run_monthly_investment, save_monthly_invest_config
from .performance_analytics import performance_payload
from .rebalance_monthly import generate_rebalance_report
from .risk_controller import CAPITAL_MODE_LABELS, get_risk_state
from .schema import ensure_schema
from .state_store import bot_controls, bot_heartbeats, capital_state_rows, equity_curve, get_app_setting, latest_risk_state, set_app_setting, write_risk_state
from .sync_positions import last_sync_error, sync_all_positions

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


def _json_default(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


_QUOTE_REFRESH_TS = 0.0


def _safe_float(value, default: float = 0.0) -> float:
    """把数据库/接口里的数字安全转成 float。"""
    try:
        if value is None or str(value).strip() == "":
            return default
        from math import isfinite
        number = float(value)
        return number if isfinite(number) else default
    except Exception:
        return default


from .web_auth import (
    COOKIE_NAME as AUTH_COOKIE_NAME, login_password as _login_password,
    issue_token as _auth_token, verify_token, cookie as auth_cookie, allow_login,
)


def _allocation_payload() -> dict:
    """组装资金池接口数据。"""
    allocation = get_capital_allocation()
    if allocation is None:
        return {"ok": False, "error": "account_snapshot_failed"}
    margin_mode, margin_usage, margin_reason = resolve_margin_usage_pct()
    from .capital_manager import margin_budget_summary
    margin_summary = margin_budget_summary(allocation)
    used = allocation.used
    available = allocation.available
    usable_total = sum(allocation.target_for(g) for g in ("A", "B", "C", "D"))
    base_total = sum(allocation.base_targets.get(g, 0.0) for g in ("A", "B", "C", "D"))
    used_total = sum(used.get(g, 0.0) for g in ("A", "B", "C", "D"))
    x_target = _setting_float("X_CASH_POOL_TARGET", _setting_float("ANNUAL_CASH_MIN_TARGET", 12500.0))
    x_current = _setting_float("X_CASH_POOL_CURRENT", _setting_float("ANNUAL_CASH_CURRENT", 0.0))
    z_target_default = max(float(allocation.equity or 0.0) * _setting_float("Z_BOND_POOL_TARGET_PCT", 0.15), 0.0)
    z_target = _setting_float("Z_BOND_POOL_TARGET", z_target_default)
    z_current = _setting_float("Z_BOND_POOL_CURRENT", 0.0)
    defensive_pools = {
        "X": {
            "label": "现金底仓",
            "current": x_current,
            "target": x_target,
            "available": max(0.0, x_target - x_current),
            "base_percent": 0.0,
            "risk_percent": 1.0,
            "note": "最低现金安全垫",
        },
        "Z": {
            "label": "国债底仓",
            "current": z_current,
            "target": z_target,
            "available": max(0.0, z_target - z_current),
            "base_percent": z_target / allocation.equity if allocation.equity > 0 else 0.0,
            "risk_percent": 1.0,
            "note": "短债/货币类慢现金",
        },
    }
    return {
        "ok": True,
        "mode": allocation.mode,
        "mode_label": CAPITAL_MODE_LABELS.get(allocation.mode, allocation.mode),
        "allocation_month": allocation.allocation_month,
        "equity": allocation.equity,
        "buying_power": allocation.buying_power,
        "cash": allocation.cash,
        "portfolio_value": allocation.portfolio_value,
        "account_blocked": allocation.account_blocked,
        "trading_blocked": allocation.trading_blocked,
        "trade_suspended_by_user": allocation.trade_suspended_by_user,
        "base_total": base_total,
        "usable_total": usable_total,
        "margin_summary": margin_summary,
        "used_total": used_total,
        "total_risk_percent": allocation.total_risk_percent,
        "margin_usage_percent": margin_usage,
        "margin_usage_mode": margin_mode,
        "margin_usage_reason": margin_reason,
        "market_exposure_percent": allocation.total_risk_percent / margin_usage if margin_usage > 0 else 0.0,
        "targets": {
            "A": allocation.A_target,
            "B": allocation.B_target,
            "C": allocation.C_target,
            "D": allocation.D_target,
        },
        "base_targets": allocation.base_targets,
        "base_percents": allocation.base_percents,
        "pool_risk_percents": allocation.pool_risk_percents,
        "pool_enabled": allocation.pool_enabled,
        "pool_brokers": allocation.pool_brokers,
        "broker_snapshots": allocation.broker_snapshots,
        "used": used,
        "available": available,
        "defensive_pools": defensive_pools,
        "annual_goals": _annual_goals_payload(allocation),
    }


def _setting_float(key: str, default: float) -> float:
    raw = get_app_setting(key, env_str(key, str(default)))
    try:
        return float(raw or default)
    except Exception:
        return default


def _weekly_goal_key() -> str:
    """按洛杉矶时间生成每周任务 key，新的一周自动重置。"""
    now = _now_market_tz()
    year, week, _weekday = now.isocalendar()
    return f"{year}-W{week:02d}"


def _ensure_weekly_goal_reset() -> None:
    current_key = _weekly_goal_key()
    stored_key = get_app_setting("WEEKLY_GOALS_WEEK_KEY", "")
    if stored_key == current_key:
        return
    set_app_setting("WEEKLY_FITNESS_CURRENT", "0")
    set_app_setting("WEEKLY_WORDS_CURRENT", "0")
    set_app_setting("WEEKLY_GOALS_WEEK_KEY", current_key)


def _annual_goals_payload(allocation) -> list[dict]:
    """年度任务完成进度。金额类任务可通过 app_settings 或同名环境变量覆盖。"""
    _ensure_weekly_goal_reset()

    retirement_target = _setting_float("ANNUAL_RETIREMENT_TARGET", 7500.0)
    retirement_current = _setting_float("ANNUAL_RETIREMENT_CURRENT", 0.0)

    cash_target = _setting_float("ANNUAL_CASH_MIN_TARGET", 12500.0)
    cash_current = _setting_float("ANNUAL_CASH_CURRENT", 0.0)

    return_target = _setting_float("ANNUAL_STOCK_RETURN_TARGET", 0.30)
    start_equity = _setting_float("ANNUAL_STOCK_START_EQUITY", 0.0)
    equity = float(allocation.equity or 0.0)
    if start_equity <= 0 and equity > 0:
        start_equity = equity
        set_app_setting("ANNUAL_STOCK_START_EQUITY", f"{start_equity:.2f}")
    return_current = ((equity - start_equity) / start_equity) if start_equity > 0 else 0.0
    stock_completions = int(_setting_float("ANNUAL_STOCK_COMPLETIONS", 0.0))
    if return_target > 0 and return_current >= return_target and equity > 0 and start_equity > 0:
        stock_completions += 1
        set_app_setting("ANNUAL_STOCK_COMPLETIONS", str(stock_completions))
        set_app_setting("ANNUAL_STOCK_LAST_COMPLETED_AT", _now_market_tz().date().isoformat())
        set_app_setting("ANNUAL_STOCK_LAST_COMPLETED_EQUITY", f"{equity:.2f}")
        set_app_setting("ANNUAL_STOCK_START_EQUITY", f"{equity:.2f}")
        start_equity = equity
        return_current = 0.0

    weekly_fitness_target = _setting_float("WEEKLY_FITNESS_TARGET", 4.0)
    weekly_fitness_current = _setting_float("WEEKLY_FITNESS_CURRENT", 0.0)
    weekly_words_target = _setting_float("WEEKLY_WORDS_TARGET", 50.0)
    weekly_words_current = _setting_float("WEEKLY_WORDS_CURRENT", 0.0)

    return [
        {
            "key": "stock_growth",
            "name": "股票账户跃迁",
            "desc": f"年度回报目标 30% · 已完成 {stock_completions} 次",
            "current": return_current,
            "target": return_target,
            "unit": "percent",
            "start_equity": start_equity,
            "equity": equity,
            "completed_count": stock_completions,
            "status_label": f"第 {stock_completions + 1} 轮",
        },
        {
            "key": "cash_guard",
            "name": "现金安全垫",
            "desc": f"最低保留 ${cash_target:,.0f}",
            "current": cash_current,
            "target": cash_target,
            "unit": "money",
            "step": 500,
            "action_label": "+500",
        },
        {
            "key": "retirement",
            "name": "退休金满额计划",
            "desc": f"目标存满 ${retirement_target:,.0f}",
            "current": retirement_current,
            "target": retirement_target,
            "unit": "money",
            "step": 500,
            "action_label": "+500",
        },
        {
            "key": "fitness",
            "name": "体能基石计划",
            "desc": "每周 3 次健身房 + 1 次 10 公里",
            "current": weekly_fitness_current,
            "target": weekly_fitness_target,
            "unit": "count",
            "suffix": "次",
            "step": 1,
            "action_label": "+",
        },
        {
            "key": "vocabulary",
            "name": "词汇复利计划",
            "desc": "每周记 50 个单词",
            "current": weekly_words_current,
            "target": weekly_words_target,
            "unit": "count",
            "suffix": "个",
            "step": 10,
            "action_label": "+10",
        },
    ]


def _advance_annual_goal(goal_key: str) -> dict:
    """推进可手动打卡的年度任务。"""
    _ensure_weekly_goal_reset()

    specs = {
        "retirement": ("ANNUAL_RETIREMENT_CURRENT", "ANNUAL_RETIREMENT_TARGET", 500.0),
        "cash_guard": ("ANNUAL_CASH_CURRENT", "ANNUAL_CASH_MIN_TARGET", 500.0),
        "fitness": ("WEEKLY_FITNESS_CURRENT", "WEEKLY_FITNESS_TARGET", 1.0),
        "vocabulary": ("WEEKLY_WORDS_CURRENT", "WEEKLY_WORDS_TARGET", 10.0),
    }
    if goal_key not in specs:
        return {"ok": False, "error": "不支持的年度任务"}
    current_key, target_key, step = specs[goal_key]
    current = _setting_float(current_key, 0.0)
    target = _setting_float(target_key, 0.0)
    next_value = current + step
    if target > 0:
        next_value = min(next_value, target)
    set_app_setting(current_key, str(int(next_value) if float(next_value).is_integer() else next_value))
    return {"ok": True, "goal": goal_key, "current": next_value, "target": target}


def _parse_margin_usage_setting() -> float:
    return resolve_margin_usage_pct()[1]


def _risk_payload() -> dict:
    """组装风控接口数据。"""
    state = get_risk_state()
    return {
        "enabled": state.enabled,
        "mode": state.mode,
        "mode_label": CAPITAL_MODE_LABELS.get(state.mode, state.mode),
        "daily_pnl_pct": state.daily_pnl_pct,
        "loss_days": state.loss_days,
        "max_drawdown": state.max_drawdown,
        "risk_multiplier": state.risk_multiplier,
        "block_all_new": state.block_all_new,
        "block_a": state.block_a,
        "block_b": state.block_b,
        "block_c": state.block_c,
        "block_d": state.block_d,
        "suggest_mode": state.suggest_mode,
        "reason": state.reason,
        "market_trend": state.market_trend,
        "market_reason": state.market_reason,
        "qqq_price": state.qqq_price,
        "qqq_change_pct": state.qqq_change_pct,
        "vix": state.vix,
        "risk_preference": state.risk_preference,
        "allocation_mode": state.allocation_mode,
        "recommended_exposure": state.recommended_exposure,
        "recommended_weights": state.recommended_weights or {},
        "account_metrics_source": state.account_metrics_source,
        "vix_source": state.vix_source,
    }


STRATEGY_2_CONFIG_KEY = "STRATEGY_2_CONFIG"


STRATEGY_2_DEFAULT_CONFIG = {
    "version": "2.0",
    "capital": {
        "title": "A 养老金 + B 自动策略 + C 长期股票 + D 日内交易",
        "desc": "A 用养老金账户配置 50% 基金和 50% 主题股票；B 自动动量；C 按 28 个长期标的自动建仓并做 T；D 做日内交易。",
        "rules": [
            {"key": "a_capital_role", "label": "A 职责", "value": "养老金账户/长期定投", "unit": "", "enabled": True},
            {"key": "b_capital_role", "label": "B 职责", "value": "Alpaca/策略B自动执行", "unit": "", "enabled": True},
            {"key": "c_capital_role", "label": "C 职责", "value": "Alpaca/长期股票", "unit": "", "enabled": True},
            {"key": "d_capital_role", "label": "D 职责", "value": "Alpaca/日内交易", "unit": "", "enabled": True},
            {"key": "auto_execute", "label": "机器人自动执行", "value": "B/C/D 自动，A 按月", "unit": "", "enabled": True},
        ],
    },
    "strategies": [
        {
            "key": "A",
            "name": "A 养老金长期定投",
            "broker": "Alpaca 养老金账户",
            "capital": "A 资金池",
            "mission": "负责养老金账户长期定投，并以严格频率限制围绕核心仓做 T；不参与 B 动量和 D 日内交易。",
            "select_rules": [
                {"key": "a_stock_type", "label": "长期核心标记", "value": "stock_type=A", "unit": "", "enabled": True},
                {"key": "a_market_filter", "label": "市场环境过滤", "value": "向上/横盘优先", "unit": "", "enabled": True},
                {"key": "a_rebalance_source", "label": "资金来源", "value": "A 养老金账户", "unit": "", "enabled": True},
            ],
            "buy_rules": [
                {"key": "a_buy_style", "label": "买入方式", "value": "每月15号按目标缺口补仓", "unit": "", "enabled": True},
                {"key": "a_position_role", "label": "仓位角色", "value": "长期核心仓", "unit": "", "enabled": True},
                {"key": "a_t_frequency", "label": "做T频率", "value": "A全账户每日最多1轮", "unit": "", "enabled": True},
                {"key": "a_t_serial", "label": "循环约束", "value": "上一轮闭环后才可开启下一轮", "unit": "", "enabled": True},
            ],
            "sell_rules": [
                {"key": "a_sell_trigger", "label": "卖出触发", "value": "再平衡/风险关闭", "unit": "", "enabled": True},
                {"key": "a_hold_horizon", "label": "持有周期", "value": "长期", "unit": "", "enabled": True},
                {"key": "a_options_block", "label": "禁止期权", "value": "是", "unit": "", "enabled": True},
            ],
        },
        {
            "key": "B",
            "name": "B 股票动量",
            "broker": "Alpaca",
            "capital": "策略不变",
            "mission": "从强势股票里筛出确认度足够的进攻买点；为了省精力，主流程使用 Alpaca 券商数据自动执行，买入后由 B 卖出机器人接管止损和分批止盈。",
            "select_rules": [
                {"key": "b_min_up_pct", "label": "日内最低涨幅", "value": 3, "unit": "%", "enabled": True},
                {"key": "b_max_buy_up_pct", "label": "买入最高涨幅", "value": 10, "unit": "%", "enabled": True},
                {"key": "b_min_price", "label": "最低股价", "value": 5, "unit": "$", "enabled": True},
                {"key": "b_score_top_n", "label": "每轮 Top N", "value": 3, "unit": "只", "enabled": True},
                {"key": "b_score_confirmations", "label": "Top 确认次数", "value": 3, "unit": "次", "enabled": True},
            ],
            "buy_rules": [
                {"key": "b_buy_window", "label": "买入窗口", "value": "06:50-10:40", "unit": "LA", "enabled": True},
                {"key": "b_max_positions", "label": "最大持仓数", "value": 4, "unit": "只", "enabled": True},
                {"key": "b_trade_notional", "label": "单笔目标金额", "value": 2500, "unit": "$", "enabled": True},
            ],
            "sell_rules": [
                {"key": "b_initial_stop", "label": "初始止损", "value": 2, "unit": "%", "enabled": True},
                {"key": "b_trailing_stop", "label": "动态止损", "value": "按 stage 阶梯收紧", "unit": "", "enabled": True},
                {"key": "b_take_profit", "label": "分批止盈", "value": "盈利后阶梯减仓", "unit": "", "enabled": True},
            ],
        },
        {
            "key": "C",
            "name": "C 长期股票",
            "broker": "Alpaca",
            "capital": "长期资金",
            "mission": "B/C/D 在原保证金账户内按 4:4:2 分配。C 有可用现金时按目标权重自动补仓，随后只用完整股做日内 T，碎股始终留在长期核心仓。",
            "select_rules": [
                {"key": "c_universe", "label": "长期预选池", "value": "30 只（25只股票+5只ETF）", "unit": "", "enabled": True},
                {"key": "c_foundation", "label": "多元底仓", "value": "QQQ 10% / VOO 9% / XLV 6% / IAU 3% / IBIT 2%", "unit": "", "enabled": True},
                {"key": "c_fill_order", "label": "建仓顺序", "value": "指数底仓 → 核心龙头 → 成长卫星", "unit": "", "enabled": True},
                {"key": "c_stock_type", "label": "持仓归属", "value": "stock_type=C / capital_pool=C", "unit": "", "enabled": True},
                {"key": "c_up_trigger", "label": "上涨做T触发", "value": 1, "unit": "%", "enabled": True},
                {"key": "c_down_trigger", "label": "下跌做T触发", "value": 1, "unit": "%", "enabled": True},
            ],
            "buy_rules": [
                {"key": "c_auto_core_buy", "label": "自动建仓", "value": "由环境变量控制", "unit": "", "enabled": True},
                {"key": "c_core_buy_window", "label": "自动买入窗口", "value": "06:40-12:30", "unit": "LA", "enabled": True},
                {"key": "c_daily_budget", "label": "每日建仓额度", "value": "C目标资金10%，最多$250", "unit": "", "enabled": True},
                {"key": "c_order_count", "label": "每轮最多下单", "value": 3, "unit": "笔", "enabled": True},
                {"key": "c_order_type", "label": "建仓订单", "value": "实时价 DAY 限价碎股单", "unit": "", "enabled": True},
                {"key": "c_rebound_buy", "label": "低开/下跌反弹买回", "value": 1, "unit": "%", "enabled": True},
                {"key": "c_buy_limit_buffer", "label": "买入限价缓冲", "value": 0.2, "unit": "%", "enabled": True},
                {"key": "c_min_hold_minutes", "label": "单腿最短持有", "value": 30, "unit": "分钟", "enabled": True},
            ],
            "sell_rules": [
                {"key": "c_up_pullback_sell", "label": "上涨回落卖新增仓", "value": 1, "unit": "%", "enabled": True},
                {"key": "c_gap_pullback_sell", "label": "高开回撤临时卖出", "value": 1, "unit": "%", "enabled": True},
                {"key": "c_force_recover", "label": "收盘前强制恢复/平新增仓", "value": "12:50", "unit": "LA", "enabled": True},
                {"key": "c_sell_limit_buffer", "label": "卖出限价缓冲", "value": 0.2, "unit": "%", "enabled": True},
            ],
        },
        {
            "key": "D",
            "name": "D 日内交易",
            "broker": "Alpaca 原保证金账户",
            "capital": "独立日内额度",
            "mission": "负责日内交易，额度独立管理，收盘前优先降低隔夜风险。",
            "select_rules": [
                {"key": "d_intraday_candidates", "label": "日内股票候选", "value": "强趋势/高流动性", "unit": "", "enabled": True},
                {"key": "d_option_underlying", "label": "期权标的", "value": "QQQ/SPY/高流动性", "unit": "", "enabled": True},
                {"key": "d_market_phase", "label": "交易阶段", "value": "盘中确认", "unit": "", "enabled": True},
            ],
            "buy_rules": [
                {"key": "d_daytrade_window", "label": "日内开仓窗口", "value": "开盘后确认", "unit": "LA", "enabled": True},
                {"key": "d_option_spread", "label": "期权结构", "value": "价差优先", "unit": "", "enabled": True},
                {"key": "d_max_risk", "label": "单笔风险", "value": "按D额度限制", "unit": "", "enabled": True},
            ],
            "sell_rules": [
                {"key": "d_flatten_intraday", "label": "日内收尾", "value": "收盘前降风险", "unit": "", "enabled": True},
                {"key": "d_stop_loss", "label": "止损", "value": "预设价/时间止损", "unit": "", "enabled": True},
                {"key": "d_take_profit", "label": "止盈", "value": "分批/结构退出", "unit": "", "enabled": True},
            ],
        },
    ],
}


def _deep_merge_strategy_config(default: dict, saved: dict) -> dict:
    if not isinstance(saved, dict):
        return default
    merged = json.loads(json.dumps(default, ensure_ascii=False))
    if isinstance(saved.get("capital"), dict):
        merged["capital"].update(saved["capital"])
    saved_by_key = {str(s.get("key")): s for s in saved.get("strategies", []) if isinstance(s, dict)}
    for strategy in merged["strategies"]:
        override = saved_by_key.get(strategy["key"])
        if not override:
            continue
        if strategy["key"] == "C" and "期权" in json.dumps(override, ensure_ascii=False):
            continue
        for field in ("name", "broker", "capital", "mission"):
            if field in override:
                strategy[field] = override[field]
        for section in ("select_rules", "buy_rules", "sell_rules"):
            override_rules = {str(r.get("key")): r for r in override.get(section, []) if isinstance(r, dict)}
            for rule in strategy.get(section, []):
                if rule.get("key") in override_rules:
                    rule.update(override_rules[rule["key"]])
    return merged


def _strategy_2_config_payload() -> dict:
    raw = get_app_setting(STRATEGY_2_CONFIG_KEY, "")
    try:
        saved = json.loads(raw) if raw else {}
    except Exception:
        saved = {}
    config = _deep_merge_strategy_config(STRATEGY_2_DEFAULT_CONFIG, saved)
    for strategy in config.get("strategies", []):
        if strategy.get("key") != "C":
            continue
        runtime_values = {
            "c_auto_core_buy": "已开启" if env_bool("C_CORE_AUTO_BUY_ENABLED", False) else "已关闭",
            "c_daily_budget": (
                f"C可用资金 {env_float('C_CORE_DAILY_BUDGET_PCT', 1.0):.0%}，"
                + (
                    f"最多 ${env_float('C_CORE_DAILY_BUDGET_MAX_USD', 0.0):,.0f}"
                    if env_float('C_CORE_DAILY_BUDGET_MAX_USD', 0.0) > 0
                    else "不设每日金额上限"
                )
            ),
            "c_order_count": env_int("C_CORE_MAX_ORDERS_PER_RUN", 3),
        }
        for rule in strategy.get("buy_rules", []):
            if rule.get("key") in runtime_values:
                rule["value"] = runtime_values[rule["key"]]
    return {"ok": True, "config": config}


def _strategy_b_config_payload() -> dict:
    """读取策略 B 当前真实买卖参数，给配置页展示。"""
    try:
        from app.strategy_b import get_strategy_b_runtime_config

        return {"ok": True, "config": get_strategy_b_runtime_config()}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _account_config_payload() -> dict:
    payload = public_account_config()
    payload["monthly_invest"] = load_monthly_invest_config()
    return payload


def _save_account_config_payload(payload: dict) -> dict:
    config = load_account_config()
    incoming = payload.get("config") if isinstance(payload.get("config"), dict) else {}
    profiles_in = incoming.get("profiles") if isinstance(incoming.get("profiles"), dict) else {}
    for key, profile in profiles_in.items():
        if not isinstance(profile, dict):
            continue
        current_profile = (config.get("profiles") or {}).setdefault(
            key,
            {"label": key, "mode": "paper", "key_id": "", "secret_key": "", "base_url": "", "env_prefix": str(key).upper()},
        )
        for field in ("label", "mode", "base_url", "env_prefix"):
            if field in profile:
                current_profile[field] = str(profile.get(field) or "").strip()
        if str(profile.get("key_id") or "").strip():
            current_profile["key_id"] = str(profile.get("key_id") or "").strip()
        if str(profile.get("secret_key") or "").strip():
            current_profile["secret_key"] = str(profile.get("secret_key") or "").strip()
    if isinstance(incoming.get("pool_profiles"), dict):
        config["pool_profiles"].update({str(k).upper(): str(v).strip() for k, v in incoming["pool_profiles"].items()})
    if str(incoming.get("active_profile") or "").strip():
        config["active_profile"] = str(incoming.get("active_profile")).strip()
    saved = save_account_config(config)
    monthly = payload.get("monthly_invest")
    if isinstance(monthly, dict):
        save_monthly_invest_config(monthly)
    public = public_account_config()
    public["monthly_invest"] = load_monthly_invest_config()
    public["saved"] = True
    public["active_profile"] = saved.get("active_profile")
    return public


def _save_strategy_2_config(payload: dict) -> dict:
    config = payload.get("config")
    if not isinstance(config, dict):
        return {"ok": False, "error": "配置格式错误"}
    set_app_setting(STRATEGY_2_CONFIG_KEY, json.dumps(config, ensure_ascii=False))
    return {"ok": True, "config": config}


def _event_date(value) -> date | None:
    try:
        return date.fromisoformat(str(value or "")[:10])
    except Exception:
        return None


def _major_events_payload() -> dict:
    """从本地 CSV 读取未来 10 个重大事件。"""
    today = date.today()
    csv_path = Path(env_str("MAJOR_EVENTS_CSV", "ultimate_v1/strategies/major_events.csv"))
    if not csv_path.is_absolute():
        csv_path = Path.cwd() / csv_path
    events: list[dict] = []
    if not csv_path.exists():
        return {
            "ok": True,
            "rows": [],
            "message": f"请在 {csv_path.relative_to(Path.cwd()) if csv_path.is_relative_to(Path.cwd()) else csv_path} 里维护重大事件",
            "path": str(csv_path),
        }

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            event_day = _event_date(row.get("date"))
            if not event_day or event_day < today:
                continue
            events.append({
                "date": event_day.isoformat(),
                "type": str(row.get("type") or "事件").strip(),
                "title": str(row.get("title") or "").strip(),
                "symbol": str(row.get("symbol") or "").strip().upper(),
                "impact": str(row.get("impact") or "").strip(),
                "source": str(row.get("source") or "manual").strip(),
            })
    events.sort(key=lambda e: (e.get("date") or "9999-12-31", {"宏观": 0, "IPO": 1, "财报": 2, "个股": 3}.get(e.get("type"), 9), e.get("symbol") or ""))
    return {"ok": True, "rows": events[:10], "path": str(csv_path)}


def _ensure_stock_quote_cache() -> None:
    """缓存本地日线缺失的观察票价格，主要补 ETF/ADR/OTC 代码。"""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_quote_cache (
                  symbol VARCHAR(64) PRIMARY KEY,
                  current_price DOUBLE NULL,
                  prev_close DOUBLE NULL,
                  day_change_pct DOUBLE NULL,
                  source VARCHAR(64) NULL,
                  fetched_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )


def _quote_cache(symbols: list[str]) -> dict[str, dict]:
    _ensure_stock_quote_cache()
    symbols = sorted({s.strip().upper() for s in symbols if s})
    if not symbols:
        return {}
    placeholders = ", ".join(["%s"] * len(symbols))
    rows = fetch_all(
        f"""
        SELECT symbol, current_price, prev_close, day_change_pct, source, fetched_at
        FROM stock_quote_cache
        WHERE symbol IN ({placeholders})
          AND fetched_at >= DATE_SUB(NOW(), INTERVAL 30 MINUTE)
        """,
        tuple(symbols),
    )
    return {str(r.get("symbol") or "").upper(): r for r in rows}


def _write_quote_cache(cur, symbol: str, current: float, prev: float, source: str) -> None:
    """写入持仓表现价缓存。"""
    change_pct = (current - prev) / prev if current > 0 and prev > 0 else None
    cur.execute(
        """
        INSERT INTO stock_quote_cache
            (symbol, current_price, prev_close, day_change_pct, source, fetched_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
        ON DUPLICATE KEY UPDATE
            current_price=VALUES(current_price),
            prev_close=VALUES(prev_close),
            day_change_pct=VALUES(day_change_pct),
            source=VALUES(source),
            fetched_at=NOW(),
            updated_at=NOW()
        """,
        (symbol, current, prev or None, change_pct, source),
    )


def _refresh_missing_quotes(symbols: list[str]) -> None:
    """本地日线没有价格时，用 Yahoo Finance 补现价/昨收。"""
    global _QUOTE_REFRESH_TS
    if time.time() - _QUOTE_REFRESH_TS < 120:
        return

    _QUOTE_REFRESH_TS = time.time()
    max_refresh = int(env_str("HOLDINGS_QUOTE_REFRESH_LIMIT", "12") or "12")
    targets = [s for s in sorted({v.strip().upper() for v in symbols if v})][:max_refresh]
    if not targets:
        return

    try:
        from .yahoo_market_data import get_yahoo_stock_quote
    except Exception as exc:
        print(f"[HOLDINGS QUOTE] Yahoo quote module unavailable: {exc}", flush=True)
        return

    with db_conn() as conn:
        with conn.cursor() as cur:
            for symbol in targets:
                try:
                    quote = get_yahoo_stock_quote(symbol)
                    current = float(quote.last or 0)
                    prev = float(quote.prev_close or quote.regular_close or 0)
                    if current > 0:
                        _write_quote_cache(cur, symbol, current, prev, quote.source)
                except Exception as exc:
                    print(f"[HOLDINGS QUOTE] {symbol} Yahoo quote failed: {exc}", flush=True)


def _latest_price_meta(symbols: list[str]) -> dict[str, dict]:
    """从本地日线取最新收盘和上一交易日收盘，用来补未持仓股票的现价/日涨跌。"""
    symbols = sorted({s.strip().upper() for s in symbols if s})
    if not symbols:
        return {}
    placeholders = ", ".join(["%s"] * len(symbols))
    rows = fetch_all(
        f"""
        SELECT symbol, `date`, `close`, rn
        FROM (
            SELECT UPPER(symbol) AS symbol, `date`, `close`,
                   ROW_NUMBER() OVER (PARTITION BY UPPER(symbol) ORDER BY `date` DESC) AS rn
            FROM stock_prices_pool
            WHERE UPPER(symbol) IN ({placeholders})
              AND `close` IS NOT NULL
        ) x
        WHERE rn <= 2
        ORDER BY symbol, rn
        """,
        tuple(symbols),
    )
    out: dict[str, dict] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        bucket = out.setdefault(symbol, {})
        if int(row.get("rn") or 0) == 1:
            bucket["latest_close"] = _safe_float(row.get("close"))
            bucket["latest_date"] = row.get("date")
        elif int(row.get("rn") or 0) == 2:
            bucket["prev_close"] = _safe_float(row.get("close"))
    for bucket in out.values():
        latest = _safe_float(bucket.get("latest_close"))
        prev = _safe_float(bucket.get("prev_close"))
        bucket["day_change_pct"] = (latest - prev) / prev if latest > 0 and prev > 0 else None
    cached = _quote_cache(symbols)
    stale_symbols = [symbol for symbol in symbols if symbol not in cached]
    _refresh_missing_quotes(stale_symbols)
    if stale_symbols:
        cached = _quote_cache(symbols)
    for symbol, row in cached.items():
        current = _safe_float(row.get("current_price"))
        prev = _safe_float(row.get("prev_close"))
        if current <= 0:
            continue
        bucket = out.setdefault(symbol, {})
        bucket["latest_close"] = current
        if prev > 0:
            bucket["prev_close"] = prev
        else:
            prev = _safe_float(bucket.get("prev_close"))
        bucket["day_change_pct"] = row.get("day_change_pct") if row.get("day_change_pct") is not None else ((current - prev) / prev if current > 0 and prev > 0 else bucket.get("day_change_pct"))
        bucket["latest_date"] = row.get("fetched_at")
    return out


def _enrich_holdings_rows(rows: list[dict]) -> list[dict]:
    """给持仓/观察票补日涨跌和现价；未买入股票也能看到行情状态。"""
    symbols = [str(row.get("symbol") or "").strip().upper() for row in rows]
    price_meta = _latest_price_meta(symbols)
    c_flags: dict[str, dict] = {}
    clean_symbols = sorted({symbol for symbol in symbols if symbol})
    if clean_symbols:
        placeholders = ", ".join(["%s"] * len(clean_symbols))
        try:
            flag_rows = fetch_all(
                f"""
                SELECT id AS operation_id,
                       UPPER(stock_code) AS symbol,
                       ac_t_type,
                       ac_t_enabled,
                       ac_t_state,
                       ac_t_core_qty,
                       ac_t_qty,
                       ac_t_temporarily_out,
                       updated_at
                FROM stock_operations
                WHERE UPPER(stock_code) IN ({placeholders})
                  AND UPPER(COALESCE(NULLIF(strategy_group,''), stock_type))='C'
                ORDER BY
                  CASE WHEN UPPER(COALESCE(NULLIF(ac_t_type,''), ''))='C' AND COALESCE(ac_t_enabled,0)=1 THEN 0 ELSE 1 END,
                  id DESC
                """,
                tuple(clean_symbols),
            )
            for flag in flag_rows:
                symbol = str(flag.get("symbol") or "").strip().upper()
                if symbol and symbol not in c_flags:
                    c_flags[symbol] = flag
        except Exception as exc:
            print(f"[WEB HOLDINGS] C AC_T flags unavailable: {exc}", flush=True)

    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        meta = price_meta.get(symbol) or {}
        latest = _safe_float(meta.get("latest_close"))
        current = latest or _safe_float(row.get("current_price"))
        prev = _safe_float(meta.get("prev_close"))
        day_change_pct = (current - prev) / prev if current > 0 and prev > 0 else meta.get("day_change_pct")
        qty = _safe_float(row.get("qty"))

        row["symbol"] = symbol
        row["current_price"] = current
        row["day_change_pct"] = day_change_pct
        row["price_as_of"] = meta.get("latest_date")
        flag = c_flags.get(symbol) or {}
        if str(row.get("strategy_group") or "").strip().upper() == "C":
            row["operation_id"] = row.get("operation_id") or flag.get("operation_id")
            row["ac_t_type"] = flag.get("ac_t_type")
            row["ac_t_enabled"] = int(_safe_float(flag.get("ac_t_enabled")))
            row["ac_t_state"] = flag.get("ac_t_state") or ""
            row["ac_t_core_qty"] = int(_safe_float(flag.get("ac_t_core_qty")))
            row["ac_t_qty"] = int(_safe_float(flag.get("ac_t_qty")))
            row["ac_t_temporarily_out"] = int(_safe_float(flag.get("ac_t_temporarily_out")))
        if _safe_float(row.get("market_value")) <= 0 and qty > 0 and current > 0:
            row["market_value"] = qty * current
    return rows


def _holdings_payload() -> dict:
    """读取持仓展示表，供前端表格渲染。"""
    rows = fetch_all(
        """
        SELECT symbol, normalized_group AS strategy_group, stock_type, status, qty,
               initial_entry_price, avg_entry_price,
               (
                   SELECT so.trigger_price
                   FROM stock_operations so
                   WHERE UPPER(so.stock_code)=UPPER(ranked.symbol)
                     AND UPPER(COALESCE(NULLIF(so.strategy_group,''), so.stock_type))=UPPER(ranked.normalized_group)
                     AND so.trigger_price IS NOT NULL
                   ORDER BY so.id DESC
                   LIMIT 1
               ) AS trigger_price,
               current_price, market_value, cost_basis, unrealized_pnl,
               unrealized_pnl_pct, realized_pnl, entry_time, exit_time,
               holding_days, stop_loss_price, take_profit_price, b_stage,
               capital_pool, margin_used, last_order_side, last_update_time,
               'position_holdings' AS row_source,
               CASE
                   WHEN LOWER(status) = 'open' AND ABS(COALESCE(qty, 0)) > 0 THEN 1
                   ELSE 0
               END AS is_bought
        FROM (
            SELECT h.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY UPPER(symbol), normalized_group
                       ORDER BY FIELD(status, 'open', 'needs_review', 'closed'),
                                ABS(COALESCE(qty, 0)) DESC,
                                id DESC
                   ) AS rn
            FROM (
                SELECT position_holdings.*,
                       CASE
                           WHEN strategy_group IN ('A','B','C','D','F') THEN strategy_group
                           WHEN stock_type IN ('A','B','C','D','F') THEN stock_type
                           ELSE strategy_group
                       END AS normalized_group
                FROM position_holdings
            ) h
        ) ranked
        WHERE rn=1
          AND ABS(COALESCE(qty, 0)) > 0
        ORDER BY FIELD(status, 'open', 'needs_review', 'closed'), strategy_group, symbol
        LIMIT 500
        """
    )
    holdings = list(rows or [])
    held_keys = {
        (str(row.get("symbol") or "").strip().upper(), str(row.get("strategy_group") or "").strip().upper())
        for row in holdings
        if str(row.get("status") or "").lower() == "open" and _safe_float(row.get("qty")) > 0
    }
    try:
        pool_rows = fetch_all(
            """
            SELECT id AS operation_id,
                   UPPER(stock_code) AS symbol,
                   CASE
                       WHEN COALESCE(NULLIF(strategy_group,''), stock_type) IN ('A','B','C','D','F')
                           THEN COALESCE(NULLIF(strategy_group,''), stock_type)
                       ELSE stock_type
                   END AS strategy_group,
                   stock_type,
                   CASE
                       WHEN UPPER(stock_type)='C' THEN 'needs_review'
                       ELSE 'candidate'
                   END AS status,
                   0 AS qty,
                   trigger_price,
                   COALESCE(trigger_price, entry_open, close_price) AS initial_entry_price,
                   COALESCE(cost_price, trigger_price, entry_open, close_price) AS avg_entry_price,
                   COALESCE(current_price, close_price, trigger_price, entry_close) AS current_price,
                   0 AS market_value,
                   0 AS cost_basis,
                   0 AS unrealized_pnl,
                   0 AS unrealized_pnl_pct,
                   0 AS realized_pnl,
                   entry_date AS entry_time,
                   NULL AS exit_time,
                   0 AS holding_days,
                   stop_loss_price,
                   take_profit_price,
                   b_stage,
                   capital_pool,
                   margin_used,
                   last_order_side,
                   updated_at AS last_update_time,
                   'stock_operations' AS row_source,
                   can_buy,
                   can_sell,
                   is_bought
            FROM stock_operations
            WHERE COALESCE(is_bought, 0)=0
              AND COALESCE(can_buy, 1)=1
            ORDER BY FIELD(strategy_group, 'A','B','C','D','F'), stock_type, updated_at DESC, id DESC
            LIMIT 500
            """
        )
        for row in pool_rows:
            key = (
                str(row.get("symbol") or "").strip().upper(),
                str(row.get("strategy_group") or "").strip().upper(),
            )
            if key in held_keys:
                continue
            holdings.append(row)
    except Exception as exc:
        print(f"[WEB HOLDINGS] stock_operations candidate pool unavailable: {exc}", flush=True)
    try:
        d_rows = fetch_all(
            """
            SELECT d.symbol,
                   'D' AS strategy_group,
                   'D' AS stock_type,
                   'candidate' AS status,
                   0 AS qty,
                   d.signal_close AS trigger_price,
                   d.signal_close AS initial_entry_price,
                   d.signal_close AS avg_entry_price,
                   d.signal_close AS current_price,
                   0 AS market_value,
                   0 AS cost_basis,
                   0 AS unrealized_pnl,
                   0 AS unrealized_pnl_pct,
                   0 AS realized_pnl,
                   d.signal_date AS entry_time,
                   NULL AS exit_time,
                   0 AS holding_days,
                   NULL AS stop_loss_price,
                   NULL AS take_profit_price,
                   0 AS b_stage,
                   'D' AS capital_pool,
                   0 AS margin_used,
                   NULL AS last_order_side,
                   d.updated_at AS last_update_time,
                   'd_candidate_pool' AS row_source,
                   0 AS is_bought,
                   d.base_score AS d_candidate_score,
                   d.signal_gain_pct AS d_signal_gain_pct,
                   d.signal_volume AS d_signal_volume,
                   CASE WHEN COALESCE(s.enabled, 0)=1 THEN 1 ELSE 0 END AS d_selected,
                   COALESCE(c.state, 'IDLE') AS d_cycle_state
            FROM d_candidate_pool d
            LEFT JOIN d_grid_symbols s ON s.symbol=d.symbol
            LEFT JOIN d_grid_cycles c ON c.symbol=d.symbol
            WHERE d.enabled=1
            ORDER BY d.base_score DESC, d.signal_dollar_volume DESC, d.symbol
            """
        )
        existing_d = {
            str(row.get("symbol") or "").strip().upper()
            for row in holdings
            if str(row.get("strategy_group") or "").strip().upper() == "D"
        }
        d_meta = {str(row.get("symbol") or "").strip().upper(): row for row in d_rows}
        for row in holdings:
            if str(row.get("strategy_group") or "").strip().upper() != "D":
                continue
            meta = d_meta.get(str(row.get("symbol") or "").strip().upper()) or {}
            row["d_selected"] = int(meta.get("d_selected") or 0)
            row["d_cycle_state"] = meta.get("d_cycle_state") or "IDLE"
            row["d_candidate_score"] = meta.get("d_candidate_score")
        for row in d_rows:
            symbol = str(row.get("symbol") or "").strip().upper()
            if symbol and symbol not in existing_d:
                holdings.append(row)
    except Exception as exc:
        print(f"[WEB HOLDINGS] D candidate pool unavailable: {exc}", flush=True)
    return {"ok": True, "rows": _enrich_holdings_rows(holdings)}


def _delete_stock_pool_payload(payload: dict) -> dict:
    """手动从 stock_operations 删除不符合要求的入选池股票。"""
    try:
        operation_id = int(payload.get("operation_id") or 0)
    except Exception:
        operation_id = 0
    if operation_id <= 0:
        return {"ok": False, "error": "缺少入选池记录 id"}
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, stock_code, COALESCE(NULLIF(strategy_group,''), stock_type) AS strategy_group,
                       is_bought, can_buy
                FROM stock_operations
                WHERE id=%s
                """,
                (operation_id,),
            )
            row = cur.fetchone()
            if not row:
                return {"ok": False, "error": "入选池记录不存在"}
            if int(row.get("is_bought") or 0) != 0:
                return {"ok": False, "error": "已买入记录不能在这里删除"}
            cur.execute("DELETE FROM stock_operations WHERE id=%s", (operation_id,))
        conn.commit()
    return {"ok": True, "operation_id": operation_id}


def _add_stock_pool_payload(payload: dict) -> dict:
    """手动把选股复盘里的股票加入指定策略预选池，默认进入 B。"""
    symbol = str(payload.get("symbol") or "").strip().upper()
    pool = str(payload.get("pool") or "B").strip().upper()
    if not symbol or not re.fullmatch(r"[A-Z0-9.]{1,16}", symbol):
        return {"ok": False, "error": "股票代码无效"}
    if pool not in {"A", "B", "C", "D"}:
        return {"ok": False, "error": "预选池无效"}

    open_price = _safe_float(payload.get("open"))
    close_price = _safe_float(payload.get("close"))
    trigger_price = close_price if close_price > 0 else open_price
    if trigger_price <= 0:
        return {"ok": False, "error": "缺少有效价格，不能入池"}
    try:
        intraday_volume = int(_safe_float(payload.get("volume")))
    except Exception:
        intraday_volume = 0
    snapshot_date = str(payload.get("snapshot_date") or "").strip()[:10] or None
    intraday_pct = _safe_float(payload.get("intraday_change_pct"))
    day_pct = _safe_float(payload.get("day_change_pct"))
    intent = (
        f"{pool}:MANUAL selected_from_gainers "
        f"open_up={intraday_pct:.2%} day={day_pct:.2%}"
    )[:80]

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, is_bought
                FROM stock_operations
                WHERE UPPER(stock_code)=%s AND UPPER(stock_type)=%s
                LIMIT 1
                """,
                (symbol, pool),
            )
            existing = cur.fetchone()
            if existing and int(existing.get("is_bought") or 0) == 1:
                return {"ok": False, "error": f"{symbol} 已是 {pool} 持仓，未修改"}

            if existing:
                cur.execute(
                    """
                    UPDATE stock_operations
                    SET can_buy=1,
                        can_sell=0,
                        is_bought=0,
                        trigger_price=%s,
                        close_price=%s,
                        entry_open=%s,
                        entry_close=%s,
                        entry_date=%s,
                        intraday_volume=%s,
                        strategy_group=%s,
                        capital_pool=%s,
                        margin_used=0,
                        last_order_side=NULL,
                        last_order_id=NULL,
                        last_order_time=NULL,
                        last_order_intent=%s,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=%s
                    """,
                    (
                        trigger_price,
                        close_price,
                        open_price,
                        close_price,
                        snapshot_date,
                        intraday_volume,
                        pool,
                        pool,
                        intent,
                        existing["id"],
                    ),
                )
                operation_id = int(existing["id"])
            else:
                cur.execute(
                    """
                    INSERT INTO stock_operations (
                        stock_code, stock_type, is_bought, can_buy, can_sell,
                        trigger_price, close_price, entry_open, entry_close, entry_date,
                        cost_price, stop_loss_price, take_profit_price, qty,
                        b_stage, b_peak_price, b_peak_profit, b_last_profit,
                        intraday_volume, strategy_group, capital_pool, margin_used,
                        last_order_intent, created_at, updated_at
                    )
                    VALUES (
                        %s, %s, 0, 1, 0,
                        %s, %s, %s, %s, %s,
                        NULL, NULL, NULL, 0,
                        0, NULL, 0, 0,
                        %s, %s, %s, 0,
                        %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                    """,
                    (
                        symbol,
                        pool,
                        trigger_price,
                        close_price,
                        open_price,
                        close_price,
                        snapshot_date,
                        intraday_volume,
                        pool,
                        pool,
                        intent,
                    ),
                )
                operation_id = int(cur.lastrowid or 0)
        conn.commit()

    return {"ok": True, "symbol": symbol, "pool": pool, "operation_id": operation_id}


def _set_c_core_payload(payload: dict) -> dict:
    """把一只 C 股票设为唯一做T核心；关闭时只关做T，不删除观察/持仓记录。"""
    symbol = str(payload.get("symbol") or "").strip().upper()
    enable = bool(payload.get("enable") is True or str(payload.get("enable") or "").lower() in {"1", "true", "yes", "on"})
    if not symbol or not re.fullmatch(r"[A-Z0-9.]{1,16}", symbol):
        return {"ok": False, "error": "股票代码无效"}

    try:
        operation_id = int(payload.get("operation_id") or 0)
    except Exception:
        operation_id = 0

    with db_conn() as conn:
        with conn.cursor() as cur:
            row = None
            if operation_id > 0:
                cur.execute(
                    """
                    SELECT id, stock_code
                    FROM stock_operations
                    WHERE id=%s
                      AND UPPER(COALESCE(NULLIF(strategy_group,''), stock_type))='C'
                    LIMIT 1
                    """,
                    (operation_id,),
                )
                row = cur.fetchone()
            if not row:
                cur.execute(
                    """
                    SELECT id, stock_code
                    FROM stock_operations
                    WHERE UPPER(stock_code)=%s
                      AND UPPER(COALESCE(NULLIF(strategy_group,''), stock_type))='C'
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (symbol,),
                )
                row = cur.fetchone()

            if not row:
                cur.execute(
                    """
                    INSERT INTO stock_operations (
                        stock_code, stock_type, strategy_group, capital_pool,
                        is_bought, can_buy, can_sell, qty,
                        ac_t_type, ac_t_enabled, ac_t_state,
                        margin_used, last_order_intent, created_at, updated_at
                    )
                    VALUES (
                        %s, 'C', 'C', 'C',
                        0, 1, 0, 0,
                        NULL, 0, 'IDLE',
                        0, 'C:CORE created_from_ui', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                    """,
                    (symbol,),
                )
                operation_id = int(cur.lastrowid or 0)
            else:
                operation_id = int(row.get("id") or 0)

            if enable:
                cur.execute(
                    """
                    UPDATE stock_operations
                    SET ac_t_enabled=0,
                        ac_t_state='IDLE',
                        ac_t_qty=0,
                        ac_t_temporarily_out=0,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE UPPER(COALESCE(NULLIF(strategy_group,''), stock_type))='C'
                      AND COALESCE(id,0)<>%s
                    """,
                    (operation_id,),
                )
                cur.execute(
                    """
                    UPDATE stock_operations
                    SET stock_type='C',
                        strategy_group='C',
                        capital_pool='C',
                        margin_used=0,
                        ac_t_type='C',
                        ac_t_enabled=1,
                        ac_t_state=COALESCE(NULLIF(ac_t_state,''), 'IDLE'),
                        can_buy=1,
                        last_order_intent='C:CORE enabled_from_ui',
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=%s
                    """,
                    (operation_id,),
                )
                cur.execute(
                    """
                    UPDATE stock_operations
                    SET can_buy=0,
                        last_order_intent='B:DISABLED by C core',
                        updated_at=CURRENT_TIMESTAMP
                    WHERE UPPER(stock_code)=%s
                      AND UPPER(COALESCE(NULLIF(strategy_group,''), stock_type))='B'
                      AND COALESCE(is_bought,0)=0
                    """,
                    (symbol,),
                )
            else:
                cur.execute(
                    """
                    UPDATE stock_operations
                    SET ac_t_enabled=0,
                        ac_t_state='IDLE',
                        ac_t_qty=0,
                        ac_t_temporarily_out=0,
                        last_order_intent='C:CORE disabled_from_ui',
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=%s
                    """,
                    (operation_id,),
                )
        conn.commit()

    return {"ok": True, "symbol": symbol, "operation_id": operation_id, "enabled": enable}


def _state_payload() -> dict:
    """读取中央状态：最新风控、资金状态、机器人心跳。"""
    return {
        "ok": True,
        "risk_state": latest_risk_state(),
        "capital_state": capital_state_rows(),
        "bot_heartbeats": bot_heartbeats(),
        "bot_controls": bot_controls(),
        "bot_processes": process_status(),
        "exposure_state": latest_exposure_state(),
        "rebalance_actions": latest_rebalance_actions(30),
    }


def _latest_table_date(table: str, column: str) -> dict:
    """读取定时任务产物表的最新日期和行数。"""
    rows = fetch_all(
        f"""
        SELECT MAX(DATE(`{column}`)) AS latest_date, COUNT(*) AS total_rows
        FROM `{table}`
        """
    )
    latest = rows[0] if rows else {}
    date_value = latest.get("latest_date")
    latest_date = date_value.isoformat() if isinstance(date_value, date) else str(date_value or "")
    latest_count = 0
    if latest_date:
        count_rows = fetch_all(f"SELECT COUNT(*) AS row_count FROM `{table}` WHERE DATE(`{column}`)=%s", (latest_date,))
        latest_count = int((count_rows[0] if count_rows else {}).get("row_count") or 0)
    return {
        "latest_date": latest_date,
        "latest_rows": latest_count,
        "total_rows": int(latest.get("total_rows") or 0),
    }


def _date_age_days(value: str) -> int | None:
    try:
        return (_now_market_tz().date() - datetime.strptime(value[:10], "%Y-%m-%d").date()).days
    except Exception:
        return None


def _schedule_status(latest_date: str, warn_after_days: int) -> str:
    age = _date_age_days(latest_date)
    if age is None:
        return "unknown"
    return "ok" if age <= warn_after_days else "warn"


def _schedules_payload() -> dict:
    """汇总本地/容器定时任务的关键产物状态，用于配置页展示。"""
    price_pool = _latest_table_date("stock_prices_pool", "date")
    categories = _latest_table_date("stock_price_category_snapshots", "snapshot_date")
    b_levels = _latest_table_date("strategy_b_levels", "pressure_date")
    heartbeats = {str(row.get("bot_name") or ""): row for row in bot_heartbeats()}
    controls = {str(row.get("bot_name") or ""): row for row in bot_controls()}
    processes = {str(row.get("bot_name") or ""): row for row in process_status()}

    tasks = [
        {
            "key": "monthly_invest_ac",
            "name": "A 养老金月度按比例买入",
            "schedule": f"每月 {load_monthly_invest_config().get('day', 15)} 号",
            "source": "python -m ultimate_v1.monthly_investment --loop",
            "target": "stock_operations A",
            "latest_date": get_app_setting("MONTHLY_INVEST_LAST_RUN_AT", ""),
            "latest_rows": 0,
            "total_rows": 0,
            "status": "ok" if get_app_setting("MONTHLY_INVEST_LAST_RUN_MONTH", "") == _now_market_tz().strftime("%Y-%m") else "warn",
            "message": "默认预览；开启自动执行后才提交 Alpaca 买单",
        },
        {
            "key": "local_getdata_daily",
            "name": "日线行情同步",
            "schedule": "交易日收盘后",
            "source": "scripts/local_getdata_daily.sh",
            "target": "stock_prices_pool",
            **price_pool,
            "status": _schedule_status(price_pool["latest_date"], 3),
        },
        {
            "key": "price_categories",
            "name": "行情分类快照",
            "schedule": "容器循环刷新",
            "source": "scripts/refresh_stock_price_categories.py --loop",
            "target": "stock_price_category_snapshots",
            **categories,
            "status": _schedule_status(categories["latest_date"], 3),
        },
        {
            "key": "strategy_b_levels",
            "name": "B 压力位周更新",
            "schedule": "每周/按需刷新",
            "source": "scripts/local_strategy_b_levels_weekly.sh",
            "target": "strategy_b_levels",
            **b_levels,
            "status": _schedule_status(b_levels["latest_date"], 14),
        },
    ]

    bot_names = ["dashboard_bot", "risk_bot", "rebalance_bot", "ac_bot", "d_buy_bot", "d_sell_bot", "q_sell_bot"]
    bot_tasks = []
    for bot_name in bot_names:
        hb = heartbeats.get(bot_name, {})
        proc = processes.get(bot_name, {})
        control = controls.get(bot_name, {})
        running = bool(proc.get("running"))
        enabled = bool(int(control.get("enabled") or 0)) if control else running
        last_seen = str(hb.get("last_seen_at") or "")
        bot_tasks.append(
            {
                "key": bot_name,
                "name": bot_name,
                "schedule": "机器人循环",
                "source": str(proc.get("cmd") or proc.get("command") or ""),
                "target": "bot_heartbeats",
                "latest_date": last_seen,
                "latest_rows": 0,
                "total_rows": 0,
                "running": running,
                "enabled": enabled,
                "status": "ok" if running else ("warn" if enabled else "off"),
                "message": str(hb.get("last_message") or ""),
            }
        )

    return {"ok": True, "generated_at": _now_market_tz().isoformat(timespec="seconds"), "tasks": tasks, "bot_tasks": bot_tasks}


def _sync_buy_bot_control(bot_name: str, enabled: bool) -> None:
    """让网页机器人开关同步旧买入总控，避免进程开着但策略仍被 bot_control 挡住。"""
    if bot_name not in {"b_buy_bot", "f_buy_bot"}:
        return
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_control (
                    id INT NOT NULL PRIMARY KEY DEFAULT 1,
                    global_buy_enabled TINYINT NOT NULL DEFAULT 1,
                    strategy_b_enabled TINYINT NOT NULL DEFAULT 1,
                    strategy_f_enabled TINYINT NOT NULL DEFAULT 1,
                    sell_only_mode TINYINT NOT NULL DEFAULT 0,
                    emergency_stop TINYINT NOT NULL DEFAULT 0,
                    note VARCHAR(255) NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cur.execute("INSERT IGNORE INTO bot_control (id) VALUES (1)")
            if bot_name == "b_buy_bot":
                cur.execute("UPDATE bot_control SET strategy_b_enabled=%s WHERE id=1", (1 if enabled else 0,))
            elif bot_name == "f_buy_bot":
                cur.execute("UPDATE bot_control SET strategy_f_enabled=%s WHERE id=1", (1 if enabled else 0,))
            cur.execute(
                """
                SELECT bot_name, enabled
                FROM bot_controls
                WHERE bot_name IN ('b_buy_bot', 'f_buy_bot')
                """
            )
            buy_controls = {str(r.get("bot_name") or ""): int(r.get("enabled") or 0) for r in (cur.fetchall() or [])}
            any_buy_enabled = int(buy_controls.get("b_buy_bot", 0) == 1 or buy_controls.get("f_buy_bot", 0) == 1)
            if any_buy_enabled:
                cur.execute(
                    """
                    UPDATE bot_control
                    SET global_buy_enabled=1,
                        sell_only_mode=0
                    WHERE id=1
                    """
                )
            else:
                cur.execute("UPDATE bot_control SET global_buy_enabled=0 WHERE id=1")
        conn.commit()


def _exposure_payload() -> dict:
    """读取自动调仓机器人最新状态。"""
    return {
        "ok": True,
        "state": latest_exposure_state(),
        "actions": latest_rebalance_actions(100),
    }


def _curve_payload(period: str) -> dict:
    """读取账户收益曲线数据。"""
    payload = equity_curve(period)
    payload["ok"] = True
    return payload


def _trade_records_payload() -> dict:
    """读取最近 30 天买卖机器人记录，限制在面板内滚动展示。"""
    rows: list[dict] = []

    def _qty_from_note(row: dict) -> float | None:
        if str(row.get("source") or "") != "stock_operations":
            return None
        note = str(row.get("note") or "")
        side = str(row.get("side") or "").strip().upper()
        patterns = (r"\bsold=([0-9]+(?:\.[0-9]+)?)", r"\bqty=([0-9]+(?:\.[0-9]+)?)") if side == "SELL" else (r"\bqty=([0-9]+(?:\.[0-9]+)?)",)
        for pattern in patterns:
            match = re.search(pattern, note)
            if match:
                qty = _safe_float(match.group(1), 0.0)
                if qty > 0:
                    return qty
        return None
    try:
        rows.extend(
            fetch_all(
                """
                SELECT
                    event_time,
                    symbol,
                    UPPER(side) AS side,
                    strategy_group,
                    COALESCE(NULLIF(filled_qty,0), qty) AS qty,
                    COALESCE(NULLIF(filled_avg_price,0), price) AS price,
                    status,
                    note,
                    order_id,
                    'manual_trade_records' AS source
                FROM manual_trade_records
                WHERE event_time >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                ORDER BY event_time DESC, id DESC
                LIMIT 500
                """
            )
        )
    except Exception as exc:
        print(f"[WEB TRADE RECORDS] manual records unavailable: {exc}", flush=True)
    try:
        rows.extend(
            fetch_all(
                """
                SELECT
                    created_at AS event_time,
                    symbol,
                    UPPER(side) AS side,
                    strategy_code AS strategy_group,
                    qty,
                    limit_price AS price,
                    status,
                    note,
                    alpaca_order_id AS order_id,
                    'orders' AS source
                FROM orders
                WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                  AND UPPER(side) IN ('BUY','SELL')
                ORDER BY created_at DESC, order_id DESC
                LIMIT 500
                """
            )
        )
    except Exception as exc:
        print(f"[WEB TRADE RECORDS] orders unavailable: {exc}", flush=True)

    try:
        rows.extend(
            fetch_all(
                """
                SELECT
                    last_order_time AS event_time,
                    stock_code AS symbol,
                    UPPER(last_order_side) AS side,
                    COALESCE(NULLIF(strategy_group,''), stock_type) AS strategy_group,
                    qty,
                    COALESCE(current_price, close_price, cost_price) AS price,
                    'RECORDED' AS status,
                    last_order_intent AS note,
                    last_order_id AS order_id,
                    'stock_operations' AS source
                FROM stock_operations
                WHERE last_order_time >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                  AND LOWER(last_order_side) IN ('buy','sell')
                ORDER BY last_order_time DESC, id DESC
                LIMIT 500
                """
            )
        )
    except Exception as exc:
        print(f"[WEB TRADE RECORDS] stock_operations unavailable: {exc}", flush=True)

    # Broker history fills gaps created before local manual-order logging existed.
    # A uses the retirement account; B/C/D share the trading account.
    try:
        from alpaca.trading.enums import QueryOrderStatus
        from alpaca.trading.requests import GetOrdersRequest

        group_rows = fetch_all(
            """
            SELECT UPPER(stock_code) AS symbol,
                   UPPER(COALESCE(NULLIF(strategy_group,''), stock_type)) AS strategy_group,
                   last_order_id
            FROM stock_operations
            WHERE updated_at >= DATE_SUB(NOW(), INTERVAL 90 DAY)
            ORDER BY updated_at DESC, id DESC
            """
        )
        group_by_order: dict[str, str] = {}
        group_by_symbol: dict[str, str] = {}
        for item in group_rows:
            group = str(item.get("strategy_group") or "").upper()
            symbol = str(item.get("symbol") or "").upper()
            order_id = str(item.get("last_order_id") or "")
            if group in {"A", "B", "C", "D"}:
                if order_id:
                    group_by_order.setdefault(order_id, group)
                if symbol:
                    group_by_symbol.setdefault(symbol, group)

        after = _now_market_tz() - timedelta(days=30)
        for profile, default_group in (("retirement", "A"), ("trading", "")):
            try:
                client = alpaca_gateway.trading_client(profile=profile)
                broker_orders = client.get_orders(
                    filter=GetOrdersRequest(status=QueryOrderStatus.ALL, limit=500, after=after, nested=False)
                ) or []
            except Exception as exc:
                print(f"[WEB TRADE RECORDS] Alpaca {profile} history unavailable: {exc}", flush=True)
                continue
            for order in broker_orders:
                asset_class = str(getattr(order, "asset_class", "") or "").upper()
                if asset_class and "EQUITY" not in asset_class:
                    continue
                order_id = str(getattr(order, "id", "") or "")
                symbol = str(getattr(order, "symbol", "") or "").upper()
                client_order_id = str(getattr(order, "client_order_id", "") or "")
                client_match = re.search(r"cszy-manual-([ABCDQ])-", client_order_id, re.I)
                automated_group = ""
                normalized_client_id = client_order_id.lower()
                if normalized_client_id.startswith("dgrid-"):
                    automated_group = "D"
                elif normalized_client_id.startswith("c-core-"):
                    automated_group = "C"
                elif normalized_client_id.startswith(("q-", "qopt-", "option-q-")):
                    automated_group = "Q"
                group = (
                    (client_match.group(1).upper() if client_match else "")
                    or automated_group
                    or group_by_order.get(order_id)
                    or default_group
                    or group_by_symbol.get(symbol)
                    or "MANUAL"
                )
                side = str(getattr(order, "side", "") or "").split(".")[-1].upper()
                position_intent = str(getattr(order, "position_intent", "") or "").upper()
                if side == "SELL" and "OPEN" in position_intent:
                    side = "SHORT"
                status = str(getattr(order, "status", "") or "").split(".")[-1].upper()
                event_time = getattr(order, "filled_at", None) or getattr(order, "submitted_at", None) or getattr(order, "created_at", None)
                if isinstance(event_time, datetime) and event_time.tzinfo is not None:
                    event_time = event_time.astimezone(_now_market_tz().tzinfo).replace(tzinfo=None)
                filled_qty = _safe_float(getattr(order, "filled_qty", 0))
                qty = filled_qty or _safe_float(getattr(order, "qty", 0))
                if filled_qty > 0 and status in {"CANCELED", "CANCELLED", "EXPIRED"}:
                    status = "PARTIAL_FILLED"
                price = _safe_float(getattr(order, "filled_avg_price", 0)) or _safe_float(getattr(order, "limit_price", 0))
                rows.append(
                    {
                        "event_time": event_time,
                        "symbol": symbol,
                        "side": side,
                        "strategy_group": group,
                        "qty": qty,
                        "price": price,
                        "status": status,
                        "note": f"Alpaca 订单历史 · {client_order_id or 'broker'}",
                        "order_id": order_id,
                        "source": "alpaca_orders",
                    }
                )
    except Exception as exc:
        print(f"[WEB TRADE RECORDS] Alpaca history setup failed: {exc}", flush=True)

    def key(row: dict) -> str:
        order_id = str(row.get("order_id") or "").strip()
        if order_id:
            return f"order:{order_id}"
        return "|".join(
            [
                str(row.get("event_time") or ""),
                str(row.get("symbol") or ""),
                str(row.get("side") or ""),
                str(row.get("order_id") or ""),
                str(row.get("source") or ""),
            ]
        )

    seen = set()
    cleaned = []
    source_priority = {
        # Alpaca 历史是成交状态的最终事实，避免本地 PENDING/RECORDED 覆盖 FILLED/EXPIRED。
        "alpaca_orders": 0,
        "orders": 1,
        "manual_trade_records": 2,
        "stock_operations": 3,
    }
    rows.sort(key=lambda row: source_priority.get(str(row.get("source") or ""), 9))
    for row in rows:
        k = key(row)
        if k in seen:
            continue
        seen.add(k)
        note_qty = _qty_from_note(row)
        if note_qty is not None:
            row = {**row, "qty": note_qty}
        cleaned.append(row)
    cleaned.sort(key=lambda r: str(r.get("event_time") or ""), reverse=True)
    terminal_attempt_statuses = {"CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "FAILED", "ERROR"}
    fill_statuses = {"FILLED", "PARTIAL_FILLED"}
    filled_rows: list[dict] = []
    attempt_rows: list[dict] = []
    for row in cleaned[:500]:
        status = str(row.get("status") or "").upper()
        note = str(row.get("note") or "").upper()
        if status in fill_statuses:
            filled_rows.append(row)
        elif status in terminal_attempt_statuses or any(marker in note for marker in ("_ERR", "NO_FILL", "CANCELED", "EXPIRED", "REJECTED")):
            attempt_rows.append(row)
        else:
            # 未终结委托和旧版 RECORDED 事件不是已确认成交，不能污染真实成交统计。
            attempt_rows.append(row)

    d_orders = [row for row in cleaned if str(row.get("strategy_group") or "").upper() == "D"]
    d_filled = sum(1 for row in d_orders if str(row.get("status") or "").upper() in fill_statuses)
    d_unfilled = sum(1 for row in d_orders if str(row.get("status") or "").upper() in terminal_attempt_statuses)
    d_terminal = d_filled + d_unfilled
    return {
        "ok": True,
        "rows": cleaned[:500],
        "filled_rows": filled_rows,
        "attempt_rows": attempt_rows,
        "diagnostics": {
            "D": {
                "submitted": len(d_orders),
                "filled": d_filled,
                "unfilled": d_unfilled,
                "fill_rate": (d_filled / d_terminal) if d_terminal else None,
            }
        },
    }


def _stock_selection_payload() -> dict:
    """复盘选股：使用 B/D 共用的基础流动性口径，并分别标记候选池。"""
    min_up_pct = _safe_float(env_str("SELECTION_MIN_UP_PCT", "0.05"), 0.05)
    min_price = _safe_float(env_str("SELECTION_MIN_PRICE", "5"), 5.0)
    min_volume = _safe_float(env_str("SELECTION_MIN_VOLUME", "3000000"), 3000000.0)
    min_dollar_volume = _safe_float(env_str("SELECTION_MIN_DOLLAR_VOLUME", "30000000"), 30000000.0)
    latest = fetch_all("SELECT MAX(DATE(`date`)) AS d FROM stock_prices_pool")
    snapshot_date = (latest[0] or {}).get("d") if latest else None
    if not snapshot_date:
        return {"ok": True, "snapshot_date": None, "min_up_pct": min_up_pct, "rows": [], "b_rows": []}
    previous = fetch_all(
        """
        SELECT MAX(DATE(`date`)) AS d
        FROM stock_prices_pool
        WHERE DATE(`date`) < DATE(%s)
        """,
        (snapshot_date,),
    )
    previous_date = (previous[0] or {}).get("d") if previous else None

    rows = fetch_all(
        """
        SELECT
            UPPER(p.symbol) AS symbol,
            DATE(p.`date`) AS snapshot_date,
            p.`open`, p.high, p.low, p.`close`, p.volume,
            ((p.`close` - p.`open`) / p.`open`) AS intraday_change_pct,
            pp.`close` AS prev_close,
            b.id AS operation_id,
            b.trigger_price,
            b.entry_open,
            b.entry_close,
            b.entry_date,
            b.can_buy,
            b.is_bought,
            b.last_order_side,
            b.last_order_intent,
            b.updated_at AS b_updated_at
            ,d.symbol AS d_candidate_symbol
        FROM stock_prices_pool p
        LEFT JOIN stock_prices_pool pp
          ON DATE(pp.`date`) = DATE(%s)
         AND UPPER(CONVERT(pp.symbol USING utf8mb4)) COLLATE utf8mb4_unicode_ci = UPPER(CONVERT(p.symbol USING utf8mb4)) COLLATE utf8mb4_unicode_ci
        LEFT JOIN (
            SELECT so.*
            FROM stock_operations so
            INNER JOIN (
                SELECT UPPER(CONVERT(stock_code USING utf8mb4)) COLLATE utf8mb4_unicode_ci AS symbol, MAX(id) AS id
                FROM stock_operations
                WHERE UPPER(stock_type)='B'
                GROUP BY UPPER(CONVERT(stock_code USING utf8mb4)) COLLATE utf8mb4_unicode_ci
            ) latest_b ON latest_b.id = so.id
        ) b ON UPPER(CONVERT(b.stock_code USING utf8mb4)) COLLATE utf8mb4_unicode_ci = UPPER(CONVERT(p.symbol USING utf8mb4)) COLLATE utf8mb4_unicode_ci
        LEFT JOIN d_candidate_pool d
          ON UPPER(CONVERT(d.symbol USING utf8mb4)) COLLATE utf8mb4_unicode_ci = UPPER(CONVERT(p.symbol USING utf8mb4)) COLLATE utf8mb4_unicode_ci
         AND d.enabled=1 AND DATE(d.signal_date)=DATE(p.`date`)
        WHERE DATE(p.`date`) = DATE(%s)
          AND p.`open` > 0
          AND p.`close` >= %s
          AND COALESCE(p.volume, 0) >= %s
          AND (p.`close` * COALESCE(p.volume, 0)) >= %s
          AND ((p.`close` - p.`open`) / p.`open`) >= %s
        ORDER BY ((p.`close` - p.`open`) / p.`open`) DESC, UPPER(p.symbol) ASC
        LIMIT 1000
        """,
        (previous_date or snapshot_date, snapshot_date, min_price, min_volume, min_dollar_volume, min_up_pct),
    )

    out = []
    b_rows = []
    for row in rows or []:
        close = _safe_float(row.get("close"))
        prev_close = _safe_float(row.get("prev_close"))
        day_change_pct = (close - prev_close) / prev_close if close > 0 and prev_close > 0 else None
        can_buy = int(row.get("can_buy") or 0)
        is_bought = int(row.get("is_bought") or 0)
        b_match = bool(row.get("operation_id") and can_buy == 1 and is_bought != 1)
        d_match = bool(row.get("d_candidate_symbol"))
        item = {
            "symbol": str(row.get("symbol") or "").upper(),
            "snapshot_date": row.get("snapshot_date"),
            "open": _safe_float(row.get("open")),
            "high": _safe_float(row.get("high")),
            "low": _safe_float(row.get("low")),
            "close": close,
            "volume": _safe_float(row.get("volume")),
            "intraday_change_pct": _safe_float(row.get("intraday_change_pct")),
            "prev_close": prev_close,
            "day_change_pct": day_change_pct,
            "b_match": b_match,
            "d_match": d_match,
            "dollar_volume": close * _safe_float(row.get("volume")),
            "operation_id": row.get("operation_id"),
            "trigger_price": _safe_float(row.get("trigger_price")),
            "entry_open": _safe_float(row.get("entry_open")),
            "entry_close": _safe_float(row.get("entry_close")),
            "entry_date": row.get("entry_date"),
            "can_buy": can_buy,
            "is_bought": is_bought,
            "last_order_side": row.get("last_order_side"),
            "last_order_intent": row.get("last_order_intent"),
            "b_updated_at": row.get("b_updated_at"),
        }
        out.append(item)
        if b_match:
            b_rows.append(item)

    return {
        "ok": True,
        "snapshot_date": snapshot_date,
        "previous_date": previous_date,
        "min_up_pct": min_up_pct,
        "min_price": min_price,
        "min_volume": min_volume,
        "min_dollar_volume": min_dollar_volume,
        "rows": out,
        "b_rows": b_rows,
    }


def _candidate_log_dirs() -> list[Path]:
    root = Path(__file__).resolve().parents[1]
    candidates = [
        env_str("LOG_DIR", ""),
        env_str("BOT_LOG_DIR", ""),
        str(root / "logs"),
        "/app/logs",
        "/tmp/logs",
    ]
    seen: set[str] = set()
    out: list[Path] = []
    for raw in candidates:
        if not raw:
            continue
        path = Path(raw)
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        out.append(path)
    return out


def _tail_text_file(path: Path, lines: int = 120) -> list[str]:
    try:
        if not path.exists() or not path.is_file():
            return []
        size = path.stat().st_size
        with path.open("rb") as fh:
            fh.seek(max(0, size - 120_000))
            data = fh.read()
        text = data.decode("utf-8", errors="replace")
        return text.splitlines()[-max(1, min(lines, 500)):]
    except Exception as exc:
        return [f"[log read error] {path}: {exc}"]


_LOG_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})")
_LOG_IMPORTANT_RE = re.compile(
    r"(\[CSZY_DECISION\]|\[B SCORE|B SCORE|score=|打分|候选|买入|卖出|下单|成交|开仓|平仓|止损|止盈|submit|submitted|filled|executed|"
    r"\border\b|buy[_\s-]?(order|submitted|filled|executed)|"
    r"sell[_\s-]?(order|submitted|filled|executed))",
    re.I,
)
_LOG_DECISION_TAG = "[CSZY_DECISION]"
_LOG_IMPORTANT_HINTS = (
    "[b score",
    "b score",
    "score=",
    "打分",
    "候选",
    "买入",
    "卖出",
    "下单",
    "成交",
    "开仓",
    "平仓",
    "止损",
    "止盈",
    "submit",
    "submitted",
    "filled",
    "executed",
    " order",
    "buy_order",
    "buy order",
    "sell_order",
    "sell order",
)
_LOG_NOISE_RE = re.compile(
    r"(market closed|sleep \d+s|heartbeat|using key|key_prefix|split .* bot (start|stop)|"
    r"loop round=|round phase=|round done.*traded=0|BUY_GATE|buy_allowed|FORCE phase|outside .*window|"
    r"journal|life|生活|休闲|暂无|idle)",
    re.I,
)
_LOG_CODE_NOISE_RE = re.compile(r'^\s*(File ".+", line \d+|return |raise |\w+\s*=|Traceback \(most recent call last\)|[\w.]+Error:|[A-Za-z_][\w.]*\()', re.I)
_LOG_SYMBOL_RE = re.compile(
    r"""["']symbol["']\s*:\s*["']([A-Z][A-Z0-9.]{0,9})["']|"""
    r"(?:\b(?:symbol|stock|code)=|scan\s+[A-Z]\s+)([A-Z][A-Z0-9.]{0,9})\b|"
    r"\[[A-Z _]+\]\s+([A-Z][A-Z0-9.]{0,9})\s+(?:price=|bid=|ask=|last=|buy|sell|skip|submit|order|下单|买入|卖出)|"
    r"confirmed\s+[A-Z]\s+([A-Z][A-Z0-9.]{0,9})\b|"
    r"\b([A-Z]{1,6})\s+(?:price=|bid=|ask=|last=|submit|order)",
    re.I,
)


def _parse_log_dt(line: str) -> datetime | None:
    match = _LOG_TS_RE.match(line or "")
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _strip_log_prefix(line: str) -> str:
    return re.sub(
        r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s*\|\s*(INFO|WARNING|ERROR|DEBUG|PRINT)\s*\|\s*",
        "",
        str(line or ""),
        flags=re.I,
    ).strip()


def _is_important_log_line(line: str) -> bool:
    text = str(line or "")
    if _LOG_DECISION_TAG in text:
        return True
    lowered = text.lower()
    if not any(hint in lowered for hint in _LOG_IMPORTANT_HINTS):
        return False
    return bool(_LOG_IMPORTANT_RE.search(text))


def _recent_trading_day_cutoff(trading_days: int) -> date:
    """Return the first weekday in the recent trading-day window."""
    remaining = max(1, int(trading_days or 1))
    cursor = _now_market_tz().date()
    while remaining > 1:
        cursor -= timedelta(days=1)
        if cursor.weekday() < 5:
            remaining -= 1
    return cursor


def _log_file_candidates(path: Path | None, days: int) -> list[Path]:
    if not path:
        return []
    cutoff_date = _recent_trading_day_cutoff(days)
    candidates: list[Path] = []
    seen: set[str] = set()
    for item in [path, *path.parent.glob(f"{path.name}.*")]:
        try:
            if not item.exists() or not item.is_file():
                continue
            suffix_date = None
            suffix = item.name.rsplit(".", 1)[-1]
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", suffix):
                suffix_date = datetime.strptime(suffix, "%Y-%m-%d").date()
            if suffix_date and suffix_date < cutoff_date:
                continue
            key = str(item.resolve())
            if key in seen:
                continue
            seen.add(key)
            candidates.append(item)
        except Exception:
            continue
    def sort_key(item: Path):
        suffix = item.name.rsplit(".", 1)[-1]
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", suffix):
            try:
                return datetime.strptime(suffix, "%Y-%m-%d")
            except Exception:
                pass
        return datetime.max

    return sorted(candidates, key=sort_key)


def _read_recent_log_lines(
    path: Path | None,
    days: int,
    important_only: bool = False,
    limit: int = 1200,
    search: str = "",
) -> tuple[list[dict], int, list[str]]:
    cutoff = datetime.combine(_recent_trading_day_cutoff(days), dt_time.min)
    selected: list[dict] = []
    seen: set[str] = set()
    files = _log_file_candidates(path, days)
    search_text = str(search or "").strip().lower()
    if search_text:
        max_bytes = int(float(env_str("BOT_LOG_SEARCH_READ_MAX_BYTES", "24000000") or "24000000"))
    elif important_only:
        max_bytes = int(float(env_str("BOT_LOG_IMPORTANT_READ_MAX_BYTES", "12000000") or "12000000"))
    else:
        max_bytes = int(float(env_str("BOT_LOG_READ_MAX_BYTES", "2000000") or "2000000"))
    for file_path in files:
        inherited_dt: datetime | None = None
        suffix_dt: datetime | None = None
        suffix = file_path.name.rsplit(".", 1)[-1]
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", suffix):
            try:
                suffix_dt = datetime.strptime(suffix, "%Y-%m-%d")
            except Exception:
                suffix_dt = None
        try:
            raw = file_path.read_bytes()
            if len(raw) > max_bytes:
                raw = raw[-max_bytes:]
                first_newline = raw.find(b"\n")
                if first_newline >= 0:
                    raw = raw[first_newline + 1 :]
            for line in raw.decode("utf-8", errors="replace").splitlines():
                text = line.strip()
                if not text:
                    continue
                if search_text and search_text not in text.lower():
                    continue
                dt = _parse_log_dt(text)
                if dt:
                    inherited_dt = dt
                display_dt = dt or inherited_dt or suffix_dt
                if dt and dt < cutoff:
                    continue
                important = _is_important_log_line(text)
                if important_only:
                    if _LOG_DECISION_TAG not in text:
                        continue
                    important = True
                if not search_text and not important_only and _LOG_NOISE_RE.search(text):
                    continue
                if important_only and re.search(r"\[B SCORE\]\s+(confirmed|pending)=|\[B SCORE PROGRESS\]", text, re.I):
                    continue
                if _LOG_CODE_NOISE_RE.search(text):
                    continue
                if text.startswith("RuntimeError:") or "YFRateLimitError:" in text:
                    continue
                key = _strip_log_prefix(text)
                if key in seen:
                    continue
                seen.add(key)
                selected.append(
                    {
                        "line": text,
                        "display_time": display_dt.strftime("%Y-%m-%d %H:%M:%S") if display_dt else "",
                        "source_file": str(file_path),
                    }
                )
        except Exception as exc:
            selected.append(
                {
                    "line": f"[log read error] {file_path}: {exc}",
                    "display_time": suffix_dt.strftime("%Y-%m-%d %H:%M:%S") if suffix_dt else "",
                    "source_file": str(file_path),
                }
            )
    selected.sort(key=lambda item: _parse_log_dt(str(item.get("line") or "")) or _parse_log_dt(str(item.get("display_time") or "")) or datetime.min)
    if len(selected) > limit:
        selected = selected[-limit:]
    return selected, len(seen), [str(p) for p in files]


def _extract_log_symbols(lines: list) -> list[str]:
    symbols: set[str] = set()
    ignored = {"BOT", "BUY", "SELL", "INFO", "WARNING", "ERROR", "TRUE", "FALSE", "LOOP", "ROUND"}
    for item in lines:
        line = item.get("line") if isinstance(item, dict) else str(item or "")
        for match in _LOG_SYMBOL_RE.finditer(line):
            raw_symbol = match.group(1) or match.group(2) or match.group(3) or match.group(4) or match.group(5) or ""
            if raw_symbol != raw_symbol.upper():
                continue
            symbol = raw_symbol.upper()
            if symbol and symbol not in ignored:
                symbols.add(symbol)
    return sorted(symbols)


def _bot_log_path(bot_name: str) -> Path | None:
    trade_env = (env_str("TRADE_ENV", env_str("ALPACA_MODE", "paper")) or "paper").strip().lower()
    names = [
        f"AAA_{bot_name}_{trade_env}.log",
        f"AAA_{bot_name}_paper.log",
        f"AAA_{bot_name}_live.log",
    ]
    for directory in _candidate_log_dirs():
        for name in names:
            path = directory / name
            if path.exists():
                return path
    return None


def _bot_log_fallback_lines(bot_name: str) -> list[dict]:
    lines: list[str] = []
    try:
        heartbeat_map = {row["bot_name"]: row for row in bot_heartbeats()}
        hb = heartbeat_map.get(bot_name)
        if hb:
            lines.append(
                f"[heartbeat] status={hb.get('status') or '--'} "
                f"last_seen={hb.get('last_seen_at') or '--'} message={hb.get('last_message') or ''}"
            )
    except Exception:
        pass
    try:
        rows = fetch_all(
            """
            SELECT created_at, action, status, message, pid
            FROM bot_lifecycle_events
            WHERE bot_name=%s
            ORDER BY created_at DESC, id DESC
            LIMIT 30
            """,
            (bot_name,),
        )
        for row in reversed(rows):
            lines.append(
                f"{row.get('created_at')} | {row.get('action')} | {row.get('status')} | "
                f"{row.get('message') or ''}{(' pid=' + str(row.get('pid'))) if row.get('pid') else ''}"
            )
    except Exception:
        pass
    now_str = _now_market_tz().strftime("%Y-%m-%d %H:%M:%S")
    return [
        {"line": line, "display_time": (_parse_log_dt(line).strftime("%Y-%m-%d %H:%M:%S") if _parse_log_dt(line) else now_str), "source_file": ""}
        for line in (lines or ["暂无日志文件；机器人启动后会写入独立日志。"])
    ]


def _bot_logs_payload(lines: int = 120, bot_name: str = "", summary_only: bool = False, search: str = "") -> dict:
    """读取每个机器人最近日志，给日志聚焦页展示。"""
    requested_bot = (bot_name or "").strip().lower()
    search_text = str(search or "").strip()
    all_bots = sorted(managed_bot_names())
    bots = [requested_bot] if requested_bot in set(all_bots) else all_bots
    process_map = {row["bot_name"]: row for row in process_status()}
    control_map = {str(row.get("bot_name") or ""): bool(int(row.get("enabled") or 0)) for row in bot_controls()}
    heartbeat_map = {str(row.get("bot_name") or ""): row for row in bot_heartbeats()}
    rows = []
    general_days = int(float(env_str("BOT_LOG_GENERAL_DAYS", "5") or "5"))
    important_days = int(float(env_str("BOT_LOG_IMPORTANT_DAYS", "5") or "5"))
    general_limit = max(60, min(int(lines or 180), 1200 if search_text else 300))
    important_limit = int(float(env_str("BOT_LOG_IMPORTANT_LIMIT", "160") or "160"))
    for bot_name in bots:
        path = _bot_log_path(bot_name)
        if summary_only:
            log_lines: list[dict] = []
            important_lines: list[dict] = []
            source_line_count = 0
            important_line_count = 0
            source_files = [str(path)] if path else []
            important_files: list[str] = []
        else:
            log_lines, source_line_count, source_files = _read_recent_log_lines(
                path,
                days=general_days,
                important_only=False,
                limit=general_limit,
                search=search_text,
            )
            important_lines, important_line_count, important_files = _read_recent_log_lines(
                path,
                days=important_days,
                important_only=True,
                limit=important_limit,
                search=search_text,
            )
        if not summary_only and not log_lines:
            log_lines = _bot_log_fallback_lines(bot_name)
        proc = process_map.get(bot_name) or {}
        heartbeat_row = heartbeat_map.get(bot_name) or {}
        rows.append(
            {
                "bot_name": bot_name,
                "running": bool(proc.get("running")),
                "enabled": bool(control_map.get(bot_name, False)),
                "heartbeat_status": str(heartbeat_row.get("status") or ""),
                "last_seen_at": heartbeat_row.get("last_seen_at"),
                "pid": proc.get("pid"),
                "log_path": str(path) if path else "",
                "lines": log_lines,
                "important_lines": important_lines,
                "symbols": _extract_log_symbols(important_lines),
                "all_symbols": _extract_log_symbols(log_lines),
                "important_symbols": _extract_log_symbols(important_lines),
                "source_files": source_files or important_files,
                "source_line_count": source_line_count,
                "important_line_count": important_line_count,
                "general_days": general_days,
                "important_days": important_days,
                "logs_loaded": not summary_only,
            }
        )
    return {"ok": True, "rows": rows, "general_days": general_days, "important_days": important_days}


def _clear_bot_log_payload(payload: dict) -> dict:
    """清空指定机器人的日志文件，只允许操作受管机器人自己的日志。"""
    bot_name = str(payload.get("bot_name") or "").strip()
    if bot_name not in managed_bot_names():
        return {"ok": False, "error": "不支持的机器人"}
    path = _bot_log_path(bot_name)
    if not path:
        return {"ok": False, "error": "这个机器人暂时没有可清空的日志文件"}
    try:
        resolved = path.resolve()
        allowed_dirs = [directory.resolve() for directory in _candidate_log_dirs() if directory.exists()]
        if not any(resolved.parent == directory for directory in allowed_dirs):
            return {"ok": False, "error": "日志路径不在允许目录内"}
        resolved.write_text("", encoding="utf-8")
    except Exception as exc:
        return {"ok": False, "error": f"清空日志失败：{exc}"}
    return {"ok": True, "bot_name": bot_name, "log_path": str(path)}


def _now_market_tz() -> datetime:
    """读取配置时区里的当前时间，默认美西。"""
    tz_name = settings().timezone or "America/Los_Angeles"
    if ZoneInfo:
        return datetime.now(ZoneInfo(tz_name))
    return datetime.now()


def _trade_phase_code(now_dt: datetime | None = None) -> str:
    """按美股时间段判断当前交易阶段。"""
    now_dt = now_dt or _now_market_tz()
    if now_dt.weekday() >= 5:
        return "closed"
    tnow = now_dt.time()
    if dt_time(4, 0) <= tnow < dt_time(6, 30):
        return "premarket_sell"
    if dt_time(6, 30) <= tnow < dt_time(6, 40):
        return "preopen_record"
    if dt_time(6, 40) <= tnow <= dt_time(13, 0):
        return "regular"
    if dt_time(13, 0) < tnow <= dt_time(17, 0):
        return "afterhours_add"
    return "closed"


def _trade_phase_label(phase: str) -> str:
    """交易阶段中文名称。"""
    return {
        "premarket_sell": "盘前保护",
        "preopen_record": "只记录",
        "regular": "盘中主策略",
        "afterhours_add": "盘后加仓",
        "closed": "休眠",
    }.get(phase, phase)


def _trade_phase_tone(phase: str) -> str:
    """前端颜色状态。"""
    if phase == "regular":
        return "ok"
    if phase in {"premarket_sell", "afterhours_add"}:
        return "blue"
    if phase == "preopen_record":
        return "warn"
    return "sleep"


def _trade_phase_payload() -> dict:
    """给顶部状态胶囊和详情弹层提供交易阶段数据。"""
    now_dt = _now_market_tz()
    phase = _trade_phase_code(now_dt)
    rules = [
        {
            "range": "04:00-06:30",
            "code": "premarket_sell",
            "title": "盘前保护",
            "desc": "B/F 持仓若盘前涨幅>=10%，先限价卖20%；若从盘前最高价回撤3%，按回撤价限价清仓。",
        },
        {
            "range": "06:30-06:40",
            "code": "preopen_record",
            "title": "只记录",
            "desc": "只记录盘前实时价、最高价和浮盈，不买不卖，等 06:40 后交给盘中规则。",
        },
        {
            "range": "06:40-13:00",
            "code": "regular",
            "title": "盘中主策略",
            "desc": "保持 B/F/C 主逻辑：卖出管理、候选刷新、盘中买入仍受总开关、大盘 gate 和资金 gate 控制。",
        },
        {
            "range": "13:00-17:00",
            "code": "afterhours_add",
            "title": "盘后加仓",
            "desc": "已持有 B/F 若盘后实时价>=正常收盘价*1.05，则按规则挂盘后加仓单。",
        },
    ]
    for rule in rules:
        rule["active"] = rule["code"] == phase
    return {
        "ok": True,
        "timezone": settings().timezone,
        "now": now_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "phase": phase,
        "label": _trade_phase_label(phase),
        "tone": _trade_phase_tone(phase),
        "rules": rules,
    }


def _ensure_price_category_table() -> None:
    """确保行情分类快照表存在。数据由 scripts/refresh_stock_price_categories.py 生成。"""
    table = env_str("PRICE_CATEGORY_TABLE", "stock_price_category_snapshots")
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{table}` (
                  snapshot_date DATE NOT NULL,
                  category_group VARCHAR(32) NOT NULL,
                  category_group_label VARCHAR(64) NOT NULL,
                  category_key VARCHAR(64) NOT NULL,
                  category_label VARCHAR(64) NOT NULL,
                  category_order INT NOT NULL,
                  symbol VARCHAR(64) NOT NULL,
                  `open` DOUBLE NULL,
                  high DOUBLE NULL,
                  low DOUBLE NULL,
                  `close` DOUBLE NULL,
                  volume BIGINT NULL,
                  change_pct DOUBLE NULL,
                  up_streak INT NOT NULL DEFAULT 0,
                  down_streak INT NOT NULL DEFAULT 0,
                  up_days_2 INT NULL,
                  up_days_3 INT NULL,
                  up_days_4 INT NULL,
                  up_days_5 INT NULL,
                  down_days_2 INT NULL,
                  down_days_3 INT NULL,
                  down_days_4 INT NULL,
                  down_days_5 INT NULL,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (snapshot_date, category_key, symbol),
                  KEY idx_snapshot_order (snapshot_date, category_order),
                  KEY idx_symbol_date (symbol, snapshot_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )


def _market_categories_payload(selected_key: str = "") -> dict:
    """读取最新行情分类快照，供持仓区切换展示。"""
    _ensure_price_category_table()
    table = env_str("PRICE_CATEGORY_TABLE", "stock_price_category_snapshots")
    excluded_groups = ("up_days", "down_days")
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(snapshot_date) AS d FROM `{table}`")
            latest = cur.fetchone() or {}
            snapshot_date = latest.get("d")
            if not snapshot_date:
                return {"ok": True, "meta": [], "rows": [], "selected_key": "", "snapshot_date": None}

            cur.execute(
                f"""
                SELECT snapshot_date, category_group, category_group_label, category_key,
                       category_label, category_order, COUNT(*) AS symbol_count,
                       MAX(updated_at) AS snapshot_updated_at
                FROM `{table}`
                WHERE snapshot_date=%s
                  AND category_group NOT IN (%s, %s)
                GROUP BY snapshot_date, category_group, category_group_label,
                         category_key, category_label, category_order
                ORDER BY category_order ASC
                """,
                (snapshot_date, *excluded_groups),
            )
            meta = list(cur.fetchall() or [])
            if not meta:
                return {"ok": True, "meta": [], "rows": [], "selected_key": "", "snapshot_date": snapshot_date}

            valid_keys = {str(row.get("category_key") or "") for row in meta}
            selected_key = selected_key if selected_key in valid_keys else str(meta[0].get("category_key") or "")
            cur.execute(
                f"""
                SELECT snapshot_date, category_group, category_group_label, category_key,
                       category_label, category_order, symbol,
                       ROUND(`open`, 2) AS `open`,
                       ROUND(high, 2) AS high,
                       ROUND(low, 2) AS low,
                       ROUND(`close`, 2) AS `close`,
                       volume, change_pct, up_streak, down_streak,
                       updated_at
                FROM `{table}`
                WHERE snapshot_date=%s AND category_key=%s
                  AND category_group NOT IN (%s, %s)
                ORDER BY change_pct DESC, symbol ASC
                LIMIT 500
                """,
                (snapshot_date, selected_key, *excluded_groups),
            )
            rows = list(cur.fetchall() or [])

    return {
        "ok": True,
        "meta": meta,
        "rows": rows,
        "selected_key": selected_key,
        "snapshot_date": snapshot_date,
    }


def _refresh_market_categories_payload(selected_key: str = "") -> dict:
    """重建最新行情分类快照，然后返回刷新后的分类数据。"""
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "refresh_stock_price_categories.py"
    spec = importlib.util.spec_from_file_location("refresh_stock_price_categories_runtime", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("无法加载行情分类刷新脚本")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        module._run_once(dry_run=False)
    payload = _market_categories_payload(selected_key)
    payload["refreshed"] = True
    payload["refresh_log"] = buffer.getvalue().strip().splitlines()[-8:]
    return payload


def _stock_quote_payload(symbol: str) -> dict:
    """给手动买入入口提供现价、bid、ask 和建议限价。"""
    symbol = (symbol or "").strip().upper()
    if not symbol or not symbol.replace(".", "").isalpha():
        return {"ok": False, "error": "invalid_symbol"}
    position_price = 0.0
    position_qty = 0.0
    try:
        pos = alpaca_gateway.trading_client().get_open_position(symbol)
        position_price = _safe_float(getattr(pos, "current_price", 0))
        position_qty = _safe_float(getattr(pos, "qty", 0))
    except Exception:
        pass
    try:
        from app.strategy_b import get_snapshot_quote_realtime

        quote = get_snapshot_quote_realtime(symbol)
        snapshot_last = _safe_float(quote.get("last_price"))
        last = position_price or snapshot_last
        bid = _safe_float(quote.get("bid"))
        ask = _safe_float(quote.get("ask"))
        prev = _safe_float(quote.get("prev_close") or quote.get("previous_close"))
        if last > 0:
            try:
                with db_conn() as conn:
                    with conn.cursor() as cur:
                        source = "alpaca_position" if position_price > 0 else str(quote.get("feed") or "yahoo")
                        _write_quote_cache(cur, symbol, last, prev, source)
            except Exception as cache_exc:
                print(f"[WEB QUOTE CACHE] {symbol} write failed: {cache_exc}", flush=True)
        return {
            "ok": True,
            "symbol": symbol,
            "last": last,
            "bid": bid,
            "ask": ask,
            "prev_close": prev,
            "day_volume": int(_safe_float(quote.get("day_volume"))),
            "position_qty": position_qty,
            "snapshot_last": snapshot_last,
            "limit_price": ask if ask > 0 else last,
            "source": "alpaca_position" if position_price > 0 else str(quote.get("feed") or "yahoo"),
            "fetched_at": _now_market_tz().strftime("%Y-%m-%d %H:%M:%S"),
        }
    except Exception as exc:
        meta = _latest_price_meta([symbol]).get(symbol) or {}
        last = position_price or _safe_float(meta.get("latest_close"))
        if last > 0:
            return {
                "ok": True,
                "symbol": symbol,
                "last": last,
                "bid": 0.0,
                "ask": 0.0,
                "prev_close": _safe_float(meta.get("prev_close")),
                "day_volume": 0,
                "position_qty": position_qty,
                "limit_price": last,
                "source": "alpaca_position" if position_price > 0 else meta.get("source") or "price_cache",
                "warning": str(exc)[:160],
                "fetched_at": _now_market_tz().strftime("%Y-%m-%d %H:%M:%S"),
            }
        return {"ok": False, "symbol": symbol, "error": str(exc)[:180]}


from .manual_policy import _manual_strategy_b_stop_loss, _manual_stop_policy, _manual_stock_qty


def _manual_order_fill(
    client,
    symbol: str,
    order_id: str,
    fallback_qty: float,
    fallback_price: float,
    *,
    allow_position_fallback: bool = True,
) -> tuple[float, float, str]:
    from .order_fills import wait_for_fill
    return wait_for_fill(client, order_id, 6.0)


from .trade_history import _record_manual_trade


from .manual_execution import _pool_account_buying_power, _manual_stock_order_payload, _plan_manual_stock_order


INDEX_HTML = (Path(__file__).resolve().parent / "templates" / "dashboard.html").read_text(encoding="utf-8")


LOGIN_HTML = (Path(__file__).resolve().parent / "templates" / "login.html").read_text(encoding="utf-8")


class Handler(BaseHTTPRequestHandler):
    def handle_one_request(self):
        from .metrics import record
        started = time.monotonic()
        try:
            return super().handle_one_request()
        finally:
            record("http_request", time.monotonic() - started)

    def setup(self):
        self.request.settimeout(30)
        super().setup()

    def end_headers(self):
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        super().end_headers()

    def _send_json(self, payload: dict | list, status: int = 200, headers: dict[str, str] | None = None) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=_json_default).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self) -> None:
        body = INDEX_HTML.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_login_html(self) -> None:
        body = LOGIN_HTML.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_asset(self, path: str) -> None:
        """发送项目内静态资源，目前用于 logo。"""
        asset_root = Path(__file__).resolve().parent / "assets"
        name = Path(path).name
        asset_path = asset_root / name
        if not asset_path.exists() or not asset_path.is_file():
            self._send_json({"ok": False, "error": "asset_not_found"}, 404)
            return
        content_type = "image/png" if asset_path.suffix.lower() == ".png" else "application/octet-stream"
        body = asset_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "public, max-age=3600")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args) -> None:
        print(f"[WEB] {self.address_string()} {fmt % args}", flush=True)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length <= 0:
            return {}
        if length > 65536:
            raise ValueError("request body too large")
        if self.headers.get("Content-Type", "").split(";", 1)[0] != "application/json":
            raise ValueError("application/json required")
        origin = self.headers.get("Origin")
        if origin and urlparse(origin).netloc != self.headers.get("Host"):
            raise ValueError("cross-origin request rejected")
        raw = self.rfile.read(length).decode("utf-8")
        payload = json.loads(raw or "{}")
        if not isinstance(payload, dict):
            raise ValueError("JSON object required")
        return payload

    def _check_password(self, payload: dict) -> bool:
        password = str(payload.get("password") or "")
        expected = env_str("DASHBOARD_ACTION_PASSWORD", env_str("MOBILE_CONTROL_TOKEN", ""))
        return bool(expected and hmac.compare_digest(password, expected))

    def _cookie_value(self, name: str) -> str:
        """从请求头里读取指定 cookie。"""
        raw = self.headers.get("Cookie", "")
        for part in raw.split(";"):
            if "=" not in part:
                continue
            key, value = part.strip().split("=", 1)
            if key == name:
                return value
        return ""

    def _authenticated(self) -> bool:
        """判断当前浏览器是否已经登录；未配置密码时拒绝访问。"""
        return verify_token(self._cookie_value(AUTH_COOKIE_NAME))

    def _handle_login(self, payload: dict) -> None:
        if not allow_login(self.client_address[0]):
            self._send_json({"ok": False, "error": "尝试次数过多，请稍后重试"}, 429)
            return
        expected = _login_password()
        if not expected:
            self._send_json({"ok": False, "error": "未配置登录密码"}, 503)
            return
        password = str(payload.get("password") or "")
        if not hmac.compare_digest(password, expected):
            self._send_json({"ok": False, "error": "登录密码错误"}, 403)
            return
        cookie = auth_cookie(_auth_token())
        self._send_json({"ok": True}, headers={"Set-Cookie": cookie})

    def do_GET(self) -> None:
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            if path.startswith("/assets/"):
                self._send_asset(path)
                return
            if path == "/" and not self._authenticated():
                self._send_login_html()
                return
            if not self._authenticated():
                self._send_json({"ok": False, "error": "unauthorized"}, 401)
                return
            if path == "/":
                self._send_html()
            elif path == "/api/daily_advice":
                from .daily_advice import status
                self._send_json(status())
            elif path == "/api/health":
                from .metrics import snapshot
                pending = fetch_all("SELECT state,COUNT(*) AS n FROM execution_orders WHERE state IN ('unknown','submitting','prepared') GROUP BY state")
                beats = bot_heartbeats()
                self._send_json({"ok": True, "metrics": snapshot(), "orders_requiring_review": pending, "heartbeats": beats})
            elif path == "/api/retirement_allocation":
                from .retirement_allocation import load_config
                self._send_json({"ok": True, "config": load_config()})
            elif path == "/api/capital":
                self._send_json(self.server.snapshots.get("capital"))
            elif path == "/api/risk":
                self._send_json(self.server.snapshots.get("risk"))
            elif path == "/api/holdings":
                self._send_json(self.server.snapshots.get("holdings"))
            elif path == "/api/major_events":
                self._send_json(_major_events_payload())
            elif path == "/api/d_tactical":
                self._send_json(d_tactical_payload())
            elif path == "/api/d_option_preview":
                qs = parse_qs(parsed.query)
                symbol = qs.get("symbol", [""])[0]
                mode = qs.get("mode", ["BULL_CALL"])[0]
                width_raw = qs.get("width", ["10"])[0]
                try:
                    width = float(width_raw)
                except Exception:
                    width = 10.0
                self._send_json(option_preview(symbol, mode, width))
            elif path == "/api/state":
                self._send_json(_state_payload())
            elif path == "/api/schedules":
                self._send_json(_schedules_payload())
            elif path == "/api/exposure":
                self._send_json(_exposure_payload())
            elif path == "/api/trade_phase":
                self._send_json(_trade_phase_payload())
            elif path == "/api/market_categories":
                selected = parse_qs(parsed.query).get("category", [""])[0]
                self._send_json(_market_categories_payload(selected))
            elif path == "/api/equity_curve":
                period = parse_qs(parsed.query).get("period", ["week"])[0]
                self._send_json(_curve_payload(period))
            elif path == "/api/trade_records":
                self._send_json(_trade_records_payload())
            elif path == "/api/performance":
                period = parse_qs(parsed.query).get("period", ["90d"])[0]
                self._send_json(performance_payload(period))
            elif path == "/api/stock_selection":
                self._send_json(_stock_selection_payload())
            elif path == "/api/bot_logs":
                qs = parse_qs(parsed.query)
                try:
                    lines = int(qs.get("lines", ["120"])[0])
                except Exception:
                    lines = 120
                bot_name = str(qs.get("bot_name", [""])[0] or "")
                summary_only = str(qs.get("summary", ["0"])[0] or "0").lower() in {"1", "true", "yes"}
                search = str(qs.get("q", [""])[0] or "")
                self._send_json(_bot_logs_payload(lines, bot_name=bot_name, summary_only=summary_only, search=search))
            elif path == "/api/stock_quote":
                symbol = parse_qs(parsed.query).get("symbol", [""])[0]
                self._send_json(_stock_quote_payload(symbol))
            elif path == "/api/strategy_2_config":
                self._send_json(_strategy_2_config_payload())
            elif path == "/api/account_config":
                self._send_json(_account_config_payload())
            elif path == "/api/strategy_b_config":
                self._send_json(_strategy_b_config_payload())
            elif path == "/api/d_grid_config":
                self._send_json(d_grid_config_payload())
            elif path == "/api/rebalance":
                self._send_json({"ok": True, "rows": generate_rebalance_report()})
            else:
                self._send_json({"ok": False, "error": "not_found"}, 404)
        except Exception as exc:
            self._send_json({"ok": False, "error": str(exc)}, 500)

    def do_POST(self) -> None:
        try:
            path = urlparse(self.path).path
            payload = self._read_json()
            if path == "/api/login":
                self._handle_login(payload)
                return
            if not self._authenticated():
                self._send_json({"ok": False, "error": "unauthorized"}, 401)
                return
            if path == "/api/logout":
                self._send_json(
                    {"ok": True},
                    headers={"Set-Cookie": auth_cookie("")},
                )
            elif path == "/api/retirement_allocation":
                from .retirement_allocation import save_config, validate_config, load_config
                try:
                    config = validate_config(payload.get("config"))
                    existing = {r['symbol'] for r in load_config()['items']}
                    additions = [r['symbol'] for r in config['items'] if r['symbol'] not in existing]
                    if additions:
                        client = alpaca_gateway.trading_client(pool='A')
                        for symbol in additions:
                            asset = client.get_asset(symbol)
                            if not asset.tradable or str(getattr(asset.asset_class, 'value', asset.asset_class)) != 'us_equity':
                                raise ValueError(f'{symbol} 不是账户可交易的美股或 ETF')
                    result = save_config(config)
                except ValueError as exc:
                    self._send_json({"ok": False, "error": str(exc)}, 400)
                    return
                self._send_json({"ok": True, "config": result})
            elif path == "/api/daily_advice":
                from .daily_advice import start
                result = start(str(payload.get('mode','rules')))
                self._send_json(result, 202 if result.get('ok') else 400)
            elif path == "/api/clear_position":
                if not self._check_password(payload):
                    self._send_json({"ok": False, "error": "密码错误或未配置操作密码"}, 403)
                    return
                dry_run = bool(payload.get("dry_run") is True or str(payload.get("dry_run") or "").lower() in {"1", "true", "yes", "on"})
                result = alpaca_gateway.submit_current_price_limit_sell_all(dry_run=dry_run)
                action = "预检" if dry_run else "提交"
                result["ok"] = True
                result["message"] = f"清仓实时价限价卖单{action}完成：成功={result.get('ok_count', 0)} 失败={result.get('error_count', 0)} 总数={result.get('count', 0)}"
                self._send_json(result)
            elif path == "/api/d_option_buy":
                self._send_json(submit_option_combo(payload))
            elif path == "/api/manual_stock_order":
                self._send_json(_manual_stock_order_payload(payload))
            elif path == "/api/annual_goal_step":
                goal = str(payload.get("goal") or "").strip()
                self._send_json(_advance_annual_goal(goal))
            elif path == "/api/bot_control":
                bot_name = str(payload.get("bot_name") or "")
                enabled_raw = payload.get("enabled")
                if bot_name not in managed_bot_names():
                    self._send_json({"ok": False, "error": "不支持的机器人"}, 400)
                    return
                enabled = bool(enabled_raw is True or str(enabled_raw).lower() in {"1", "true", "yes", "on"})
                running = set_bot_runtime(bot_name, enabled)
                _sync_buy_bot_control(bot_name, enabled)
                self._send_json({"ok": True, "bot_name": bot_name, "enabled": enabled, "running": running})
            elif path == "/api/risk_settings":
                risk_preference = str(payload.get("risk_preference") or "").strip()
                margin_usage = payload.get("margin_usage")
                margin_mode = str(payload.get("margin_mode") or "").strip().upper()
                pool_enabled = payload.get("pool_enabled")
                response = {"ok": True}
                if risk_preference:
                    if risk_preference not in {"保守", "中性", "激进"}:
                        self._send_json({"ok": False, "error": "不支持的风险偏好"}, 400)
                        return
                    set_app_setting("RISK_PREFERENCE", risk_preference)
                    response["risk_preference"] = risk_preference
                if margin_mode:
                    if margin_mode not in {"AUTO", "MANUAL"}:
                        self._send_json({"ok": False, "error": "不支持的额度模式"}, 400)
                        return
                    set_app_setting("RISK_MARGIN_MODE", margin_mode)
                    response["margin_mode"] = margin_mode
                if margin_usage is not None:
                    try:
                        margin_value = float(margin_usage)
                    except Exception:
                        margin_value = 0.0
                    if margin_value not in {1.0, 1.1, 1.2, 1.3, 1.4, 1.5}:
                        self._send_json({"ok": False, "error": "不支持的保证金额度"}, 400)
                        return
                    set_app_setting("RISK_MARGIN_MODE", "MANUAL")
                    set_app_setting("RISK_TOTAL_CAPITAL_PCT", f"{margin_value:.1f}")
                    response["margin_usage"] = margin_value
                if isinstance(pool_enabled, dict):
                    current = {
                        group: str(get_app_setting(f"RISK_{group}_POOL_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on", "y"}
                        for group in ("A", "B", "C", "D")
                    }
                    for group, enabled_raw in pool_enabled.items():
                        group = str(group or "").upper()
                        if group not in current:
                            self._send_json({"ok": False, "error": "不支持的资金池"}, 400)
                            return
                        current[group] = bool(enabled_raw is True or str(enabled_raw).lower() in {"1", "true", "yes", "on"})
                    if not any(current.values()):
                        self._send_json({"ok": False, "error": "至少保留一个资金池开启"}, 400)
                        return
                    for group, enabled in current.items():
                        set_app_setting(f"RISK_{group}_POOL_ENABLED", "1" if enabled else "0")
                    response["pool_enabled"] = current
                if not any(key in response for key in ("risk_preference", "margin_usage", "margin_mode", "pool_enabled")):
                    self._send_json({"ok": False, "error": "没有可更新的设置"}, 400)
                    return
                try:
                    write_risk_state(get_risk_state())
                except Exception as exc:
                    response["risk_refresh_error"] = str(exc)[:180]
                try:
                    refresh_exposure_plan(mode="SUGGEST", execute=True)
                except Exception as exc:
                    response["exposure_refresh_error"] = str(exc)[:180]
                self._send_json(response)
            elif path == "/api/strategy_2_config":
                self._send_json(_save_strategy_2_config(payload))
            elif path == "/api/account_config":
                self._send_json(_save_account_config_payload(payload))
            elif path == "/api/d_grid_config":
                self._send_json(save_d_grid_config(payload))
            elif path == "/api/monthly_invest":
                force = bool(payload.get("force") is True)
                execute = bool(payload.get("execute") is True)
                self._send_json(run_monthly_investment(force=force, execute=execute))
            elif path == "/api/bot_logs/delete":
                result = _clear_bot_log_payload(payload)
                self._send_json(result, 200 if result.get("ok") else 400)
            elif path == "/api/stock_pool/delete":
                result = _delete_stock_pool_payload(payload)
                self._send_json(result, 200 if result.get("ok") else 400)
            elif path == "/api/stock_pool/add":
                result = _add_stock_pool_payload(payload)
                self._send_json(result, 200 if result.get("ok") else 400)
            elif path == "/api/c_core/set":
                result = _set_c_core_payload(payload)
                self._send_json(result, 200 if result.get("ok") else 400)
            elif path == "/api/sync_positions":
                ok = sync_all_positions()
                if not ok:
                    detail = last_sync_error()
                    self._send_json({"ok": False, "error": f"券商仓位同步失败：{detail or '请检查 Alpaca 配置和服务日志'}"}, 500)
                    return
                self._send_json({"ok": True, "message": "展示表和交易控制表已同步"})
            elif path == "/api/refresh_exposure":
                if not self._check_password(payload):
                    self._send_json({"ok": False, "error": "密码错误或未配置操作密码"}, 403)
                    return
                mode = str(payload.get("mode") or "SUGGEST").strip().upper()
                if mode not in {"SUGGEST", "AUTO"}:
                    mode = "SUGGEST"
                plan = refresh_exposure_plan(mode=mode, execute=True)
                self._send_json(
                    {
                        "ok": True,
                        "mode": plan.mode,
                        "action": plan.action,
                        "current_exposure_pct": plan.current_exposure_pct,
                        "target_exposure_pct": plan.target_exposure_pct,
                        "actions": plan.actions,
                    }
                )
            elif path == "/api/refresh_market_categories":
                selected = str(payload.get("category") or "")
                self._send_json(_refresh_market_categories_payload(selected))
            else:
                self._send_json({"ok": False, "error": "not_found"}, 404)
        except Exception as exc:
            self._send_json({"ok": False, "error": str(exc)}, 500)


def run() -> None:
    """启动内置 HTTP 服务。"""
    if not _login_password():
        raise RuntimeError("配置 DASHBOARD_LOGIN_PASSWORD 后才能启动交易看板")
    s = settings()
    from .main import startup

    startup()
    if env_str("ULTIMATE_SKIP_BOT_SYNC_ON_START", "0").strip().lower() not in {"1", "true", "yes"}:
        sync_from_controls()
        start_watchdog()
    from .dashboard_cache import DashboardCache
    from .manual_ledger import reconcile_manual_orders
    from threading import Event, Thread
    stop_reconcile = Event()
    def reconcile_loop():
        while not stop_reconcile.is_set():
            try:
                reconcile_manual_orders()
            except Exception as exc:
                print(f"[ORDER RECONCILE] {type(exc).__name__}: manual orders require attention", flush=True)
            stop_reconcile.wait(5)
    Thread(target=reconcile_loop, name="order-reconciliation", daemon=True).start()
    server = DashboardServer((s.web_host, s.web_port), Handler)
    server.snapshots = DashboardCache({"capital": _allocation_payload, "risk": _risk_payload, "holdings": _holdings_payload})
    server.snapshots.start()
    print(f"[WEB] http://127.0.0.1:{s.web_port}", flush=True)
    try:
        server.serve_forever()
    finally:
        stop_reconcile.set()
        server.snapshots.close()
        server.server_close()
        shutdown_supervisor()


if __name__ == "__main__":
    run()
