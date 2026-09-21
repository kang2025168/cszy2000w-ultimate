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
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from . import alpaca_gateway
from .account_config import load_account_config, public_account_config, save_account_config
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
        return float(value)
    except Exception:
        return default


AUTH_COOKIE_NAME = "cszy_ultimate_auth"


def _login_password() -> str:
    """读取网页登录密码；未单独配置时复用手机控制密码，避免部署后锁死。"""
    return env_str(
        "DASHBOARD_LOGIN_PASSWORD",
        env_str("ULTIMATE_LOGIN_PASSWORD", env_str("DASHBOARD_ACTION_PASSWORD", env_str("MOBILE_CONTROL_TOKEN", ""))),
    )


def _auth_secret() -> str:
    """读取登录签名密钥；生产环境建议单独配置，避免 cookie 被猜到。"""
    return env_str("DASHBOARD_AUTH_SECRET", _login_password() or "cszy-ultimate-v1")


def _auth_token() -> str:
    """生成浏览器登录 cookie 的签名值。"""
    password = _login_password()
    if not password:
        return ""
    return hmac.new(_auth_secret().encode("utf-8"), f"dashboard:{password}".encode("utf-8"), hashlib.sha256).hexdigest()


def _allocation_payload() -> dict:
    """组装资金池接口数据。"""
    allocation = get_capital_allocation()
    if allocation is None:
        return {"ok": False, "error": "account_snapshot_failed"}
    margin_mode, margin_usage, margin_reason = resolve_margin_usage_pct()
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
        "desc": "A 用养老金账户定投三只指数基金；B 自动动量；C 按 28 个长期标的自动建仓并做 T；D 做日内交易。",
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
            "mission": "负责养老金账户长期定投，按月把 A 可用资金按比例买入，不参与 B 动量和 D 日内交易。",
            "select_rules": [
                {"key": "a_stock_type", "label": "长期核心标记", "value": "stock_type=A", "unit": "", "enabled": True},
                {"key": "a_market_filter", "label": "市场环境过滤", "value": "向上/横盘优先", "unit": "", "enabled": True},
                {"key": "a_rebalance_source", "label": "资金来源", "value": "A 养老金账户", "unit": "", "enabled": True},
            ],
            "buy_rules": [
                {"key": "a_buy_style", "label": "买入方式", "value": "每月15号按比例买入", "unit": "", "enabled": True},
                {"key": "a_position_role", "label": "仓位角色", "value": "长期核心仓", "unit": "", "enabled": True},
                {"key": "a_no_intraday", "label": "禁止日内投机", "value": "是", "unit": "", "enabled": True},
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
                {"key": "c_universe", "label": "长期预选池", "value": "28 只（25只股票+QQQ/VOO/XLV）", "unit": "", "enabled": True},
                {"key": "c_foundation", "label": "指数底仓", "value": "QQQ 12% / VOO 12% / XLV 6%", "unit": "", "enabled": True},
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
                {"key": "c_force_recover", "label": "收盘前强制恢复核心仓", "value": "12:55", "unit": "LA", "enabled": True},
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
                f"C目标资金 {env_float('C_CORE_DAILY_BUDGET_PCT', 0.10):.0%}，"
                f"最多 ${env_float('C_CORE_DAILY_BUDGET_MAX_USD', 250.0):,.0f}"
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
                        sell_only_mode=0,
                        emergency_stop=0
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
                client_match = re.search(r"cszy-manual-([ABCD])-", client_order_id, re.I)
                group = (
                    (client_match.group(1).upper() if client_match else "")
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
        "manual_trade_records": 0,
        "orders": 1,
        "alpaca_orders": 2,
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
    return {"ok": True, "rows": cleaned[:500]}


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


def _manual_strategy_b_stop_loss(entry_price: float) -> float:
    try:
        from app.strategy_b import B_INITIAL_STOP_MULT

        mult = float(B_INITIAL_STOP_MULT)
    except Exception:
        mult = 0.95
    return round(max(0.0, float(entry_price or 0.0) * mult), 2)


def _manual_stop_policy(price: float, pool: str, side: str) -> dict:
    """Return the protection policy recorded for a manual buy.

    B already has an automatic sell worker. A/C/D levels are recorded as
    protective references until their dedicated exit workers are implemented.
    """
    if str(side or "").lower() != "buy":
        return {}
    pool = str(pool or "").upper()
    policies = {
        "A": (0.85, "A 灾难保护 -15%", "长期养老金仓；仅记录保护线，不做日内自动止损", False),
        "B": (None, "B 初始止损 -5%", "策略 B 卖出机器人自动接管", True),
        "C": (0.88, "C 结构保护 -12%", "长期成长仓；仅记录保护线，等待专用退出规则确认", False),
        "D": (0.97, "D 日内保护 -3%", "记录日内保护线，并保留收盘前强制平仓", False),
    }
    policy = policies.get(pool)
    if not policy:
        return {}
    mult, rule, note, automated = policy
    stop_loss = _manual_strategy_b_stop_loss(price) if pool == "B" else round(max(0.0, float(price or 0) * mult), 2)
    if stop_loss <= 0:
        return {}
    return {
        "auto_stop_loss": automated,
        "protection_recorded": True,
        "stop_loss_price": stop_loss,
        "stop_loss_rule": rule,
        "stop_loss_note": note,
    }


def _manual_stock_qty(raw_qty: float, price: float, full_qty: float | None = None) -> float:
    """手动股票数量：高价股允许 0.1 股，普通股仍按整股。"""
    raw = max(0.0, float(raw_qty or 0.0))
    if full_qty is not None:
        full = max(0.0, float(full_qty or 0.0))
        if abs(raw - full) < 1e-9:
            return round(full, 4)
    if price > 50:
        return round(int(raw * 10) / 10, 1)
    return float(int(raw))


def _manual_order_fill(client, symbol: str, order_id: str, fallback_qty: float, fallback_price: float) -> tuple[float, float, str]:
    status = ""
    filled_qty = 0.0
    filled_avg = 0.0
    deadline = time.time() + 6.0
    while time.time() < deadline:
        try:
            order = client.get_order_by_id(str(order_id))
            status = str(getattr(order, "status", "") or "")
            filled_qty = _safe_float(getattr(order, "filled_qty", 0))
            filled_avg = _safe_float(getattr(order, "filled_avg_price", 0))
            if filled_qty > 0 and filled_avg > 0:
                return filled_qty, filled_avg, status
            if status.lower() in {"canceled", "cancelled", "expired", "rejected"}:
                return 0.0, 0.0, status
        except Exception:
            pass
        try:
            pos = client.get_open_position(symbol)
            pos_qty = _safe_float(getattr(pos, "qty", 0))
            pos_avg = _safe_float(getattr(pos, "avg_entry_price", 0))
            if pos_qty > 0 and pos_avg > 0:
                return min(pos_qty, float(fallback_qty or pos_qty)), pos_avg, status or "position_synced"
        except Exception:
            pass
        time.sleep(0.5)
    if filled_qty > 0 and filled_avg <= 0:
        filled_avg = float(fallback_price or 0.0)
    return filled_qty, filled_avg, status


def _manual_table_columns(conn, table: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(f"SHOW COLUMNS FROM `{table}`")
        return {str(row.get("Field") or "") for row in cur.fetchall() or []}


def _manual_update_ops_row(conn, table: str, columns: set[str], row_id, symbol: str, stock_type: str, values: dict) -> None:
    pairs = []
    args = []
    for key, value in values.items():
        if key not in columns:
            continue
        pairs.append(f"`{key}`=%s")
        args.append(value)
    if "updated_at" in columns:
        pairs.append("`updated_at`=CURRENT_TIMESTAMP")
    if not pairs:
        return
    if row_id is not None and "id" in columns:
        where_sql = "id=%s"
        args.append(row_id)
    else:
        where_sql = "stock_code=%s AND stock_type=%s"
        args.extend((symbol, stock_type))
    with conn.cursor() as cur:
        cur.execute(f"UPDATE `{table}` SET {', '.join(pairs)} WHERE {where_sql}", tuple(args))


def _manual_insert_ops_row(conn, table: str, columns: set[str], values: dict) -> None:
    keys = [key for key in values if key in columns]
    if not keys:
        return
    fields = ", ".join(f"`{key}`" for key in keys)
    placeholders = ", ".join(["%s"] * len(keys))
    args = tuple(values[key] for key in keys)
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO `{table}` ({fields}) VALUES ({placeholders})", args)


def _record_manual_trade(preview: dict) -> None:
    """Persist one manual order event; repeated writes update its broker status."""
    order_id = str(preview.get("order_id") or "").strip()
    if not order_id:
        return
    qty = _safe_float(preview.get("qty"))
    filled_qty = _safe_float(preview.get("filled_qty"))
    price = _safe_float(preview.get("price"))
    filled_avg = _safe_float(preview.get("filled_avg_price"))
    note = (
        f"手动{ {'buy':'买入','sell':'卖出','short':'卖空'}.get(str(preview.get('side') or ''), '交易') }"
        f" · {str(preview.get('order_type') or 'limit').upper()}"
        f" · 资金池 {str(preview.get('pool') or '').upper()}"
    )
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO manual_trade_records (
                    event_time, symbol, side, strategy_group,
                    qty, filled_qty, price, filled_avg_price,
                    order_type, status, note, order_id
                ) VALUES (
                    NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    filled_qty=VALUES(filled_qty),
                    filled_avg_price=VALUES(filled_avg_price),
                    status=VALUES(status),
                    note=VALUES(note),
                    updated_at=CURRENT_TIMESTAMP
                """,
                (
                    str(preview.get("symbol") or "").upper(),
                    str(preview.get("side") or "").upper(),
                    str(preview.get("pool") or "").upper(),
                    qty,
                    filled_qty,
                    price,
                    filled_avg,
                    str(preview.get("order_type") or "limit").lower(),
                    str(preview.get("status") or "submitted")[:32],
                    note[:512],
                    order_id,
                ),
            )


def _record_manual_buy(symbol: str, pool: str, qty: float, avg_price: float, current_price: float, order_id: str) -> dict:
    pool = str(pool or "").upper()
    protection = _manual_stop_policy(avg_price, pool, "buy")
    stop_loss = _safe_float(protection.get("stop_loss_price"))
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        from app.strategy_b import OPS_TABLE, _intent_short
    except Exception:
        OPS_TABLE = "stock_operations"

        def _intent_short(value: str) -> str:
            return value[:80]

    values = {
        "stock_code": symbol,
        "stock_type": pool,
        "strategy_group": pool,
        "capital_pool": pool,
        "margin_used": 1 if pool == "D" else 0,
        "is_bought": 1,
        "can_sell": 1,
        "can_buy": 0,
        "qty": float(qty or 0),
        "base_qty": float(qty or 0),
        "cost_price": round(float(avg_price or 0), 2),
        "close_price": round(float(current_price or avg_price or 0), 2),
        "current_price": round(float(current_price or avg_price or 0), 2),
        "stop_loss_price": stop_loss,
        "take_profit_price": 0,
        "b_stage": 0,
        "b_peak_price": round(float(avg_price or 0), 2),
        "b_peak_profit": 0,
        "b_last_profit": 0,
        "b_stop_pending_since": None,
        "b_stop_pending_sl": None,
        "last_order_side": "buy",
        "last_order_intent": _intent_short(f"{pool}:MANUAL_BUY protection_recorded"),
        "last_order_id": str(order_id or ""),
        "last_order_time": now_text,
        "last_capital_check_at": now_text,
    }
    total_qty = float(qty or 0)
    blended_avg = float(avg_price or 0)
    with db_conn() as conn:
        columns = _manual_table_columns(conn, OPS_TABLE)
        with conn.cursor() as cur:
            desired_fields = ["id", "stock_code", "qty", "cost_price", "last_order_id"]
            select_fields = ", ".join(field for field in desired_fields if field in columns)
            cur.execute(
                f"""
                SELECT {select_fields}
                FROM `{OPS_TABLE}`
                WHERE stock_code=%s AND stock_type=%s
                ORDER BY {"id DESC" if "id" in columns else "stock_code"}
                LIMIT 1
                """,
                (symbol, pool),
            )
            existing = cur.fetchone()
        if existing:
            previous_order_id = str(existing.get("last_order_id") or "")
            previous_qty = _safe_float(existing.get("qty"))
            previous_avg = _safe_float(existing.get("cost_price"))
            if str(order_id or "") and previous_order_id != str(order_id or ""):
                total_qty = previous_qty + float(qty or 0)
                if total_qty > 0:
                    blended_avg = (
                        previous_qty * previous_avg + float(qty or 0) * float(avg_price or 0)
                    ) / total_qty
            elif previous_order_id == str(order_id or "") and previous_qty > 0:
                total_qty = previous_qty
                blended_avg = previous_avg or blended_avg
            protection = _manual_stop_policy(blended_avg, pool, "buy")
            stop_loss = _safe_float(protection.get("stop_loss_price"))
            values.update(
                {
                    "qty": total_qty,
                    "base_qty": total_qty,
                    "cost_price": round(blended_avg, 4),
                    "stop_loss_price": stop_loss,
                }
            )
            _manual_update_ops_row(conn, OPS_TABLE, columns, existing.get("id"), symbol, pool, values)
        else:
            _manual_insert_ops_row(conn, OPS_TABLE, columns, values)

    try:
        from .position_holdings import upsert_buy_holding

        upsert_buy_holding(
            symbol,
            pool,
            total_qty,
            blended_avg,
            stock_type=pool,
            current_price=float(current_price or avg_price or 0),
            stop_loss_price=stop_loss,
            take_profit_price=0,
            b_stage=0 if pool == "B" else None,
            capital_pool=pool,
            margin_used=1 if pool == "D" else 0,
            last_order_id=str(order_id or ""),
        )
    except Exception as exc:
        print(f"[WEB MANUAL BUY] {symbol} position_holding write failed: {exc}", flush=True)

    return {**protection, "stop_loss_added": True, "recorded_stock_type": pool}


def _pool_account_buying_power(capital: dict, pool: str) -> float:
    """Return buying power for the broker account that owns this pool."""
    profile = str((capital.get("pool_brokers") or {}).get(pool) or "").strip()
    snapshot = (capital.get("broker_snapshots") or {}).get(profile) or {}
    return max(0.0, _safe_float(snapshot.get("buying_power")))


def _manual_stock_order_payload(payload: dict) -> dict:
    """手动股票下单预览/执行。买入/卖空按资金池额度，卖出按当前持仓比例。"""
    symbol = str(payload.get("symbol") or "").strip().upper()
    side = str(payload.get("side") or "").strip().lower()
    pool = str(payload.get("pool") or "C").strip().upper()
    size = str(payload.get("size") or "1/4").strip()
    order_type = str(payload.get("order_type") or "limit").strip().lower()
    execute = bool(payload.get("execute") is True)
    if not symbol or not symbol.replace(".", "").isalpha():
        return {"ok": False, "error": "股票代码无效"}
    if side not in {"buy", "sell", "short"}:
        return {"ok": False, "error": "只支持买入、卖出或卖空"}
    if pool not in {"A", "B", "C", "D"}:
        return {"ok": False, "error": "资金池无效"}
    if side == "short" and pool == "A":
        return {"ok": False, "error": "A 养老金账户不支持卖空"}
    fractions = {"1/4": 0.25, "1/3": 1 / 3, "1/2": 0.5, "1/1": 1.0, "full": 1.0}
    fraction = fractions.get(size)
    if fraction is None:
        return {"ok": False, "error": "额度选项无效"}
    if order_type not in {"limit", "market"}:
        return {"ok": False, "error": "订单类型无效"}

    quote = _stock_quote_payload(symbol)
    last = _safe_float(quote.get("last"))
    bid = _safe_float(quote.get("bid"))
    ask = _safe_float(quote.get("ask"))
    client_last = _safe_float(payload.get("client_last"))
    client_bid = _safe_float(payload.get("client_bid"))
    client_ask = _safe_float(payload.get("client_ask"))
    if client_last > 0:
        last = client_last
    if client_bid > 0:
        bid = client_bid
    if client_ask > 0:
        ask = client_ask
    limit_price = _safe_float(payload.get("limit_price"))
    price = limit_price if order_type == "limit" and limit_price > 0 else last
    if price <= 0:
        return {"ok": False, "error": "暂时没有可用报价"}

    qty = 0.0
    notional = 0.0
    available = 0.0
    held_qty = 0.0
    if side in {"sell", "short"}:
        try:
            for pos in alpaca_gateway.list_positions(pool=pool):
                if str(getattr(pos, "symbol", "") or "").upper() == symbol:
                    held_qty = _safe_float(getattr(pos, "qty", 0))
                    break
        except Exception as exc:
            return {"ok": False, "error": f"读取持仓失败: {str(exc)[:120]}"}
        if side == "short" and held_qty > 0:
            return {"ok": False, "error": f"{pool} 资金池当前持有 {held_qty:.4f} 股，先用卖出平仓后再卖空"}
    if side in {"buy", "short"}:
        cap = _allocation_payload()
        if not cap.get("ok"):
            return {"ok": False, "error": cap.get("error") or "资金池不可用"}
        available = _safe_float((cap.get("available") or {}).get(pool))
        buying_power = _pool_account_buying_power(cap, pool)
        notional = max(0.0, min(available * fraction, buying_power))
        qty = _manual_stock_qty(notional / price, price)
        notional = qty * price
    else:
        qty = _manual_stock_qty(max(0.0, held_qty * fraction), price, held_qty if fraction >= 1.0 else None)
        notional = qty * price

    if qty <= 0 or notional <= 0:
        return {"ok": False, "error": "按当前额度/持仓计算后数量为 0，无法下单"}

    preview = {
        "ok": True,
        "execute": execute,
        "symbol": symbol,
        "side": side,
        "pool": pool,
        "size": size,
        "fraction": fraction,
        "order_type": order_type,
        "limit_price": price,
        "price": price,
        "bid": bid,
        "ask": ask,
        "last": last,
        "qty": qty,
        "notional": notional,
        "available": available,
        "held_qty": held_qty,
        "message": "预览完成，未提交订单",
    }
    preview.update(_manual_stop_policy(price, pool, side))
    if not execute:
        return preview

    from alpaca.trading.enums import OrderSide, TimeInForce
    from alpaca.trading.requests import LimitOrderRequest, MarketOrderRequest

    client = alpaca_gateway.trading_client(pool=pool)
    order_side = OrderSide.BUY if side == "buy" else OrderSide.SELL
    client_order_id = f"cszy-manual-{pool}-{side}-{int(time.time() * 1000)}"
    if order_type == "market":
        req = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=order_side,
            time_in_force=TimeInForce.DAY,
            client_order_id=client_order_id,
        )
    else:
        req = LimitOrderRequest(
            symbol=symbol,
            qty=qty,
            side=order_side,
            limit_price=alpaca_gateway.stock_limit_price(price),
            time_in_force=TimeInForce.DAY,
            extended_hours=True,
            client_order_id=client_order_id,
        )
    order = client.submit_order(order_data=req)
    order_id = str(getattr(order, "id", "") or getattr(order, "order_id", "") or "")
    status = str(getattr(order, "status", "") or "")
    preview.update(
        {
            "message": "订单已提交",
            "order_id": order_id,
            "status": status,
        }
    )
    _record_manual_trade(preview)
    if side == "buy":
        filled_qty, filled_avg, fill_status = _manual_order_fill(client, symbol, order_id, qty, price)
        preview.update(
            {
                "filled_qty": filled_qty,
                "filled_avg_price": filled_avg,
                "status": fill_status or status,
            }
        )
        if filled_qty > 0 and filled_avg > 0:
            stop_meta = _record_manual_buy(symbol, pool, filled_qty, filled_avg, last or filled_avg, order_id)
            preview.update(stop_meta)
            preview["message"] = f"订单已提交，已归入 {pool} 类型"
            if pool == "B":
                preview["stop_loss_note"] = "B 初始止损已写入，策略 B 卖出机器人会自动接管"
        else:
            preview["stop_loss_added"] = False
            preview["stop_loss_note"] = "订单尚未成交，未写入本地止损"
        _record_manual_trade(preview)
    return preview


INDEX_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>CSZY Ultimate V1</title>
  <style>
    :root { color-scheme: light; --bg:#f4f7fb; --panel:#ffffff; --panel-soft:#f8fbff; --ink:#17202a; --muted:#667085; --line:#d7e0ea; --line-soft:#e8eef6; --green:#15936a; --red:#c62828; --amber:#b76e00; --blue:#2563eb; --cyan:#0891b2; --violet:#7c3aed; --gold:#d97706; --shadow:0 16px 42px rgba(15,23,42,.07); --shadow-soft:0 10px 26px rgba(15,23,42,.045); }
    * { box-sizing: border-box; }
    body { margin:0; font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background:linear-gradient(180deg, #edf4fb 0%, #f8fafc 36%, var(--bg) 100%); color:var(--ink); }
    header { display:none; }
    h1 { font-size:26px; margin:0; letter-spacing:0; line-height:1; }
    h2 { font-size:15px; margin:0; }
    button { border:1px solid var(--line); background:#fff; color:var(--ink); height:34px; padding:0 12px; border-radius:6px; cursor:pointer; transition:transform .12s ease, border-color .12s ease, box-shadow .12s ease, background .12s ease; }
    button:hover { border-color:#bfd0e4; box-shadow:0 6px 16px rgba(15,23,42,.05); }
    main { padding:20px 24px 36px; max-width:1680px; margin:0 auto; }
    .left-titlebar { grid-column:1 / -1; position:relative; z-index:20; min-height:58px; display:flex; align-items:center; justify-content:space-between; gap:14px; padding:0 0 4px; border:0; border-radius:0; background:transparent; box-shadow:none; }
    .brand-lockup { display:flex; align-items:center; gap:12px; min-width:0; }
    .brand-logo { width:42px; height:42px; border-radius:8px; object-fit:contain; background:#fff; box-shadow:0 10px 24px rgba(15,23,42,.10); border:1px solid #edf2f7; }
    .brand-copy { min-width:0; display:flex; flex-direction:column; gap:0; }
    .left-titlebar h1 { color:#17202a; text-shadow:none; white-space:nowrap; }
    .dashboard-motto { display:none; }
    .title-actions { display:flex; align-items:center; gap:7px; flex:0 0 auto; padding:4px 6px; border:1px solid #d8e4f0; border-radius:8px; background:rgba(255,255,255,.54); }
    .phase-chip { min-width:112px; height:34px; border:1px solid #cbd8e6; border-radius:999px; background:rgba(255,255,255,.96); display:flex; align-items:center; justify-content:center; gap:6px; padding:0 11px; font-size:12px; font-weight:850; color:var(--ink); box-shadow:0 7px 16px rgba(15,23,42,.06); }
    .phase-chip .phase-dot { width:9px; height:9px; border-radius:50%; background:var(--muted); box-shadow:0 0 0 4px rgba(102,112,133,.1); }
    .phase-chip.ok .phase-dot { background:var(--green); box-shadow:0 0 0 4px rgba(21,147,106,.12); }
    .phase-chip.blue .phase-dot { background:var(--blue); box-shadow:0 0 0 4px rgba(37,99,235,.12); }
    .phase-chip.warn .phase-dot { background:var(--amber); box-shadow:0 0 0 4px rgba(183,110,0,.14); }
    .phase-chip.sleep .phase-dot { background:var(--red); box-shadow:0 0 0 4px rgba(198,40,40,.12); }
    .trade-focus-btn { height:34px; min-width:66px; padding:0 12px; border:1px solid #bfd4ee; border-radius:8px; background:#fff; color:#075985; font-size:13px; font-weight:900; box-shadow:0 7px 16px rgba(15,23,42,.06); transition:background .14s ease, color .14s ease, transform .12s ease; }
    .trade-focus-btn:hover { background:#eff6ff; color:#0f172a; }
    .trade-focus-btn.active { background:#101828; border-color:#101828; color:#fff; }
    .trade-focus-btn:active { transform:scale(.96); }
    .phase-popover { position:absolute; z-index:12; top:72px; left:24px; width:min(680px, calc(100vw - 48px)); display:none; background:#fff; border:1px solid var(--line); border-radius:10px; box-shadow:0 24px 70px rgba(15,23,42,.18); padding:14px; }
    .phase-popover.show { display:block; }
    .phase-summary { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:10px; }
    .phase-pill { border-radius:999px; background:#eef2f6; color:var(--muted); padding:5px 9px; font-size:12px; font-weight:750; }
    .phase-rule-grid { display:grid; gap:8px; }
    .phase-rule { border:1px solid var(--line); border-radius:8px; padding:10px; background:#fbfcfe; }
    .phase-rule.active { border-color:var(--blue); box-shadow:0 0 0 1px rgba(37,99,235,.16) inset; background:#eff6ff; }
    .phase-rule-title { display:flex; gap:10px; align-items:baseline; font-weight:850; }
    .phase-rule-title span { color:var(--muted); font-size:12px; }
    .phase-rule p { margin:6px 0 0; color:var(--muted); font-size:12px; line-height:1.45; }
    .refresh-btn { height:34px; padding:0 14px; border:0; border-radius:8px; background:#2563eb; color:#fff; font-weight:850; box-shadow:0 7px 16px rgba(37,99,235,.18); transition:transform .12s ease, background .12s ease, opacity .12s ease; }
    .refresh-btn:hover { background:#1d4ed8; }
    .refresh-btn:active { transform:scale(.96); }
    .refresh-btn.loading { opacity:.72; pointer-events:none; }
    .dash { display:grid; grid-template-columns:minmax(560px, 1.08fr) minmax(520px, .92fr); column-gap:18px; row-gap:8px; align-items:stretch; padding:16px; border:1px solid #c9d7e6; border-radius:8px; background:linear-gradient(180deg,#fff 0%,#f8fbff 100%); box-shadow:0 18px 44px rgba(15,23,42,.08); }
    .panel { background:linear-gradient(180deg, #fff 0%, #fbfdff 100%); border:1px solid var(--line); border-radius:8px; padding:16px; box-shadow:var(--shadow-soft); }
    .mobile-collapse-toggle { display:none; }
    .left-stack, .right-stack { display:flex; flex-direction:column; gap:18px; min-width:0; }
    .right-stack { padding-top:0; }
    body.trade-focus main { max-width:none; gap:12px; }
    body.trade-focus .dash { display:block; }
    body.trade-focus .left-stack { display:block; }
    body.trade-focus .right-stack, body.trade-focus .capital-hero, body.trade-focus .phase-popover, body.trade-focus .log-focus-panel, body.trade-focus .life-focus-panel, body.trade-focus .stock-focus-panel { display:none !important; }
    body.trade-focus .holdings-panel { display:block; margin-top:12px; min-height:calc(100vh - 116px); }
    body.trade-focus .holding-head { display:none !important; }
    body.trade-focus .lower-slider { display:none !important; }
    body.trade-focus .manual-buy-entry { display:grid; }
    body.config-focus main { max-width:none; gap:12px; }
    body.config-focus .dash { display:block; }
    body.config-focus .left-stack { display:block; }
    body.config-focus .right-stack, body.config-focus .capital-hero, body.config-focus .phase-popover, body.config-focus .log-focus-panel, body.config-focus .life-focus-panel, body.config-focus .stock-focus-panel { display:none !important; }
    body.config-focus .holdings-panel { display:block; margin-top:12px; min-height:calc(100vh - 116px); }
    body.config-focus .holding-head { display:none !important; }
    body.config-focus .lower-slider { display:block; margin-top:0; }
    body.holdings-focus main { max-width:none; gap:12px; }
    body.holdings-focus .dash { display:block; }
    body.holdings-focus .left-stack { display:block; }
    body.holdings-focus .right-stack, body.holdings-focus .capital-hero, body.holdings-focus .phase-popover, body.holdings-focus .log-focus-panel, body.holdings-focus .life-focus-panel, body.holdings-focus .stock-focus-panel { display:none !important; }
    body.holdings-focus .holdings-panel { display:block; margin-top:12px; min-height:calc(100vh - 116px); }
    body.holdings-focus .holdings-panel .scroll { max-height:calc(100vh - 238px); }
    body.holdings-focus .holding-tabs { display:flex; }
    body.holdings-focus .holding-right-tools { display:none !important; }
    body.log-focus main { max-width:none; gap:12px; }
    body.log-focus .dash { display:block; }
    body.log-focus .left-stack { display:block; }
    body.log-focus .right-stack, body.log-focus .capital-hero, body.log-focus .holdings-panel, body.log-focus .phase-popover, body.log-focus .life-focus-panel { display:none !important; }
    body.log-focus .log-focus-panel { display:block; min-height:calc(100vh - 116px); margin-top:12px; }
    body.life-focus main { max-width:none; gap:12px; }
    body.life-focus .dash { display:block; }
    body.life-focus .left-stack { display:block; }
    body.life-focus .right-stack, body.life-focus .capital-hero, body.life-focus .holdings-panel, body.life-focus .phase-popover, body.life-focus .log-focus-panel { display:none !important; }
    body.life-focus .life-focus-panel { display:block; min-height:calc(100vh - 116px); margin-top:12px; }
    body.stock-focus main { max-width:none; gap:12px; }
    body.stock-focus .dash { display:block; }
    body.stock-focus .left-stack { display:block; }
    body.stock-focus .right-stack, body.stock-focus .capital-hero, body.stock-focus .holdings-panel, body.stock-focus .phase-popover, body.stock-focus .log-focus-panel, body.stock-focus .life-focus-panel { display:none !important; }
    body.stock-focus .stock-focus-panel { display:block; min-height:calc(100vh - 116px); margin-top:12px; }
    .capital-hero { flex:0 0 auto; }
    .hero-top { display:grid; grid-template-columns:minmax(340px,1fr) minmax(300px,.78fr); gap:12px; align-items:start; padding:14px; border:1px solid #c5d5e6; border-radius:8px; background:linear-gradient(145deg,#eef5fb 0%,#f8fbff 45%,#edf4fa 100%); box-shadow:inset 0 1px 0 rgba(255,255,255,.86), 0 10px 26px rgba(15,23,42,.06); }
    .hero-top:before { content:""; grid-column:1 / -1; height:3px; border-radius:999px; background:linear-gradient(90deg,#15936a,#2563eb,#d97706); opacity:.72; margin:-2px 0 0; }
    .hero-main-column { display:grid; gap:12px; min-width:0; }
    .hero-donut { border:1px solid #cbd8e6; border-radius:8px; min-height:164px; padding:14px; overflow:hidden; background:linear-gradient(135deg,#f0f7ff 0%,#f8fbff 58%,#eef6fb 100%); box-shadow:inset 0 0 0 1px rgba(255,255,255,.82), 0 12px 28px rgba(15,23,42,.07); }
    .hero-donut-head { display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:4px; }
    .mode-pill { display:inline-flex; align-items:center; justify-content:center; min-width:48px; height:26px; padding:0 10px; border-radius:999px; background:#101828; color:#fff; font-size:12px; font-weight:900; }
    .hero-carousel-viewport { overflow:hidden; width:100%; }
    .hero-carousel-track { width:300%; display:flex; transition:transform .28s ease; }
    .hero-carousel-track.allocation { transform:translateX(-33.3333%); }
    .hero-carousel-track.bots { transform:translateX(-66.6667%); }
    .hero-carousel-page { width:33.3333%; flex:0 0 33.3333%; min-width:0; display:flex; flex-direction:column; }
    .hero-carousel-page .donut-wrap { min-height:112px; }
    .allocation-grid { display:grid; grid-template-columns:1fr; gap:8px; padding:9px 2px 2px 0; min-height:174px; max-height:186px; overflow:auto; }
    .allocation-card { border:1px solid #d7e4f1; border-radius:8px; background:rgba(255,255,255,.72); padding:9px; display:grid; gap:7px; min-width:0; }
    .allocation-head { display:flex; align-items:center; justify-content:space-between; gap:8px; }
    .allocation-name { color:var(--ink); font-size:13px; font-weight:950; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .allocation-role { color:var(--muted); font-size:11px; font-weight:850; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .allocation-meta { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:6px; }
    .allocation-metric { border:1px solid #e6edf5; border-radius:7px; padding:5px 6px; background:#fff; min-width:0; }
    .allocation-metric span { display:block; color:var(--muted); font-size:10px; font-weight:850; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .allocation-metric b { display:block; margin-top:2px; color:var(--ink); font-size:12px; font-weight:950; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .hero-side-column { display:grid; gap:10px; align-self:stretch; min-width:0; padding:10px; border:1px solid #d5e2ef; border-radius:8px; background:linear-gradient(135deg,#fffaf3 0%,#f7fbff 54%,#eef5fb 100%); box-shadow:inset 0 1px 0 rgba(255,255,255,.78); }
    .mobile-real-assets { display:none; }
    .hero-pools-list { display:grid; gap:8px; }
    .hero-pool-row { border:1px solid #d8e3ef; border-radius:8px; background:linear-gradient(180deg,#fff,#f8fbff); padding:9px 10px; display:grid; gap:6px; box-shadow:0 6px 14px rgba(15,23,42,.035); }
    .hero-pool-top { display:flex; align-items:center; justify-content:space-between; gap:10px; }
    .hero-pool-title { display:flex; align-items:center; gap:8px; flex-wrap:wrap; min-width:0; }
    .hero-pool-name { color:var(--ink); font-size:13px; font-weight:950; }
    .hero-pool-total { display:inline-flex; align-items:center; min-height:22px; padding:2px 8px; border-radius:7px; background:#eef6ff; color:#075985; font-size:12px; font-weight:950; line-height:1; white-space:nowrap; }
    .hero-pool-meta { color:var(--muted); font-size:11px; font-weight:800; }
    .hero-pool-mid { display:flex; align-items:baseline; justify-content:space-between; gap:10px; }
    .hero-pool-used { color:var(--ink); font-size:18px; font-weight:950; line-height:1; }
    .hero-pool-available { color:var(--muted); font-size:11px; font-weight:850; white-space:nowrap; }
    .metric-grid { display:grid; grid-template-columns:repeat(2, minmax(0,1fr)); gap:10px; align-self:start; }
    .metric { border:1px solid #cfdae7; border-radius:8px; padding:10px 12px; min-height:68px; background:linear-gradient(180deg,#fff,#f8fafc); box-shadow:0 8px 18px rgba(15,23,42,.06); }
    .metric-label, .pool-meta, .small-muted { color:var(--muted); font-size:12px; }
    .metric-value { font-size:15px; font-weight:850; margin-top:5px; line-height:1.1; white-space:nowrap; font-variant-numeric:tabular-nums; }
    .risk-compact-card { border:1px solid #d5e6f8; border-radius:8px; padding:12px; display:grid; gap:10px; background:linear-gradient(135deg, #fff 0%, #f4fbff 100%); box-shadow:inset 0 0 0 1px rgba(255,255,255,.72), 0 10px 24px rgba(15,23,42,.045); }
    .risk-topbar { display:grid; grid-template-columns:1fr; gap:10px; }
    .risk-main { min-width:0; display:grid; gap:10px; }
    .risk-head { display:grid; grid-template-columns:1fr; gap:8px; min-width:0; }
    .risk-head h2 { white-space:nowrap; }
    .risk-body { min-width:0; }
    .risk-line { display:flex; gap:8px; flex-wrap:wrap; align-content:flex-start; color:var(--muted); font-size:11px; }
    .risk-chip { min-height:24px; display:inline-flex; align-items:center; border-radius:999px; padding:4px 9px; background:#eef2f6; color:#475467; font-size:11px; font-weight:850; white-space:nowrap; border:1px solid rgba(255,255,255,.72); box-shadow:0 4px 10px rgba(15,23,42,.035); }
    .risk-chip-label { color:inherit; opacity:.72; font-size:10px; font-weight:850; line-height:1; }
    .risk-chip-value { color:inherit; font-size:11px; font-weight:950; line-height:1.15; max-width:100%; white-space:nowrap; }
    .risk-chip.ok { background:#e7f6ef; color:#08734f; }
    .risk-chip.warn { background:#fff3d6; color:#9a5b00; }
    .risk-chip.danger { background:#fee2e2; color:#b42318; }
    .risk-chip.info { background:#e0f2fe; color:#075985; }
    .market-risk-inline { display:grid; grid-template-columns:repeat(3, minmax(0,1fr)); gap:8px; align-items:stretch; min-width:0; }
    .market-risk-inline .risk-chip { min-height:46px; justify-content:center; flex-direction:column; gap:4px; border-radius:8px; padding:7px 6px; font-size:11px; overflow:visible; }
    .market-risk-inline.fresh .risk-chip { animation:freshPulse .85s ease-out 1; }
    @keyframes freshPulse {
      0% { transform:scale(1); box-shadow:0 0 0 0 rgba(21,147,106,.24); filter:brightness(1); }
      42% { transform:scale(1.035); box-shadow:0 0 0 7px rgba(21,147,106,.10); filter:brightness(1.04); }
      100% { transform:scale(1); box-shadow:0 0 0 0 rgba(21,147,106,0); filter:brightness(1); }
    }
    .risk-actions { display:grid; grid-template-columns:minmax(0,1fr) auto minmax(0,1fr); align-items:center; gap:8px; width:100%; }
    .pool-switches { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:8px; }
    .pool-switch { height:34px; display:flex; align-items:center; justify-content:space-between; gap:8px; border:1px solid #d7e4f1; border-radius:8px; background:#fff; padding:0 9px; color:var(--muted); font-size:12px; font-weight:900; }
    .pool-switch input { display:none; }
    .pool-switch-dot { width:28px; height:16px; border-radius:999px; background:#d0d5dd; padding:2px; transition:background .15s ease; flex:0 0 auto; }
    .pool-switch-dot:after { content:""; display:block; width:12px; height:12px; border-radius:50%; background:#fff; box-shadow:0 1px 3px rgba(15,23,42,.2); transition:transform .15s ease; }
    .pool-switch.on { color:var(--ink); border-color:#b7d5f5; background:#f8fbff; }
    .pool-switch.on .pool-switch-dot { background:#15936a; }
    .pool-switch.on .pool-switch-dot:after { transform:translateX(12px); }
    .risk-badge { font-size:13px; font-weight:700; padding:5px 9px; border-radius:999px; background:#e7f6ef; color:var(--green); white-space:nowrap; }
    .risk-badge.warn { background:#fff3d6; color:#9a5b00; }
    .risk-badge.danger { background:#fee2e2; color:#b42318; }
    .risk-control-select { width:100%; height:38px; border:1px solid var(--line); border-radius:7px; padding:0 10px; background:#fff; color:var(--ink); font-weight:800; box-shadow:0 5px 14px rgba(15,23,42,.035); }
    .clear-btn { height:38px; padding:0 16px; border:0; border-radius:7px; background:#fee2e2; color:#b42318; font-weight:850; white-space:nowrap; }
    .clear-btn:hover { background:#fecaca; }
    .capital-bottom-grid { display:grid; grid-template-columns:1fr; gap:12px; }
    .rebalance-card { margin-top:0; }
    .rebalance-advice { min-height:74px; display:grid; grid-template-columns:auto 1fr; grid-template-areas:"icon title" "icon detail"; align-items:center; column-gap:10px; row-gap:4px; padding:11px 12px; border:1px solid #c4ddf6; border-radius:8px; background:linear-gradient(135deg,#eef8ff,#f8fbff); color:var(--muted); font-size:12px; font-weight:750; box-shadow:0 10px 22px rgba(15,23,42,.07); }
    .rebalance-icon { grid-area:icon; width:34px; height:34px; border-radius:8px; display:grid; place-items:center; background:#fff; color:#075985; font-weight:950; box-shadow:inset 0 0 0 1px #bfdbfe; }
    .rebalance-title { grid-area:title; display:flex; align-items:center; gap:8px; flex-wrap:wrap; color:var(--ink); font-weight:900; }
    .rebalance-detail { grid-area:detail; display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
    .daily-action-panel { grid-column:1 / -1; display:grid; grid-template-columns:1.05fr 1fr 1.2fr; gap:0; padding:13px 14px; border:1px solid #d6e2ef; border-radius:8px; background:linear-gradient(135deg,#fff 0%,#f7fbff 62%,#fffaf0 100%); box-shadow:0 8px 20px rgba(15,23,42,.045); }
    .daily-action-section { min-height:82px; display:grid; gap:8px; align-content:start; padding:4px 14px; border-right:1px solid #e6edf5; }
    .daily-action-section:first-child { padding-left:0; }
    .daily-action-section:last-child { border-right:0; padding-right:0; }
    .daily-action-title { color:var(--muted); font-size:12px; font-weight:950; }
    .daily-action-main { color:var(--ink); font-size:20px; font-weight:950; line-height:1.12; font-variant-numeric:tabular-nums; }
    .daily-action-main.ok { color:var(--green); }
    .daily-action-main.warn { color:#9a5b00; }
    .daily-action-main.danger { color:var(--red); }
    .daily-pill-row { display:flex; flex-wrap:wrap; gap:6px; }
    .daily-pill { display:inline-flex; align-items:center; gap:5px; min-height:24px; padding:3px 7px; border-radius:7px; background:#eef6ff; color:#075985; font-size:12px; font-weight:900; white-space:nowrap; }
    .daily-pill.off { background:#eef1f5; color:var(--muted); }
    .daily-pill.warn { background:#fff3d6; color:#9a5b00; }
    .daily-pill.danger { background:#fee2e2; color:#b42318; }
    .daily-action-note { color:var(--muted); font-size:12px; font-weight:750; line-height:1.4; }
    .daily-action-advice { color:var(--ink); font-size:13px; font-weight:850; line-height:1.45; }
    .pool-card { border:1px solid var(--line); border-radius:8px; padding:14px; min-height:126px; background:linear-gradient(180deg,#fff,#fafcff); box-shadow:0 10px 22px rgba(15,23,42,.04); position:relative; overflow:hidden; }
    .pool-card:before { content:""; position:absolute; left:0; top:0; bottom:0; width:3px; background:#d7e0ea; }
    .pool-card.defensive-pool { background:linear-gradient(180deg,#f9fbff,#f4f8fd); }
    .pool-head { display:flex; justify-content:space-between; align-items:center; gap:10px; }
    .pool-name { font-size:13px; color:var(--muted); font-weight:700; }
    .pool-label { color:var(--ink); font-weight:850; }
    .pool-value { font-size:25px; font-weight:850; margin-top:8px; line-height:1.1; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .pool-amounts { margin-top:2px; display:flex; justify-content:space-between; gap:10px; color:var(--muted); font-size:12px; }
    .pool-amounts span { min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .bar { height:9px; border-radius:999px; overflow:hidden; background:#e9edf3; margin-top:11px; }
    .fill { height:100%; width:0%; background:var(--blue); }
    .annual-panel { min-height:270px; }
    .annual-panel .mobile-collapse-body { display:block; }
    .annual-head { display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:12px; }
    .annual-kicker { color:var(--muted); font-size:12px; font-weight:800; }
    .annual-grid { display:grid; grid-template-columns:repeat(6, minmax(0,1fr)); gap:10px; }
    .annual-goal { border:1px solid #e0e8f2; border-radius:8px; padding:12px; background:linear-gradient(180deg,#fff,#f9fbff); min-height:94px; display:grid; gap:9px; align-content:start; box-shadow:0 8px 18px rgba(15,23,42,.035); }
    .annual-goal { grid-column:span 2; }
    .annual-goal.stock_growth { grid-column:1 / -1; min-height:92px; }
    .annual-goal.stock_growth .annual-goal-top { align-items:center; }
    .annual-goal.stock_growth .annual-name { font-size:15px; }
    .annual-goal.stock_growth .annual-desc { white-space:normal; }
    .annual-goal.retirement, .annual-goal.cash_guard { grid-column:span 3; }
    .annual-goal.fitness { grid-column:span 3; }
    .annual-goal.vocabulary { grid-column:span 3; }
    .annual-goal-top { display:flex; align-items:flex-start; justify-content:space-between; gap:8px; }
    .annual-name { font-size:13px; font-weight:950; color:var(--ink); line-height:1.25; }
    .annual-desc { margin-top:3px; color:var(--muted); font-size:11px; font-weight:750; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .annual-pct { color:var(--muted); font-size:12px; font-weight:950; white-space:nowrap; }
    .annual-actions { display:flex; align-items:center; gap:8px; flex:0 0 auto; }
    .annual-step-btn { width:42px; height:28px; border:1px solid #bfdbfe; border-radius:7px; background:linear-gradient(180deg,#f8fbff,#eaf4ff); color:#075985; font-size:13px; font-weight:950; padding:0; }
    .annual-step-btn:hover { background:#dbeafe; }
    .annual-step-btn:active { transform:scale(.96); }
    .annual-bar { height:8px; border-radius:999px; background:#e9edf3; overflow:hidden; }
    .annual-fill { height:100%; width:0%; border-radius:999px; background:var(--blue); }
    .annual-goal.retirement .annual-fill { background:var(--violet); }
    .annual-goal.cash_guard .annual-fill { background:var(--green); }
    .annual-goal.stock_growth .annual-fill { background:var(--gold); }
    .annual-goal.fitness .annual-fill { background:var(--blue); }
    .annual-goal.vocabulary .annual-fill { background:var(--cyan); }
    .annual-foot { display:flex; align-items:center; justify-content:space-between; gap:8px; color:var(--muted); font-size:11px; font-weight:800; }
    .annual-foot span { min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .event-date { color:var(--muted); font-weight:850; }
    .event-type { display:inline-flex; align-items:center; justify-content:center; height:22px; border-radius:999px; background:#eef2f6; color:#475467; font-size:11px; font-weight:900; }
    .event-type.macro { background:#fee2e2; color:#b42318; }
    .event-type.ipo { background:#fff3d6; color:#9a5b00; }
    .event-type.earnings { background:#e7f6ef; color:#08734f; }
    .event-title { min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .event-impact { color:var(--muted); text-align:right; font-size:11px; font-weight:850; }
    .carousel-head { display:flex; align-items:center; justify-content:space-between; gap:12px; }
    .carousel-actions { display:flex; align-items:center; gap:7px; }
    .carousel-tab { height:28px; min-width:36px; padding:0 10px; border:1px solid #cfd9e6; border-radius:7px; background:#fff; color:var(--muted); font-weight:850; }
    .carousel-tab.active { background:#101828; color:#fff; border-color:#101828; box-shadow:0 8px 18px rgba(16,24,40,.18); }
    .donut-wrap { flex:1; display:flex; align-items:center; justify-content:center; gap:18px; min-height:160px; }
    canvas { max-width:100%; }
    #capitalDonut { width:176px; height:176px; }
    .legend { display:grid; gap:8px; min-width:120px; }
    .legend-row { display:flex; align-items:center; gap:8px; font-size:12px; color:var(--muted); }
    .legend-amount { display:none; }
    .swatch { width:9px; height:9px; border-radius:2px; }
    .bot-grid { flex:1; min-height:174px; display:flex; flex-direction:column; gap:9px; padding:12px 2px 4px; }
    .bot-row { display:grid; grid-template-columns:minmax(90px,1fr) 18px 42px; align-items:center; gap:10px; min-height:28px; font-size:12px; color:var(--ink); padding:2px 6px; border-radius:7px; background:rgba(255,255,255,.62); }
    .bot-name { overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .bot-dot { width:14px; height:14px; flex:0 0 auto; border-radius:50%; box-shadow:0 0 0 4px rgba(21,147,106,.10), inset 0 0 0 1px rgba(255,255,255,.8); background:var(--green); }
    .bot-dot.bad { background:var(--red); box-shadow:0 0 0 4px rgba(198,40,40,.10), inset 0 0 0 1px rgba(255,255,255,.8); }
    .bot-switch { width:38px; height:20px; border-radius:999px; border:0; padding:2px; background:#d0d5dd; position:relative; }
    .bot-switch::after { content:""; display:block; width:16px; height:16px; border-radius:50%; background:#fff; box-shadow:0 1px 4px rgba(15,23,42,.2); transition:transform .15s ease; }
    .bot-switch.on { background:#15936a; }
    .bot-switch.on::after { transform:translateX(18px); }
    .bot-pager { margin-top:auto; display:flex; align-items:center; justify-content:space-between; gap:8px; padding:7px 2px 0; border-top:1px solid #eef2f6; }
    .bot-page-btn { width:28px; height:26px; border:1px solid var(--line); border-radius:6px; background:#fff; color:var(--muted); font-weight:900; line-height:1; }
    .bot-page-btn:disabled { opacity:.35; }
    .bot-page-dots { display:flex; align-items:center; justify-content:center; gap:6px; flex:1; }
    .bot-page-dot { width:7px; height:7px; border-radius:999px; background:#d0d5dd; cursor:pointer; }
    .bot-page-dot.active { width:18px; background:#101828; }
    .bot-page-label { min-width:42px; color:var(--muted); font-size:11px; font-weight:800; text-align:right; }
    .chart-panel { flex:0 0 auto; min-height:0; display:flex; flex-direction:column; }
    .chart-panel .mobile-collapse-body { flex:0 0 auto; min-height:0; display:flex; flex-direction:column; }
    .chart-head { display:flex; justify-content:space-between; align-items:center; gap:12px; margin-bottom:10px; }
    .chart-title { display:flex; align-items:baseline; gap:14px; }
    .today-pnl { font-size:15px; font-weight:850; color:var(--green); }
    .tabs { display:flex; gap:6px; flex-wrap:wrap; }
    .tab { height:28px; border-radius:6px; padding:0 10px; color:var(--muted); }
    .tab.active { background:#101828; color:#fff; border-color:#101828; }
    .trade-records { flex:0 0 auto; display:flex; flex-direction:column; }
    .trade-records-head { display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom:8px; }
    .trade-records-title { font-size:13px; font-weight:850; color:var(--ink); }
    .trade-records-count { color:var(--muted); font-size:11px; font-weight:800; }
    .trade-records-scroll { height:520px; overflow:auto; overscroll-behavior:contain; -webkit-overflow-scrolling:touch; border:1px solid #eef2f6; border-radius:8px; }
    .trade-records table { width:100%; min-width:940px; table-layout:auto; }
    .trade-records th, .trade-records td { padding:8px 10px; font-size:11px; }
    .trade-records th, .trade-records td { white-space:nowrap; }
    .life-focus-panel { display:none; }
    .stock-focus-panel { display:none; }
    .stock-selection-head { display:flex; align-items:flex-start; justify-content:space-between; gap:14px; margin-bottom:14px; }
    .stock-selection-title { display:grid; gap:5px; }
    .stock-selection-title h2 { font-size:20px; }
    .stock-selection-actions { display:flex; align-items:center; gap:8px; flex-wrap:wrap; justify-content:flex-end; }
    .stock-selection-refresh { height:34px; border:0; border-radius:8px; background:#101828; color:#fff; font-weight:850; }
    .stock-selection-refresh.loading { opacity:.65; pointer-events:none; }
    .stock-selection-subtabs { display:flex; gap:6px; align-items:center; width:max-content; background:linear-gradient(180deg,#eef3f8,#e7edf4); padding:5px; border-radius:8px; border:1px solid #e2e8f0; margin-bottom:12px; }
    .stock-selection-subtab { height:34px; min-width:92px; border-radius:7px; font-weight:850; color:var(--muted); border:0; background:transparent; }
    .stock-selection-subtab.active { background:#101828; color:#fff; border-color:#101828; }
    .stock-selection-panel { display:none; }
    .stock-selection-panel.active { display:block; }
    .stock-selection-meta { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:12px; }
    .selection-summary-grid { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:10px; margin-bottom:12px; }
    .selection-summary-card { border:1px solid #d8e4f0; border-radius:8px; background:linear-gradient(180deg,#fff,#f8fbff); padding:12px; display:grid; gap:5px; min-height:72px; box-shadow:0 8px 18px rgba(15,23,42,.035); }
    .selection-summary-label { color:var(--muted); font-size:11px; font-weight:900; }
    .selection-summary-value { color:var(--ink); font-size:20px; font-weight:950; line-height:1.1; font-variant-numeric:tabular-nums; }
    .selection-summary-value.pos { color:var(--green); }
    .stock-selection-grid { display:grid; grid-template-columns:minmax(0,1.2fr) minmax(360px,.8fr); gap:14px; align-items:start; }
    .stock-selection-card { border:1px solid #d8e4f0; border-radius:8px; background:#fff; overflow:hidden; box-shadow:0 8px 20px rgba(15,23,42,.035); }
    .stock-selection-card-head { min-height:46px; display:flex; align-items:center; justify-content:space-between; gap:10px; padding:11px 12px; border-bottom:1px solid #e8eef6; background:linear-gradient(180deg,#fff,#f8fbff); }
    .stock-selection-card-title { color:var(--ink); font-size:15px; font-weight:950; }
    .stock-selection-title-row { display:flex; align-items:center; gap:10px; flex-wrap:wrap; min-width:0; }
    .stock-selection-filter-pills { display:flex; align-items:center; gap:6px; flex-wrap:wrap; }
    .stock-selection-filter-pill { min-height:24px; display:inline-flex; align-items:center; border-radius:999px; padding:0 8px; background:#eef6ff; color:#075985; font-size:11px; font-weight:900; white-space:nowrap; }
    .stock-selection-head-actions { display:flex; align-items:center; justify-content:flex-end; gap:8px; flex:0 0 auto; }
    .stock-selection-download { height:30px; border:0; border-radius:8px; background:#101828; color:#fff; padding:0 10px; font-size:12px; font-weight:900; white-space:nowrap; }
    .stock-selection-download:disabled { opacity:.45; cursor:not-allowed; }
    .stock-selection-count { color:var(--muted); font-size:11px; font-weight:850; }
    .stock-selection-scroll { max-height:calc(100vh - 310px); min-height:360px; overflow:auto; }
    .stock-selection-table { border:0; border-radius:0; min-width:920px; }
    .stock-selection-table th, .stock-selection-table td { font-size:12px; padding:9px 10px; }
    .stock-selection-table .note-cell { max-width:280px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; color:var(--muted); }
    .stock-action-cell { min-width:156px; }
    .stock-action-wrap { position:relative; display:inline-flex; align-items:center; gap:5px; }
    .stock-pool-add-btn { height:28px; border:0; border-radius:8px; background:#101828; color:#fff; padding:0 10px; font-size:12px; font-weight:950; white-space:nowrap; }
    .stock-pool-menu-btn { width:28px; height:28px; border-radius:8px; border:1px solid #d6e0ea; background:#fff; color:#475467; font-size:13px; font-weight:950; }
    .stock-pool-menu { position:absolute; top:32px; right:0; min-width:148px; padding:5px; border:1px solid #d8e4f0; border-radius:8px; background:#fff; box-shadow:0 12px 28px rgba(15,23,42,.14); display:none; z-index:20; }
    .stock-action-wrap.open .stock-pool-menu { display:grid; gap:4px; }
    .stock-pool-menu button { height:28px; border:0; border-radius:6px; background:#fff; color:#344054; text-align:left; padding:0 8px; font-size:12px; font-weight:850; }
    .stock-pool-menu button:hover { background:#f2f6fb; color:#101828; }
    .b-match-pill { display:inline-flex; align-items:center; justify-content:center; height:24px; min-width:56px; padding:0 8px; border-radius:8px; background:#e7f6ef; color:#08734f; font-size:12px; font-weight:950; }
    .b-match-pill.off { background:#eef2f6; color:#667085; }
    .stock-selection-empty { min-height:220px; display:flex; align-items:center; justify-content:center; color:var(--muted); font-weight:800; }
    .pullback-toolbar { display:grid; grid-template-columns:130px 140px minmax(180px,1fr) auto; gap:8px; align-items:end; margin-bottom:12px; }
    .pullback-field { display:grid; gap:5px; }
    .pullback-field label { color:var(--muted); font-size:11px; font-weight:900; }
    .pullback-field input { height:34px; border:1px solid #d8e4f0; border-radius:8px; background:#fff; padding:0 10px; color:var(--ink); font-weight:800; }
    .pullback-add-btn { height:34px; border:0; border-radius:8px; background:#101828; color:#fff; padding:0 14px; font-weight:900; white-space:nowrap; }
    .pullback-card { border:1px solid #d8e4f0; border-radius:8px; background:#fff; overflow:hidden; box-shadow:0 8px 20px rgba(15,23,42,.035); }
    .pullback-table { border:0; border-radius:0; min-width:840px; }
    .pullback-table th, .pullback-table td { font-size:12px; padding:10px 12px; }
    .pullback-table .note-cell { max-width:360px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; color:var(--muted); }
    .pullback-actions { display:flex; gap:6px; align-items:center; }
    .pullback-small-btn { height:28px; border:1px solid #d6e0ea; border-radius:8px; background:#fff; color:#344054; padding:0 9px; font-weight:850; white-space:nowrap; }
    .pullback-small-btn.primary { border-color:#101828; background:#101828; color:#fff; }
    .life-head { display:flex; align-items:flex-start; justify-content:space-between; gap:14px; margin-bottom:14px; }
    .life-title { display:grid; gap:5px; }
    .life-title h2 { font-size:20px; }
    .life-date { color:var(--muted); font-size:12px; font-weight:850; }
    .life-layout { display:grid; grid-template-columns:minmax(280px,.42fr) minmax(520px,1fr); gap:14px; align-items:stretch; }
    .life-rules { display:grid; gap:12px; align-content:start; }
    .life-main { min-width:0; display:flex; flex-direction:column; gap:12px; }
    .life-card { border:1px solid #d8e4f0; border-radius:8px; background:linear-gradient(180deg,#fff,#f8fbff); padding:14px; box-shadow:0 8px 20px rgba(15,23,42,.035); }
    .life-card h3 { margin:0 0 10px; font-size:15px; color:var(--ink); }
    .life-card-head { display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom:10px; }
    .life-card-head h3 { margin:0; }
    .life-edit-btn { height:28px; min-width:52px; border-radius:8px; padding:0 10px; color:#075985; background:#eff8ff; border-color:#bfd7f1; font-size:12px; font-weight:900; }
    .rule-editor { display:none; gap:8px; }
    .rule-editor.open { display:grid; }
    .rule-editor textarea { width:100%; min-height:150px; resize:vertical; border:1px solid #d8e4f0; border-radius:8px; padding:10px 12px; color:#17202a; background:#fff; font:13px/1.55 ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; outline:none; }
    .rule-editor textarea:focus { border-color:#93c5fd; box-shadow:0 0 0 3px rgba(37,99,235,.10); }
    .rule-editor-actions { display:flex; justify-content:flex-end; gap:8px; }
    .rule-editor-actions button { height:30px; border-radius:8px; font-weight:850; }
    .rule-editor-actions .primary { border:0; background:#101828; color:#fff; }
    .journal-date-list { display:grid; gap:7px; max-height:220px; overflow:auto; padding-right:2px; }
    .journal-date-btn { width:100%; min-height:38px; border:1px solid #dbe6f2; border-radius:8px; background:#fff; color:#344054; display:flex; align-items:center; justify-content:space-between; gap:8px; padding:7px 10px; font-size:12px; font-weight:900; text-align:left; }
    .journal-date-btn:hover { background:#f1f7ff; border-color:#bfd7f1; }
    .journal-date-btn.active { background:#101828; border-color:#101828; color:#fff; }
    .journal-date-btn .date-note { color:var(--muted); font-size:11px; font-weight:800; }
    .journal-date-btn.active .date-note { color:rgba(255,255,255,.72); }
    .journal-new-day { width:100%; margin-top:9px; border-radius:8px; font-weight:900; color:#075985; background:#eff8ff; }
    .life-rule-list { display:grid; gap:9px; margin:0; padding:0; list-style:none; }
    .life-rule-list li { display:grid; grid-template-columns:24px 1fr; gap:8px; align-items:start; color:#344054; font-size:13px; font-weight:800; line-height:1.45; }
    .life-rule-list span { width:24px; height:24px; border-radius:8px; display:grid; place-items:center; background:#eef6ff; color:#075985; font-size:12px; font-weight:950; }
    .five-year-plan { border:1px solid #d8e4f0; border-radius:8px; background:linear-gradient(180deg,#fff,#f8fbff); overflow:hidden; box-shadow:0 8px 20px rgba(15,23,42,.035); }
    .five-year-head { display:flex; align-items:center; justify-content:space-between; gap:12px; padding:11px 12px; border-bottom:1px solid #e8eef6; }
    .five-year-copy { display:grid; gap:3px; min-width:0; }
    .five-year-title { color:var(--ink); font-size:15px; font-weight:950; }
    .five-year-sub { color:var(--muted); font-size:11px; font-weight:800; }
    .five-year-actions { display:flex; align-items:center; gap:8px; flex:0 0 auto; }
    .five-year-status { color:var(--muted); font-size:11px; font-weight:800; min-width:54px; text-align:right; }
    .five-year-btn { height:30px; border-radius:8px; font-size:12px; font-weight:850; }
    .five-year-btn.primary { border:0; background:#101828; color:#fff; }
    .five-year-textarea { width:100%; min-height:112px; border:0; outline:0; resize:vertical; padding:14px 16px; color:#17202a; background:#fff; font:13px/1.55 ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    .five-year-textarea:focus { box-shadow:inset 0 0 0 3px rgba(37,99,235,.08); }
    .life-journal { display:flex; flex-direction:column; min-height:620px; border:1px solid #d8e4f0; border-radius:8px; background:#fff; overflow:hidden; box-shadow:0 10px 24px rgba(15,23,42,.045); }
    .journal-toolbar { display:flex; align-items:center; justify-content:space-between; gap:10px; padding:12px; border-bottom:1px solid #e8eef6; background:linear-gradient(180deg,#fff,#f8fbff); }
    .journal-toolbar-left { display:grid; gap:3px; min-width:0; }
    .journal-label { color:var(--muted); font-size:11px; font-weight:900; }
    .journal-title { color:var(--ink); font-size:16px; font-weight:950; }
    .journal-actions { display:flex; gap:8px; align-items:center; }
    .journal-btn { height:32px; border-radius:8px; font-weight:850; }
    .journal-btn.primary { border:0; background:#101828; color:#fff; }
    .journal-status { color:var(--muted); font-size:11px; font-weight:800; min-width:80px; text-align:right; }
    .journal-textarea { flex:1; width:100%; min-height:540px; border:0; outline:0; resize:none; padding:20px 22px; color:#17202a; background:linear-gradient(#fff 31px,#eef3f8 32px); background-size:100% 32px; font:15px/32px ui-serif, Georgia, "Times New Roman", serif; }
    .journal-prompts { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:8px; padding:10px 12px 12px; border-top:1px solid #e8eef6; background:#fbfdff; }
    .journal-prompt { min-height:36px; border:1px solid #dbe6f2; border-radius:8px; background:#fff; color:#344054; font-size:12px; font-weight:850; text-align:left; }
    .log-focus-panel { display:none; }
    .log-head { display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:12px; }
    .log-head h2 { margin:0; font-size:18px; }
    .log-subtabs { display:inline-flex; gap:5px; align-items:center; padding:4px; border:1px solid #dbe6f2; border-radius:8px; background:#f8fbff; }
    .log-subtab { height:30px; min-width:82px; border:0; border-radius:7px; background:transparent; color:#667085; font-size:12px; font-weight:950; }
    .log-subtab.active { background:#101828; color:#fff; box-shadow:0 5px 12px rgba(15,23,42,.12); }
    .log-actions { display:flex; align-items:center; justify-content:flex-end; gap:8px; flex-wrap:wrap; }
    .log-actions[hidden], .log-section[hidden] { display:none !important; }
    .log-search-input { width:220px; height:34px; border:1px solid #d8e4f0; border-radius:8px; background:#fff; padding:0 10px; color:var(--ink); font-size:12px; font-weight:850; outline:none; }
    .log-search-input:focus { border-color:#93c5fd; box-shadow:0 0 0 3px rgba(37,99,235,.10); }
    .log-search-input.important:focus { border-color:#fdba74; box-shadow:0 0 0 3px rgba(217,119,6,.12); }
    .log-refresh-btn { height:34px; padding:0 13px; border:0; border-radius:8px; background:#101828; color:#fff; font-weight:850; }
    .bot-log-layout { display:grid; grid-template-columns:260px minmax(0,1fr); gap:14px; align-items:stretch; min-height:calc(100vh - 228px); }
    .bot-log-sidebar { border:1px solid #d8e4f0; border-radius:8px; background:linear-gradient(180deg,#fff,#f7fbff); padding:10px; display:flex; flex-direction:column; gap:8px; box-shadow:0 8px 20px rgba(15,23,42,.035); }
    .bot-log-group-label { margin:8px 4px 2px; color:var(--muted); font-size:10px; font-weight:950; text-transform:uppercase; letter-spacing:.04em; }
    .bot-log-nav-btn { width:100%; min-height:46px; border:1px solid #dbe6f2; border-radius:8px; background:#fff; display:grid; grid-template-columns:1fr auto; align-items:center; gap:8px; padding:8px 10px; text-align:left; color:var(--ink); font-weight:950; box-shadow:0 5px 12px rgba(15,23,42,.025); }
    .bot-log-nav-btn:hover { background:#f1f7ff; border-color:#bfd7f1; }
    .bot-log-nav-btn.active { background:#101828; border-color:#101828; color:#fff; box-shadow:0 10px 22px rgba(15,23,42,.16); }
    .bot-log-nav-name { overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .bot-log-nav-sub { grid-column:1 / -1; color:var(--muted); font-size:11px; font-weight:800; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .bot-log-nav-btn.active .bot-log-nav-sub { color:rgba(255,255,255,.72); }
    .bot-log-nav-status { display:inline-flex; align-items:center; gap:5px; color:var(--muted); font-size:11px; font-weight:900; }
    .bot-log-nav-status::before { content:""; width:8px; height:8px; border-radius:999px; background:var(--red); box-shadow:0 0 0 3px rgba(198,40,40,.10); }
    .bot-log-nav-status.running::before { background:var(--green); box-shadow:0 0 0 3px rgba(21,147,106,.10); }
    .bot-log-nav-btn.active .bot-log-nav-status { color:#fff; }
    .bot-log-grid { min-width:0; }
    .bot-log-window { min-height:100%; border:1px solid #d8e4f0; border-radius:8px; background:linear-gradient(180deg,#fff,#f8fbff); overflow:hidden; box-shadow:0 8px 20px rgba(15,23,42,.04); display:flex; flex-direction:column; }
    .bot-log-window.important { border-color:#fed7aa; background:linear-gradient(180deg,#fffaf2,#fff); }
    .bot-log-title { min-height:42px; display:flex; align-items:center; justify-content:space-between; gap:10px; padding:10px 12px; border-bottom:1px solid #e8eef6; font-size:12px; font-weight:950; color:var(--ink); }
    .bot-log-title-left { min-width:0; display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
    .bot-log-name { color:var(--ink); font-size:13px; font-weight:950; }
    .bot-log-mode-tabs { display:inline-flex; align-items:center; gap:4px; padding:3px; border:1px solid #dbe6f2; border-radius:8px; background:#f8fbff; }
    .bot-log-mode-tab { height:28px; min-width:64px; padding:0 10px; border:0; border-radius:6px; background:transparent; color:#667085; font-size:12px; font-weight:950; }
    .bot-log-mode-tab.active { background:#101828; color:#fff; box-shadow:0 5px 12px rgba(15,23,42,.12); }
    .bot-log-mode-tab.important.active { background:#b45309; color:#fff; }
    .bot-log-title-actions { display:flex; align-items:center; gap:8px; flex:0 0 auto; }
    .bot-log-clear-btn { height:28px; border:1px solid #fecaca; border-radius:8px; background:#fff5f5; color:#b42318; font-size:11px; font-weight:950; padding:0 9px; }
    .bot-log-clear-btn:hover { background:#fee2e2; border-color:#fca5a5; }
    .bot-log-meta { padding:7px 12px; color:var(--muted); font-size:11px; font-weight:800; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; border-bottom:1px solid #eef2f6; }
    .bot-log-status { display:inline-flex; align-items:center; gap:6px; color:var(--muted); font-size:11px; font-weight:900; }
    .bot-log-status::before { content:""; width:8px; height:8px; border-radius:999px; background:var(--red); box-shadow:0 0 0 3px rgba(198,40,40,.10); }
    .bot-log-status.running::before { background:var(--green); box-shadow:0 0 0 3px rgba(21,147,106,.10); }
    .bot-event-feed { flex:1; min-height:520px; max-height:calc(100vh - 318px); overflow:auto; padding:12px; display:grid; gap:9px; align-content:start; background:linear-gradient(180deg,#fbfdff,#fff); }
    .bot-log-window.important .bot-event-feed { background:linear-gradient(180deg,#fffaf2,#fff); }
    .bot-log-window.important .bot-log-meta { background:#fff7ed; color:#9a3412; }
    .bot-event-card { border:1px solid #dbe6f2; border-radius:8px; background:#fff; padding:10px 12px; display:grid; gap:7px; box-shadow:0 6px 14px rgba(15,23,42,.035); }
    .bot-event-card.trade { border-color:#bbf7d0; background:#f7fef9; }
    .bot-event-card.sell { border-color:#fecaca; background:#fffafa; }
    .bot-event-card.quote { border-color:#bae6fd; background:#f8fcff; }
    .bot-event-card.scan { border-color:#dbe6f2; }
    .bot-event-card.risk { border-color:#fed7aa; background:#fffaf5; }
    .bot-event-card.status { border-color:#e4e7ec; background:#fcfcfd; }
    .bot-event-top { display:flex; justify-content:space-between; align-items:center; gap:10px; }
    .bot-event-left { min-width:0; display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
    .bot-event-kind { height:24px; display:inline-flex; align-items:center; border-radius:999px; padding:0 8px; background:#eef2f6; color:#344054; font-size:11px; font-weight:950; white-space:nowrap; }
    .bot-event-card.trade .bot-event-kind { background:#dcfce7; color:#08734f; }
    .bot-event-card.sell .bot-event-kind { background:#fee2e2; color:#b42318; }
    .bot-event-card.quote .bot-event-kind { background:#e0f2fe; color:#075985; }
    .bot-event-card.risk .bot-event-kind { background:#ffedd5; color:#b45309; }
    .bot-event-symbol { color:var(--ink); font-size:15px; font-weight:950; }
    .bot-event-time { color:var(--muted); font-size:11px; font-weight:850; white-space:nowrap; }
    .bot-event-text { color:#344054; font-size:12px; font-weight:800; line-height:1.45; word-break:break-word; }
    .bot-event-metrics { display:flex; flex-wrap:wrap; gap:6px; }
    .bot-event-chip { min-height:23px; display:inline-flex; align-items:center; border-radius:999px; padding:0 8px; background:#f2f4f7; color:#344054; font-size:11px; font-weight:850; }
    .bot-event-chip.price, .bot-event-chip.bid, .bot-event-chip.ask, .bot-event-chip.last { background:#e0f2fe; color:#075985; }
    .bot-event-chip.up_pct, .bot-event-chip.peak_gain, .bot-event-chip.profit_now, .bot-event-chip.peak_profit { background:#dcfce7; color:#08734f; }
    .bot-event-chip.sl, .bot-event-chip.stop, .bot-event-chip.stop_loss { background:#fee2e2; color:#b42318; }
    .bot-event-empty { min-height:260px; display:grid; place-items:center; color:var(--muted); font-size:13px; font-weight:850; border:1px dashed #d8e4f0; border-radius:8px; background:#fff; }
    .bot-symbol-strip { display:flex; align-items:center; gap:6px; flex-wrap:wrap; padding:8px 12px; border-bottom:1px solid #eef2f6; background:#fbfdff; }
    .bot-symbol-chip { height:24px; border:1px solid #dbe6f2; border-radius:999px; background:#fff; color:#344054; padding:0 8px; font-size:11px; font-weight:900; cursor:pointer; }
    .bot-symbol-chip:hover { border-color:#93c5fd; background:#eff6ff; }
    .bot-log-fold { margin-top:6px; border-top:1px dashed #d8e4f0; padding-top:8px; }
    .bot-log-fold summary { cursor:pointer; margin:0 4px 6px; color:var(--muted); font-size:11px; font-weight:950; list-style:none; display:flex; align-items:center; justify-content:space-between; gap:8px; }
    .bot-log-fold summary::-webkit-details-marker { display:none; }
    .bot-log-fold summary::after { content:"展开"; color:#075985; font-size:11px; }
    .bot-log-fold[open] summary::after { content:"收起"; }
    .bot-log-fold-list { display:grid; gap:8px; }
    .side-pill { border-radius:999px; padding:3px 7px; font-weight:850; font-size:11px; }
    .side-pill.buy { background:#e7f6ef; color:#08734f; }
    .side-pill.sell { background:#fee2e2; color:#b42318; }
    #equityChart { width:100%; height:260px; flex:0 0 260px; min-height:0; }
    .section-head { display:flex; align-items:center; justify-content:space-between; margin:18px 0 10px; }
    table { width:100%; border-collapse:collapse; background:#fff; border:1px solid var(--line); border-radius:8px; overflow:hidden; }
    th, td { border-bottom:1px solid var(--line-soft); padding:10px 9px; text-align:left; font-size:13px; white-space:nowrap; }
    th { background:linear-gradient(180deg,#eef4fa,#e8eef5); color:#344054; font-size:12px; }
    tbody tr:hover td { background:#f8fbff; }
    tr:last-child td { border-bottom:0; }
    .symbol-fill-btn { border:0; background:transparent; padding:0; height:auto; color:var(--ink); font:inherit; font-weight:950; cursor:pointer; }
    .symbol-fill-btn:hover { color:#075985; text-decoration:underline; box-shadow:none; }
    .status { display:inline-block; min-width:64px; text-align:center; padding:3px 8px; border-radius:999px; background:#eef2f6; }
    .open { color:var(--green); background:#e7f6ef; }
    .closed { color:var(--muted); }
    .needs_review { color:var(--amber); background:#fff3d6; }
    .holding-status { display:inline-flex; align-items:center; justify-content:center; gap:6px; min-width:58px; height:26px; padding:0 9px; border-radius:8px; font-size:12px; font-weight:950; letter-spacing:0; }
    .holding-status::before { content:""; width:7px; height:7px; border-radius:50%; background:currentColor; flex:0 0 auto; }
    .holding-status.open { color:#08734f; background:#e7f6ef; }
    .holding-status.watch { color:#9a5b00; background:#fff7e6; }
    .holding-status.candidate { color:#075985; background:#e0f2fe; }
    .holding-status.target { color:#fff; background:#0f766e; box-shadow:0 0 0 3px rgba(15,118,110,.12); }
    .holding-status.closed { color:#667085; background:#eef2f6; }
    .d-execution-row td { background:#f0fdfa; }
    .pool-delete-btn { min-width:42px; height:28px; border:1px solid #fecaca; border-radius:8px; background:#fff5f5; color:#b42318; font-size:12px; font-weight:950; }
    .pool-delete-btn:hover { background:#fee2e2; border-color:#fca5a5; }
    .neg { color:var(--red); }
    .pos { color:var(--green); }
    .scroll { overflow:auto; border-radius:8px; }
    .holdings-panel { display:block; grid-column:1 / -1; margin-top:0; min-height:0; overflow:hidden; box-shadow:var(--shadow-soft); }
    body:not(.trade-focus) .manual-buy-entry { display:none !important; }
    body:not(.holdings-focus) .holding-tabs,
    body:not(.holdings-focus) .holding-right-tools { display:none !important; }
    body:not(.holdings-focus):not(.trade-focus):not(.config-focus) .holding-head { margin:0 0 10px; min-height:34px; }
    body:not(.holdings-focus):not(.trade-focus):not(.config-focus) .holding-left-tools h2 { flex:0 0 auto; width:auto; }
    body:not(.holdings-focus):not(.trade-focus):not(.config-focus) .holdings-panel .scroll { max-height:320px; }
    body:not(.holdings-focus):not(.trade-focus):not(.config-focus) .lower-slider { overflow:visible; }
    body:not(.holdings-focus):not(.trade-focus):not(.config-focus) .lower-track { display:block; width:100%; transform:none !important; }
    body:not(.holdings-focus):not(.trade-focus):not(.config-focus) .lower-page { width:100%; padding:0; }
    body:not(.holdings-focus):not(.trade-focus):not(.config-focus) .lower-page:not(:first-child) { display:none; }
    .manual-buy-entry { display:none; gap:10px; margin:0 0 14px; padding:12px; border:1px solid #cfe0f3; border-radius:8px; background:linear-gradient(135deg,#fff 0%,#f5fbff 52%,#eef6ff 100%); box-shadow:0 10px 24px rgba(15,23,42,.045); }
    .manual-buy-entry.open { display:grid; }
    .manual-buy-top { display:flex; align-items:center; justify-content:space-between; gap:12px; }
    .manual-buy-copy { min-width:0; display:grid; gap:3px; }
    .manual-buy-title { color:var(--ink); font-size:15px; font-weight:950; }
    .manual-trade-panel[hidden] { display:none !important; }
    .manual-buy-form { display:grid; gap:10px; }
    .manual-symbol-row { display:grid; grid-template-columns:minmax(180px, 260px) 92px minmax(170px, 260px) minmax(260px, 1fr); gap:10px; align-items:end; }
    .manual-order-row { position:relative; display:grid; grid-template-columns:72px minmax(126px,.72fr) minmax(150px,.86fr) minmax(172px,.95fr) minmax(92px,.5fr) minmax(116px,.56fr) minmax(150px,.72fr) 122px; gap:8px; align-items:end; padding:12px 14px 12px 16px; border:1px solid #d8e7f4; border-radius:8px; background:rgba(255,255,255,.82); box-shadow:inset 3px 0 0 #15936a, 0 8px 18px rgba(15,23,42,.035); }
    .manual-order-row.sell-row { box-shadow:inset 3px 0 0 #b42318, 0 8px 18px rgba(15,23,42,.035); }
    .manual-order-row.short-row { box-shadow:inset 3px 0 0 #7f1d1d, 0 8px 18px rgba(15,23,42,.035); }
    .manual-row-label { align-self:center; justify-self:start; min-width:54px; height:30px; display:inline-flex; align-items:center; justify-content:center; border-radius:999px; background:#ecfdf3; color:#067647; font-size:14px; font-weight:950; }
    .sell-row .manual-row-label { background:#fff1f0; color:#b42318; }
    .short-row .manual-row-label { background:#fef3f2; color:#7f1d1d; }
    .manual-field { display:grid; gap:5px; min-width:0; }
    .manual-field label { color:#667085; font-size:10px; font-weight:900; letter-spacing:.01em; }
    .manual-field input, .manual-field select { width:100%; height:38px; border:1px solid #cfd9e6; border-radius:8px; background:#fff; padding:0 10px; color:var(--ink); font-weight:850; box-shadow:0 5px 12px rgba(15,23,42,.035); }
    .manual-field input:disabled { background:#f2f4f7; color:var(--muted); }
    .manual-field.estimate label { color:#667085; }
    .manual-field.estimate input:disabled { height:38px; background:#f8fafc; border-color:#dde8f3; color:#475467; font-weight:950; box-shadow:none; }
    .manual-field.estimate.primary input:disabled { color:#344054; background:#f3f8ff; border-color:#cfe0f3; }
    .manual-buy-action { align-self:end; width:100%; height:38px; border:0; border-radius:8px; background:#15936a; color:#fff; font-weight:950; padding:0 14px; white-space:nowrap; box-shadow:0 8px 16px rgba(21,147,106,.18); }
    .manual-buy-action.sell { background:#b42318; }
    .manual-buy-action.short { background:#7f1d1d; }
    .manual-lock-btn { width:100%; height:38px; border:1px solid #cfd9e6; border-radius:8px; background:#fff; color:var(--muted); font-weight:950; box-shadow:0 5px 12px rgba(15,23,42,.035); }
    .manual-lock-btn.locked { background:#101828; border-color:#101828; color:#fff; }
    .manual-actions { display:flex; gap:8px; align-items:center; }
    .manual-buy-note { display:none; color:#075985; font-size:12px; font-weight:800; padding:8px 10px; border-radius:8px; background:#e0f2fe; }
    .manual-buy-note.show { display:block; }
    .trade-subtabs { display:none; gap:6px; align-items:center; width:max-content; background:linear-gradient(180deg,#eef3f8,#e7edf4); padding:5px; border-radius:8px; border:1px solid #e2e8f0; }
    .trade-subtab { height:34px; min-width:72px; border-radius:7px; font-weight:850; color:var(--muted); border:0; background:transparent; }
    .trade-subtab.active { background:#101828; color:#fff; border-color:#101828; }
    .manual-limit-control { display:none; grid-template-columns:32px minmax(0,1fr) 32px; gap:5px; }
    .manual-limit-control.show { display:grid; }
    .manual-limit-control button { width:32px; height:38px; padding:0; border-radius:8px; font-weight:950; }
    .manual-limit-control input { text-align:center; }
    .manual-quote-strip { display:grid; grid-template-columns:repeat(3, minmax(0,1fr)); gap:7px; min-width:0; }
    .manual-quote-card { min-height:38px; display:grid; align-content:center; gap:2px; padding:6px 8px; border:1px solid #d6e2ef; border-radius:8px; background:#fff; box-shadow:0 5px 12px rgba(15,23,42,.03); }
    .manual-quote-label { color:var(--muted); font-size:10px; font-weight:850; line-height:1; }
    .manual-quote-value { color:var(--ink); font-size:12px; font-weight:950; line-height:1.1; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .manual-quote-card.primary .manual-quote-value { color:#08734f; }
    .manual-quote-card.fresh { animation:freshPulse .55s ease-out 1; }
    @media (max-width: 1280px) {
      .manual-order-row { grid-template-columns:72px repeat(3, minmax(120px,1fr)) 118px; }
      .manual-field.estimate { grid-column:auto; }
      .manual-buy-action { grid-column:5; }
    }
    .holding-head { gap:16px; margin:6px 0 14px; min-height:42px; }
    .holding-left-tools { display:flex; align-items:center; gap:12px; min-width:0; flex:1 1 auto; }
    .holding-left-tools h2 { flex:0 0 6em; width:6em; margin:0; white-space:nowrap; }
    .holding-right-tools { margin-left:auto; display:flex; align-items:center; gap:10px; flex:0 0 auto; }
    .holding-tabs { display:flex; gap:6px; flex-wrap:wrap; background:linear-gradient(180deg,#eef3f8,#e7edf4); padding:5px; border-radius:8px; border:1px solid #e2e8f0; }
    .holding-tab { height:30px; min-width:58px; border-radius:7px; font-weight:750; color:var(--muted); border:0; background:transparent; }
    .holding-tab.active { background:#101828; color:#fff; border-color:#101828; }
    .sync-positions-btn { height:34px; border:1px solid #bfdbfe; border-radius:8px; padding:0 14px; background:linear-gradient(180deg,#eff8ff,#dff1ff); color:#075985; font-weight:850; transition:transform .12s ease, background .12s ease, opacity .12s ease; flex:0 0 auto; }
    .sync-positions-btn:hover { background:#bae6fd; }
    .sync-positions-btn:active { transform:scale(.97); }
    .sync-positions-btn.loading { opacity:.65; pointer-events:none; }
    .view-toggle-btn { height:34px; border:0; border-radius:8px; padding:0 14px; background:#101828; color:#fff; font-weight:850; min-width:82px; }
    .view-toggle-btn:hover { background:#1f2937; }
    .page-dots { display:flex; align-items:center; justify-content:center; gap:6px; min-width:38px; }
    .page-dot { width:7px; height:7px; border-radius:50%; background:#d0d5dd; border:0; padding:0; }
    .page-dot.active { width:22px; border-radius:999px; background:#101828; }
    .holding-tabs, .sync-positions-btn { transition:opacity .18s ease, filter .18s ease; }
    .holdings-panel.market-view .holding-tabs, .holdings-panel.market-view .sync-positions-btn { opacity:.18; pointer-events:none; filter:grayscale(.2); }
    .lower-slider { overflow:hidden; touch-action:pan-y; }
    .lower-track { display:flex; width:400%; transition:transform .32s cubic-bezier(.22,.61,.36,1); }
    .lower-track.market { transform:translateX(-25%); }
    .lower-track.d { transform:translateX(-50%); }
    .lower-track.strategy { transform:translateX(-75%); }
    .lower-page { width:25%; flex:0 0 25%; padding:0 2px; }
    .strategy2-page { display:grid; gap:12px; }
    .strategy2-hero { display:flex; justify-content:space-between; gap:12px; align-items:flex-start; padding:14px; border:1px solid #d8e4f0; border-radius:8px; background:linear-gradient(135deg,#fff,#f4f9ff); }
    .strategy2-title { display:grid; gap:4px; min-width:0; }
    .strategy2-title h3 { margin:0; color:var(--ink); font-size:18px; font-weight:950; }
    .strategy2-title p { margin:0; color:var(--muted); font-size:12px; font-weight:800; line-height:1.45; }
    .strategy2-actions { min-width:360px; max-width:520px; display:grid; gap:9px; justify-items:stretch; }
    .strategy2-actions button { height:34px; border-radius:8px; font-weight:900; }
    .strategy2-actions .primary { border:0; background:#101828; color:#fff; }
    .strategy2-config-actions { display:flex; gap:8px; flex-wrap:wrap; justify-content:flex-end; align-items:center; }
    .quote-test-panel { border:1px solid #d8e4f0; border-radius:8px; background:rgba(255,255,255,.86); padding:10px; display:grid; gap:8px; box-shadow:0 8px 20px rgba(15,23,42,.035); }
    .quote-test-head { display:flex; justify-content:space-between; gap:10px; align-items:center; }
    .quote-test-title { color:var(--ink); font-size:13px; font-weight:950; }
    .quote-test-source { color:var(--muted); font-size:11px; font-weight:850; text-align:right; }
    .quote-test-form { display:grid; grid-template-columns:minmax(110px, 1fr) auto; gap:7px; }
    .quote-test-input { width:100%; height:34px; border:1px solid #d6e2ef; border-radius:8px; padding:0 10px; color:var(--ink); font-weight:900; text-transform:uppercase; background:#fff; }
    .quote-test-btn { border:0; background:#0f766e; color:#fff; padding:0 12px; }
    .quote-test-btn.loading { opacity:.68; pointer-events:none; }
    .quote-test-grid { display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:6px; }
    .quote-test-metric { border:1px solid #e4edf6; border-radius:8px; background:#fff; padding:7px 8px; min-width:0; }
    .quote-test-label { color:var(--muted); font-size:10px; font-weight:850; line-height:1; }
    .quote-test-value { color:var(--ink); font-size:13px; font-weight:950; line-height:1.25; margin-top:4px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .quote-test-value.pos { color:var(--green); }
    .quote-test-value.warn { color:var(--red); }
    .quote-test-note { min-height:16px; color:var(--muted); font-size:11px; font-weight:800; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .strategy2-capital { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:8px; }
    .strategy2-capital-card { padding:10px; border:1px solid #dbe6f2; border-radius:8px; background:#fff; display:grid; gap:6px; }
    .strategy2-capital-label { color:var(--muted); font-size:11px; font-weight:900; }
    .strategy2-capital-value { color:var(--ink); font-size:18px; font-weight:950; }
    .strategy2-grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:12px; }
    .strategy-card { border:1px solid #d8e4f0; border-radius:8px; background:#fff; overflow:hidden; box-shadow:0 8px 20px rgba(15,23,42,.035); }
    .strategy-card-head { display:flex; justify-content:space-between; gap:10px; align-items:flex-start; padding:12px; background:linear-gradient(180deg,#fff,#f7fbff); border-bottom:1px solid #e8eef6; }
    .strategy-card h3 { margin:0; color:var(--ink); font-size:17px; font-weight:950; }
    .strategy-card-meta { color:var(--muted); font-size:12px; font-weight:850; margin-top:3px; }
    .strategy-badge { display:inline-flex; align-items:center; height:28px; padding:0 10px; border-radius:999px; background:#101828; color:#fff; font-size:12px; font-weight:950; white-space:nowrap; }
    .strategy-mission { margin:0; padding:11px 12px; color:#344054; font-size:12px; font-weight:800; line-height:1.45; border-bottom:1px solid #eef2f6; }
    .rule-section { padding:10px 12px 12px; display:grid; gap:7px; }
    .rule-section-title { color:var(--ink); font-size:13px; font-weight:950; }
    .rule-row { display:grid; grid-template-columns:22px minmax(100px,.8fr) minmax(120px,1fr) 48px; gap:7px; align-items:center; }
    .rule-row input[type="checkbox"] { width:18px; height:18px; accent-color:#101828; }
    .rule-label { color:#344054; font-size:12px; font-weight:850; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .rule-row input[type="text"], .rule-row input[type="number"] { width:100%; height:32px; border:1px solid #d6e2ef; border-radius:8px; padding:0 9px; color:var(--ink); font-weight:850; background:#fff; }
    .rule-unit { color:var(--muted); font-size:11px; font-weight:850; }
    .strategy2-status { color:var(--muted); font-size:12px; font-weight:850; min-width:120px; text-align:right; }
    .schedule-panel { border:1px solid #d8e4f0; border-radius:8px; background:#fff; overflow:hidden; box-shadow:0 8px 20px rgba(15,23,42,.035); }
    .schedule-head { min-height:46px; display:flex; align-items:center; justify-content:space-between; gap:12px; padding:11px 12px; border-bottom:1px solid #e8eef6; background:linear-gradient(180deg,#fff,#f8fbff); }
    .schedule-title { display:flex; align-items:center; gap:8px; min-width:0; color:var(--ink); font-size:15px; font-weight:950; }
    .schedule-title::before { content:""; width:9px; height:9px; border-radius:50%; background:#2563eb; box-shadow:0 0 0 4px rgba(37,99,235,.10); flex:0 0 auto; }
    .schedule-meta { color:var(--muted); font-size:11px; font-weight:850; text-align:right; white-space:nowrap; }
    .schedule-grid { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:8px; padding:10px 12px 12px; }
    .schedule-card { border:1px solid #dbe6f2; border-radius:8px; background:linear-gradient(180deg,#fff,#fbfdff); padding:10px; display:grid; gap:8px; min-width:0; }
    .schedule-card.warn { border-color:#fed7aa; background:#fffaf5; }
    .schedule-card.off { background:#f8fafc; color:#667085; }
    .schedule-top { display:flex; align-items:center; justify-content:space-between; gap:8px; }
    .schedule-name { color:var(--ink); font-size:13px; font-weight:950; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .schedule-pill { height:23px; display:inline-flex; align-items:center; border-radius:999px; padding:0 8px; background:#e7f6ef; color:#08734f; font-size:11px; font-weight:950; white-space:nowrap; }
    .schedule-card.warn .schedule-pill { background:#ffedd5; color:#b45309; }
    .schedule-card.off .schedule-pill { background:#eef2f6; color:#667085; }
    .schedule-line { color:#344054; font-size:12px; font-weight:850; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .schedule-sub { color:var(--muted); font-size:11px; font-weight:800; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .schedule-empty { padding:16px; color:var(--muted); font-size:12px; font-weight:850; }
    .b-config-panel { border:1px solid #d8e4f0; border-radius:8px; background:#fff; overflow:hidden; box-shadow:0 8px 20px rgba(15,23,42,.035); }
    .b-config-head { min-height:46px; display:flex; align-items:center; justify-content:space-between; gap:12px; padding:11px 12px; border-bottom:1px solid #e8eef6; background:linear-gradient(180deg,#fff,#f8fbff); }
    .b-config-title { color:var(--ink); font-size:15px; font-weight:950; }
    .b-config-meta { color:var(--muted); font-size:11px; font-weight:850; text-align:right; }
    .b-config-grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:10px; padding:10px 12px 12px; }
    .b-config-card { border:1px solid #dbe6f2; border-radius:8px; background:linear-gradient(180deg,#fff,#fbfdff); overflow:hidden; min-width:0; }
    .b-config-card h4 { margin:0; padding:10px 12px; border-bottom:1px solid #eef2f6; color:var(--ink); font-size:14px; font-weight:950; }
    .b-config-list { display:grid; gap:0; padding:4px 0; }
    .b-config-row { display:grid; grid-template-columns:minmax(130px,.8fr) minmax(0,1fr); gap:10px; padding:8px 12px; border-top:1px solid #f1f4f8; align-items:center; }
    .b-config-row:first-child { border-top:0; }
    .b-config-label { color:var(--muted); font-size:12px; font-weight:850; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .b-config-value { color:#17202a; font-size:12px; font-weight:900; text-align:right; overflow:hidden; text-overflow:ellipsis; white-space:normal; word-break:break-word; line-height:1.35; }
    .b-config-value.good { color:#08734f; }
    .b-config-value.warn { color:#b45309; }
    .b-config-value.bad { color:#b42318; }
    .config-subnav { position:sticky; top:8px; z-index:12; display:flex; align-items:center; gap:6px; padding:7px; border:1px solid #d8e4f0; border-radius:8px; background:rgba(255,255,255,.96); box-shadow:0 8px 22px rgba(15,23,42,.08); backdrop-filter:blur(10px); overflow-x:auto; }
    .config-subnav button { flex:0 0 auto; height:36px; min-width:82px; border:1px solid transparent; border-radius:7px; background:transparent; color:#475467; font-size:12px; font-weight:900; padding:0 13px; }
    .config-subnav button.active { background:#101828; color:#fff; box-shadow:0 4px 10px rgba(15,23,42,.16); }
    .config-subnav button:hover:not(.active) { background:#f2f6fb; color:#175cd3; }
    .config-tab-intro { display:flex; align-items:center; justify-content:space-between; gap:12px; min-height:48px; padding:10px 12px; border:1px solid #d8e4f0; border-radius:8px; background:#f8fbff; }
    .config-tab-intro strong { color:var(--ink); font-size:15px; }
    .config-tab-intro span { color:var(--muted); font-size:12px; font-weight:800; }
    .config-log-panel { border:1px solid #d8e4f0; border-radius:8px; padding:16px; background:#fff; }
    .config-log-actions { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:10px; margin-top:12px; }
    .config-log-action { min-height:90px; display:flex; flex-direction:column; align-items:flex-start; justify-content:center; gap:6px; border:1px solid #dbe6f2; border-radius:8px; background:#f8fbff; padding:14px; text-align:left; }
    .config-log-action strong { color:var(--ink); font-size:14px; }
    .config-log-action span { color:var(--muted); font-size:12px; font-weight:800; }
    .config-module[hidden] { display:none !important; }
    .strategy2-page[data-active-config-tab="A"] [data-account-profile="trading"],
    .strategy2-page[data-active-config-tab="A"] [data-pool-map="B"],
    .strategy2-page[data-active-config-tab="A"] [data-pool-map="C"],
    .strategy2-page[data-active-config-tab="A"] [data-pool-map="D"] { display:none; }
    .config-bot-wrap { padding:0 12px 12px; }
    .config-bot-title { margin:2px 0 8px; color:var(--ink); font-size:13px; font-weight:950; }
    .config-bot-grid { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:10px; }
    .config-bot-card { min-width:0; padding:12px; border:1px solid #dbe6f2; border-radius:8px; background:#fff; display:grid; gap:11px; transition:border-color .15s ease, box-shadow .15s ease; }
    .config-bot-card.enabled { border-color:#b7dfd1; box-shadow:inset 3px 0 0 #15936a; }
    .config-bot-card-head, .config-bot-card-foot { display:flex; align-items:center; justify-content:space-between; gap:10px; min-width:0; }
    .config-bot-card-head strong { display:block; color:var(--ink); font-size:13px; font-weight:950; }
    .config-bot-card-head div span { display:block; margin-top:2px; color:var(--muted); font-size:10px; font-weight:800; }
    .config-bot-state { display:inline-flex; align-items:center; gap:5px; flex:0 0 auto; color:#667085; font-size:10px; font-weight:900; }
    .config-bot-state::before { content:""; width:7px; height:7px; border-radius:50%; background:#98a2b3; }
    .config-bot-state.enabled::before { background:#f59e0b; box-shadow:0 0 0 3px rgba(245,158,11,.12); }
    .config-bot-state.running { color:#08734f; }
    .config-bot-state.running::before { background:#15936a; box-shadow:0 0 0 3px rgba(21,147,106,.12); }
    .config-bot-message { min-height:30px; color:#475467; font-size:11px; font-weight:800; line-height:1.35; overflow:hidden; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; }
    .config-bot-card-foot { padding-top:9px; border-top:1px solid #eef2f6; color:var(--muted); font-size:10px; font-weight:800; }
    .config-bot-switch { position:relative; width:44px; height:24px; flex:0 0 44px; border:0; border-radius:999px; padding:2px; background:#d0d5dd; box-shadow:inset 0 0 0 1px rgba(15,23,42,.04); transition:background .15s ease; }
    .config-bot-switch::after { content:""; display:block; width:20px; height:20px; border-radius:50%; background:#fff; box-shadow:0 2px 5px rgba(15,23,42,.22); transition:transform .15s ease; }
    .config-bot-switch.on { background:#15936a; }
    .config-bot-switch.on::after { transform:translateX(20px); }
    .config-bot-switch:focus-visible { outline:3px solid rgba(37,99,235,.22); outline-offset:2px; }
    .d-grid-config-panel { border:1px solid #d8e4f0; border-radius:8px; background:#fff; overflow:hidden; box-shadow:0 8px 20px rgba(15,23,42,.035); }
    .d-grid-config-head { min-height:48px; display:flex; align-items:center; justify-content:space-between; gap:12px; padding:11px 12px; border-bottom:1px solid #e8eef6; background:#f8fbff; }
    .d-grid-config-title { color:var(--ink); font-size:15px; font-weight:950; }
    .d-grid-config-meta { color:var(--muted); font-size:11px; font-weight:850; margin-top:3px; }
    .d-grid-config-actions { display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
    .d-grid-config-actions button { height:32px; border-radius:8px; font-weight:900; }
    .d-grid-config-actions .primary { border:0; background:#101828; color:#fff; }
    .d-grid-runtime { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:8px; padding:12px; border-bottom:1px solid #eef2f6; }
    .d-grid-field { min-width:0; display:grid; gap:6px; }
    .d-grid-field label { color:var(--muted); font-size:11px; font-weight:850; }
    .d-grid-field input, .d-grid-field select { width:100%; height:34px; border:1px solid #d6e2ef; border-radius:8px; padding:0 9px; color:var(--ink); background:#fff; font-weight:850; min-width:0; }
    .d-grid-symbols { display:grid; gap:8px; padding:12px; }
    .d-grid-symbol-row { display:grid; grid-template-columns:64px minmax(100px,1fr) repeat(4,minmax(100px,1fr)) minmax(145px,1.15fr) 34px; gap:8px; align-items:end; padding:10px; border:1px solid #dbe6f2; border-radius:8px; background:#fbfdff; }
    .d-grid-symbol-switch { height:34px; display:flex; align-items:center; gap:7px; color:#344054; font-size:12px; font-weight:900; }
    .d-grid-symbol-state { min-height:34px; padding:7px 9px; border-radius:8px; background:#eef2f6; color:#475467; font-size:11px; font-weight:850; line-height:1.25; }
    .d-grid-symbol-state.active { background:#e7f6ef; color:#08734f; }
    .d-grid-remove { width:34px; height:34px; border-color:#fecaca; color:#b42318; background:#fff; font-size:18px; }
    .d-grid-flow { margin:0 12px 12px; padding:9px 10px; border-radius:8px; background:#eff8ff; color:#175cd3; font-size:12px; font-weight:850; }
    .account-config-panel { border:1px solid #d8e4f0; border-radius:8px; background:#fff; overflow:hidden; box-shadow:0 8px 20px rgba(15,23,42,.035); }
    .account-config-head { min-height:46px; display:flex; align-items:center; justify-content:space-between; gap:12px; padding:11px 12px; border-bottom:1px solid #e8eef6; background:linear-gradient(180deg,#fff,#f8fbff); }
    .account-config-title { color:var(--ink); font-size:15px; font-weight:950; }
    .account-config-actions { display:flex; align-items:center; gap:8px; flex-wrap:wrap; justify-content:flex-end; }
    .account-config-actions button { height:32px; border-radius:8px; font-weight:900; }
    .account-config-actions .primary { border:0; background:#101828; color:#fff; }
    .account-config-meta { color:var(--muted); font-size:11px; font-weight:850; text-align:right; }
    .account-config-grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:10px; padding:10px 12px 12px; }
    .account-card { border:1px solid #dbe6f2; border-radius:8px; background:linear-gradient(180deg,#fff,#fbfdff); padding:10px; display:grid; gap:8px; min-width:0; }
    .account-card h4 { margin:0; color:var(--ink); font-size:13px; font-weight:950; }
    .account-fields { display:grid; gap:6px; }
    .account-fields label, .pool-map-card label, .monthly-card label { color:var(--muted); font-size:11px; font-weight:850; }
    .account-fields input, .account-fields select, .pool-map-card select, .monthly-card input, .monthly-card select { width:100%; height:32px; border:1px solid #d6e2ef; border-radius:8px; padding:0 8px; color:var(--ink); font-weight:850; background:#fff; min-width:0; }
    .account-mask { color:var(--muted); font-size:11px; font-weight:850; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .pool-map-grid { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:8px; padding:0 12px 12px; }
    .pool-map-card, .monthly-card { border:1px solid #dbe6f2; border-radius:8px; background:#f8fbff; padding:10px; display:grid; gap:7px; min-width:0; }
    .pool-map-value { color:var(--ink); font-size:13px; font-weight:950; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .account-mode-readonly { display:flex; align-items:center; height:32px; border:1px solid #d6e2ef; border-radius:8px; padding:0 8px; color:var(--muted); font-weight:850; background:#f8fafc; }
    .monthly-config { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:8px; padding:0 12px 12px; }
    .monthly-result { padding:0 12px 12px; color:var(--muted); font-size:12px; font-weight:800; white-space:pre-wrap; }
    .market-toolbar { display:grid; grid-template-columns:minmax(260px,1fr) auto; gap:10px; align-items:center; margin-bottom:10px; }
    .market-select { width:100%; height:38px; border:1px solid var(--line); border-radius:8px; background:#fff; padding:0 10px; font-weight:750; color:var(--ink); box-shadow:0 6px 14px rgba(15,23,42,.035); }
    .market-meta { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:10px; }
    .market-pill { border-radius:999px; background:#eef2f6; color:var(--muted); padding:5px 9px; font-size:12px; font-weight:750; }
    .market-refresh-btn { height:38px; border:0; border-radius:8px; background:#e0f2fe; color:#075985; font-weight:850; padding:0 14px; }
    .market-refresh-btn:hover { background:#bae6fd; }
    .market-refresh-btn.loading { opacity:.65; pointer-events:none; }
    .d-panel { margin-top:16px; }
    .d-grid { display:grid; grid-template-columns:1fr; gap:14px; align-items:start; }
    .d-subpanel { border:1px solid var(--line); border-radius:8px; padding:18px; background:linear-gradient(180deg,#fff,#f9fbff); min-height:160px; box-shadow:0 8px 20px rgba(15,23,42,.035); }
    .d-subpanel[hidden] { display:none; }
    .d-subhead { display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom:10px; }
    .d-option-capital { display:flex; align-items:stretch; gap:8px; }
    .d-option-capital > div { min-width:118px; padding:7px 10px; border:1px solid var(--line); border-radius:7px; background:#fff; display:flex; flex-direction:column; gap:2px; }
    .d-option-capital span { color:var(--muted); font-size:11px; font-weight:750; }
    .d-option-capital b { color:var(--ink); font-size:14px; }
    .d-subtitle { font-weight:950; font-size:18px; color:var(--ink); }
    .d-submeta { display:flex; align-items:center; gap:8px; flex-wrap:wrap; color:var(--muted); font-size:12px; font-weight:800; }
    .d-code-pill { display:inline-flex; align-items:center; height:24px; padding:0 9px; border-radius:999px; background:#101828; color:#fff; font-size:12px; font-weight:950; }
    .d-intraday-table-wrap { margin-top:12px; max-height:520px; overflow:auto; border:1px solid #eef2f6; border-radius:8px; background:#fff; }
    .d-option-layout { display:grid; grid-template-columns:minmax(180px, 250px) minmax(0, 1fr); gap:14px; align-items:start; }
    .d-option-sidebar { display:grid; gap:12px; align-content:start; }
    .d-symbols, .d-modes { display:flex; flex-wrap:wrap; gap:8px; margin-bottom:10px; }
    .d-option-controls { display:grid; gap:10px; }
    .d-symbol-btn, .d-mode-btn { height:32px; border-radius:7px; font-weight:850; color:#344054; background:#fff; }
    .d-symbol-btn.active, .d-mode-btn.active { background:#101828; color:#fff; border-color:#101828; }
    .d-symbols, .d-modes { margin-bottom:0; }
    .d-width-control { display:flex; align-items:center; gap:8px; padding:6px 9px; border:1px solid var(--line); border-radius:8px; background:#fff; color:#344054; font-size:12px; font-weight:850; }
    .d-width-control input { width:74px; height:28px; border:1px solid #dbe3ee; border-radius:6px; padding:0 8px; font-weight:850; color:var(--ink); }
    .d-preview-grid { display:grid; grid-template-columns:repeat(2, minmax(0,1fr)); gap:10px; }
    .d-preview-grid.refreshing .d-preview-card { animation:dRefreshPulse .75s ease-in-out 1; }
    .d-preview-grid.refreshing { pointer-events:none; }
    @keyframes dRefreshPulse {
      0% { opacity:1; transform:translateY(0); filter:brightness(1); }
      42% { opacity:.62; transform:translateY(1px); filter:brightness(.98); }
      100% { opacity:1; transform:translateY(0); filter:brightness(1); }
    }
    .d-preview-card { border:1px solid #dbe3ee; border-radius:8px; background:linear-gradient(180deg,#fff,#fbfdff); padding:12px; display:grid; gap:10px; box-shadow:0 8px 18px rgba(15,23,42,.035); }
    .d-preview-top { display:flex; justify-content:space-between; gap:10px; align-items:flex-start; }
    .d-preview-title { font-weight:900; }
    .d-preview-actions { display:flex; align-items:center; gap:10px; flex-wrap:wrap; justify-content:flex-end; }
    .d-qty-control { height:32px; display:flex; align-items:center; gap:6px; padding:0 8px; border:1px solid #dbe3ee; border-radius:8px; background:#fff; color:#344054; font-size:12px; font-weight:900; }
    .d-qty-control input { width:54px; height:24px; border:0; outline:0; color:var(--ink); font-weight:900; font-size:14px; background:transparent; }
    .d-option-scroll { max-height:640px; overflow-y:auto; display:grid; gap:0; padding-right:4px; overscroll-behavior:contain; -webkit-overflow-scrolling:touch; scrollbar-gutter:stable; }
    .d-current-marker { position:relative; display:flex; align-items:center; justify-content:center; min-height:34px; margin:4px 0; color:#475467; font-size:12px; font-weight:900; }
    .d-current-marker:before { content:""; position:absolute; left:0; right:0; top:50%; border-top:2px solid #f04438; }
    .d-current-marker span { position:relative; z-index:1; background:#fff; border:1px solid #fecaca; border-radius:999px; padding:4px 10px; color:#b42318; box-shadow:0 1px 3px rgba(16,24,40,.08); }
    .d-leg { display:flex; align-items:center; justify-content:space-between; gap:8px; border-top:1px solid #eef2f6; padding-top:7px; font-size:12px; color:#344054; }
    .d-option-row { border:1px solid transparent; border-top-color:#eef2f6; border-radius:8px; padding:9px 10px; display:grid; gap:7px; font-size:12px; color:#344054; cursor:pointer; transition:background .12s ease, border-color .12s ease, box-shadow .12s ease; }
    .d-option-row:hover { background:#f8fafc; border-color:#dbe3ee; }
    .d-option-row.selected { background:#eff6ff; border-color:#2563eb; box-shadow:inset 0 0 0 1px rgba(37,99,235,.15); }
    .d-option-head { display:flex; justify-content:space-between; align-items:center; gap:10px; font-weight:900; }
    .d-option-price { color:var(--green); font-weight:900; white-space:nowrap; }
    .d-leg-line { display:grid; grid-template-columns:86px minmax(180px,1fr); gap:10px; align-items:start; }
    .d-leg-line .d-option-code { font-weight:850; color:var(--ink); overflow-wrap:anywhere; line-height:1.35; }
    .d-leg-quote { color:var(--muted); line-height:1.45; }
    .d-error { color:var(--red); font-size:12px; line-height:1.45; }
    .d-note { color:var(--muted); font-size:12px; line-height:1.5; }
    .d-confirm-btn { height:32px; border:0; border-radius:7px; background:#e0f2fe; color:#075985; font-weight:850; }
    .d-confirm-btn:disabled { opacity:.45; cursor:not-allowed; }
    .d-mode-help { border:1px solid #dbeafe; border-radius:8px; background:#f8fbff; padding:0; color:#344054; overflow:hidden; }
    .d-mode-help summary { list-style:none; cursor:pointer; padding:12px; }
    .d-mode-help summary::-webkit-details-marker { display:none; }
    .d-help-title { display:flex; align-items:center; justify-content:space-between; gap:10px; font-weight:950; color:var(--ink); }
    .d-help-title:after { content:"展开"; color:var(--blue); font-size:12px; font-weight:900; white-space:nowrap; }
    .d-mode-help[open] .d-help-title:after { content:"收起"; }
    .d-help-grid { display:grid; grid-template-columns:repeat(2, minmax(0,1fr)); gap:10px; }
    .d-mode-help .d-help-grid { padding:0 12px 12px; }
    .d-help-item { border-top:1px solid #e8eef7; padding-top:8px; line-height:1.5; font-size:12px; }
    .d-help-item b { display:block; color:#101828; margin-bottom:3px; font-size:12px; }
    .empty-state { min-height:260px; display:flex; align-items:center; justify-content:center; color:var(--muted); font-weight:750; }
    .modal-backdrop { position:fixed; inset:0; background:rgba(15,23,42,.36); display:none; align-items:center; justify-content:center; z-index:20; }
    .modal-backdrop.show { display:flex; }
    .modal { width:min(420px, calc(100vw - 32px)); background:#fff; border-radius:10px; border:1px solid var(--line); box-shadow:0 24px 70px rgba(15,23,42,.22); padding:18px; }
    .modal p { margin:10px 0 14px; color:var(--muted); font-size:13px; white-space:pre-line; line-height:1.5; }
    .modal input { width:100%; height:38px; border:1px solid var(--line); border-radius:7px; padding:0 10px; }
    .modal-actions { margin-top:14px; display:flex; justify-content:flex-end; gap:8px; }
    .danger-action { border:0; background:#b42318; color:#fff; font-weight:800; }
    @media (max-width: 1180px) { .dash { grid-template-columns:1fr; } .capital-hero { flex:none; } .chart-panel { min-height:324px; } }
    @media (max-width: 760px) {
      body { background:#f7f9fc; }
      main { padding:10px 10px calc(28px + env(safe-area-inset-bottom)); max-width:none; display:flex; flex-direction:column; gap:10px; }
      h1 { font-size:21px; line-height:1.05; max-width:none; }
      h2 { font-size:15px; }
      .dash, .left-stack, .right-stack { display:contents; }
      .right-stack { padding-top:0; }
      .left-titlebar { order:0; }
      .capital-hero { order:1; }
      .annual-panel { order:2; }
      .chart-panel { order:3; }
      .holdings-panel { order:5; }
      .left-titlebar, .chart-panel, .holdings-panel, .capital-hero, .annual-panel { width:100%; }
      .left-titlebar { height:auto; min-height:48px; padding:4px 2px 8px; gap:10px; align-items:stretch; flex-direction:column; }
      .brand-lockup { gap:8px; flex:1 1 auto; width:100%; }
      .brand-logo { width:36px; height:36px; border-radius:8px; }
      .brand-copy { gap:5px; }
      .dashboard-motto { font-size:11px; max-width:190px; }
      .title-actions { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:6px; align-self:stretch; width:100%; max-width:none; min-width:0; overflow:visible; padding:5px; }
      .title-actions .trade-focus-btn { min-width:0; width:100%; height:40px; padding:0 6px; font-size:13px; box-shadow:none; }
      .title-actions #optionTradeFocusBtn,
      .title-actions #stockFocusBtn,
      .title-actions #logFocusBtn,
      .title-actions #configFocusBtn,
      .title-actions #lifeFocusBtn,
      .title-actions .phase-chip,
      .title-actions .refresh-btn { display:none !important; }
      #overviewFocusBtn { font-size:0; }
      #overviewFocusBtn::after { content:"账户资金"; font-size:13px; }
      .phase-popover { top:64px; left:10px; width:calc(100vw - 20px); padding:12px; }
      .panel { padding:10px; border-radius:10px; }
      .mobile-collapsible { padding:0; overflow:hidden; }
      .mobile-collapsible:not(.mobile-open) { min-height:0 !important; }
      .mobile-collapse-toggle { width:100%; height:48px; border:0; border-radius:0; background:#fff; display:flex; align-items:center; justify-content:space-between; padding:0 14px; font-size:15px; font-weight:850; color:var(--ink); }
      .mobile-collapse-toggle span:last-child { color:var(--blue); font-size:12px; font-weight:850; }
      .mobile-collapse-body { display:none; padding:12px; border-top:1px solid #eef2f6; }
      .mobile-collapsible:not(.mobile-open) > .mobile-collapse-body { display:none !important; }
      .mobile-collapsible.mobile-open .mobile-collapse-body { display:block; }
      .mobile-collapsible.mobile-open .mobile-collapse-toggle span:last-child::before { content:"收起"; }
      .mobile-collapsible:not(.mobile-open) .mobile-collapse-toggle span:last-child::before { content:"展开"; }
      .capital-hero .mobile-collapse-toggle { display:none; }
      .capital-hero.mobile-collapsible:not(.mobile-open) > .mobile-collapse-body,
      .capital-hero > .mobile-collapse-body { display:block !important; padding:0; border-top:0; }
      .hero-top { grid-template-columns:1fr; gap:8px; padding:8px; }
      .hero-main-column { display:none; }
      .hero-side-column { gap:8px; padding:0; border:0; background:transparent; box-shadow:none; }
      .hero-pools-list { gap:8px; }
      .mobile-real-assets { display:grid; gap:8px; padding:11px; border:1px solid #cdddec; border-radius:9px; background:linear-gradient(135deg,#ffffff 0%,#f2f8ff 100%); box-shadow:0 6px 16px rgba(15,23,42,.04); }
      .mobile-real-assets-head { display:flex; align-items:baseline; justify-content:space-between; gap:10px; }
      .mobile-real-assets-title { color:#475467; font-size:11px; font-weight:900; }
      .mobile-real-assets-total { color:#101828; font-size:20px; font-weight:950; white-space:nowrap; }
      .mobile-real-assets-grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:6px; }
      .mobile-real-asset { min-width:0; min-height:32px; display:flex; align-items:center; justify-content:space-between; gap:8px; padding:6px 8px; border-radius:7px; background:#fff; border:1px solid #e1e9f2; }
      .mobile-real-asset b { color:#667085; font-size:10px; line-height:1; }
      .mobile-real-asset span { color:#344054; font-size:11px; font-weight:950; white-space:nowrap; }
      .daily-action-panel { grid-template-columns:1fr; }
      .metric-grid { grid-template-columns:repeat(2, minmax(0,1fr)); gap:9px; }
      .metric { min-height:64px; padding:10px; }
      .metric-label, .pool-meta, .small-muted { font-size:11px; }
      .metric-value { font-size:15px; margin-top:6px; }
      .risk-topbar, .risk-head { align-items:flex-start; }
      .risk-topbar { display:grid; grid-template-columns:1fr; gap:10px; }
      .risk-line { gap:8px; line-height:1.4; }
      .risk-actions { width:100%; }
      .rebalance-advice { min-height:0; }
      .pool-card { min-height:96px; padding:11px; }
      .pool-value { font-size:22px; }
      .pool-amounts { font-size:11px; gap:6px; }
      .annual-panel, .chart-panel { display:none !important; }
      .annual-panel { min-height:auto; }
      .annual-panel .mobile-collapse-body { display:none; }
      .annual-panel.mobile-open .mobile-collapse-body { display:block; }
      .annual-head { margin-bottom:10px; }
      .annual-grid { grid-template-columns:1fr; gap:9px; }
      .annual-goal { min-height:84px; padding:11px; }
      .donut-wrap { min-height:188px; justify-content:center; gap:14px; }
      .allocation-grid { min-height:188px; max-height:260px; padding-right:2px; }
      #capitalDonut { width:150px; height:150px; }
      .legend { min-width:132px; gap:7px; }
      .legend-row { gap:7px; font-size:12px; flex-wrap:wrap; }
      .legend-amount { display:inline; flex-basis:100%; margin-left:16px; color:#344054; font-weight:800; }
      .bot-grid { padding-top:10px; gap:8px; }
      .bot-row { grid-template-columns:minmax(120px,1fr) 18px 42px; }
      .bot-pager { padding-top:8px; }
      .chart-panel { min-height:0; }
      .chart-head { align-items:flex-start; flex-direction:column; gap:9px; }
      .chart-title { width:100%; justify-content:space-between; gap:8px; }
      .chart-title h2 { display:none; }
      .today-pnl { font-size:14px; }
      .tabs { width:100%; justify-content:flex-end; }
      #equityChart { height:238px; flex-basis:238px; }
      .chart-panel.mobile-open .mobile-collapse-body { display:flex; flex-direction:column; }
      .trade-records-scroll { height:260px; overflow:auto; }
      .trade-records table { min-width:980px; }
      .trade-records th, .trade-records td { padding:8px 7px; font-size:11px; }
      .life-layout { grid-template-columns:1fr; }
      .stock-selection-head { flex-direction:column; }
      .stock-selection-actions { justify-content:flex-start; width:100%; }
      .stock-selection-subtabs { width:100%; justify-content:stretch; }
      .stock-selection-subtab { flex:1; min-width:0; }
      .selection-summary-grid, .stock-selection-grid { grid-template-columns:1fr; }
      .stock-selection-scroll { max-height:520px; min-height:260px; }
      .stock-selection-table { min-width:900px; }
      .pullback-toolbar { grid-template-columns:1fr; }
      .pullback-add-btn { width:100%; }
      .pullback-table { min-width:760px; }
      .life-head { flex-direction:column; }
      .five-year-head { flex-direction:column; align-items:stretch; }
      .five-year-actions { justify-content:flex-end; flex-wrap:wrap; }
      .journal-prompts { grid-template-columns:1fr; }
      .life-journal { min-height:520px; }
      .journal-textarea { min-height:420px; }
      .daily-action-panel { grid-template-columns:1fr; }
      .daily-action-section { border-right:0; border-bottom:1px solid #e6edf5; padding:10px 0; }
      .daily-action-section:first-child { padding-top:0; }
      .daily-action-section:last-child { border-bottom:0; padding-bottom:0; }
      .bot-log-layout { grid-template-columns:1fr; min-height:auto; }
      .bot-log-sidebar { flex-direction:row; overflow:auto; }
      .bot-log-group-label { display:none; }
      .bot-log-nav-btn { min-width:172px; }
      .bot-event-feed { min-height:420px; max-height:520px; }
      .holdings-panel { min-height:0; margin-top:4px; padding:8px; }
      body:not(.holdings-focus):not(.trade-focus) .holdings-panel { display:none !important; }
      body.trade-focus .holdings-panel { margin-top:4px; min-height:0; }
      body.trade-focus .manual-buy-entry { margin:0; }
      .manual-buy-entry { gap:10px; padding:10px; border-radius:10px; background:#f7fbff; box-shadow:none; }
      .manual-buy-top { display:none; }
      .manual-buy-toggle { width:100%; }
      .manual-buy-form { grid-template-columns:1fr; }
      .manual-symbol-row { grid-template-columns:minmax(0,1fr) 76px; gap:8px; padding-bottom:2px; }
      .manual-symbol-row .manual-pool-field,
      .manual-symbol-row .manual-quote-strip { grid-column:1 / -1; }
      .manual-order-row { grid-template-columns:repeat(2,minmax(0,1fr)); gap:9px; padding:12px; border-radius:10px; align-items:end; }
      .manual-row-label { grid-column:1 / -1; align-self:start; justify-self:start; min-width:64px; height:32px; }
      .manual-order-row .manual-limit-field,
      .manual-order-row .manual-constraint-field,
      .manual-order-row .manual-buy-action { grid-column:1 / -1; }
      .manual-order-row .manual-buy-action { height:44px; margin-top:2px; font-size:15px; }
      .manual-limit-control { grid-template-columns:40px minmax(0,1fr) 40px; }
      .manual-limit-control button { width:40px; height:42px; }
      .manual-field input, .manual-field select, .manual-limit-control input { font-size:16px; }
      .manual-field input, .manual-field select { height:42px; padding:0 9px; }
      .manual-field label { font-size:11px; }
      .manual-quote-strip { grid-template-columns:1fr 1fr 1fr; }
      .manual-quote-card { min-height:52px; padding:8px; }
      .manual-quote-label { font-size:10px; }
      .manual-quote-value { font-size:14px; }
      .manual-lock-btn { height:42px; }
      .manual-actions { display:grid; grid-template-columns:1fr 1fr; }
      .manual-buy-action { width:100%; }
      .holding-head { flex-direction:column; align-items:stretch; gap:10px; margin:0 0 12px; }
      .holding-left-tools { flex-wrap:wrap; gap:8px; align-items:center; }
      .holding-left-tools h2 { flex-basis:6em; width:6em; min-width:6em; }
      .sync-positions-btn { order:2; height:32px; padding:0 11px; }
      .holding-tabs { order:3; width:100%; flex-wrap:nowrap; overflow-x:auto; justify-content:flex-start; padding:4px; }
      .holding-tab { min-width:52px; height:32px; }
      .holding-right-tools { display:none !important; }
      .page-dots { margin-right:auto; min-width:42px; }
      .view-toggle-btn { height:34px; min-width:82px; }
      .scroll { overflow:auto; -webkit-overflow-scrolling:touch; }
      body.holdings-focus .holdings-panel { margin-top:4px; padding:10px; }
      body.holdings-focus .holdings-panel .scroll { max-height:none; }
      table { min-width:980px; }
      th, td { padding:9px 10px; font-size:12px; }
      th:first-child, td:first-child { position:sticky; left:0; z-index:1; background:#fff; }
      th:first-child { background:#eef2f6; }
      .market-toolbar { grid-template-columns:1fr; }
      .market-meta { gap:6px; }
      .market-pill { font-size:11px; }
      .d-panel { margin-top:12px; }
      .d-grid, .d-option-layout, .d-preview-grid, .d-help-grid { grid-template-columns:1fr; }
      .d-option-capital { width:100%; overflow-x:auto; }
      .d-subhead { align-items:flex-start; flex-wrap:wrap; }
      .d-subpanel { padding:12px; }
      .d-option-scroll { max-height:520px; }
      .strategy2-hero { flex-direction:column; }
      .strategy2-actions { width:100%; justify-content:flex-start; }
      .strategy2-config-actions { justify-content:flex-start; }
      .quote-test-grid { grid-template-columns:repeat(2,minmax(0,1fr)); }
      .strategy2-capital, .strategy2-grid { grid-template-columns:1fr; }
      .schedule-head { align-items:flex-start; flex-direction:column; }
      .schedule-grid { grid-template-columns:1fr; }
      .b-config-head { align-items:flex-start; flex-direction:column; }
      .b-config-grid { grid-template-columns:1fr; }
      .config-subnav { top:4px; }
      .config-subnav button { min-width:72px; }
      .config-tab-intro { align-items:flex-start; flex-direction:column; }
      .config-log-actions { grid-template-columns:1fr; }
      .config-bot-grid { grid-template-columns:1fr; }
      .d-grid-config-head { align-items:flex-start; flex-direction:column; }
      .d-grid-runtime { grid-template-columns:1fr 1fr; }
      .d-grid-symbol-row { grid-template-columns:1fr 1fr; align-items:end; }
      .d-grid-symbol-state { grid-column:1 / -1; }
      .account-config-head { align-items:flex-start; flex-direction:column; }
      .account-config-actions { justify-content:flex-start; }
      .account-config-grid, .pool-map-grid, .monthly-config { grid-template-columns:1fr; }
      .rule-row { grid-template-columns:22px minmax(88px,.8fr) minmax(0,1fr) 40px; }
    }
  </style>
</head>
<body>
  <header>
  </header>
  <main>
    <section class="dash">
      <div class="left-titlebar">
        <div class="brand-lockup"><img class="brand-logo" src="/assets/cszy_ultimate_logo.png" alt="此生只要2000万 logo" /><div class="brand-copy"><h1>此生只要2000万</h1><div class="dashboard-motto"></div></div></div>
        <div class="title-actions">
          <button class="trade-focus-btn active" id="overviewFocusBtn" onclick="showOverview()">总览</button>
          <button class="trade-focus-btn" id="holdingsFocusBtn" onclick="toggleHoldingsFocus()">持仓</button>
          <button class="trade-focus-btn" id="stockTradeFocusBtn" onclick="toggleTradeFocus('stock')">股票交易</button>
          <button class="trade-focus-btn" id="optionTradeFocusBtn" onclick="toggleTradeFocus('option')">期权交易</button>
          <button class="trade-focus-btn" id="stockFocusBtn" onclick="toggleStockFocus()">选股</button>
          <button class="trade-focus-btn" id="logFocusBtn" onclick="toggleLogFocus()">日志</button>
          <button class="trade-focus-btn" id="configFocusBtn" onclick="toggleConfigFocus()">配置</button>
          <button class="trade-focus-btn" id="lifeFocusBtn" onclick="toggleLifeFocus()">生活</button>
          <button class="phase-chip sleep" id="phaseChip" onclick="togglePhasePopover()"><span class="phase-dot"></span><span id="phaseChipText">阶段 --</span></button>
          <button class="refresh-btn" onclick="loadAll()">刷新</button>
        </div>
      </div>
      <div class="phase-popover" id="phasePopover"></div>
      <div class="left-stack">
        <div class="panel capital-hero mobile-collapsible" id="capitalPanel">
          <button class="mobile-collapse-toggle" onclick="toggleMobilePanel('capitalPanel')"><span>账户资金</span><span></span></button>
          <div class="mobile-collapse-body">
            <div class="hero-top">
              <div class="hero-main-column">
                <div class="hero-donut">
                  <div class="hero-donut-head">
                    <h2 id="toolsPanelTitle">资金比例</h2>
                    <div class="carousel-actions">
                      <span class="mode-pill" id="modeValue">--</span>
                      <button class="carousel-tab active" id="toolTabDonut" onclick="setToolsPage('donut')">资金</button>
                      <button class="carousel-tab" id="toolTabAllocation" onclick="setToolsPage('allocation')">分配</button>
                      <button class="carousel-tab" id="toolTabBots" onclick="setToolsPage('bots')">机器人</button>
                    </div>
                  </div>
                  <div class="hero-carousel-viewport">
                    <div class="hero-carousel-track" id="toolsTrack">
                      <div class="hero-carousel-page">
                        <div class="donut-wrap">
                          <canvas id="capitalDonut" width="220" height="220"></canvas>
                          <div class="legend" id="donutLegend"></div>
                        </div>
                      </div>
                      <div class="hero-carousel-page">
                        <div class="allocation-grid" id="capitalAllocationGrid"></div>
                      </div>
                      <div class="hero-carousel-page">
                        <div class="bot-grid" id="botLights"></div>
                        <div class="bot-pager" id="botPager"></div>
                      </div>
                    </div>
                  </div>
                </div>
                <div class="capital-bottom-grid">
                  <div class="risk-compact-card">
                    <div class="risk-topbar">
                      <div class="risk-head">
                        <h2>风险状态</h2>
                        <div class="market-risk-inline" id="marketRisk"></div>
                      </div>
                      <div class="risk-actions">
                        <select class="risk-control-select" id="riskPreferenceSelect" onchange="updateRiskPreference(this.value)">
                          <option value="保守">保守</option>
                          <option value="中性">中性</option>
                          <option value="激进">激进</option>
                        </select>
                        <button class="clear-btn" onclick="openClearModal()">清仓</button>
                        <select class="risk-control-select" id="marginUsageSelect" onchange="updateMarginUsage(this.value)" title="A/B/C 保证金使用额度">
                          <option value="auto">自动额度</option>
                          <option value="1.0">固定 100%</option>
                          <option value="1.1">固定 110%</option>
                          <option value="1.2">固定 120%</option>
                          <option value="1.3">固定 130%</option>
                          <option value="1.4">固定 140%</option>
                          <option value="1.5">固定 150%</option>
                        </select>
                      </div>
                      <div class="pool-switches" id="poolSwitches"></div>
                    </div>
                  </div>
                  <div class="rebalance-card"><div class="rebalance-advice" id="rebalanceAdvice"></div></div>
                </div>
              </div>
              <div class="hero-side-column">
                <div class="mobile-real-assets" id="mobileRealAssets"></div>
                <div class="hero-pools-list" id="heroPools"></div>
              </div>
              <div class="daily-action-panel" id="brokerBalances">
                <div class="daily-action-section">
                  <div class="daily-action-title">今日作战</div>
                  <div class="daily-action-main">--</div>
                  <div class="daily-action-note">等待资金池与风控数据</div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
      <div class="right-stack">
        <div class="panel annual-panel mobile-collapsible mobile-open" id="annualPanel">
          <button class="mobile-collapse-toggle" onclick="toggleMobilePanel('annualPanel')"><span>年度任务完成进度</span><span></span></button>
          <div class="mobile-collapse-body">
            <div class="annual-head">
              <h2>年度任务完成进度</h2>
              <span class="annual-kicker">2026 Goals</span>
            </div>
            <div class="annual-grid" id="annualGoals"></div>
          </div>
        </div>
        <div class="panel chart-panel mobile-collapsible mobile-open" id="chartPanel">
          <button class="mobile-collapse-toggle" onclick="toggleMobilePanel('chartPanel')"><span>收益曲线</span><span></span></button>
          <div class="mobile-collapse-body">
            <div class="chart-head">
              <div class="chart-title"><h2>收益曲线</h2><span class="today-pnl" id="todayPnl">今日收益 --</span></div>
              <div class="tabs">
                <button class="tab active" data-period="week">周</button>
                <button class="tab" data-period="month">月</button>
                <button class="tab" data-period="year">年</button>
                <button class="tab" data-period="all">所有</button>
              </div>
            </div>
            <canvas id="equityChart" width="760" height="260"></canvas>
          </div>
        </div>
      </div>
    </section>
    <section class="panel stock-focus-panel" id="stockFocusPanel">
      <div class="stock-selection-head">
        <div class="stock-selection-title">
          <h2>选股复盘</h2>
          <div class="small-muted">按涨幅、价格、成交量和成交额筛选，并分别标记 B 与 D 候选池。</div>
        </div>
        <div class="stock-selection-actions">
          <span class="small-muted" id="stockSelectionStatus">未加载</span>
          <button class="stock-selection-refresh" id="stockSelectionRefreshBtn" onclick="loadStockSelection()">刷新选股</button>
        </div>
      </div>
      <div class="stock-selection-subtabs">
        <button class="stock-selection-subtab active" id="selectionReviewTab" onclick="setStockSelectionTab('review')">选股复盘</button>
        <button class="stock-selection-subtab" id="selectionPullbackTab" onclick="setStockSelectionTab('pullback')">等待回调股</button>
      </div>
      <div class="stock-selection-panel active" id="selectionReviewPanel">
        <div class="stock-selection-meta" id="stockSelectionMeta"></div>
        <div class="selection-summary-grid" id="stockSelectionSummary"></div>
        <div class="stock-selection-grid">
          <div class="stock-selection-card">
            <div class="stock-selection-card-head">
              <div class="stock-selection-title-row">
                <span class="stock-selection-card-title">涨幅 >5% 全量</span>
                <span class="stock-selection-filter-pills" id="gainersFilterPills"></span>
              </div>
              <div class="stock-selection-head-actions">
                <button class="stock-selection-download" id="downloadGainersBtn" onclick="downloadGainerSymbolsCsv()">下载CSV</button>
                <span class="stock-selection-count" id="gainersCount">--</span>
              </div>
            </div>
            <div class="stock-selection-scroll"><table class="stock-selection-table" id="gainersTable"></table></div>
          </div>
          <div class="stock-selection-card">
            <div class="stock-selection-card-head">
              <span class="stock-selection-card-title">符合 B 策略</span>
              <span class="stock-selection-count" id="bSelectionCount">--</span>
            </div>
            <div class="stock-selection-scroll"><table class="stock-selection-table" id="bSelectionTable"></table></div>
          </div>
        </div>
      </div>
      <div class="stock-selection-panel" id="selectionPullbackPanel">
        <div class="pullback-toolbar">
          <div class="pullback-field">
            <label for="pullbackSymbolInput">代码</label>
            <input id="pullbackSymbolInput" placeholder="QQQ" autocomplete="off" />
          </div>
          <div class="pullback-field">
            <label for="pullbackTargetInput">等待价格</label>
            <input id="pullbackTargetInput" placeholder="目标价" inputmode="decimal" />
          </div>
          <div class="pullback-field">
            <label for="pullbackNoteInput">备注</label>
            <input id="pullbackNoteInput" placeholder="等待回调、好股低吸、财报后观察..." />
          </div>
          <button class="pullback-add-btn" onclick="addPullbackStockFromInputs()">加入等待</button>
        </div>
        <div class="pullback-card">
          <div class="stock-selection-card-head">
            <span class="stock-selection-card-title">等待回调股</span>
            <span class="stock-selection-count" id="pullbackCount">--</span>
          </div>
          <div class="stock-selection-scroll"><table class="pullback-table" id="pullbackTable"></table></div>
        </div>
      </div>
    </section>
    <section class="panel holdings-panel">
      <div class="manual-buy-entry open" id="manualBuyEntry">
        <div class="manual-buy-top">
          <div class="manual-buy-copy">
            <div class="manual-buy-title">交易</div>
          </div>
          <div class="trade-subtabs">
            <button class="trade-subtab active" id="manualStockTradeTab" onclick="setManualTradeTab('stock')">股票交易</button>
            <button class="trade-subtab" id="manualOptionTradeTab" onclick="setManualTradeTab('option')">期权交易</button>
          </div>
        </div>
        <div class="manual-buy-form manual-trade-panel" id="manualStockPanel">
          <div class="manual-symbol-row">
            <div class="manual-field">
              <label for="manualBuySymbol">股票代码</label>
              <input id="manualBuySymbol" placeholder="QQQ" autocomplete="off" oninput="handleManualBuySymbolInput(this)" />
            </div>
            <button class="manual-lock-btn" id="manualSymbolLockBtn" onclick="toggleManualSymbolLock()" title="锁定当前股票代码">锁定</button>
            <div class="manual-field manual-pool-field">
              <label for="manualBuyPool">资金与股票类型</label>
              <select id="manualBuyPool" onchange="updateManualPoolAvailable()">
                <option value="A">A 养老金账户 / 买入归 A · 可买入 --</option>
                <option value="B">B 策略资金池 / 买入归 B · 可买入 --</option>
                <option value="C" selected>C 长期股票池 / 买入归 C · 可买入 --</option>
                <option value="D">D 日内交易池 / 买入归 D · 可买入 --</option>
              </select>
            </div>
            <div class="manual-quote-strip" id="manualQuoteStrip">
              <div class="manual-quote-card primary"><span class="manual-quote-label">实时现价</span><span class="manual-quote-value" id="manualQuoteLast">--</span></div>
              <div class="manual-quote-card"><span class="manual-quote-label">Bid</span><span class="manual-quote-value" id="manualQuoteBid">--</span></div>
              <div class="manual-quote-card"><span class="manual-quote-label">Ask</span><span class="manual-quote-value" id="manualQuoteAsk">--</span></div>
            </div>
          </div>
          <div class="manual-order-row buy-row">
            <div class="manual-row-label">买入</div>
            <div class="manual-field manual-type-field">
              <label for="manualBuyOrderType">订单类型</label>
              <select id="manualBuyOrderType" onchange="updateManualOrderType('buy')">
                <option value="limit" selected>实时价限价</option>
                <option value="market">市价</option>
              </select>
            </div>
            <div class="manual-field manual-size-field">
              <label for="manualBuySize">使用额度</label>
              <select id="manualBuySize" onchange="updateManualTradePreviews()">
                <option value="1/4">可用额度 1/4</option>
                <option value="1/3">可用额度 1/3</option>
                <option value="1/2">可用额度 1/2</option>
                <option value="full">可用额度 1/1</option>
              </select>
            </div>
            <div class="manual-field manual-limit-field">
              <label for="manualBuyLimitPrice">买入限价</label>
              <div class="manual-limit-control show" id="manualBuyLimitControl">
                <button onclick="stepManualLimit('buy', -0.01)" title="买入限价 -0.01">-</button>
                <input id="manualBuyLimitPrice" type="number" min="0" step="0.01" placeholder="自动" oninput="markManualLimitEdited('buy'); updateManualBStopNotice(); updateManualTradePreviews()" />
                <button onclick="stepManualLimit('buy', 0.01)" title="买入限价 +0.01">+</button>
              </div>
            </div>
            <div class="manual-field estimate primary">
              <label>预计股数</label>
              <input id="manualBuyQtyPreview" value="--" disabled />
            </div>
            <div class="manual-field estimate manual-amount-field">
              <label>预计金额</label>
              <input id="manualBuyNotionalPreview" value="--" disabled />
            </div>
            <div class="manual-field estimate manual-constraint-field">
              <label>资金约束</label>
              <input id="manualBuyConstraintPreview" value="--" disabled />
            </div>
            <button class="manual-buy-action" onclick="previewManualStockOrder('buy')">买入确认</button>
          </div>
          <div class="manual-order-row sell-row">
            <div class="manual-row-label">卖出</div>
            <div class="manual-field manual-type-field">
              <label for="manualSellOrderType">订单类型</label>
              <select id="manualSellOrderType" onchange="updateManualOrderType('sell')">
                <option value="limit" selected>实时价限价</option>
                <option value="market">市价</option>
              </select>
            </div>
            <div class="manual-field manual-size-field">
              <label for="manualHeldQty">股票数量</label>
              <input id="manualHeldQty" value="--" disabled />
            </div>
            <div class="manual-field manual-size-field">
              <label for="manualSellSize">卖出数量</label>
              <select id="manualSellSize" onchange="updateManualTradePreviews()">
                <option value="1/4">持仓 1/4</option>
                <option value="1/3">持仓 1/3</option>
                <option value="1/2">持仓 1/2</option>
                <option value="full">全仓</option>
              </select>
            </div>
            <div class="manual-field manual-limit-field">
              <label for="manualSellLimitPrice">卖出限价</label>
              <div class="manual-limit-control show" id="manualSellLimitControl">
                <button onclick="stepManualLimit('sell', -0.01)" title="卖出限价 -0.01">-</button>
                <input id="manualSellLimitPrice" type="number" min="0" step="0.01" placeholder="自动" oninput="markManualLimitEdited('sell'); updateManualTradePreviews()" />
                <button onclick="stepManualLimit('sell', 0.01)" title="卖出限价 +0.01">+</button>
              </div>
            </div>
            <div class="manual-field estimate primary">
              <label>预计股数</label>
              <input id="manualSellQtyPreview" value="--" disabled />
            </div>
            <div class="manual-field estimate manual-amount-field">
              <label>预计金额</label>
              <input id="manualSellNotionalPreview" value="--" disabled />
            </div>
            <div class="manual-field estimate manual-constraint-field">
              <label>持仓约束</label>
              <input id="manualSellConstraintPreview" value="--" disabled />
            </div>
            <button class="manual-buy-action sell" onclick="previewManualStockOrder('sell')">卖出确认</button>
          </div>
          <div class="manual-order-row short-row">
            <div class="manual-row-label">卖空</div>
            <div class="manual-field manual-type-field">
              <label for="manualShortOrderType">订单类型</label>
              <select id="manualShortOrderType" onchange="updateManualOrderType('short')">
                <option value="limit" selected>实时价限价</option>
                <option value="market">市价</option>
              </select>
            </div>
            <div class="manual-field manual-size-field">
              <label for="manualShortSize">资金</label>
              <select id="manualShortSize" onchange="updateManualTradePreviews()">
                <option value="1/4">资金 1/4</option>
                <option value="1/3">资金 1/3</option>
                <option value="1/2">资金 1/2</option>
                <option value="full">资金 1/1</option>
              </select>
            </div>
            <div class="manual-field manual-limit-field">
              <label for="manualShortLimitPrice">卖空限价</label>
              <div class="manual-limit-control show" id="manualShortLimitControl">
                <button onclick="stepManualLimit('short', -0.01)" title="卖空限价 -0.01">-</button>
                <input id="manualShortLimitPrice" type="number" min="0" step="0.01" placeholder="自动" oninput="markManualLimitEdited('short'); updateManualTradePreviews()" />
                <button onclick="stepManualLimit('short', 0.01)" title="卖空限价 +0.01">+</button>
              </div>
            </div>
            <div class="manual-field estimate primary">
              <label>预计股数</label>
              <input id="manualShortQtyPreview" value="--" disabled />
            </div>
            <div class="manual-field estimate manual-amount-field">
              <label>预计金额</label>
              <input id="manualShortNotionalPreview" value="--" disabled />
            </div>
            <div class="manual-field estimate manual-constraint-field">
              <label>账户约束</label>
              <input value="A 不支持卖空" disabled />
            </div>
            <button class="manual-buy-action short" onclick="previewManualStockOrder('short')">卖空确认</button>
          </div>
        </div>
        <div class="manual-trade-panel" id="manualOptionPanel" hidden>
          <div class="d-subpanel" id="dOptionPanel">
            <div class="d-subhead">
              <div>
                <div class="d-subtitle">期权交易</div>
                <div class="d-submeta"><span class="d-code-pill">D</span><span id="dOptionMeta">选择标的和类型</span></div>
              </div>
              <div class="d-option-capital" id="dOptionCapital">
                <div><span>D 资金可用</span><b id="dOptionAvailable">--</b></div>
                <div><span>期权购买力</span><b id="dOptionBrokerBp">--</b></div>
                <div><span>卖出监督</span><b class="positive">Q 机器人</b></div>
              </div>
            </div>
            <div class="d-option-layout">
              <div class="d-option-sidebar">
                <div class="d-symbols" id="dOptionSymbols"></div>
                <div class="d-option-controls">
                  <div class="d-modes" id="dOptionModes"></div>
                  <label class="d-width-control">宽度 <input id="dOptionWidth" type="number" min="1" step="1" value="10" onchange="changeDOptionWidth(this.value)" /></label>
                </div>
              </div>
              <div class="d-preview-grid" id="dOptionPreview"></div>
            </div>
          </div>
        </div>
        <div class="manual-buy-note" id="manualBuyNote"></div>
      </div>
      <div class="section-head holding-head">
        <div class="holding-left-tools">
          <h2 id="lowerPanelTitle">持仓</h2>
          <button class="sync-positions-btn" id="syncPositionsBtn" onclick="syncPositions()">同步仓位</button>
          <div class="holding-tabs" id="holdingTabs">
            <button class="holding-tab active" data-holding="ALL">总</button>
            <button class="holding-tab" data-holding="A">A</button>
            <button class="holding-tab" data-holding="B">B</button>
            <button class="holding-tab" data-holding="C">C</button>
            <button class="holding-tab" data-holding="D">D</button>
            <button class="holding-tab" data-holding="F">F</button>
          </div>
        </div>
        <div class="holding-right-tools">
          <div class="page-dots">
            <button class="page-dot active" id="dotHoldings" onclick="setLowerView('holdings')" title="持仓"></button>
            <button class="page-dot" id="dotMarket" onclick="setLowerView('market')" title="行情分析"></button>
          </div>
          <button class="view-toggle-btn" id="viewToggleBtn" onclick="toggleLowerView()">看行情</button>
        </div>
      </div>
      <div class="lower-slider" id="lowerSlider">
        <div class="lower-track" id="lowerTrack">
          <div class="lower-page">
            <div class="scroll"><table id="holdings"></table></div>
          </div>
          <div class="lower-page">
            <div class="market-meta" id="marketMeta"></div>
            <div class="market-toolbar">
              <select class="market-select" id="marketCategorySelect" onchange="loadMarketCategories(this.value)"></select>
              <button class="market-refresh-btn" id="marketRefreshBtn" onclick="refreshMarketCategories()">刷新分类</button>
            </div>
            <div class="scroll"><table id="marketTable"></table></div>
          </div>
          <div class="lower-page">
            <div class="d-grid">
              <div class="d-subpanel" id="dIntradayPanel">
                <div class="d-subhead">
                  <div>
                    <div class="d-subtitle">日内股票</div>
                    <div class="d-submeta"><span class="d-code-pill">D</span><span>盘中候选、确认状态和后续可交易清单</span></div>
                  </div>
                  <span class="small-muted" id="dIntradayCount">--</span>
                </div>
                <div class="d-note">第一版先只展示候选和确认状态；后续筛选脚本会把可交易股票写入这里或导出 CSV。</div>
                <div class="d-intraday-table-wrap"><table id="dIntradayTable"></table></div>
              </div>
            </div>
          </div>
          <div class="lower-page">
            <div class="strategy2-page" id="strategy2Page">
              <nav class="config-subnav" aria-label="配置模块">
                <button class="active" data-config-tab="account" onclick="setConfigTab('account')">账户</button>
                <button data-config-tab="A" onclick="setConfigTab('A')">A 养老金</button>
                <button data-config-tab="B" onclick="setConfigTab('B')">B 动量</button>
                <button data-config-tab="C" onclick="setConfigTab('C')">C 长期</button>
                <button data-config-tab="D" onclick="setConfigTab('D')">D 日内</button>
                <button data-config-tab="robots" onclick="setConfigTab('robots')">机器人</button>
                <button data-config-tab="logs" onclick="setConfigTab('logs')">日志</button>
              </nav>
              <div class="config-tab-intro" id="configTabIntro"><strong>账户与资金映射</strong><span>A 使用养老金账户，B/C/D 使用原保证金账户。</span></div>
              <div class="strategy2-hero config-module" data-config-modules="account">
                <div class="strategy2-title">
                  <h3>系统配置 · 账户、自动化与策略规则</h3>
                  <p id="strategy2Desc">A 使用养老金账户月投；B 自动策略；C 自动建仓并做 T；D 日内交易。这里集中查看账户映射、自动化状态和策略参数。</p>
                </div>
                <div class="strategy2-actions">
                  <div class="quote-test-panel">
                    <div class="quote-test-head">
                      <span class="quote-test-title">实时行情测试</span>
                      <span class="quote-test-source" id="quoteTestSource">同系统报价接口</span>
                    </div>
                    <div class="quote-test-form">
                      <input class="quote-test-input" id="quoteTestSymbol" value="QQQ" autocomplete="off" onkeydown="handleQuoteTestKey(event)" />
                      <button class="quote-test-btn" id="quoteTestBtn" onclick="testRealtimeQuote()">查询</button>
                    </div>
                    <div class="quote-test-grid">
                      <div class="quote-test-metric"><div class="quote-test-label">Last</div><div class="quote-test-value" id="quoteTestLast">--</div></div>
                      <div class="quote-test-metric"><div class="quote-test-label">Bid</div><div class="quote-test-value" id="quoteTestBid">--</div></div>
                      <div class="quote-test-metric"><div class="quote-test-label">Ask</div><div class="quote-test-value" id="quoteTestAsk">--</div></div>
                      <div class="quote-test-metric"><div class="quote-test-label">昨收</div><div class="quote-test-value" id="quoteTestPrev">--</div></div>
                      <div class="quote-test-metric"><div class="quote-test-label">成交量</div><div class="quote-test-value" id="quoteTestVolume">--</div></div>
                    </div>
                    <div class="quote-test-note" id="quoteTestNote">输入代码后查询</div>
                  </div>
                  <div class="strategy2-config-actions">
                    <span class="strategy2-status" id="strategy2Status">未加载</span>
                    <button onclick="loadStrategy2Config()">重载</button>
                    <button class="primary" onclick="saveStrategy2Config()">保存配置</button>
                  </div>
                </div>
              </div>
              <div class="account-config-panel config-module" data-config-modules="account A">
                <div class="account-config-head">
                  <div>
                    <div class="account-config-title">账户与资金映射</div>
                    <div class="account-config-meta" id="accountConfigMeta">未加载</div>
                  </div>
                  <div class="account-config-actions">
                    <button onclick="loadAccountConfig()">重载账户</button>
                    <button class="primary" onclick="saveAccountConfig()">保存账户配置</button>
                    <button onclick="previewMonthlyInvest()">月投预览</button>
                  </div>
                </div>
                <div class="account-config-grid" id="accountConfigGrid"></div>
                <div class="pool-map-grid" id="poolMapGrid"></div>
                <div class="monthly-config" id="monthlyConfigGrid"></div>
                <div class="monthly-result" id="monthlyInvestResult">A 每月 15 号按比例购买；C 不参与月投，由 C 机器人持续补齐长期核心仓并做 T。</div>
              </div>
              <div class="schedule-panel config-module" data-config-modules="robots">
                <div class="schedule-head">
                  <div class="schedule-title">自动化任务状态</div>
                  <div class="schedule-meta" id="scheduleMeta">未加载</div>
                </div>
                <div class="schedule-grid" id="scheduleGrid">
                  <div class="schedule-empty">正在读取任务状态...</div>
                </div>
                <div class="config-bot-wrap">
                  <div class="config-bot-title">交易机器人</div>
                  <div class="config-bot-grid" id="configBotGrid"><div class="schedule-empty">正在读取机器人状态...</div></div>
                </div>
              </div>
              <div class="b-config-panel config-module" data-config-modules="B">
                <div class="b-config-head">
                  <div class="b-config-title">策略 B 买卖规则</div>
                  <div class="b-config-meta" id="strategyBConfigMeta">未加载</div>
                </div>
                <div class="b-config-grid" id="strategyBConfigGrid">
                  <div class="schedule-empty">正在读取 B 策略配置...</div>
                </div>
              </div>
              <div class="d-grid-config-panel config-module" data-config-modules="D">
                <div class="d-grid-config-head">
                  <div>
                    <div class="d-grid-config-title">策略 D · 单循环日内做 T</div>
                    <div class="d-grid-config-meta" id="dGridConfigMeta">未加载</div>
                  </div>
                  <div class="d-grid-config-actions">
                    <button onclick="addDGridSymbol()">添加股票</button>
                    <button onclick="loadDGridConfig()">重载</button>
                    <button class="primary" onclick="saveDGridConfig()">保存 D 配置</button>
                  </div>
                </div>
                <div class="d-grid-runtime" id="dGridRuntime"></div>
                <div class="d-grid-symbols" id="dGridSymbols"><div class="schedule-empty">正在读取 D 策略配置...</div></div>
                <div class="d-grid-flow">定价：按检查时实时价回落 0.25% 挂买单，成交后按实际成交价上涨 1% 挂卖单。执行顺序：等待买入 → 买单成交 → 挂卖单 → 卖单成交 → 冷却 → 下一轮；上一轮未完成时不会重复下单。</div>
              </div>
              <div class="strategy2-capital config-module" data-config-modules="account" id="strategy2Capital"></div>
              <div class="strategy2-grid config-module" data-config-modules="A B C D" id="strategy2Grid"></div>
              <div class="config-log-panel config-module" data-config-modules="logs" hidden>
                <div class="account-config-title">日志与交易记录</div>
                <div class="account-config-meta">运行日志和真实交易记录继续使用独立日志页面，配置页只保留清晰入口。</div>
                <div class="config-log-actions">
                  <button class="config-log-action" onclick="openConfigLogs('bots')"><strong>机器人日志</strong><span>查看各机器人运行、错误和心跳记录</span></button>
                  <button class="config-log-action" onclick="openConfigLogs('trades')"><strong>交易记录</strong><span>查看自动交易和手动交易记录</span></button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
    <section class="panel life-focus-panel" id="lifeFocusPanel">
      <div class="life-head">
        <div class="life-title">
          <h2>生活</h2>
          <div class="life-date" id="lifeDateText">--</div>
        </div>
        <div class="small-muted">交易准则和日记本，先把心放稳，再看市场。</div>
      </div>
      <div class="life-layout">
        <div class="life-rules">
          <div class="life-card">
            <div class="life-card-head">
              <h3>交易准则</h3>
              <button class="life-edit-btn" onclick="editTradingRules()">编辑</button>
            </div>
            <ul class="life-rule-list" id="tradingRuleList">
              <li><span>1</span><div>先确认风险，再考虑收益；没有计划的单不下。</div></li>
              <li><span>2</span><div>亏损达到规则就执行，不和市场讲道理。</div></li>
              <li><span>3</span><div>盈利时分批落袋，剩余仓位交给趋势。</div></li>
              <li><span>4</span><div>状态不好时减仓或休息，现金也是仓位。</div></li>
            </ul>
            <div class="rule-editor" id="tradingRuleEditor">
              <textarea id="tradingRulesText" placeholder="一行写一条交易准则"></textarea>
              <div class="rule-editor-actions">
                <button onclick="cancelTradingRuleEdit()">取消</button>
                <button class="primary" onclick="saveTradingRules()">保存</button>
              </div>
            </div>
          </div>
          <div class="life-card">
            <h3>今日复盘</h3>
            <ul class="life-rule-list">
              <li><span>问</span><div>今天有没有冲动交易？</div></li>
              <li><span>问</span><div>止损和止盈有没有按规则执行？</div></li>
              <li><span>问</span><div>明天最重要的一条纪律是什么？</div></li>
            </ul>
          </div>
          <div class="life-card">
            <h3>日记日期</h3>
            <div class="journal-date-list" id="journalDateList"></div>
            <button class="journal-new-day" onclick="newTodayJournal()">写今天</button>
          </div>
        </div>
        <div class="life-main">
          <div class="five-year-plan">
            <div class="five-year-head">
              <div class="five-year-copy">
                <div class="five-year-title">5年计划</div>
                <div class="five-year-sub">写下长期目标、阶段路径、关键约束；可以随时手动编辑。</div>
              </div>
              <div class="five-year-actions">
                <span class="five-year-status" id="fiveYearStatus">未保存</span>
                <button class="five-year-btn" onclick="clearFiveYearPlan()">清空</button>
                <button class="five-year-btn primary" onclick="saveFiveYearPlan()">保存</button>
              </div>
            </div>
            <textarea class="five-year-textarea" id="fiveYearPlanText" placeholder="例如：2026-2031 的资金目标、事业节奏、健康计划、家庭安排、每年必须完成的里程碑。"></textarea>
          </div>
        <div class="life-journal">
          <div class="journal-toolbar">
            <div class="journal-toolbar-left">
              <div class="journal-label">日记本</div>
              <div class="journal-title">今天写点什么</div>
            </div>
            <div class="journal-actions">
              <span class="journal-status" id="journalStatus">未保存</span>
              <button class="journal-btn" onclick="clearJournal()">清空</button>
              <button class="journal-btn primary" onclick="saveJournal()">保存</button>
            </div>
          </div>
          <textarea class="journal-textarea" id="journalText" placeholder="写下今天的交易、情绪、身体状态，或者一个简单的想法。"></textarea>
          <div class="journal-prompts">
            <button class="journal-prompt" onclick="appendJournalPrompt('今天最好的一个决定：')">今天最好的决定</button>
            <button class="journal-prompt" onclick="appendJournalPrompt('今天需要避免重复的错误：')">需要避免的错误</button>
            <button class="journal-prompt" onclick="appendJournalPrompt('明天只做这一件事：')">明天一件事</button>
          </div>
        </div>
        </div>
      </div>
    </section>
    <section class="panel log-focus-panel" id="logFocusPanel">
      <div class="log-head">
        <div>
          <h2>日志</h2>
          <div class="small-muted">机器人运行日志和真实交易记录分开查看</div>
        </div>
        <div class="log-subtabs">
          <button class="log-subtab active" id="botLogsTab" onclick="setLogView('bots')">机器人日志</button>
          <button class="log-subtab" id="tradeLogsTab" onclick="setLogView('trades')">交易记录</button>
        </div>
        <div class="log-actions">
          <input class="log-search-input" id="botLogSearch" placeholder="搜索全部日志 / 股票代码" oninput="setBotLogSearch(this.value, 'all')" />
          <input class="log-search-input important" id="botImportantSearch" placeholder="搜索重要日志 / 下单股票" oninput="setBotLogSearch(this.value, 'important')" />
          <span class="small-muted" id="botLogsMeta">--</span>
          <button class="log-refresh-btn" onclick="loadBotLogs()">刷新日志</button>
        </div>
      </div>
      <div class="bot-log-layout log-section" id="botLogsSection">
        <div class="bot-log-sidebar" id="botLogNav"></div>
        <div class="bot-log-grid" id="botLogGrid"></div>
      </div>
      <div class="trade-records log-section" id="tradeLogsSection" hidden>
        <div class="trade-records-head">
          <span class="trade-records-title">近30天交易记录</span>
          <span>
            <span class="trade-records-count" id="tradeRecordsCount">--</span>
            <button class="log-refresh-btn" onclick="loadTradeRecords()">刷新记录</button>
          </span>
        </div>
        <div class="trade-records-scroll">
          <table id="tradeRecords"></table>
        </div>
      </div>
    </section>
  </main>
  <div class="modal-backdrop" id="clearModal">
    <div class="modal">
      <h2>确认清仓</h2>
      <p>该操作会提交 DAY 实时价限价卖单。可先预检持仓，不会下单。</p>
      <input id="clearPassword" type="password" placeholder="操作密码" />
      <div class="modal-actions">
        <button onclick="closeClearModal()">取消</button>
        <button onclick="submitClearPosition(true)">预检</button>
        <button class="danger-action" onclick="submitClearPosition(false)">确认清仓</button>
      </div>
    </div>
  </div>
  <div class="modal-backdrop" id="manualOrderModal">
    <div class="modal">
      <h2 id="manualOrderTitle">预览下单</h2>
      <p id="manualOrderBody">--</p>
      <div class="modal-actions">
        <button onclick="closeManualOrderModal()">取消</button>
        <button class="danger-action" id="manualOrderExecuteBtn" onclick="executeManualStockOrder()">确认执行</button>
      </div>
    </div>
  </div>
  <script>
    const money = v => {
      const n = Number(v || 0);
      const sign = n < 0 ? '-' : '';
      return `${sign}$${Math.abs(n).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2})}`;
    };
    const pct = v => `${(Number(v || 0) * 100).toFixed(2)}%`;
    const cls = v => Number(v || 0) < 0 ? 'neg' : Number(v || 0) > 0 ? 'pos' : '';
    const esc = v => String(v ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
    const colors = {A:'#2563eb', B:'#d97706', C:'#15936a', D:'#7c3aed', X:'#0f766e', Z:'#475569', CASH:'#d0d5dd'};
    let currentPeriod = 'week';
    let currentHolding = 'ALL';
    let lowerView = 'holdings';
    let dSection = 'options';
    let currentCategory = '';
    let botPage = 0;
    let toolsPage = 'donut';
    let latestHoldings = [];
    let latestMarketMeta = [];
    let latestTradeRecords = [];
    let latestStockSelection = null;
    let latestEquityCurve = null;
    let latestBotHeartbeats = [];
    let latestBotControls = [];
    let dOptionSymbol = '';
    let dOptionMode = 'BULL_CALL';
    let dOptionWidth = 10;
    let dOptionQty = 1;
    let selectedDCombo = null;
    let latestDOptionCapital = null;
    let dOptionScrollMode = 'preserve';
    let manualTradeTab = 'stock';
    let stockSelectionTab = 'review';
    let manualBuyQuoteTimer = null;
    let manualQuoteInterval = null;
    let latestManualQuote = null;
    let manualOrderPreview = null;
    let manualSymbolLocked = false;
    let strategy2Config = null;
    let configTab = 'account';
    let latestStrategyBConfig = null;
    let accountConfig = null;
    let botLogRows = [];
    let selectedBotLog = '';
    let selectedBotLogMode = 'all';
    let botLogSearch = '';
    let botImportantSearch = '';
    let botLogSearchTimer = null;
    let logView = 'bots';
    const botLogScrollState = {};
    const defaultTradingRules = [
      '先确认风险，再考虑收益；没有计划的单不下。',
      '亏损达到规则就执行，不和市场讲道理。',
      '盈利时分批落袋，剩余仓位交给趋势。',
      '状态不好时减仓或休息，现金也是仓位。'
    ];
    const tradingRulesStorageKey = 'cszy2000.life.trading_rules';
    const fiveYearPlanStorageKey = 'cszy2000.life.five_year_plan';
    const journalStorageKey = 'cszy2000.life.journals';
    const legacyJournalStorageKey = 'cszy2000.life.journal';
    const pullbackStorageKey = 'cszy2000.stock.pullback_watchlist';
    const manualSymbolLockStorageKey = 'cszy2000.trade.locked_symbol';
    let selectedJournalDate = '';
    async function api(path) {
      const r = await fetch(path);
      if (r.status === 401) { location.reload(); return {ok:false, error:'unauthorized'}; }
      return await r.json();
    }
    async function postJson(path, body) {
      const r = await fetch(path, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body || {})});
      if (r.status === 401) { location.reload(); return {ok:false, error:'unauthorized'}; }
      return await r.json();
    }
    function compactNumber(v) {
      const n = Number(v || 0);
      if (Math.abs(n) >= 1e9) return `${(n/1e9).toFixed(2)}B`;
      if (Math.abs(n) >= 1e6) return `${(n/1e6).toFixed(2)}M`;
      if (Math.abs(n) >= 1e3) return `${(n/1e3).toFixed(1)}K`;
      return String(Math.round(n));
    }
    function maybeMoney(v) {
      const n = Number(v || 0);
      return n > 0 ? money(n) : '--';
    }
    function maybeCompact(v) {
      const n = Number(v || 0);
      return n > 0 ? compactNumber(n) : '--';
    }
    function holdingStatusLabel(status) {
      const s = String(status || '').toLowerCase();
      if (s === 'open') return '持仓';
      if (s === 'needs_review') return '观察';
      if (s === 'candidate') return '候选';
      if (s === 'closed') return '已清';
      return status || '--';
    }
    function holdingStatusClass(status) {
      const s = String(status || '').toLowerCase();
      if (s === 'open') return 'open';
      if (s === 'needs_review') return 'watch';
      if (s === 'candidate') return 'candidate';
      if (s === 'closed') return 'closed';
      return 'closed';
    }
    function cCoreAction(row) {
      if (String(row.strategy_group || '').toUpperCase() !== 'C') return '';
      const symbol = String(row.symbol || '').toUpperCase();
      if (!symbol) return '';
      const enabled = String(row.ac_t_type || '').toUpperCase() === 'C' && Number(row.ac_t_enabled || 0) === 1;
      const state = String(row.ac_t_state || '').trim() || 'IDLE';
      if (enabled) {
        return `<span class="holding-status open" title="做T状态 ${esc(state)}">C核心</span><button class="pool-delete-btn" onclick="setCCore('${symbol}', false, ${Number(row.operation_id || 0)})">关T</button>`;
      }
      return `<button class="pool-delete-btn" onclick="setCCore('${symbol}', true, ${Number(row.operation_id || 0)})">设为C核心</button>`;
    }
    function metric(label, value) { return `<div class="metric"><div class="metric-label">${label}</div><div class="metric-value">${value}</div></div>`; }
    function goalValue(goal, value) {
      if (goal.unit === 'percent') return `${(Number(value || 0) * 100).toFixed(1)}%`;
      if (goal.unit === 'count') return `${Number(value || 0).toFixed(0)}${goal.suffix || ''}`;
      return money(value);
    }
    function renderAnnualGoals(goals) {
      const box = document.getElementById('annualGoals');
      if (!box) return;
      const rows = goals || [];
      box.innerHTML = rows.length ? rows.map(goal => {
        const target = Number(goal.target || 0);
        const current = Number(goal.current || 0);
        const rawPct = target > 0 ? current / target * 100 : 0;
        const donePct = Math.max(0, Math.min(100, rawPct));
        const pctText = `${Math.max(0, rawPct).toFixed(0)}%`;
        const currentLabel = goalValue(goal, current);
        const targetLabel = goalValue(goal, target);
        const extra = `${currentLabel} / ${targetLabel}`;
        const statusLabel = goal.status_label || (rawPct >= 100 ? '已达成' : '推进中');
        return `
          <div class="annual-goal ${goal.key || ''}">
            <div class="annual-goal-top">
              <div><div class="annual-name">${goal.name || '--'}</div><div class="annual-desc">${goal.desc || ''}</div></div>
              <div class="annual-actions">${goal.step ? `<button class="annual-step-btn" onclick="advanceAnnualGoal('${goal.key}')">${goal.action_label || '+'}</button>` : ''}<span class="annual-pct">${pctText}</span></div>
            </div>
            <div class="annual-bar"><div class="annual-fill" style="width:${donePct}%"></div></div>
            <div class="annual-foot"><span>${extra}</span><span>${statusLabel}</span></div>
          </div>`;
      }).join('') : '<div class="small-muted">暂无年度任务数据</div>';
    }
    async function advanceAnnualGoal(goalKey) {
      const messages = {
        retirement: '确认退休账户已新增 $500？',
        cash_guard: '确认现金安全垫已新增 $500？',
        fitness: '确认完成一次健身/10公里任务？',
        vocabulary: '确认已经记了 10 个单词？'
      };
      if (!confirm(messages[goalKey] || '确认推进这个任务？')) return;
      const result = await postJson('/api/annual_goal_step', {goal: goalKey});
      if (!result.ok) { alert(result.error || '年度任务更新失败'); return; }
      await loadAll();
    }
    function brokerLabel(value) {
      const key = String(value || '').toLowerCase();
      if (key === 'fidelity') return 'Fidelity';
      if (key === 'robinhood') return 'Robinhood';
      if (key === 'alpaca') return 'Alpaca';
      if (key === 'webull') return 'Webull';
      return value || '--';
    }
    function poolRiskPct(g, cap) {
      const poolPct = Number(cap.pool_risk_percents?.[g] || 0);
      if (g === 'A') return poolPct * 100;
      return (g === 'D' ? poolPct : Number(cap.total_risk_percent || 0) * poolPct) * 100;
    }
    function poolRole(g) {
      return {
        A: '养老金账户 / 独立资金',
        B: '策略B / 原保证金 40%',
        C: '长期股票 / 原保证金 40%',
        D: '日内交易 / 原保证金 20%',
      }[g] || '--';
    }
    function poolName(g) {
      return {
        A: 'A 养老金账户',
        B: 'B 策略资金池',
        C: 'C 长期股票池',
        D: 'D 日内交易池',
      }[g] || `${g} 资金池`;
    }
    function poolBaseLabel(g, basePct) {
      return g === 'A' ? `养老金现金账户 ${basePct.toFixed(0)}%` : `保证金内 ${basePct.toFixed(0)}%`;
    }
    function poolRiskLabel(g, riskPct) {
      return g === 'A' ? '无杠杆' : `可开 ${riskPct.toFixed(0)}%`;
    }
    function poolAvailableLabel(g) {
      return g === 'A' ? '可用现金' : '可开仓';
    }
    function poolTargetLabel(g) {
      return g === 'A' ? '现金额度' : '目标';
    }
    function renderPoolSwitches(cap) {
      const box = document.getElementById('poolSwitches');
      if (!box) return;
      const enabled = cap.pool_enabled || {};
      box.innerHTML = ['A','B','C','D'].map(g => {
        const on = enabled[g] !== false;
        return `
          <label class="pool-switch ${on ? 'on' : ''}" title="${poolRole(g)}">
            <span>${g}</span>
            <input type="checkbox" ${on ? 'checked' : ''} onchange="updatePoolEnabled('${g}', this.checked)" />
            <span class="pool-switch-dot"></span>
          </label>`;
      }).join('');
    }
    function poolCard(g, cap) {
      const defensive = cap.defensive_pools?.[g];
      if (defensive) {
        const target = Number(defensive.target || 0);
        const used = Number(defensive.current || 0);
        const av = Number(defensive.available || 0);
        const w = target > 0 ? Math.min(100, used / target * 100) : 0;
        const basePct = Number(defensive.base_percent || 0) * 100;
        const sub = g === 'X' ? '底仓现金 · 不参与交易' : `目标 ${basePct.toFixed(1)}% · 不参与交易`;
        return `<div class="pool-card defensive-pool"><div class="pool-head"><div><div class="pool-name">${g} 资金池 <span class="pool-label">${defensive.label}</span></div><div class="small-muted">${sub}</div></div><div class="small-muted">${w.toFixed(1)}%</div></div><div class="pool-value">${money(used)}</div><div class="pool-amounts"><span>底仓目标 ${money(target)}</span><span>缺口 ${money(av)}</span></div><div class="bar"><div class="fill" style="width:${w}%;background:${colors[g]}"></div></div></div>`;
      }
      const riskTarget = Number(cap.targets[g] || 0), baseTarget = Number(cap.base_targets?.[g] || 0);
      const displayTarget = riskTarget > 0 ? riskTarget : baseTarget;
      const used = Number(cap.used[g] || 0), av = Number(cap.available[g] || 0);
      const w = displayTarget > 0 ? Math.min(100, used / displayTarget * 100) : 0;
      const basePct = Number(cap.base_percents?.[g] || 0) * 100;
      const riskPct = poolRiskPct(g, cap);
      return `<div class="pool-card"><div class="pool-head"><div><div class="pool-name">${poolName(g)}</div><div class="small-muted">${poolBaseLabel(g, basePct)} · ${poolRiskLabel(g, riskPct)}</div></div><div class="small-muted">${w.toFixed(1)}%</div></div><div class="pool-value">${money(used)}</div><div class="pool-amounts"><span>${poolTargetLabel(g)} ${money(displayTarget)}</span><span>${poolAvailableLabel(g)} ${money(av)}</span></div><div class="bar"><div class="fill" style="width:${w}%;background:${colors[g]}"></div></div></div>`;
    }
    function poolRow(g, cap) {
      const riskTarget = Number(cap.targets?.[g] || 0), baseTarget = Number(cap.base_targets?.[g] || 0);
      const displayTarget = riskTarget > 0 ? riskTarget : baseTarget;
      const used = Number(cap.used?.[g] || 0), av = Number(cap.available?.[g] || 0);
      const w = displayTarget > 0 ? Math.min(100, used / displayTarget * 100) : 0;
      const basePct = Number(cap.base_percents?.[g] || 0) * 100;
      const riskPct = poolRiskPct(g, cap);
      const totalLabel = g === 'A' ? '可用现金' : '总可用';
      return `<div class="hero-pool-row"><div class="hero-pool-top"><div><div class="hero-pool-title"><span class="hero-pool-name">${poolName(g)}</span><span class="hero-pool-total">${totalLabel} ${money(displayTarget)}</span></div><div class="hero-pool-meta">${poolBaseLabel(g, basePct)} · ${poolRiskLabel(g, riskPct)}</div></div><span class="small-muted">${w.toFixed(1)}%</span></div><div class="hero-pool-mid"><span class="hero-pool-used">${money(used)}</span><span class="hero-pool-available">${poolAvailableLabel(g)} ${money(av)}</span></div><div class="bar"><div class="fill" style="width:${w}%;background:${colors[g]}"></div></div></div>`;
    }
    function renderMobileRealAssets(cap) {
      const box = document.getElementById('mobileRealAssets');
      if (!box) return;
      const snapshots = cap.broker_snapshots || {};
      const profiles = cap.pool_brokers || {};
      const equityFor = group => Math.max(0, Number(snapshots[profiles[group]]?.equity || 0));
      const basePercents = cap.base_percents || {};
      const amounts = {
        A: equityFor('A'),
        B: equityFor('B') * Number(basePercents.B ?? 0.40),
        C: equityFor('C') * Number(basePercents.C ?? 0.40),
        D: equityFor('D') * Number(basePercents.D ?? 0.20),
      };
      const uniqueProfiles = [...new Set(Object.values(profiles).filter(Boolean))];
      const total = uniqueProfiles.reduce((sum, profile) => sum + Math.max(0, Number(snapshots[profile]?.equity || 0)), 0);
      box.innerHTML = `
        <div class="mobile-real-assets-head">
          <span class="mobile-real-assets-title">真实资产</span>
          <span class="mobile-real-assets-total">${money(total || Number(cap.equity || 0))}</span>
        </div>
        <div class="mobile-real-assets-grid">
          ${['A','B','C','D'].map(group => `<div class="mobile-real-asset"><b>${group}</b><span title="${money(amounts[group])}">${money(amounts[group])}</span></div>`).join('')}
        </div>`;
    }
    function renderCapitalAllocation(cap) {
      const grid = document.getElementById('capitalAllocationGrid');
      if (!grid) return;
      grid.innerHTML = ['A','B','C','D'].map(g => {
        const riskTarget = Number(cap.targets?.[g] || 0);
        const baseTarget = Number(cap.base_targets?.[g] || 0);
        const displayTarget = riskTarget > 0 ? riskTarget : baseTarget;
        const used = Number(cap.used?.[g] || 0);
        const av = Number(cap.available?.[g] || 0);
        const w = displayTarget > 0 ? Math.min(100, used / displayTarget * 100) : 0;
        const basePct = Number(cap.base_percents?.[g] || 0) * 100;
        const riskPct = poolRiskPct(g, cap);
        const riskMetricLabel = g === 'A' ? '账户约束' : '可开比例';
        const riskMetricValue = g === 'A' ? '无杠杆' : `${riskPct.toFixed(0)}%`;
        return `
          <div class="allocation-card">
            <div class="allocation-head">
              <div>
                <div class="allocation-name">${poolName(g)}</div>
                <div class="allocation-role">${poolRole(g)}</div>
              </div>
            </div>
            <div class="allocation-meta">
              <div class="allocation-metric"><span>${g === 'A' ? '账户比例' : '保证金比例'}</span><b>${basePct.toFixed(0)}%</b></div>
              <div class="allocation-metric"><span>${riskMetricLabel}</span><b>${riskMetricValue}</b></div>
              <div class="allocation-metric"><span>已用</span><b>${money(used)}</b></div>
              <div class="allocation-metric"><span>${poolAvailableLabel(g)}</span><b>${money(av)}</b></div>
            </div>
            <div class="bar"><div class="fill" style="width:${w}%;background:${colors[g]}"></div></div>
          </div>`;
      }).join('');
    }
    function drawDonutOn(canvasId, legendId, cap) {
      const canvas = document.getElementById(canvasId);
      const legend = document.getElementById(legendId);
      if (!canvas || !legend) return;
      const ctx = canvas.getContext('2d');
      const usedEntries = ['A','B','C','D'].map(g => [g, Math.abs(Number(cap.used?.[g] || 0))]).filter(x => x[1] > 0);
      const defensiveEntries = ['X','Z'].map(g => [g, Math.abs(Number(cap.defensive_pools?.[g]?.current || 0))]).filter(x => x[1] > 0);
      const usedTotal = usedEntries.reduce((s, x) => s + x[1], 0);
      const defensiveTotal = defensiveEntries.reduce((s, x) => s + x[1], 0);
      const cash = Math.max(0, Number(cap.equity || 0) - usedTotal - defensiveTotal);
      const entries = cash > 0 ? [...usedEntries, ...defensiveEntries, ['未分配', cash, 'CASH']] : [...usedEntries, ...defensiveEntries];
      const total = entries.reduce((s, x) => s + x[1], 0) || 1;
      ctx.clearRect(0,0,canvas.width,canvas.height);
      let start = -Math.PI / 2;
      entries.forEach(([g, value, colorKey]) => {
        const a = value / total * Math.PI * 2;
        ctx.beginPath(); ctx.moveTo(110,110); ctx.arc(110,110,92,start,start+a); ctx.closePath(); ctx.fillStyle = colors[colorKey || g]; ctx.fill(); start += a;
      });
      ctx.beginPath(); ctx.arc(110,110,58,0,Math.PI*2); ctx.fillStyle = '#fff'; ctx.fill();
      ctx.fillStyle = '#17202a'; ctx.font = '700 20px system-ui'; ctx.textAlign='center'; ctx.fillText(money(cap.equity || 0).replace('.00',''),110,106);
      ctx.fillStyle = '#667085'; ctx.font = '12px system-ui'; ctx.fillText('equity',110,126);
      legend.innerHTML = entries.length
        ? entries.map(([g,v,colorKey]) => `<div class="legend-row"><span class="swatch" style="background:${colors[colorKey || g]}"></span><span>${g}</span><span>${((v/total)*100).toFixed(1)}%</span><span class="legend-amount">${money(v)}</span></div>`).join('')
        : `<div class="legend-row"><span class="small-muted">暂无持仓占用</span></div>`;
    }
    function drawDonut(cap) {
      drawDonutOn('capitalDonut', 'donutLegend', cap);
    }
    function renderBots(bots, controls) {
      const botPages = [
        ['rebalance_bot','b_buy_bot','b_sell_bot'],
        ['dashboard_bot','risk_bot','ac_bot'],
        ['f_buy_bot','f_sell_bot']
      ];
      botPage = Math.max(0, Math.min(botPage, botPages.length - 1));
      const known = botPages[botPage];
      const byName = Object.fromEntries((bots || []).map(b => [b.bot_name, b]));
      const processMap = Object.fromEntries(((window.latestBotProcesses || [])).map(b => [b.bot_name, b]));
      const controlMap = Object.fromEntries((controls || []).map(b => [b.bot_name, Number(b.enabled) === 1]));
      document.getElementById('botLights').innerHTML = known.map(name => {
        const b = byName[name];
        const p = processMap[name];
        const ok = p ? Boolean(p.running) : Boolean(b && b.status === 'running');
        const controllable = controlMap[name] !== undefined;
        const enabled = controlMap[name] !== false;
        const title = b ? `${name} ${b.status} pid=${p?.pid || '-'} ${b.last_seen_at || ''} ${b.last_message || ''}` : `${name} no heartbeat pid=${p?.pid || '-'}`;
        return `<div class="bot-row" title="${title}"><span class="bot-name">${name}</span><span class="bot-dot ${ok ? '' : 'bad'}"></span>${controllable ? `<button class="bot-switch ${enabled ? 'on' : ''}" onclick="toggleBot('${name}', ${enabled ? 'false' : 'true'})"></button>` : '<span></span>'}</div>`;
      }).join('');
      document.getElementById('botPager').innerHTML = `
        <button class="bot-page-btn" onclick="setBotPage(${botPage - 1})" ${botPage <= 0 ? 'disabled' : ''}>‹</button>
        <div class="bot-page-dots">${botPages.map((_, i) => `<span class="bot-page-dot ${i === botPage ? 'active' : ''}" onclick="setBotPage(${i})"></span>`).join('')}</div>
        <button class="bot-page-btn" onclick="setBotPage(${botPage + 1})" ${botPage >= botPages.length - 1 ? 'disabled' : ''}>›</button>
        <span class="bot-page-label">${botPage + 1}/${botPages.length}</span>
      `;
      renderConfigBots(bots, controls);
    }
    function renderConfigBots(bots, controls) {
      const grid = document.getElementById('configBotGrid');
      if (!grid) return;
      const labels = {
        dashboard_bot:'行情与持仓同步', risk_bot:'风险控制', rebalance_bot:'资金调仓',
        ac_bot:'A/C 长期策略', b_buy_bot:'B 买入', b_sell_bot:'B 卖出',
        d_grid_bot:'D 日内循环', q_sell_bot:'期权卖出监督', f_buy_bot:'F 买入', f_sell_bot:'F 卖出'
      };
      const hidden = new Set(['d_buy_bot', 'd_sell_bot']);
      const heartbeatMap = Object.fromEntries((bots || []).map(item => [item.bot_name, item]));
      const processMap = Object.fromEntries((window.latestBotProcesses || []).map(item => [item.bot_name, item]));
      const controlRows = (controls || []).filter(item => !hidden.has(item.bot_name));
      const names = [...new Set([...Object.keys(labels), ...controlRows.map(item => item.bot_name)])]
        .filter(name => !hidden.has(name) && (labels[name] || heartbeatMap[name] || controlRows.some(item => item.bot_name === name)));
      const controlMap = Object.fromEntries(controlRows.map(item => [item.bot_name, Number(item.enabled) === 1]));
      grid.innerHTML = names.map(name => {
        const heartbeat = heartbeatMap[name];
        const process = processMap[name];
        const running = process ? Boolean(process.running) : heartbeat?.status === 'running';
        const enabled = controlMap[name] !== false;
        const controllable = Object.prototype.hasOwnProperty.call(controlMap, name);
        const heartbeatMessage = String(heartbeat?.last_message || '');
        const message = enabled && !running && /已关闭|stopped/i.test(heartbeatMessage)
          ? '开关已开启，等待机器人启动或上报心跳'
          : (heartbeatMessage || (running ? '进程运行中' : '暂无运行心跳'));
        const stateLabel = running ? '运行中' : (enabled ? '已开启 · 等待心跳' : '已关闭');
        const stateClass = running ? 'running' : (enabled ? 'enabled' : '');
        return `<article class="config-bot-card ${enabled ? 'enabled' : ''}">
          <div class="config-bot-card-head">
            <div><strong>${esc(labels[name] || name)}</strong><span>${esc(name)}</span></div>
            <span class="config-bot-state ${stateClass}">${stateLabel}</span>
          </div>
          <div class="config-bot-message" title="${esc(message)}">${esc(message)}</div>
          <div class="config-bot-card-foot">
            <span>${heartbeat?.last_seen_at ? `心跳 ${esc(heartbeat.last_seen_at)}` : '暂无心跳时间'}</span>
            ${controllable ? `<button class="config-bot-switch ${enabled ? 'on' : ''}" role="switch" aria-checked="${enabled ? 'true' : 'false'}" aria-label="${esc(labels[name] || name)}机器人开关" title="${enabled ? '关闭' : '开启'} ${esc(labels[name] || name)}" onclick="toggleBot('${name}', ${enabled ? 'false' : 'true'})"></button>` : '<span class="small-muted">跟随系统</span>'}
          </div>
        </article>`;
      }).join('') || '<div class="schedule-empty">暂无机器人配置</div>';
    }
    function setBotPage(page) {
      botPage = Math.max(0, Math.min(Number(page || 0), 2));
      renderBots(latestBotHeartbeats, latestBotControls);
    }
    function setToolsPage(page) {
      toolsPage = ['donut','allocation','bots'].includes(page) ? page : 'donut';
      const track = document.getElementById('toolsTrack');
      if (track) {
        track.classList.toggle('allocation', toolsPage === 'allocation');
        track.classList.toggle('bots', toolsPage === 'bots');
      }
      document.getElementById('toolTabDonut')?.classList.toggle('active', toolsPage === 'donut');
      document.getElementById('toolTabAllocation')?.classList.toggle('active', toolsPage === 'allocation');
      document.getElementById('toolTabBots')?.classList.toggle('active', toolsPage === 'bots');
      const title = document.getElementById('toolsPanelTitle');
      if (title) title.textContent = toolsPage === 'bots' ? '机器人' : (toolsPage === 'allocation' ? '资金分配' : '资金比例');
      if (toolsPage === 'donut' && window.latestCapitalPayload) setTimeout(() => drawDonut(window.latestCapitalPayload), 50);
      if (toolsPage === 'allocation' && window.latestCapitalPayload) renderCapitalAllocation(window.latestCapitalPayload);
    }
    function renderPhase(phase) {
      const chip = document.getElementById('phaseChip');
      chip.className = `phase-chip ${phase.tone || 'sleep'}`;
      document.getElementById('phaseChipText').textContent = `${phase.label || '--'}`;
      const rules = (phase.rules || []).map(r => `<div class="phase-rule ${r.active ? 'active' : ''}"><div class="phase-rule-title"><b>${r.range}</b><span>${r.title}</span></div><p>${r.desc}</p></div>`).join('');
      document.getElementById('phasePopover').innerHTML = `<div class="phase-summary"><span class="phase-pill">美西时间 ${phase.now || '--'}</span><span class="phase-pill">当前阶段 ${phase.label || '--'}</span><span class="phase-pill">代码 ${phase.phase || '--'}</span></div><div class="phase-rule-grid">${rules}</div>`;
    }
    function togglePhasePopover() {
      document.getElementById('phasePopover').classList.toggle('show');
    }
    function setFocusMode(mode) {
      const trade = mode === 'trade';
      const config = mode === 'config';
      const holdings = mode === 'holdings';
      const logs = mode === 'logs';
      const life = mode === 'life';
      const stock = mode === 'stock';
      document.body.classList.toggle('trade-focus', trade);
      document.body.classList.toggle('config-focus', config);
      document.body.classList.toggle('holdings-focus', holdings);
      document.body.classList.toggle('log-focus', logs);
      document.body.classList.toggle('life-focus', life);
      document.body.classList.toggle('stock-focus', stock);
      document.getElementById('overviewFocusBtn')?.classList.toggle('active', !trade && !config && !holdings && !logs && !life && !stock);
      document.getElementById('stockTradeFocusBtn')?.classList.toggle('active', trade && manualTradeTab !== 'option');
      document.getElementById('optionTradeFocusBtn')?.classList.toggle('active', trade && manualTradeTab === 'option');
      document.getElementById('configFocusBtn')?.classList.toggle('active', config);
      document.getElementById('holdingsFocusBtn')?.classList.toggle('active', holdings);
      document.getElementById('logFocusBtn')?.classList.toggle('active', logs);
      document.getElementById('lifeFocusBtn')?.classList.toggle('active', life);
      document.getElementById('stockFocusBtn')?.classList.toggle('active', stock);
    }
    function showOverview() {
      setFocusMode('overview');
      if (manualQuoteInterval) {
        clearInterval(manualQuoteInterval);
        manualQuoteInterval = null;
      }
      document.getElementById('phasePopover')?.classList.remove('show');
    }
    function toggleTradeFocus(tab='stock') {
      setFocusMode('trade');
      document.getElementById('phasePopover')?.classList.remove('show');
      setManualTradeTab(tab);
      setLowerView('holdings');
    }
    function toggleConfigFocus() {
      setFocusMode('config');
      if (manualQuoteInterval) {
        clearInterval(manualQuoteInterval);
        manualQuoteInterval = null;
      }
      document.getElementById('phasePopover')?.classList.remove('show');
      setLowerView('strategy');
    }
    function toggleHoldingsFocus() {
      setFocusMode('holdings');
      if (manualQuoteInterval) {
        clearInterval(manualQuoteInterval);
        manualQuoteInterval = null;
      }
      document.getElementById('phasePopover')?.classList.remove('show');
      setLowerView(isDSectionHolding() ? 'd' : 'holdings');
    }
    function toggleStockFocus() {
      setFocusMode('stock');
      if (manualQuoteInterval) {
        clearInterval(manualQuoteInterval);
        manualQuoteInterval = null;
      }
      document.getElementById('phasePopover')?.classList.remove('show');
      loadStockSelection();
    }
    function toggleLogFocus() {
      setFocusMode('logs');
      if (manualQuoteInterval) {
        clearInterval(manualQuoteInterval);
        manualQuoteInterval = null;
      }
      document.getElementById('phasePopover')?.classList.remove('show');
      setLogView(logView || 'bots');
    }
    function toggleLifeFocus() {
      setFocusMode('life');
      if (manualQuoteInterval) {
        clearInterval(manualQuoteInterval);
        manualQuoteInterval = null;
      }
      document.getElementById('phasePopover')?.classList.remove('show');
      loadFiveYearPlan();
      loadJournal();
    }
    function updateLifeDate() {
      const el = document.getElementById('lifeDateText');
      if (!el) return;
      el.textContent = new Date().toLocaleDateString('zh-CN', {year:'numeric', month:'long', day:'numeric', weekday:'long'});
    }
    function todayKey() {
      const d = new Date();
      return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
    }
    function journalDateLabel(key) {
      const [y, m, d] = String(key || '').split('-').map(Number);
      if (!y || !m || !d) return key || '--';
      return new Date(y, m - 1, d).toLocaleDateString('zh-CN', {month:'long', day:'numeric', weekday:'short'});
    }
    function readJournals() {
      try {
        const rows = JSON.parse(localStorage.getItem(journalStorageKey) || '{}') || {};
        const legacy = localStorage.getItem(legacyJournalStorageKey);
        if (legacy && !rows[todayKey()]) {
          rows[todayKey()] = legacy;
          localStorage.setItem(journalStorageKey, JSON.stringify(rows));
          localStorage.removeItem(legacyJournalStorageKey);
        }
        return rows;
      } catch (_) {
        return {};
      }
    }
    function writeJournals(rows) {
      localStorage.setItem(journalStorageKey, JSON.stringify(rows || {}));
    }
    function loadFiveYearPlan() {
      const text = document.getElementById('fiveYearPlanText');
      if (!text) return;
      text.value = localStorage.getItem(fiveYearPlanStorageKey) || '';
      const status = document.getElementById('fiveYearStatus');
      if (status) status.textContent = text.value.trim() ? '已加载' : '未保存';
    }
    function saveFiveYearPlan() {
      const text = document.getElementById('fiveYearPlanText');
      if (!text) return;
      localStorage.setItem(fiveYearPlanStorageKey, text.value || '');
      const status = document.getElementById('fiveYearStatus');
      if (status) status.textContent = '已保存';
    }
    function clearFiveYearPlan() {
      if (!confirm('确认清空 5 年计划？')) return;
      const text = document.getElementById('fiveYearPlanText');
      if (!text) return;
      text.value = '';
      localStorage.removeItem(fiveYearPlanStorageKey);
      const status = document.getElementById('fiveYearStatus');
      if (status) status.textContent = '未保存';
      text.focus();
    }
    function ensureJournalDate(key) {
      const day = key || todayKey();
      const rows = readJournals();
      if (!Object.prototype.hasOwnProperty.call(rows, day)) {
        rows[day] = '';
        writeJournals(rows);
      }
      return rows;
    }
    function readTradingRules() {
      try {
        const rows = JSON.parse(localStorage.getItem(tradingRulesStorageKey) || 'null');
        if (Array.isArray(rows)) {
          const cleaned = rows.map(v => String(v || '').trim()).filter(Boolean);
          if (cleaned.length) return cleaned;
        }
      } catch (_) {}
      return [...defaultTradingRules];
    }
    function writeTradingRules(rows) {
      localStorage.setItem(tradingRulesStorageKey, JSON.stringify(rows || []));
    }
    function renderTradingRules() {
      const list = document.getElementById('tradingRuleList');
      const editor = document.getElementById('tradingRuleEditor');
      if (!list) return;
      const rows = readTradingRules();
      list.innerHTML = rows.map((rule, idx) => `<li><span>${idx + 1}</span><div>${esc(rule)}</div></li>`).join('');
      list.style.display = '';
      if (editor) editor.classList.remove('open');
    }
    function editTradingRules() {
      const list = document.getElementById('tradingRuleList');
      const editor = document.getElementById('tradingRuleEditor');
      const text = document.getElementById('tradingRulesText');
      if (!editor || !text) return;
      text.value = readTradingRules().join('\n');
      if (list) list.style.display = 'none';
      editor.classList.add('open');
      text.focus();
    }
    function cancelTradingRuleEdit() {
      renderTradingRules();
    }
    function saveTradingRules() {
      const text = document.getElementById('tradingRulesText');
      if (!text) return;
      const rows = text.value.split(/\n+/).map(v => v.trim()).filter(Boolean);
      writeTradingRules(rows.length ? rows : defaultTradingRules);
      renderTradingRules();
    }
    function setStockSelectionTab(tab) {
      stockSelectionTab = tab === 'pullback' ? 'pullback' : 'review';
      document.getElementById('selectionReviewTab')?.classList.toggle('active', stockSelectionTab === 'review');
      document.getElementById('selectionPullbackTab')?.classList.toggle('active', stockSelectionTab === 'pullback');
      document.getElementById('selectionReviewPanel')?.classList.toggle('active', stockSelectionTab === 'review');
      document.getElementById('selectionPullbackPanel')?.classList.toggle('active', stockSelectionTab === 'pullback');
      renderPullbackStocks();
    }
    function readPullbackStocks() {
      try {
        const rows = JSON.parse(localStorage.getItem(pullbackStorageKey) || '[]');
        if (!Array.isArray(rows)) return [];
        return rows
          .map(row => ({
            symbol: String(row.symbol || '').trim().toUpperCase(),
            target_price: Number(row.target_price || 0) > 0 ? Number(row.target_price) : null,
            note: String(row.note || '').trim(),
            source: String(row.source || '').trim(),
            snapshot_date: String(row.snapshot_date || '').trim(),
            reference_price: Number(row.reference_price || 0) > 0 ? Number(row.reference_price) : null,
            created_at: String(row.created_at || '').trim()
          }))
          .filter(row => row.symbol);
      } catch (_) {
        return [];
      }
    }
    function writePullbackStocks(rows) {
      localStorage.setItem(pullbackStorageKey, JSON.stringify(rows || []));
    }
    function pullbackRowPayload(row) {
      return esc(JSON.stringify({
        symbol: row.symbol,
        snapshot_date: row.snapshot_date || todayKey(),
        open: row.reference_price || row.target_price,
        high: row.reference_price || row.target_price,
        low: row.reference_price || row.target_price,
        close: row.reference_price || row.target_price,
        volume: 0,
        intraday_change_pct: 0,
        day_change_pct: 0
      }));
    }
    function renderPullbackStocks() {
      const table = document.getElementById('pullbackTable');
      const count = document.getElementById('pullbackCount');
      if (!table) return;
      const rows = readPullbackStocks();
      if (count) count.textContent = `${rows.length} 只`;
      const head = ['代码','等待价格','参考价','来源','加入日','备注','操作'];
      if (!rows.length) {
        table.innerHTML = `<tbody><tr><td><div class="stock-selection-empty">暂无等待回调股</div></td></tr></tbody>`;
        return;
      }
      const rowHtml = (row, idx) => `<tr>
        <td><button class="symbol-fill-btn" onclick="fillManualSymbol('${row.symbol}')">${row.symbol}</button></td>
        <td>${maybeMoney(row.target_price)}</td>
        <td>${maybeMoney(row.reference_price)}</td>
        <td>${esc(row.source || '--')}</td>
        <td>${esc((row.created_at || '').slice(0, 10) || '--')}</td>
        <td class="note-cell" title="${esc(row.note || '')}">${esc(row.note || '')}</td>
        <td>
          <div class="pullback-actions">
            <button class="pullback-small-btn primary" onclick="addStockPoolCandidate(this, ${pullbackRowPayload(row)}, 'B')">加 B</button>
            <button class="pullback-small-btn" onclick="removePullbackStock(${idx})">删除</button>
          </div>
        </td>
      </tr>`;
      table.innerHTML = `<thead><tr>${head.map(h => `<th>${h}</th>`).join('')}</tr></thead><tbody>${rows.map(rowHtml).join('')}</tbody>`;
    }
    function upsertPullbackStock(row) {
      const symbol = String(row.symbol || '').trim().toUpperCase();
      if (!symbol || !/^[A-Z0-9.]{1,16}$/.test(symbol)) {
        alert('股票代码无效');
        return;
      }
      const rows = readPullbackStocks();
      const next = {
        symbol,
        target_price: Number(row.target_price || 0) > 0 ? Number(row.target_price) : null,
        note: String(row.note || '').trim(),
        source: String(row.source || '').trim() || '手动',
        snapshot_date: String(row.snapshot_date || '').trim(),
        reference_price: Number(row.reference_price || 0) > 0 ? Number(row.reference_price) : null,
        created_at: row.created_at || new Date().toISOString()
      };
      const existingIdx = rows.findIndex(x => x.symbol === symbol);
      if (existingIdx >= 0) rows.splice(existingIdx, 1);
      rows.unshift(next);
      writePullbackStocks(rows);
      renderPullbackStocks();
      const status = document.getElementById('stockSelectionStatus');
      if (status) status.textContent = `${symbol} 已加入等待回调股`;
    }
    function addPullbackStockFromInputs() {
      const symbolEl = document.getElementById('pullbackSymbolInput');
      const targetEl = document.getElementById('pullbackTargetInput');
      const noteEl = document.getElementById('pullbackNoteInput');
      upsertPullbackStock({
        symbol: symbolEl?.value,
        target_price: targetEl?.value,
        note: noteEl?.value,
        source: '手动'
      });
      if (symbolEl) symbolEl.value = '';
      if (targetEl) targetEl.value = '';
      if (noteEl) noteEl.value = '';
      symbolEl?.focus();
    }
    function addPullbackStockFromSelection(row) {
      closeStockPoolMenus();
      upsertPullbackStock({
        symbol: row.symbol,
        reference_price: row.close,
        target_price: '',
        snapshot_date: row.snapshot_date,
        source: '选股复盘',
        note: `等待回调，复盘收盘 ${money(row.close)}，开盘涨幅 ${pct(row.intraday_change_pct)}`
      });
      setStockSelectionTab('pullback');
    }
    function removePullbackStock(idx) {
      const rows = readPullbackStocks();
      rows.splice(idx, 1);
      writePullbackStocks(rows);
      renderPullbackStocks();
    }
    function downloadGainerSymbolsCsv() {
      const rows = latestStockSelection?.rows || [];
      const symbols = [...new Set(rows.map(r => String(r.symbol || '').trim().toUpperCase()).filter(Boolean))];
      if (!symbols.length) {
        alert('当前没有可下载的股票代码');
        return;
      }
      const csv = symbols.join('\n') + '\n';
      const blob = new Blob([csv], {type:'text/csv;charset=utf-8'});
      const url = URL.createObjectURL(blob);
      const dateText = String(latestStockSelection?.snapshot_date || todayKey()).slice(0, 10);
      const link = document.createElement('a');
      link.href = url;
      link.download = `gainers_${dateText}.csv`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    }
    function renderStockSelection(payload) {
      latestStockSelection = payload || {};
      const rows = latestStockSelection.rows || [];
      const bRows = latestStockSelection.b_rows || [];
      const dRows = rows.filter(row => row.d_match);
      const status = document.getElementById('stockSelectionStatus');
      if (status) status.textContent = `${new Date().toLocaleTimeString()} 已更新`;
      document.getElementById('gainersCount').textContent = `${rows.length} 只`;
      document.getElementById('bSelectionCount').textContent = `${bRows.length} 只`;
      document.getElementById('downloadGainersBtn')?.toggleAttribute('disabled', rows.length === 0);
      const minUpPct = Number(latestStockSelection.min_up_pct || 0.05);
      const minPrice = Number(latestStockSelection.min_price || 2);
      const minVolume = Number(latestStockSelection.min_volume || 1000000);
      const minDollarVolume = Number(latestStockSelection.min_dollar_volume || 30000000);
      const filterPills = document.getElementById('gainersFilterPills');
      if (filterPills) {
        filterPills.innerHTML = [
          `涨幅 > ${(minUpPct * 100).toFixed(1)}%`,
          `价格 ≥ ${money(minPrice)}`,
          `成交量 ≥ ${compactNumber(minVolume)}`,
          `成交额 ≥ ${compactNumber(minDollarVolume)}`
        ].map(x => `<span class="stock-selection-filter-pill">${x}</span>`).join('');
      }
      document.getElementById('stockSelectionMeta').innerHTML = [
        `交易日 ${latestStockSelection.snapshot_date || '--'}`,
        `上一交易日 ${latestStockSelection.previous_date || '--'}`,
        `阈值 > ${(minUpPct * 100).toFixed(1)}%`,
        `价格 ≥ ${money(minPrice)}`,
        `成交量 ≥ ${compactNumber(minVolume)}`,
        `成交额 ≥ ${compactNumber(minDollarVolume)}`,
        `B ${bRows.length} 只 / D ${dRows.length} 只`
      ].map(x => `<span class="market-pill">${x}</span>`).join('');
      const strongest = rows[0];
      const avg = rows.length ? rows.reduce((s, r) => s + Number(r.intraday_change_pct || 0), 0) / rows.length : 0;
      document.getElementById('stockSelectionSummary').innerHTML = [
        ['涨幅>5%', `${rows.length}`, ''],
        ['符合B', `${bRows.length}`, 'pos'],
        ['符合D', `${dRows.length}`, 'pos'],
        ['平均涨幅', pct(avg), 'pos'],
        ['最强股票', strongest ? `${strongest.symbol} ${pct(strongest.intraday_change_pct)}` : '--', 'pos']
      ].map(([label, value, tone]) => `<div class="selection-summary-card"><div class="selection-summary-label">${label}</div><div class="selection-summary-value ${tone}">${value}</div></div>`).join('');

      const gainersHead = ['代码','B / D','开盘涨幅','昨收涨跌','开','高','低','收','量','成交额','操作','B说明'];
      const bHead = ['代码','开盘涨幅','昨收涨跌','触发价','入池收盘','入池日','最近说明'];
      const rowPayload = r => esc(JSON.stringify({
        symbol: r.symbol,
        snapshot_date: r.snapshot_date,
        open: r.open,
        high: r.high,
        low: r.low,
        close: r.close,
        volume: r.volume,
        intraday_change_pct: r.intraday_change_pct,
        day_change_pct: r.day_change_pct
      }));
      const actionHtml = r => {
        const payload = rowPayload(r);
        return `<div class="stock-action-wrap">
          <button class="stock-pool-add-btn" onclick="addStockPoolCandidate(this, ${payload}, 'B')">加 B</button>
          <button class="stock-pool-menu-btn" title="选择其它预选池" onclick="toggleStockPoolMenu(this, event)">▾</button>
          <div class="stock-pool-menu">
            <button onclick="addPullbackStockFromSelection(${payload})">等回调</button>
            ${['A','C','D'].map(pool => `<button onclick="addStockPoolCandidate(this, ${payload}, '${pool}')">加 ${pool}</button>`).join('')}
          </div>
        </div>`;
      };
      const rowHtml = r => {
        const bLabel = r.b_match ? '<span class="b-match-pill">符合B</span>' : '<span class="b-match-pill off">观察</span>';
        const dLabel = r.d_match ? '<span class="b-match-pill">符合D</span>' : '';
        return `<tr>
          <td><button class="symbol-fill-btn" onclick="fillManualSymbol('${r.symbol}')">${r.symbol}</button></td>
          <td>${bLabel}${dLabel}</td>
          <td class="${cls(r.intraday_change_pct)}">${pct(r.intraday_change_pct)}</td>
          <td class="${cls(r.day_change_pct)}">${r.day_change_pct == null ? '--' : pct(r.day_change_pct)}</td>
          <td>${money(r.open)}</td>
          <td>${money(r.high)}</td>
          <td>${money(r.low)}</td>
          <td>${money(r.close)}</td>
          <td>${compactNumber(r.volume)}</td>
          <td>${compactNumber(r.dollar_volume)}</td>
          <td class="stock-action-cell">${actionHtml(r)}</td>
          <td class="note-cell" title="${esc(r.last_order_intent || '')}">${esc(r.last_order_intent || '')}</td>
        </tr>`;
      };
      const bRowHtml = r => `<tr>
        <td><button class="symbol-fill-btn" onclick="fillManualSymbol('${r.symbol}')">${r.symbol}</button></td>
        <td class="${cls(r.intraday_change_pct)}">${pct(r.intraday_change_pct)}</td>
        <td class="${cls(r.day_change_pct)}">${r.day_change_pct == null ? '--' : pct(r.day_change_pct)}</td>
        <td>${maybeMoney(r.trigger_price)}</td>
        <td>${maybeMoney(r.entry_close)}</td>
        <td>${r.entry_date || '--'}</td>
        <td class="note-cell" title="${esc(r.last_order_intent || '')}">${esc(r.last_order_intent || '')}</td>
      </tr>`;
      document.getElementById('gainersTable').innerHTML = rows.length
        ? `<thead><tr>${gainersHead.map(h => `<th>${h}</th>`).join('')}</tr></thead><tbody>${rows.map(rowHtml).join('')}</tbody>`
        : `<tbody><tr><td><div class="stock-selection-empty">暂无涨幅超过阈值的股票</div></td></tr></tbody>`;
      document.getElementById('bSelectionTable').innerHTML = bRows.length
        ? `<thead><tr>${bHead.map(h => `<th>${h}</th>`).join('')}</tr></thead><tbody>${bRows.map(bRowHtml).join('')}</tbody>`
        : `<tbody><tr><td><div class="stock-selection-empty">涨幅列表里暂无符合 B 策略的股票</div></td></tr></tbody>`;
      renderPullbackStocks();
    }
    function closeStockPoolMenus() {
      document.querySelectorAll('.stock-action-wrap.open').forEach(el => el.classList.remove('open'));
    }
    function toggleStockPoolMenu(btn, event) {
      if (event) event.stopPropagation();
      const wrap = btn?.closest('.stock-action-wrap');
      const wasOpen = wrap?.classList.contains('open');
      closeStockPoolMenus();
      if (wrap && !wasOpen) wrap.classList.add('open');
    }
    async function addStockPoolCandidate(btn, row, pool='B') {
      const targetPool = String(pool || 'B').toUpperCase();
      const label = btn ? btn.textContent : '';
      if (btn) {
        btn.disabled = true;
        btn.textContent = '加入中';
      }
      closeStockPoolMenus();
      try {
        const result = await postJson('/api/stock_pool/add', {...row, pool: targetPool});
        if (!result.ok) {
          alert(result.error || '入池失败');
          return;
        }
        const status = document.getElementById('stockSelectionStatus');
        if (status) status.textContent = `${result.symbol || row.symbol} 已加入 ${targetPool} 预选池`;
        await loadStockSelection();
        const holdings = await api('/api/holdings');
        if (holdings.ok) {
          latestHoldings = holdings.rows || [];
          renderHoldings();
        }
      } catch (e) {
        alert(e.message || '入池失败');
      } finally {
        if (btn) {
          btn.disabled = false;
          btn.textContent = label || `加 ${targetPool}`;
        }
      }
    }
    async function loadStockSelection() {
      const btn = document.getElementById('stockSelectionRefreshBtn');
      const status = document.getElementById('stockSelectionStatus');
      const oldText = btn ? btn.textContent : '';
      if (btn) { btn.classList.add('loading'); btn.textContent = '刷新中'; }
      if (status) status.textContent = '加载中...';
      try {
        const payload = await api('/api/stock_selection');
        if (!payload.ok) {
          if (status) status.textContent = payload.error || '读取失败';
          return;
        }
        renderStockSelection(payload);
      } catch (e) {
        if (status) status.textContent = e.message || '读取失败';
      } finally {
        if (btn) { btn.classList.remove('loading'); btn.textContent = oldText || '刷新选股'; }
      }
    }
    function renderJournalDates() {
      const box = document.getElementById('journalDateList');
      if (!box) return;
      const rows = readJournals();
      const keySet = new Set(Object.keys(rows));
      keySet.add(todayKey());
      if (selectedJournalDate) keySet.add(selectedJournalDate);
      const keys = Array.from(keySet).sort().reverse();
      if (!keys.includes(selectedJournalDate)) selectedJournalDate = todayKey();
      box.innerHTML = keys.length ? keys.map(key => {
        const preview = String(rows[key] || '').trim().split(/\s+/).slice(0, 8).join(' ') || '空白日记';
        return `<button class="journal-date-btn ${key === selectedJournalDate ? 'active' : ''}" onclick="selectJournalDate('${key}')"><span>${journalDateLabel(key)}</span><span class="date-note">${esc(preview)}</span></button>`;
      }).join('') : '<div class="small-muted">保存后会出现日期</div>';
    }
    function selectJournalDate(key) {
      selectedJournalDate = key || todayKey();
      loadJournal(selectedJournalDate);
    }
    function newTodayJournal() {
      selectedJournalDate = todayKey();
      ensureJournalDate(selectedJournalDate);
      loadJournal(selectedJournalDate);
      document.getElementById('journalText')?.focus();
    }
    function loadJournal(dateKey=null) {
      updateLifeDate();
      renderTradingRules();
      selectedJournalDate = dateKey || todayKey();
      const text = document.getElementById('journalText');
      if (!text) return;
      const rows = selectedJournalDate === todayKey() ? ensureJournalDate(selectedJournalDate) : readJournals();
      text.value = rows[selectedJournalDate] || '';
      const title = document.querySelector('.journal-title');
      if (title) title.textContent = `${journalDateLabel(selectedJournalDate)} · 今天写点什么`;
      const status = document.getElementById('journalStatus');
      if (status) status.textContent = text.value ? '已加载' : '未保存';
      renderJournalDates();
    }
    function saveJournal() {
      const text = document.getElementById('journalText');
      if (!text) return;
      selectedJournalDate = selectedJournalDate || todayKey();
      const rows = readJournals();
      rows[selectedJournalDate] = text.value || '';
      writeJournals(rows);
      const status = document.getElementById('journalStatus');
      if (status) status.textContent = `已保存 ${new Date().toLocaleTimeString()}`;
      renderJournalDates();
    }
    function clearJournal() {
      if (!confirm('确认清空当前日期的日记？')) return;
      const text = document.getElementById('journalText');
      if (text) text.value = '';
      const rows = readJournals();
      delete rows[selectedJournalDate || todayKey()];
      writeJournals(rows);
      const status = document.getElementById('journalStatus');
      if (status) status.textContent = '已清空';
      renderJournalDates();
    }
    function appendJournalPrompt(promptText) {
      const text = document.getElementById('journalText');
      if (!text) return;
      const prefix = text.value.trim() ? '\n\n' : '';
      text.value += `${prefix}${promptText}\n`;
      text.focus();
      text.selectionStart = text.selectionEnd = text.value.length;
      const status = document.getElementById('journalStatus');
      if (status) status.textContent = '未保存';
    }
    function renderLogView() {
      const isTrades = logView === 'trades';
      document.getElementById('botLogsTab')?.classList.toggle('active', !isTrades);
      document.getElementById('tradeLogsTab')?.classList.toggle('active', isTrades);
      document.getElementById('botLogsSection')?.toggleAttribute('hidden', isTrades);
      document.getElementById('tradeLogsSection')?.toggleAttribute('hidden', !isTrades);
      document.querySelector('.log-actions')?.toggleAttribute('hidden', isTrades);
    }
    function setLogView(view) {
      logView = view === 'trades' ? 'trades' : 'bots';
      renderLogView();
      if (logView === 'trades') loadTradeRecords();
      else loadBotLogs();
    }
    function renderBotLogs(payload) {
      renderLogView();
      const groupPriority = name => {
        const n = String(name || '').toLowerCase();
        if (n.startsWith('b_')) return 10;
        if (n === 'ac_bot') return 20;
        if (n.startsWith('d_')) return 90;
        if (n.startsWith('q_')) return 91;
        if (n.startsWith('f_')) return 92;
        return 50;
      };
      const botPriority = name => {
        const n = String(name || '').toLowerCase();
        if (n.includes('_buy_')) return 1;
        if (n.includes('_sell_')) return 2;
        return 3;
      };
      const groupLabel = name => {
        const n = String(name || '').toLowerCase();
        if (n.startsWith('b_')) return 'B 策略';
        if (n.startsWith('d_')) return 'D 策略';
        if (n === 'ac_bot') return 'AC 做T';
        if (n.startsWith('q_')) return 'Q 期权';
        if (n.startsWith('f_')) return 'F 策略';
        return '其它';
      };
      const foldedBot = name => {
        const n = String(name || '').toLowerCase();
        return n.startsWith('d_') || n.startsWith('q_') || n.startsWith('f_');
      };
      const navButton = row => {
        const active = row.enabled || row.running;
        const running = active ? 'running' : '';
        const status = row.running ? '运行' : (row.enabled ? '开启' : '关闭');
        const path = row.log_path || 'fallback';
        return `<button class="bot-log-nav-btn ${row.bot_name === selectedBotLog ? 'active' : ''}" onclick="selectBotLog('${esc(row.bot_name)}')">
          <span class="bot-log-nav-name">${esc(row.bot_name)}</span>
          <span class="bot-log-nav-status ${running}">${status}</span>
          <span class="bot-log-nav-sub">${esc(path.split('/').pop() || path)}</span>
        </button>`;
      };
      const previousRows = new Map((botLogRows || []).map(row => [row.bot_name, row]));
      const rows = [...(payload?.rows || [])].map(row => {
        const previous = previousRows.get(row.bot_name);
        if (previous?.logs_loaded && !row.logs_loaded) {
          return {...row, ...previous, running: row.running, enabled:row.enabled, heartbeat_status:row.heartbeat_status, last_seen_at:row.last_seen_at, pid: row.pid, log_path: row.log_path || previous.log_path};
        }
        return row;
      }).sort((a, b) => {
        const ga = groupPriority(a.bot_name), gb = groupPriority(b.bot_name);
        if (ga !== gb) return ga - gb;
        const ba = botPriority(a.bot_name), bb = botPriority(b.bot_name);
        if (ba !== bb) return ba - bb;
        return String(a.bot_name || '').localeCompare(String(b.bot_name || ''));
      });
      botLogRows = rows;
      const grid = document.getElementById('botLogGrid');
      const nav = document.getElementById('botLogNav');
      const meta = document.getElementById('botLogsMeta');
      if (meta) meta.textContent = `${rows.length} 个机器人 · ${new Date().toLocaleTimeString()}`;
      if (!rows.length) {
        if (nav) nav.innerHTML = '';
        if (grid) grid.innerHTML = '<div class="empty-state">暂无机器人日志</div>';
        return;
      }
      if (!rows.some(r => r.bot_name === selectedBotLog)) selectedBotLog = rows[0].bot_name;
      let lastGroup = '';
      if (nav) {
        const mainRows = rows.filter(row => !foldedBot(row.bot_name));
        const foldedRows = rows.filter(row => foldedBot(row.bot_name));
        const mainHtml = mainRows.map(row => {
          const group = groupLabel(row.bot_name);
          const label = group !== lastGroup ? `<div class="bot-log-group-label">${esc(group)}</div>` : '';
          lastGroup = group;
          return `${label}${navButton(row)}`;
        }).join('');
        const foldedOpen = foldedRows.some(row => row.bot_name === selectedBotLog) ? 'open' : '';
        const foldedHtml = foldedRows.length ? `<details class="bot-log-fold" ${foldedOpen}>
          <summary>D / Q / F 机器人</summary>
          <div class="bot-log-fold-list">${foldedRows.map(navButton).join('')}</div>
        </details>` : '';
        nav.innerHTML = `${mainHtml}${foldedHtml}`;
      }
      renderSelectedBotLog();
    }
    function selectBotLog(botName) {
      captureBotLogScroll();
      selectedBotLog = botName;
      renderBotLogs({rows: botLogRows});
      const row = botLogRows.find(r => r.bot_name === selectedBotLog);
      if (row && !row.logs_loaded) loadSelectedBotLogDetails();
    }
    function hasImportantLogWindow(row) {
      return Array.isArray(row?.important_lines) && row.important_lines.length > 0;
    }
    function logLinesForMode(row, mode='all') {
      if (mode === 'important') return row?.important_lines || [];
      return [...(row?.lines || []), ...(row?.important_lines || [])];
    }
    function normalizeLogItem(item) {
      if (item && typeof item === 'object') {
        return {
          line: String(item.line || '').trim(),
          displayTime: String(item.display_time || item.displayTime || '').trim(),
          sourceFile: String(item.source_file || item.sourceFile || '').trim()
        };
      }
      return {line: String(item || '').trim(), displayTime: '', sourceFile: ''};
    }
    function activeLogQuery(mode='all') {
      return String(mode === 'important' ? botImportantSearch : botLogSearch).trim().toLowerCase();
    }
    function setBotLogSearch(value, mode='all') {
      captureBotLogScroll();
      if (mode === 'important') botImportantSearch = value || '';
      else botLogSearch = value || '';
      renderSelectedBotLog();
      if (botLogSearchTimer) clearTimeout(botLogSearchTimer);
      const query = activeLogQuery(mode);
      if (query.length >= 2) {
        botLogSearchTimer = setTimeout(() => loadSelectedBotLogDetails(query), 220);
      } else if (!query) {
        botLogSearchTimer = setTimeout(() => loadSelectedBotLogDetails(), 220);
      }
    }
    function applyLogSearch(events, mode='all') {
      const q = activeLogQuery(mode);
      if (!q) return events;
      return events.filter(event => {
        const hay = [
          event.line || '',
          event.type || '',
          extractLogSymbol(event.line || ''),
          ...(extractLogMetrics(event.line || '').map(m => `${m.key} ${m.value}`)),
        ].join(' ').toLowerCase();
        return hay.includes(q);
      });
    }
    function normalizeLogLines(row, mode='all') {
      const seen = new Set();
      return logLinesForMode(row, mode)
        .map(normalizeLogItem)
        .filter(item => item.line)
        .filter(item => {
          const key = stripLogPrefix(item.line);
          if (seen.has(key)) return false;
          seen.add(key);
          return true;
        });
    }
    function formatLogTime(value) {
      const raw = String(value || '').trim();
      if (!raw) return '--:--:--';
      const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}:\d{2}:\d{2})/);
      if (match) return `${match[2]}-${match[3]} ${match[4]}`;
      const timeOnly = raw.match(/(\d{2}:\d{2}:\d{2})/);
      return timeOnly ? timeOnly[1] : raw;
    }
    function logLineTime(line, displayTime='') {
      if (displayTime) return formatLogTime(displayTime);
      const match = String(line || '').match(/^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})/);
      return match ? formatLogTime(match[1]) : '--:--:--';
    }
    function stripLogPrefix(line) {
      return String(line || '')
        .replace(/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s*\|\s*(INFO|WARNING|ERROR|DEBUG|PRINT)\s*\|\s*/i, '')
        .trim();
    }
    function logEventType(line) {
      const text = stripLogPrefix(line);
      const decisionEvent = (text.match(/\bevent=([a-z0-9_]+)/i)?.[1] || '').toLowerCase();
      const decisionSide = (text.match(/\bside=([a-z]+)/i)?.[1] || '').toLowerCase();
      if (decisionEvent) {
        if (decisionEvent.includes('score')) return 'score';
        if (decisionSide === 'sell' || decisionEvent.includes('sell')) return 'sell';
        if (decisionSide === 'buy' || decisionEvent.includes('buy') || decisionEvent.includes('order')) return 'trade';
      }
      const signalText = text.replace(/\[(BUY|SELL) BOT\]/ig, '');
      if (/\[B SCORE|B SCORE|score=|candidate|候选|打分/i.test(text)) return 'score';
      if (/(卖出|平仓|sell[_\s-]?(order|submitted|filled|executed)?|止损|止盈|take profit|stop[_\s-]?(loss|order))/i.test(signalText) || /\[B SELL\]|\[SELL BOT\]/i.test(text)) return 'sell';
      if (/(买入|下单|submit|submitted|filled|executed|成交|开仓|order|buy[_\s-]?(order|submitted|filled|executed))/i.test(signalText)) return 'trade';
      if (/(price=|last=|bid=|ask=|prev|feed=|quote|snapshot|实时|报价)/i.test(text)) return 'quote';
      if (/(scan\s+[A-Z]\s+[A-Z0-9.]{1,12}|confirmed\s+[A-Z]\s+[A-Z0-9.]{1,12}|\[[A-Z ]+\]\s+[A-Z0-9.]{1,12}\s*$|candidate|trigger)/i.test(text)) return 'scan';
      if (/(skip|outside|window|gate|allowed|mismatch|FORCE|emergency|sell_only|global_buy|WARNING|ERROR|失败|异常)/i.test(text)) return 'risk';
      if (/(round done|round phase|db_rows|loop round|refreshed|sync|open_count|closed_count)/i.test(text)) return 'status';
      return 'noise';
    }
    function logEventLabel(type) {
      return {
        trade: '买入/下单',
        sell: '卖出/风控',
        quote: '实时价格',
        score: '打分/候选',
        scan: '扫描标的',
        risk: '规则拦截',
        status: '轮次状态',
      }[type] || '日志';
    }
    function extractLogSymbol(line) {
      const text = String(line || '');
      const patterns = [
        /["']symbol["']\s*:\s*["']([A-Z0-9.]{1,12})["']/,
        /scan\s+[A-Z]\s+([A-Z0-9.]{1,12})\b/i,
        /\[[A-Z _]+\]\s+([A-Z0-9.]{1,12})\s+(?:price=|bid=|ask=|last=|buy|sell|skip|submit|order|下单|买入|卖出)/i,
        /confirmed\s+[A-Z]\s+([A-Z0-9.]{1,12})\b/i,
        /\b(?:symbol|stock|code)=([A-Z0-9.]{1,12})\b/i,
        /\b([A-Z]{1,6})\s+(?:price=|bid=|ask=|last=|submit|order)/,
      ];
      for (const pattern of patterns) {
        const match = text.match(pattern);
        const rawSymbol = match?.[1] || '';
        if (rawSymbol !== rawSymbol.toUpperCase()) continue;
        const symbol = rawSymbol.toUpperCase();
        if (symbol && !['BOT','BUY','SELL','INFO','WARNING','ERROR','TRUE','FALSE','LOOP','ROUND'].includes(symbol)) return symbol;
      }
      return '';
    }
    function extractLogMetrics(line) {
      const allow = new Set([
        'price','last','bid','ask','prev','cost','up_pct','qty','sl','stage','feed',
        'peak_price','peak_gain','profit_now','peak_profit','round','phase',
        'db_rows','scanned','eligible','traded','bp','threshold','window','now'
      ]);
      const metrics = [];
      const text = String(line || '');
      for (const match of text.matchAll(/\b([a-zA-Z_]+)=([^\s,，)]+)/g)) {
        const key = match[1].toLowerCase();
        if (!allow.has(key)) continue;
        metrics.push({key, value: match[2]});
      }
      return metrics.slice(0, 8);
    }
    function isBuyStageLog(line) {
      const text = stripLogPrefix(line);
      if (/(loop round=|round phase=|round done|market closed|sleep \d+s|heartbeat|using key|key_prefix|BUY_GATE|buy_allowed|FORCE phase|split .* bot start|outside LA buy window|journal|life|生活|休闲)/i.test(text)) {
        return false;
      }
      return /(\[B SCORE|\[B BUY|\[BUY BOT\] confirmed|confirmed\s+B\s+[A-Z0-9.]+|quote bid=|cap limit|submit|submitted|bought|filled|can_buy=0|skip:|candidate|trigger|price=|day_up=|entry_up=|pullback=|volume=)/i.test(text);
    }
    function isSellStageLog(line) {
      const text = stripLogPrefix(line);
      if (/(loop round=.*emergency_stop=0|round done.*traded=0|market closed|sleep \d+s|heartbeat|using key|key_prefix|BUY_GATE|buy_allowed|FORCE phase|split .* bot (start|stopped)|journal|life|生活|休闲)/i.test(text)) {
        return false;
      }
      return /(\[B SELL\]|\[SELL BOT\]|scan\s+B\s+[A-Z0-9.]+|sell|sold|submit|submitted|filled|executed|order|卖出|平仓|止损|止盈|take profit|stop[_\s-]?loss|peak_price|peak_gain|profit_now|last=|price=|qty=|cost=|skip:|mismatch|gate|WARNING|ERROR|失败|异常)/i.test(text);
    }
    function keyBotLogLines(row, mode='all') {
      const tradeOnly = mode === 'important';
      const keepTypes = tradeOnly
        ? new Set(['trade', 'sell', 'score'])
        : new Set(['trade', 'sell', 'quote', 'score', 'scan', 'risk']);
      const noisyStatus = /(loop round=.*emergency_stop=0|FORCE phase|round phase=.*db_rows=|round done.*traded=0|outside LA buy window|market closed|sleep \d+s|heartbeat|journal|life|生活|休闲)/i;
      const codeNoise = /^\s*(File ".+", line \d+|return |raise |\w+\s*=|Traceback \(most recent call last\)|[\w.]+Error:|[A-Za-z_][\w.]*\()/i;
      const events = [];
      const normalized = normalizeLogLines(row, mode);
      const sellBot = String(row?.bot_name || '').toLowerCase().includes('sell');
      for (const item of normalized) {
        const line = item.line;
        if (codeNoise.test(line)) continue;
        if (!tradeOnly && !(sellBot ? isSellStageLog(line) : isBuyStageLog(line))) continue;
        const type = logEventType(line);
        if (!keepTypes.has(type)) continue;
        if (!tradeOnly && type === 'status' && noisyStatus.test(line)) continue;
        if (!tradeOnly && type === 'risk' && noisyStatus.test(line)) continue;
        events.push({line, type, displayTime: item.displayTime, sourceFile: item.sourceFile});
      }
      const searched = applyLogSearch(events, mode);
      return searched.slice(-260).reverse();
    }
    function importantBotLogLines(row) {
      const include = /(符合|买入|卖出|需要卖出|止损|止盈|下单|开仓|平仓|减仓|加仓|buy\b|sell\b|order|submit|filled|executed|成交|stop|take profit)/i;
      const noise = /(market closed|sleep|heartbeat|using key|key_prefix|^\s*\d{4}-\d{2}-\d{2}.*\[(ENV|BP|BUY_GATE)\]|split .* bot start|split .* bot stopped)/i;
      return normalizeLogLines(row)
        .map(item => item.line)
        .filter(line => line.trim() && include.test(line) && !noise.test(line))
        .slice(-120);
    }
    function botLogModeTabs(activeMode) {
      return `<span class="bot-log-mode-tabs">
        <button class="bot-log-mode-tab ${activeMode === 'all' ? 'active' : ''}" onclick="setBotLogMode('all')">全部日志</button>
        <button class="bot-log-mode-tab important ${activeMode === 'important' ? 'active' : ''}" onclick="setBotLogMode('important')">重要日志</button>
      </span>`;
    }
    function setBotLogMode(mode) {
      captureBotLogScroll();
      selectedBotLogMode = mode === 'important' ? 'important' : 'all';
      renderSelectedBotLog();
    }
    function botEventCardHtml(event) {
      const line = event.line || '';
      const type = event.type || logEventType(line);
      const symbol = extractLogSymbol(line);
      const metrics = extractLogMetrics(line);
      const body = stripLogPrefix(line);
      const chips = metrics.map(m => `<span class="bot-event-chip ${esc(m.key)}">${esc(m.key)} ${esc(m.value)}</span>`).join('');
      return `<div class="bot-event-card ${esc(type)}">
        <div class="bot-event-top">
          <div class="bot-event-left">
            <span class="bot-event-kind">${esc(logEventLabel(type))}</span>
            ${symbol ? `<span class="bot-event-symbol">${esc(symbol)}</span>` : ''}
          </div>
          <span class="bot-event-time">${esc(logLineTime(line, event.displayTime))}</span>
        </div>
        <div class="bot-event-text">${esc(body)}</div>
        ${chips ? `<div class="bot-event-metrics">${chips}</div>` : ''}
      </div>`;
    }
    function botLogWindowHtml({title, status, running, meta, events, important=false, clearable=false, tabs='', symbols=[], scrollKey='', symbolMode='all'}) {
      const clearButton = clearable ? '<button class="bot-log-clear-btn" onclick="clearSelectedBotLog()">删除日志</button>' : '';
      const rows = events || [];
      const body = rows.length
        ? rows.map(botEventCardHtml).join('')
        : `<div class="bot-event-empty">${important ? '暂无匹配的买卖/下单关键日志' : '暂无匹配日志；市场关闭、休闲和重复轮询已隐藏'}</div>`;
      const symbolClick = symbolMode === 'important' ? 'setImportantSymbolSearch' : 'setAllSymbolSearch';
      const symbolHtml = symbols.length
        ? `<div class="bot-symbol-strip">${symbols.slice(0, 40).map(s => `<button class="bot-symbol-chip" onclick="${symbolClick}('${esc(s)}')">${esc(s)}</button>`).join('')}</div>`
        : '';
      return `<div class="bot-log-window ${important ? 'important' : ''}">
        <div class="bot-log-title">
          <span class="bot-log-title-left"><span class="bot-log-name">${esc(title)}</span>${tabs}</span>
          <span class="bot-log-title-actions">
            ${clearButton}
            <span class="bot-log-status ${running}">${esc(status)}</span>
          </span>
        </div>
        <div class="bot-log-meta" title="${esc(meta)}">${esc(meta)}</div>
        ${symbolHtml}
        <div class="bot-event-feed" data-scroll-key="${esc(scrollKey)}">${body}</div>
      </div>`;
    }
    function setImportantSymbolSearch(symbol) {
      captureBotLogScroll();
      botImportantSearch = symbol || '';
      const input = document.getElementById('botImportantSearch');
      if (input) input.value = botImportantSearch;
      selectedBotLogMode = 'important';
      renderSelectedBotLog();
    }
    function setAllSymbolSearch(symbol) {
      captureBotLogScroll();
      botLogSearch = symbol || '';
      const input = document.getElementById('botLogSearch');
      if (input) input.value = botLogSearch;
      selectedBotLogMode = 'all';
      renderSelectedBotLog();
    }
    async function clearSelectedBotLog() {
      const row = botLogRows.find(r => r.bot_name === selectedBotLog) || botLogRows[0];
      if (!row) return;
      if (!confirm(`确认删除 ${row.bot_name} 的当前日志内容？`)) return;
      const result = await postJson('/api/bot_logs/delete', {bot_name: row.bot_name});
      if (!result.ok) {
        alert(result.error || '删除日志失败');
        return;
      }
      await loadBotLogs();
    }
    function botLogScrollKey() {
      return `${selectedBotLog || ''}:${selectedBotLogMode === 'important' ? 'important' : 'all'}`;
    }
    function captureBotLogScroll() {
      const feed = document.querySelector('#botLogGrid .bot-event-feed');
      if (!feed) return null;
      const key = feed.dataset.scrollKey || botLogScrollKey();
      const maxScroll = Math.max(0, feed.scrollHeight - feed.clientHeight);
      const state = {
        key,
        top: feed.scrollTop,
        maxScroll,
        fromBottom: maxScroll - feed.scrollTop,
        pinnedBottom: maxScroll - feed.scrollTop < 24,
      };
      botLogScrollState[key] = state;
      return state;
    }
    function restoreBotLogScroll(previousState=null) {
      const feed = document.querySelector('#botLogGrid .bot-event-feed');
      if (!feed) return;
      const key = feed.dataset.scrollKey || botLogScrollKey();
      const state = previousState?.key === key ? previousState : botLogScrollState[key];
      if (!state) return;
      const maxScroll = Math.max(0, feed.scrollHeight - feed.clientHeight);
      feed.scrollTop = state.pinnedBottom
        ? maxScroll
        : Math.max(0, Math.min(maxScroll, maxScroll - Number(state.fromBottom || 0)));
    }
    function renderSelectedBotLog() {
      const grid = document.getElementById('botLogGrid');
      if (!grid) return;
      const previousScroll = captureBotLogScroll();
      const row = botLogRows.find(r => r.bot_name === selectedBotLog) || botLogRows[0];
      if (!row) {
        grid.innerHTML = '<div class="empty-state">暂无机器人日志</div>';
        return;
      }
      const active = row.enabled || row.running;
      const running = active ? 'running' : '';
      const status = row.running
        ? `运行中${row.pid ? ' pid=' + row.pid : ''}`
        : (row.enabled ? '开关已开启 · 等待进程心跳' : '已关闭');
      const mode = selectedBotLogMode === 'important' ? 'important' : 'all';
      const scrollKey = botLogScrollKey();
      if (!row.logs_loaded) {
        grid.innerHTML = botLogWindowHtml({
          title: row.bot_name,
          status,
          running,
          meta: '正在读取最近 5 个交易日日志...',
          events: [],
          clearable: true,
          tabs: botLogModeTabs(mode),
          symbols: [],
          scrollKey,
          symbolMode: mode
        });
        return;
      }
      const events = keyBotLogLines(row, mode);
      const query = activeLogQuery(mode);
      const sourceFiles = (row.source_files || []).map(p => String(p).split('/').pop()).join(', ');
      const symbolButtons = mode === 'important'
        ? (row.important_symbols || row.symbols || [])
        : ((row.all_symbols && row.all_symbols.length) ? row.all_symbols : (row.important_symbols || row.symbols || []));
      const meta = mode === 'important'
        ? `近 ${row.important_days || 3} 天买卖/下单/成交事件 · ${row.important_line_count || 0} 条${query ? ' · 搜索：' + query : ''}`
        : `近 ${row.general_days || 2} 天去噪日志 · ${row.source_line_count || 0} 条${query ? ' · 搜索：' + query : ''}${sourceFiles ? ' · ' + sourceFiles : ''}`;
      grid.innerHTML = botLogWindowHtml({
        title: row.bot_name,
        status: mode === 'important' ? '买卖关键' : status,
        running: mode === 'important' ? '' : running,
        meta,
        events,
        important: mode === 'important',
        clearable: true,
        tabs: botLogModeTabs(mode),
        symbols: symbolButtons,
        scrollKey,
        symbolMode: mode
      });
      requestAnimationFrame(() => restoreBotLogScroll(previousScroll));
    }
    async function loadSelectedBotLogDetails(search='') {
      const botName = selectedBotLog || botLogRows[0]?.bot_name || 'b_buy_bot';
      if (!botName) return;
      const query = String(search || '').trim();
      const url = `/api/bot_logs?lines=${query ? 1200 : 300}&bot_name=${encodeURIComponent(botName)}${query ? '&q=' + encodeURIComponent(query) : ''}`;
      const payload = await api(url);
      const detail = payload?.rows?.[0];
      if (!detail) return;
      botLogRows = botLogRows.map(row => row.bot_name === detail.bot_name ? {...row, ...detail} : row);
      renderSelectedBotLog();
    }
    async function loadBotLogs() {
      const meta = document.getElementById('botLogsMeta');
      if (meta) meta.textContent = '加载中...';
      try {
        const payload = await api('/api/bot_logs?lines=0&summary=1');
        renderBotLogs(payload);
        await loadSelectedBotLogDetails();
      } catch (e) {
        renderBotLogs({rows:[{bot_name:'日志', running:false, log_path:'', lines:[`读取失败：${e.message || e}`]}]});
      }
    }
    function setManualQuote(last='--', bid='--', ask='--') {
      document.getElementById('manualQuoteLast').textContent = last;
      document.getElementById('manualQuoteBid').textContent = bid;
      document.getElementById('manualQuoteAsk').textContent = ask;
      document.querySelectorAll('.manual-quote-card').forEach(card => {
        card.classList.remove('fresh');
        void card.offsetWidth;
        card.classList.add('fresh');
      });
    }
    function quoteTestMoney(v) {
      const n = Number(v || 0);
      return n > 0 ? `$${n.toFixed(2)}` : '--';
    }
    function setQuoteTestValue(id, value, cls='') {
      const el = document.getElementById(id);
      if (!el) return;
      el.className = `quote-test-value ${cls}`.trim();
      el.textContent = value;
    }
    function handleQuoteTestKey(e) {
      if (e.key === 'Enter') testRealtimeQuote();
    }
    async function testRealtimeQuote() {
      const input = document.getElementById('quoteTestSymbol');
      const btn = document.getElementById('quoteTestBtn');
      const note = document.getElementById('quoteTestNote');
      const source = document.getElementById('quoteTestSource');
      const symbol = (input?.value || '').trim().toUpperCase();
      if (input) input.value = symbol;
      if (!symbol) {
        if (note) note.textContent = '请输入股票代码';
        return;
      }
      if (btn) {
        btn.classList.add('loading');
        btn.textContent = '查询中';
      }
      if (note) note.textContent = '正在调用系统报价接口...';
      try {
        const payload = await api(`/api/stock_quote?symbol=${encodeURIComponent(symbol)}&_=${Date.now()}`);
        if (!payload.ok) {
          setQuoteTestValue('quoteTestLast', '--', 'warn');
          setQuoteTestValue('quoteTestBid', '--');
          setQuoteTestValue('quoteTestAsk', '--');
          setQuoteTestValue('quoteTestPrev', '--');
          setQuoteTestValue('quoteTestVolume', '--');
          if (source) source.textContent = '读取失败';
          if (note) note.textContent = payload.error || '报价失败';
          return;
        }
        const last = Number(payload.last || 0);
        const prev = Number(payload.prev_close || 0);
        const changePct = last > 0 && prev > 0 ? (last - prev) / prev * 100 : null;
        setQuoteTestValue('quoteTestLast', quoteTestMoney(payload.last), last >= prev && prev > 0 ? 'pos' : '');
        setQuoteTestValue('quoteTestBid', quoteTestMoney(payload.bid));
        setQuoteTestValue('quoteTestAsk', quoteTestMoney(payload.ask));
        setQuoteTestValue('quoteTestPrev', quoteTestMoney(payload.prev_close));
        const dayVolume = Number(payload.day_volume || payload.volume || payload.intraday_volume || 0);
        setQuoteTestValue('quoteTestVolume', dayVolume > 0 ? compactNumber(dayVolume) : '--');
        if (source) source.textContent = payload.source || 'system';
        const pieces = [
          `${payload.symbol || symbol}`,
          payload.fetched_at || new Date().toLocaleTimeString('zh-CN', {hour12:false}),
          changePct === null ? '' : `较昨收 ${changePct >= 0 ? '+' : ''}${changePct.toFixed(2)}%`,
          payload.warning ? `warning: ${payload.warning}` : '',
        ].filter(Boolean);
        if (note) note.textContent = pieces.join(' · ');
      } catch (e) {
        setQuoteTestValue('quoteTestLast', '--', 'warn');
        setQuoteTestValue('quoteTestBid', '--');
        setQuoteTestValue('quoteTestAsk', '--');
        setQuoteTestValue('quoteTestPrev', '--');
        setQuoteTestValue('quoteTestVolume', '--');
        if (source) source.textContent = '请求异常';
        if (note) note.textContent = e.message || String(e);
      } finally {
        if (btn) {
          btn.classList.remove('loading');
          btn.textContent = '查询';
        }
      }
    }
    function updateManualHeldQty() {
      const symbol = (document.getElementById('manualBuySymbol')?.value || '').trim().toUpperCase();
      const box = document.getElementById('manualHeldQty');
      if (!box) return;
      if (!symbol) {
        box.value = '--';
        return;
      }
      const row = (latestHoldings || []).find(r => String(r.symbol || '').toUpperCase() === symbol);
      const qty = Number(row?.total_shares ?? row?.qty ?? 0);
      box.value = qty > 0 ? `${qty.toFixed(4)} 股` : '0.0000 股';
    }
    function updateManualPoolAvailable() {
      const select = document.getElementById('manualBuyPool');
      if (!select) return;
      const labels = {
        A: 'A 养老金账户 / 买入归 A',
        B: 'B 策略资金池 / 买入归 B',
        C: 'C 长期股票池 / 买入归 C',
        D: 'D 日内交易池 / 买入归 D',
      };
      ['A','B','C','D'].forEach(pool => {
        const opt = Array.from(select.options).find(o => o.value === pool);
        const amount = Number(window.latestCapitalPayload?.available?.[pool] || 0);
        if (opt) opt.textContent = `${labels[pool]} · 可买入 ${money(amount)}`;
      });
      updateManualBStopNotice();
      updateManualTradePreviews();
    }
    function manualFraction(value) {
      return Number({'1/4':0.25, '1/3':1/3, '1/2':0.5, '1/1':1, full:1}[value] || 0);
    }
    function manualPreviewQty(rawQty, price) {
      const qty = Number(rawQty || 0);
      const px = Number(price || 0);
      if (qty <= 0) return 0;
      return px > 50 ? Math.floor(qty * 10) / 10 : Math.floor(qty);
    }
    function manualQtyText(qty, price) {
      const n = Number(qty || 0);
      return n > 0 ? `${Number(price || 0) > 50 ? n.toFixed(1) : String(Math.floor(n))} 股` : '--';
    }
    function manualPreviewPrice(side) {
      const id = side === 'sell' ? 'manualSellLimitPrice' : side === 'short' ? 'manualShortLimitPrice' : 'manualBuyLimitPrice';
      return Number(document.getElementById(id)?.value || latestManualQuote?.last || latestManualQuote?.snapshot_last || 0);
    }
    function currentManualHeldQty() {
      const symbol = (document.getElementById('manualBuySymbol')?.value || '').trim().toUpperCase();
      const row = (latestHoldings || []).find(r => String(r.symbol || '').toUpperCase() === symbol);
      return Number(row?.total_shares ?? row?.qty ?? 0);
    }
    function setManualPreview(side, qty, notional, constraint) {
      const prefix = side === 'sell' ? 'manualSell' : side === 'short' ? 'manualShort' : 'manualBuy';
      const qtyBox = document.getElementById(`${prefix}QtyPreview`);
      const notionalBox = document.getElementById(`${prefix}NotionalPreview`);
      const constraintBox = document.getElementById(`${prefix}ConstraintPreview`);
      const price = manualPreviewPrice(side);
      if (qtyBox) qtyBox.value = manualQtyText(qty, price);
      if (notionalBox) notionalBox.value = Number(notional || 0) > 0 ? money(notional) : '--';
      if (constraintBox) constraintBox.value = constraint || '--';
    }
    function updateManualBuyPreview() {
      const pool = document.getElementById('manualBuyPool')?.value || 'C';
      const size = document.getElementById('manualBuySize')?.value || '1/4';
      const fraction = manualFraction(size);
      const price = manualPreviewPrice('buy');
      const available = Number(window.latestCapitalPayload?.available?.[pool] || 0);
      const buyingPower = Number(window.latestCapitalPayload?.buying_power || available || 0);
      const usable = Math.max(0, Math.min(available * fraction, buyingPower));
      const qty = price > 0 ? manualPreviewQty(usable / price, price) : 0;
      setManualPreview('buy', qty, qty * price, `${pool} 可用 ${money(available)}`);
    }
    function updateManualSellPreview() {
      const size = document.getElementById('manualSellSize')?.value || '1/4';
      const fraction = manualFraction(size);
      const price = manualPreviewPrice('sell');
      const heldQty = currentManualHeldQty();
      const qty = price > 0 ? (fraction >= 1 ? heldQty : manualPreviewQty(heldQty * fraction, price)) : 0;
      setManualPreview('sell', qty, qty * price, `持仓 ${heldQty.toFixed(4)} 股`);
    }
    function updateManualShortPreview() {
      const pool = document.getElementById('manualBuyPool')?.value || 'C';
      const size = document.getElementById('manualShortSize')?.value || '1/4';
      const fraction = manualFraction(size);
      const price = manualPreviewPrice('short');
      const available = Number(window.latestCapitalPayload?.available?.[pool] || 0);
      const buyingPower = Number(window.latestCapitalPayload?.buying_power || available || 0);
      const usable = Math.max(0, Math.min(available * fraction, buyingPower));
      const qty = price > 0 ? manualPreviewQty(usable / price, price) : 0;
      setManualPreview('short', qty, qty * price, 'A 不支持卖空');
    }
    function updateManualTradePreviews() {
      updateManualBuyPreview();
      updateManualSellPreview();
      updateManualShortPreview();
    }
    function updateManualBStopNotice() {
      const note = document.getElementById('manualBuyNote');
      if (!note) return;
      const pool = document.getElementById('manualBuyPool')?.value || 'C';
      const orderType = document.getElementById('manualBuyOrderType')?.value || 'market';
      const limitValue = Number(document.getElementById('manualBuyLimitPrice')?.value || 0);
      const marketValue = Number(latestManualQuote?.limit_price || latestManualQuote?.ask || latestManualQuote?.last || 0);
      const refPrice = orderType === 'limit' && limitValue > 0 ? limitValue : marketValue;
      const policies = {
        A: {pct:-0.15, text:'养老金长期仓灾难保护线，仅记录不自动卖出'},
        B: {pct:Number(latestStrategyBConfig?.sell?.initial_stop_pct ?? -0.05), text:'初始止损，由 B 卖出机器人自动执行'},
        C: {pct:-0.12, text:'长期成长仓结构保护线，仅记录不自动卖出'},
        D: {pct:-0.03, text:'日内保护线；同时保留收盘前强制平仓'},
      };
      const policy = policies[pool] || policies.C;
      const stopPrice = refPrice > 0 ? refPrice * (1 + policy.pct) : 0;
      const pctText = `${policy.pct >= 0 ? '+' : ''}${(policy.pct * 100).toFixed(1)}%`;
      const priceText = stopPrice > 0 ? money(stopPrice) : '--';
      note.textContent = `使用 ${pool} 资金买入后归为 ${pool} 类型。${policy.text}：${priceText}（${pctText}）`;
      note.classList.add('show');
    }
    function applyManualQuoteToHolding(symbol, quote) {
      const key = String(symbol || '').trim().toUpperCase();
      const row = (latestHoldings || []).find(r => String(r.symbol || '').toUpperCase() === key);
      if (!row) return;
      const last = Number(quote?.last || 0);
      const prev = Number(quote?.prev_close || 0);
      const qty = Number(row.total_shares ?? row.qty ?? 0);
      if (last > 0) {
        row.current_price = last;
        if (qty > 0) row.market_value = qty * last;
      }
      if (last > 0 && prev > 0) row.day_change_pct = (last - prev) / prev;
    }
    function handleManualBuySymbolInput(input) {
      input.value = input.value.toUpperCase().replace(/[^A-Z.]/g,'');
      const symbol = input.value.trim();
      if (manualSymbolLocked && symbol) localStorage.setItem(manualSymbolLockStorageKey, symbol);
      if (manualQuoteInterval) {
        clearInterval(manualQuoteInterval);
        manualQuoteInterval = null;
      }
      latestManualQuote = null;
      setManualQuote(symbol ? '加载中' : '--', '--', '--');
      updateManualHeldQty();
      updateManualTradePreviews();
      updateManualBStopNotice();
      if (manualBuyQuoteTimer) clearTimeout(manualBuyQuoteTimer);
      if (symbol.length < 1) return;
      manualBuyQuoteTimer = setTimeout(() => {
        loadManualBuyQuote(symbol, true);
        manualQuoteInterval = setInterval(() => loadManualBuyQuote(symbol, false), 3000);
      }, 420);
    }
    function fillManualSymbol(symbol) {
      const input = document.getElementById('manualBuySymbol');
      const value = String(symbol || '').trim().toUpperCase();
      if (!input || !value) return;
      if (manualSymbolLocked && input.value.trim()) return;
      input.value = value;
      handleManualBuySymbolInput(input);
      document.getElementById('manualBuyEntry')?.scrollIntoView({block:'nearest'});
    }
    function applyManualSymbolLock(locked, symbol='') {
      manualSymbolLocked = !!locked;
      const input = document.getElementById('manualBuySymbol');
      const btn = document.getElementById('manualSymbolLockBtn');
      if (input) {
        if (symbol) input.value = String(symbol).toUpperCase().replace(/[^A-Z.]/g,'');
        input.disabled = manualSymbolLocked;
      }
      if (btn) {
        btn.classList.toggle('locked', manualSymbolLocked);
        btn.textContent = manualSymbolLocked ? '已锁定' : '锁定';
        btn.title = manualSymbolLocked ? '解除锁定股票代码' : '锁定当前股票代码';
      }
    }
    function toggleManualSymbolLock() {
      const input = document.getElementById('manualBuySymbol');
      const symbol = (input?.value || '').trim().toUpperCase();
      if (!manualSymbolLocked && !symbol) {
        alert('先输入股票代码');
        return;
      }
      if (manualSymbolLocked) {
        localStorage.removeItem(manualSymbolLockStorageKey);
        applyManualSymbolLock(false);
        input?.focus();
      } else {
        localStorage.setItem(manualSymbolLockStorageKey, symbol);
        applyManualSymbolLock(true, symbol);
        handleManualBuySymbolInput(input);
      }
    }
    function restoreManualSymbolLock() {
      const symbol = (localStorage.getItem(manualSymbolLockStorageKey) || '').trim().toUpperCase().replace(/[^A-Z.]/g,'');
      if (!symbol) {
        applyManualSymbolLock(false);
        return;
      }
      applyManualSymbolLock(true, symbol);
      const input = document.getElementById('manualBuySymbol');
      if (input) handleManualBuySymbolInput(input);
    }
    async function loadManualBuyQuote(symbol, seedLimits=false) {
      try {
        const payload = await api(`/api/stock_quote?symbol=${encodeURIComponent(symbol)}`);
        const current = (document.getElementById('manualBuySymbol')?.value || '').trim().toUpperCase();
        if (current !== symbol) return;
        if (!payload.ok) {
          setManualQuote('--', '--', '--');
          return;
        }
        latestManualQuote = payload;
        applyManualQuoteToHolding(symbol, payload);
        updateManualHeldQty();
        renderHoldings();
        setManualQuote(
          Number(payload.last || 0) > 0 ? money(payload.last) : '--',
          Number(payload.bid || 0) > 0 ? money(payload.bid) : '--',
          Number(payload.ask || 0) > 0 ? money(payload.ask) : '--'
        );
        if (Number(payload.last || payload.snapshot_last || payload.limit_price || 0) > 0) {
          const buyInput = document.getElementById('manualBuyLimitPrice');
          const sellInput = document.getElementById('manualSellLimitPrice');
          const realtimePrice = Number(payload.last || payload.snapshot_last || 0);
          if (buyInput && realtimePrice > 0 && buyInput.dataset.userEdited !== '1') {
            buyInput.value = realtimePrice.toFixed(2);
            buyInput.dataset.autoPrice = buyInput.value;
          }
          if (sellInput && realtimePrice > 0 && sellInput.dataset.userEdited !== '1') {
            sellInput.value = realtimePrice.toFixed(2);
            sellInput.dataset.autoPrice = sellInput.value;
          }
        }
        const shortInput = document.getElementById('manualShortLimitPrice');
        const realtimePrice = Number(payload.last || payload.snapshot_last || 0);
        if (shortInput && realtimePrice > 0 && shortInput.dataset.userEdited !== '1') {
          shortInput.value = realtimePrice.toFixed(2);
          shortInput.dataset.autoPrice = shortInput.value;
        }
        updateManualTradePreviews();
        updateManualBStopNotice();
      } catch (_) {
        setManualQuote('--', '--', '--');
      }
    }
    function updateManualOrderType(side) {
      const ids = side === 'sell'
        ? ['manualSellOrderType', 'manualSellLimitControl', 'manualSellLimitPrice']
        : side === 'short'
          ? ['manualShortOrderType', 'manualShortLimitControl', 'manualShortLimitPrice']
          : ['manualBuyOrderType', 'manualBuyLimitControl', 'manualBuyLimitPrice'];
      const type = document.getElementById(ids[0])?.value || 'limit';
      const box = document.getElementById(ids[1]);
      if (box) box.classList.toggle('show', type === 'limit');
      if (type === 'limit') {
        const input = document.getElementById(ids[2]);
        const n = Number(latestManualQuote?.last || latestManualQuote?.snapshot_last || 0);
        if (input && !input.value && n > 0) input.value = n.toFixed(2);
      }
      updateManualTradePreviews();
      if (side === 'buy') updateManualBStopNotice();
    }
    function stepManualLimit(side, delta) {
      const input = document.getElementById(side === 'sell' ? 'manualSellLimitPrice' : side === 'short' ? 'manualShortLimitPrice' : 'manualBuyLimitPrice');
      const base = Number(input?.value || 0);
      const next = Math.max(0, base + Number(delta || 0));
      if (input) input.value = next.toFixed(2);
      markManualLimitEdited(side);
      updateManualTradePreviews();
      if (side === 'buy') updateManualBStopNotice();
    }
    function markManualLimitEdited(side) {
      const input = document.getElementById(side === 'sell' ? 'manualSellLimitPrice' : side === 'short' ? 'manualShortLimitPrice' : 'manualBuyLimitPrice');
      if (!input) return;
      input.dataset.userEdited = input.value && input.value !== input.dataset.autoPrice ? '1' : '0';
    }
    function manualOrderPayload(side, execute=false) {
      const symbol = (document.getElementById('manualBuySymbol')?.value || '').trim().toUpperCase();
      if (!symbol) {
        alert('先输入股票代码');
        return null;
      }
      const type = document.getElementById(side === 'sell' ? 'manualSellOrderType' : side === 'short' ? 'manualShortOrderType' : 'manualBuyOrderType')?.value || 'limit';
      return {
        symbol,
        side,
        execute,
        order_type: type,
        pool: document.getElementById('manualBuyPool')?.value || 'C',
        size: document.getElementById(side === 'sell' ? 'manualSellSize' : side === 'short' ? 'manualShortSize' : 'manualBuySize')?.value || '1/4',
        limit_price: type === 'limit' ? (document.getElementById(side === 'sell' ? 'manualSellLimitPrice' : side === 'short' ? 'manualShortLimitPrice' : 'manualBuyLimitPrice')?.value || '') : '',
        client_last: latestManualQuote?.last || latestManualQuote?.snapshot_last || '',
        client_bid: latestManualQuote?.bid || '',
        client_ask: latestManualQuote?.ask || ''
      };
    }
    function manualOrderText(p) {
      const sideText = p.side === 'buy' ? '买入' : p.side === 'short' ? '卖空' : '卖出';
      const typeText = p.order_type === 'market'
        ? `市价（按当前价 ${money(p.price)} 估算，最终以成交回报为准）`
        : `实时价限价 ${money(p.price)}`;
      const basis = p.side === 'buy' || p.side === 'short'
        ? `${p.pool} 资金池可用 ${money(p.available)} 的 ${p.size}`
        : `当前持仓 ${Number(p.held_qty || 0).toFixed(4)} 股的 ${p.size === 'full' ? '全仓' : p.size}`;
      const stopLine = p.protection_recorded
        ? `\n股票归类：${p.pool} 类型\n保护规则：${p.stop_loss_rule || '--'}，${money(p.stop_loss_price)}\n执行方式：${p.auto_stop_loss ? '自动执行' : '记录保护线，不自动卖出'}`
        : '';
      const qty = Number(p.qty || 0);
      const qtyText = Number.isInteger(qty) ? String(qty) : qty.toFixed(1);
      return `${sideText} ${p.symbol}\n订单类型：${typeText}\n估算数量：${qtyText} 股\n估算金额：${money(p.notional)}\n计算依据：${basis}\n页面实时价：${Number(p.last || 0) > 0 ? money(p.last) : '--'}\nBid / Ask：${Number(p.bid || 0) > 0 ? money(p.bid) : '--'} / ${Number(p.ask || 0) > 0 ? money(p.ask) : '--'}${stopLine}\n\n确认执行后会提交 Alpaca 订单。`;
    }
    async function previewManualStockOrder(side) {
      const req = manualOrderPayload(side, false);
      if (!req) return;
      const result = await postJson('/api/manual_stock_order', req);
      if (!result.ok) { alert(result.error || '预览失败'); return; }
      manualOrderPreview = req;
      document.getElementById('manualOrderTitle').textContent = `${side === 'buy' ? '买入' : side === 'short' ? '卖空' : '卖出'}预览`;
      document.getElementById('manualOrderBody').textContent = manualOrderText(result);
      document.getElementById('manualOrderExecuteBtn').textContent = `确认执行${side === 'buy' ? '买入' : side === 'short' ? '卖空' : '卖出'}`;
      document.getElementById('manualOrderModal').classList.add('show');
    }
    function closeManualOrderModal() {
      document.getElementById('manualOrderModal').classList.remove('show');
    }
    async function executeManualStockOrder() {
      if (!manualOrderPreview) return;
      const sideText = manualOrderPreview.side === 'buy' ? '买入' : manualOrderPreview.side === 'short' ? '卖空' : '卖出';
      if (!confirm(`确认执行${sideText} ${manualOrderPreview.symbol}？`)) return;
      const result = await postJson('/api/manual_stock_order', {...manualOrderPreview, execute:true});
      if (!result.ok) { alert(result.error || '下单失败'); return; }
      const stopText = result.protection_recorded
        ? `\n股票类型：${result.recorded_stock_type || result.pool}\n保护线 ${result.stop_loss_added ? '已写入' : '未写入'}：${money(result.stop_loss_price)}\n${result.stop_loss_note || ''}`
        : '';
      alert(`${result.message || '订单已提交'}\n订单 ${result.order_id || '--'}\n状态 ${result.status || '--'}${stopText}`);
      closeManualOrderModal();
      await loadTradeRecords();
      await loadAll();
    }
    function isMobileView() {
      return window.matchMedia('(max-width: 760px)').matches;
    }
    function toggleMobilePanel(id) {
      const panel = document.getElementById(id);
      if (!panel) return;
      panel.classList.toggle('mobile-open');
      if (id === 'chartPanel' && panel.classList.contains('mobile-open')) {
        setTimeout(() => loadCurve(currentPeriod), 50);
      }
    }
    function parseDateOnly(s) {
      if (!s) return null;
      const [y,m,d] = String(s).slice(0,10).split('-').map(Number);
      return new Date(y, m - 1, d);
    }
    function dayDiff(a,b) { return Math.round((b-a)/86400000); }
    function mmdd(d) { return `${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`; }
    function axisMoney(v) { return `$${Number(v || 0).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2})}`; }
    function drawChart(curve) {
        const canvas = document.getElementById('equityChart');
        const ctx = canvas.getContext('2d');
        const rect = canvas.getBoundingClientRect();
        
        if (rect.width > 0 && rect.height > 0) {
        canvas.width = Math.floor(rect.width * window.devicePixelRatio);
        canvas.height = Math.floor(rect.height * window.devicePixelRatio);
        ctx.setTransform(window.devicePixelRatio, 0, 0, window.devicePixelRatio, 0, 0);
        }
        const w = rect.width || 760;
        const h = rect.height || 260;
        
        const padLeft = 78;
        const padRight = 34;
        const padTop = 42;
        const padBottom = 38;
        
        ctx.clearRect(0, 0, w, h);
        ctx.fillStyle = '#fff';
        ctx.fillRect(0, 0, w, h);
        
        const rows = curve.rows || [];
        
        const points = rows.map(r => {
        const rawDate = r.snapshot_date || r.created_at || r.date;
        const equity = Number(r.equity || r.portfolio_value || 0);
        return {
        d: parseDateOnly(rawDate),
        t: String(rawDate || ''),
        y: equity
        };
        }).filter(p => p.d && p.y > 0);
        
        if (points.length === 0) {
        ctx.fillStyle = '#667085';
        ctx.font = '14px system-ui';
        ctx.textAlign = 'center';
        ctx.fillText('暂无收益曲线数据，等待 dashboard_bot 记录账户快照', w / 2, h / 2);
        return;
        }
        
        const startDate = points[0].d;
        const endDate = points[points.length - 1].d;
        const totalDays = Math.max(1, dayDiff(startDate, endDate));
        
        const ys = points.map(p => p.y);
        const min = Math.min(...ys);
        const max = Math.max(...ys);
        const span = Math.max(1, max - min);
        
        // 横向网格线 + 左侧金额刻度
        ctx.strokeStyle = '#d7dde5';
        ctx.lineWidth = 1;
        ctx.fillStyle = '#667085';
        ctx.font = '11px system-ui';
        ctx.textAlign = 'right';
        
        for (let i = 0; i < 4; i++) {
        const y = padTop + i * (h - padTop - padBottom) / 3;
        const value = max - i * span / 3;
        
        ctx.beginPath();
        ctx.moveTo(padLeft, y);
        ctx.lineTo(w - padRight, y);
        ctx.stroke();
        
        ctx.fillText(axisMoney(value), padLeft - 8, y + 4);
        }
        
        // 折线
        ctx.beginPath();
        
        points.forEach((p, i) => {
        const offset = dayDiff(startDate, p.d);
        const x = padLeft + offset * (w - padLeft - padRight) / totalDays;
        const y = h - padBottom - ((p.y - min) / span) * (h - padTop - padBottom);
        
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
        });
        
        const first = points[0].y;
        const last = points[points.length - 1].y;
        const diff = last - first;
        const diffPct = first > 0 ? diff / first * 100 : 0;
        
        ctx.strokeStyle = diff >= 0 ? '#15936a' : '#c62828';
        ctx.lineWidth = 3;
        ctx.stroke();
        
        // 顶部收益文字
        ctx.fillStyle = diff >= 0 ? '#15936a' : '#c62828';
        ctx.font = '700 15px system-ui';
        ctx.textAlign = 'left';
        
        const sign = diff >= 0 ? '+' : '';
        ctx.fillText(
        `${money(last)}  ${sign}${money(diff)} (${sign}${diffPct.toFixed(2)}%)`,
        padLeft,
        24
        );
        
        // 底部日期
        ctx.fillStyle = '#667085';
        ctx.font = '11px system-ui';
        ctx.textAlign = 'center';
        
        ctx.fillText(mmdd(startDate), padLeft, h - 10);
        ctx.fillText(mmdd(endDate), w - padRight, h - 10);
        
        // 中间日期，数据跨度足够时显示
        if (totalDays >= 6) {
        const midDate = new Date(startDate.getTime() + (endDate.getTime() - startDate.getTime()) / 2);
        ctx.fillText(mmdd(midDate), w / 2, h - 10);
        }
        
        // 数据点太少时提示
	        if (points.length === 1) {
	        ctx.fillStyle = '#667085';
	        ctx.font = '12px system-ui';
	        ctx.textAlign = 'center';
	        ctx.fillText('当前只有 1 个账户快照，等待更多数据形成曲线', w / 2, h / 2 + 22);
	        }
	        }
    function renderTodayPnl(curve) {
      const rows = curve.rows || [];
      const today = new Date();
      const todayKey = `${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,'0')}-${String(today.getDate()).padStart(2,'0')}`;
      const todayRows = rows.filter(r => String(r.snapshot_date || r.created_at || '').slice(0,10) === todayKey);
      const source = todayRows.length >= 2 ? todayRows : rows.slice(-2);
      const el = document.getElementById('todayPnl');
      if (!source.length) { el.textContent = '今日收益 --'; el.className = 'today-pnl'; return; }
      const first = Number(source[0].equity || source[0].portfolio_value || 0);
      const last = Number(source[source.length-1].equity || source[source.length-1].portfolio_value || 0);
      const diff = last - first;
      const diffPct = first > 0 ? diff / first * 100 : 0;
      el.textContent = `今日收益 ${diff >= 0 ? '+' : ''}${money(diff)} (${diffPct >= 0 ? '+' : ''}${diffPct.toFixed(2)}%)`;
      el.className = `today-pnl ${diff < 0 ? 'neg' : 'pos'}`;
    }
    function renderTradeRecords(payload) {
      const rows = ((payload && payload.ok ? payload.rows : []) || []).filter(r => r.source !== 'bot_lifecycle_events');
      latestTradeRecords = rows;
      const countEl = document.getElementById('tradeRecordsCount');
      const tableEl = document.getElementById('tradeRecords');
      if (!countEl || !tableEl) return;
      countEl.textContent = `${rows.length} 条`;
      if (!rows.length) {
        tableEl.innerHTML = `<tbody><tr><td class="small-muted" style="padding:18px;text-align:center;">近30天暂无真实买卖记录</td></tr></tbody>`;
        return;
      }
      const widths = [92, 82, 90, 160, 92, 108, 128, 248];
      const colgroup = `<colgroup>${widths.map(w => `<col style="width:${w}px">`).join('')}</colgroup>`;
      tableEl.innerHTML = `${colgroup}<thead><tr>${['时间','方向','策略','代码','数量','价格','状态','说明'].map(h=>`<th>${h}</th>`).join('')}</tr></thead><tbody>` +
        rows.map(r => {
          const side = String(r.side || '').toUpperCase();
          const sideClass = side === 'SELL' || side === 'SHORT' || side === 'STOP' ? 'sell' : 'buy';
          const sideLabel = side === 'SELL' ? '卖出' : side === 'SHORT' ? '卖空' : '买入';
          const eventText = String(r.event_time || '');
          const timeText = eventText.length >= 16 ? eventText.slice(5,16) : eventText;
          const price = Number(r.price || 0);
          const priceText = price > 0 ? money(price) : '--';
          return `<tr><td>${timeText}</td><td><span class="side-pill ${sideClass}">${sideLabel}</span></td><td>${r.strategy_group || '--'}</td><td><b>${r.symbol || '--'}</b></td><td>${Number(r.qty || 0).toFixed(2)}</td><td>${priceText}</td><td>${r.status || '--'}</td><td>${r.note || ''}</td></tr>`;
        }).join('') + `</tbody>`;
    }
    async function loadTradeRecords() {
      renderTradeRecords(await api('/api/trade_records'));
    }
    async function loadCurve(period=currentPeriod) {
      currentPeriod = period;
      document.querySelectorAll('.tab').forEach(b => b.classList.toggle('active', b.dataset.period === period));
      const [curve, trades] = await Promise.all([
        api(`/api/equity_curve?period=${period}`),
        api('/api/trade_records')
      ]);
      latestEquityCurve = curve;
      drawChart(curve);
      renderTodayPnl(curve);
      renderTradeRecords(trades);
    }
    function renderDTactical(payload, options={}) {
      const underlyings = payload.option_underlyings || [];
      const modes = payload.option_modes || [];
      const candidates = payload.intraday_candidates || [];
      renderDOptionCapital(payload.option_capital || null);
      const hadOptionSymbol = !!dOptionSymbol;
      if (!dOptionSymbol && underlyings.length) dOptionSymbol = underlyings[0].symbol;
      document.getElementById('dIntradayCount').textContent = `${candidates.length} 条`;
      document.getElementById('dIntradayTable').innerHTML = candidates.length
        ? `<thead><tr><th>日期</th><th>代码</th><th>分数</th><th>确认</th><th>原因</th></tr></thead><tbody>${candidates.map(r => `<tr><td>${r.snapshot_date || ''}</td><td><b>${r.symbol}</b></td><td>${Number(r.score || 0).toFixed(1)}</td><td>${Number(r.confirmed || 0) ? '是' : '否'}</td><td>${r.reason || ''}</td></tr>`).join('')}</tbody>`
        : `<tbody><tr><td class="small-muted" style="padding:14px;text-align:center;">暂无 D 日内股票候选</td></tr></tbody>`;
      document.getElementById('dOptionSymbols').innerHTML = underlyings.map(r => `<button class="d-symbol-btn ${r.symbol === dOptionSymbol ? 'active' : ''}" onclick="selectDOptionSymbol('${r.symbol}')">${r.symbol}</button>`).join('');
      document.getElementById('dOptionModes').innerHTML = modes.map(r => `<button class="d-mode-btn ${r.mode === dOptionMode ? 'active' : ''}" title="${r.desc || ''}" onclick="selectDOptionMode('${r.mode}')">${r.label}</button>`).join('');
      renderManualTradeTabs();
      renderDSection();
      if (manualTradeTab === 'option' && dOptionSymbol) loadDOptionPreview({center: options.centerOptionPreview || !hadOptionSymbol});
    }
    function renderDOptionCapital(capital) {
      if (!capital) return;
      latestDOptionCapital = capital;
      const available = Number(capital.effective_available || 0);
      const brokerBp = Number(capital.options_buying_power || 0);
      const availableEl = document.getElementById('dOptionAvailable');
      const brokerEl = document.getElementById('dOptionBrokerBp');
      if (availableEl) availableEl.textContent = capital.ok ? money(available) : '--';
      if (brokerEl) brokerEl.textContent = brokerBp > 0 ? money(brokerBp) : '未单独返回';
      updateDOptionSelectionSummary();
    }
    function dOptionSelectionRisk() {
      return selectedDCombo ? Number(selectedDCombo.row?.max_loss_per_spread || 0) * Number(dOptionQty || 1) : 0;
    }
    function updateDOptionSelectionSummary() {
      const risk = dOptionSelectionRisk();
      const available = Number(latestDOptionCapital?.effective_available || 0);
      document.querySelectorAll('.d-confirm-btn').forEach(button => {
        const blocked = !selectedDCombo || risk <= 0 || risk > available + 0.01;
        button.disabled = blocked;
        button.title = blocked && selectedDCombo
          ? `D 资金不足：需要 ${money(risk)}，可用 ${money(available)}`
          : `占用 D 资金 ${money(risk)}`;
      });
    }
    function renderDOptionPreview(payload) {
      renderDOptionCapital(payload.option_capital || null);
      document.getElementById('dOptionMeta').textContent = `${payload.symbol} ${money(payload.price)} · ${payload.price_source || ''}`;
      const rows = payload.previews || [];
      document.getElementById('dOptionPreview').innerHTML = rows.map((p, idx) => {
        const legLine = leg => `<div class="d-leg-line"><span>${leg.label} ${Number(leg.strike).toFixed(2)}</span><span><span class="d-option-code">${leg.option_symbol || ''}</span><br><span class="d-leg-quote">mid ${money(leg.mid)} · bid ${money(leg.bid)} / ask ${money(leg.ask)}</span></span></div>`;
        const currentMarker = `<div class="d-current-marker" data-current-marker="1"><span>当前价 ${money(payload.price)}</span></div>`;
        let markerInserted = false;
        const optionRows = (p.option_rows || []).map((o, rowIdx) => {
          const key = `${payload.symbol}|${payload.mode}|${p.expiry}|${rowIdx}`;
          const selected = selectedDCombo && selectedDCombo.key === key ? ' selected' : '';
          const packed = encodeURIComponent(JSON.stringify({key, symbol:payload.symbol, mode:payload.mode, expiry:p.expiry, row:o}));
          const marker = !markerInserted && o.side === 'below' ? (markerInserted = true, currentMarker) : '';
          return `${marker}<div class="d-option-row${selected}" onclick="selectDCombo('${packed}')"><div class="d-option-head"><span>${o.side === 'below' ? '下方' : '上方'} ${Number(o.strike).toFixed(2)} · 距现价 ${Number(o.distance).toFixed(2)} · 宽 ${Number(o.width || p.width || 0).toFixed(2)}</span><span class="d-option-price">${o.price_label} ${money(o.spread_mid)}</span></div><div class="d-note">买入限价 ${Number(o.alpaca_limit_price || 0).toFixed(2)} · 单组最大亏损 ${money(o.max_loss_per_spread)}</div>${legLine(o.buy)}${legLine(o.sell)}</div>`;
        }).join('');
        const scrollKey = `${payload.symbol}|${payload.mode}|${p.expiry}`;
        const scrollRows = optionRows ? `<div class="d-option-scroll" data-scroll-key="${scrollKey}">${optionRows}${markerInserted ? '' : currentMarker}</div>` : '';
        const legs = scrollRows || (p.legs || []).map(l => `<div class="d-leg"><span>${l.side} ${l.cp} ${Number(l.strike).toFixed(2)}</span><span>${l.option_symbol || ''}</span></div>`).join('');
        const priceLine = p.error
          ? `<div class="d-error">${p.error}</div>`
          : modeHelpHtml(payload.mode);
        return `<div class="d-preview-card"><div class="d-preview-top"><div><div class="d-preview-title">${idx === 0 ? '下周五' : '下下周五'} ${p.expiry}</div><div class="small-muted">${p.mode} · width ${Number(payload.width || p.width || 0).toFixed(2)}</div></div><div class="d-preview-actions"><label class="d-qty-control">× <input type="number" min="1" max="99" step="1" value="${dOptionQty}" onchange="changeDOptionQty(this.value)" oninput="changeDOptionQty(this.value)"></label><button class="d-confirm-btn" onclick="confirmDOptionBuy()" disabled>确认买入</button></div></div>${priceLine}${legs}</div>`;
      }).join('') || `<div class="d-note">暂无预览</div>`;
      updateDOptionSelectionSummary();
      if (dOptionScrollMode === 'center') {
        dOptionScrollMode = 'preserve';
        setTimeout(centerDOptionScrolls, 0);
      }
    }
    function captureDOptionScrolls() {
      const positions = {};
      document.querySelectorAll('.d-option-scroll').forEach(scroller => {
        const key = scroller.dataset.scrollKey || '';
        if (key) positions[key] = scroller.scrollTop;
      });
      return positions;
    }
    function restoreDOptionScrolls(positions) {
      document.querySelectorAll('.d-option-scroll').forEach(scroller => {
        const key = scroller.dataset.scrollKey || '';
        if (key && positions[key] !== undefined) scroller.scrollTop = positions[key];
      });
    }
    function centerDOptionScrolls() {
      document.querySelectorAll('.d-option-scroll').forEach(scroller => {
        const marker = scroller.querySelector('[data-current-marker="1"]');
        if (!marker) return;
        scroller.scrollTop = marker.offsetTop - (scroller.clientHeight / 2) + (marker.offsetHeight / 2);
      });
    }
    function modeHelpHtml(mode) {
      const map = {
        BULL_CALL: {
          title:'看涨进攻：买低行权价 Call，卖高行权价 Call',
          earn:'标的上涨时赚钱。价格越接近或突破卖出的高行权价，组合价值越高。',
          cost:'这是借方价差，开仓要付出净成本。买入限价就是你愿意为整组价差支付的最高净价。',
          maxProfit:'最大收益约为行权价宽度 - 净成本。比如宽度 $1、成本 $0.35，最大收益约 $65/组。',
          maxLoss:'最大亏损就是净成本 x100。只要到期两条腿都归零，损失就是这笔成本。',
          trigger:'适合你判断短期继续上涨时点选。后续机器人可按盈利比例止盈，跌到止损比例自动平仓。'
        },
        BEAR_PUT: {
          title:'看跌进攻：买高行权价 Put，卖低行权价 Put',
          earn:'标的下跌时赚钱。价格越接近或跌破卖出的低行权价，组合价值越高。',
          cost:'这是借方价差，开仓要付出净成本。买入限价就是整组 Put 价差的最高支付价格。',
          maxProfit:'最大收益约为行权价宽度 - 净成本。跌幅足够大时收益接近上限。',
          maxLoss:'最大亏损就是净成本 x100。判断错方向、到期价差归零时损失这笔成本。',
          trigger:'适合你判断短期继续下跌时点选。后续机器人按盈利目标或止损比例处理。'
        },
        BULL_PUT: {
          title:'看涨收租：买低行权价 Put，卖高行权价 Put',
          earn:'标的不跌破卖出的高行权价时赚钱。横盘、微涨、小跌都可能盈利。',
          cost:'这是信用价差，开仓是收取权利金。买入限价为负数，代表向市场收钱。',
          maxProfit:'最大收益就是收到的权利金 x100。只要到期价格高于卖出 Put，通常可保留大部分权利金。',
          maxLoss:'最大亏损约为行权价宽度 - 收到权利金，再乘以100。跌破保护腿时接近最大亏损。',
          trigger:'适合你判断不会明显下跌时点选。后续机器人可在权利金回吐到目标时止盈，亏损扩大时止损。'
        },
        BEAR_CALL: {
          title:'看跌收租：买高行权价 Call，卖低行权价 Call',
          earn:'标的不突破卖出的低行权价时赚钱。横盘、微跌、小涨都可能盈利。',
          cost:'这是信用价差，开仓是收取权利金。买入限价为负数，代表向市场收钱。',
          maxProfit:'最大收益就是收到的权利金 x100。到期价格低于卖出 Call 时通常收益最好。',
          maxLoss:'最大亏损约为行权价宽度 - 收到权利金，再乘以100。向上突破保护腿时接近最大亏损。',
          trigger:'适合你判断不会明显上涨时点选。后续机器人按收租止盈、亏损扩大止损。'
        }
      };
      const h = map[mode] || map.BULL_CALL;
      return `<details class="d-mode-help"><summary><div class="d-help-title"><span>${h.title}</span><span class="small-muted">先点选组合，再确认买入</span></div></summary><div class="d-help-grid">${[
        ['怎么赚钱', h.earn],
        ['成本/收款', h.cost],
        ['最大收益', h.maxProfit],
        ['最大亏损', h.maxLoss],
        ['什么时候触发', h.trigger],
        ['当前列表怎么看', '每一块是一组可买价差。绿色金额是预估成本或预估收款；下面两行分别是买入保护腿和卖出腿的实时 bid/ask/mid。']
      ].map(([k,v]) => `<div class="d-help-item"><b>${k}</b>${v}</div>`).join('')}</div></details>`;
    }
    function selectDCombo(packed) {
      selectedDCombo = JSON.parse(decodeURIComponent(packed));
      document.querySelectorAll('.d-option-row').forEach(el => el.classList.remove('selected'));
      const scrollPositions = captureDOptionScrolls();
      renderDOptionPreview(window.latestDOptionPreview || {symbol:dOptionSymbol, mode:dOptionMode, price:0, previews:[]});
      setTimeout(() => restoreDOptionScrolls(scrollPositions), 0);
    }
    function changeDOptionQty(value) {
      const qty = Math.max(1, Math.min(99, Math.floor(Number(value || 1) || 1)));
      dOptionQty = qty;
      document.querySelectorAll('.d-qty-control input').forEach(input => {
        if (Number(input.value || 0) !== qty) input.value = qty;
      });
      updateDOptionSelectionSummary();
    }
    async function confirmDOptionBuy() {
      if (!selectedDCombo) { alert('请先选择一组期权组合'); return; }
      const r = selectedDCombo.row;
      const qty = Math.max(1, Math.min(99, Math.floor(Number(dOptionQty || 1) || 1)));
      selectedDCombo.qty = qty;
      const totalRisk = Number(r.max_loss_per_spread || 0) * qty;
      const available = Number(latestDOptionCapital?.effective_available || 0);
      if (totalRisk > available + 0.01) {
        alert(`D 资金不足\n需要 ${money(totalRisk)}\n可用 ${money(available)}`);
        return;
      }
      const msg = `确认使用 D 资金买入 ${qty} 组 ${selectedDCombo.symbol} ${selectedDCombo.mode} ${selectedDCombo.expiry}？\n限价 ${Number(r.alpaca_limit_price || 0).toFixed(2)}\n单组最大亏损 ${money(r.max_loss_per_spread)}\n占用 D 资金 ${money(totalRisk)}\n买入后由 Q 机器人监督卖出`;
      if (!confirm(msg)) return;
      const result = await postJson('/api/d_option_buy', selectedDCombo);
      if (!result.ok) { alert(result.error || '期权买入失败'); return; }
      alert(`期权买入已提交\n张数 ${result.qty || qty}\n占用 D 资金 ${money(result.max_loss || totalRisk)}\nD 剩余 ${money(result.d_available_after)}\n订单 ${result.order_id || '--'}\n状态 ${result.status || '--'}\n后续由 Q 机器人监督卖出`);
      await loadDTactical();
    }
    async function loadDOptionPreview(options={}) {
      const el = document.getElementById('dOptionPreview');
      const hasContent = el.children.length > 0;
      const scrollPositions = captureDOptionScrolls();
      const shouldCenterScroll = options.center || !hasContent;
      dOptionScrollMode = shouldCenterScroll ? 'center' : 'preserve';
      if (hasContent) el.classList.add('refreshing');
      else el.innerHTML = `<div class="d-note">正在读取期权链...</div>`;
      try {
        const payload = await api(`/api/d_option_preview?symbol=${encodeURIComponent(dOptionSymbol)}&mode=${encodeURIComponent(dOptionMode)}&width=${encodeURIComponent(dOptionWidth)}`);
        if (!payload.ok) throw new Error(payload.error || 'preview failed');
        window.latestDOptionPreview = payload;
        renderDOptionPreview(payload);
        if (!shouldCenterScroll) setTimeout(() => restoreDOptionScrolls(scrollPositions), 0);
      } catch (e) {
        if (!hasContent) el.innerHTML = `<div class="d-error">${e.message || e}</div>`;
        else document.getElementById('dOptionMeta').textContent = `刷新失败：${e.message || e}`;
      } finally {
        el.classList.remove('refreshing');
      }
    }
    function selectDOptionSymbol(symbol) {
      dOptionSymbol = symbol;
      selectedDCombo = null;
      loadDTactical({centerOptionPreview: true});
    }
    function selectDOptionMode(mode) {
      dOptionMode = mode;
      selectedDCombo = null;
      loadDTactical({centerOptionPreview: true});
    }
    function changeDOptionWidth(value) {
      const n = Number(value || 10);
      dOptionWidth = Math.max(1, n || 10);
      selectedDCombo = null;
      loadDOptionPreview({center: true});
    }
    async function loadDTactical(options={}) {
      const payload = await api('/api/d_tactical');
      if (payload.ok) renderDTactical(payload, options);
    }
    function isHoldingsFocus() {
      return document.body.classList.contains('holdings-focus');
    }
    function renderHoldings() {
      const forceTotalOnly = !isHoldingsFocus();
      if (forceTotalOnly) currentHolding = 'ALL';
      const holdingGroup = forceTotalOnly ? 'ALL' : (currentHolding === 'Q' ? 'D' : currentHolding);
      const isActiveHolding = (r) => {
        const status = String(r.status || '').toLowerCase();
        return Number(r.is_bought || 0) === 1
          || (Number(r.qty || 0) > 0 && status === 'open');
      };
      const rows = holdingGroup === 'ALL'
        ? latestHoldings.filter(isActiveHolding)
        : latestHoldings.filter(r => String(r.strategy_group || '').toUpperCase() === holdingGroup);
      document.querySelectorAll('.holding-tab').forEach(b => b.classList.toggle('active', b.dataset.holding === currentHolding));
      const colCount = 16;
      const blanks = forceTotalOnly ? '' : Array.from({length: Math.max(0, 10 - rows.length)}, () => `<tr>${Array.from({length: colCount}, (_, i) => `<td>${i === 0 ? '&nbsp;' : ''}</td>`).join('')}</tr>`).join('');
      document.getElementById('holdings').innerHTML = `<thead><tr>${['代码','策略组','状态','日涨跌','现价','触发价','数量','初始成本','均价','持仓市值','浮盈亏','浮盈亏%','已实现','持仓天数','更新时间','操作'].map(h=>`<th>${h}</th>`).join('')}</tr></thead><tbody>` +
        rows.map(r => {
          const day = Number(r.day_change_pct || 0);
          const status = String(r.status || '');
          const candidate = status.toLowerCase() === 'candidate';
          const dSelected = String(r.strategy_group || '').toUpperCase() === 'D' && Number(r.d_selected || 0) === 1;
          const dState = String(r.d_cycle_state || 'IDLE').toUpperCase();
          const statusHtml = dSelected
            ? `<span class="holding-status target" title="D 循环状态 ${esc(dState)}">执行标的</span>`
            : `<span class="holding-status ${holdingStatusClass(status)}">${holdingStatusLabel(status)}</span>`;
          const cAction = cCoreAction(r);
          const action = dSelected
            ? `<span class="small-muted">${esc(dState)}</span>`
            : cAction || (candidate && r.operation_id
            ? `<button class="pool-delete-btn" onclick="deleteStockPoolCandidate(${Number(r.operation_id)})">删</button>`
            : '');
          return `<tr class="${dSelected ? 'd-execution-row' : ''}"><td><button class="symbol-fill-btn" onclick="fillManualSymbol('${r.symbol}')">${r.symbol}</button></td><td>${r.strategy_group}</td><td>${statusHtml}</td><td class="${cls(day)}">${pct(day)}</td><td>${maybeMoney(r.current_price)}</td><td>${maybeMoney(r.trigger_price)}</td><td>${candidate ? '--' : Number(r.qty||0).toFixed(4)}</td><td>${maybeMoney(r.initial_entry_price || r.avg_entry_price)}</td><td>${candidate ? '--' : money(r.avg_entry_price)}</td><td>${candidate ? '--' : money(r.market_value)}</td><td class="${cls(r.unrealized_pnl)}">${candidate ? '--' : money(r.unrealized_pnl)}</td><td class="${cls(r.unrealized_pnl_pct)}">${candidate ? '--' : pct(r.unrealized_pnl_pct)}</td><td class="${cls(r.realized_pnl)}">${candidate ? '--' : money(r.realized_pnl)}</td><td>${candidate ? '--' : (r.holding_days || 0)}</td><td>${r.last_update_time || ''}</td><td>${action}</td></tr>`;
        }).join('') +
        blanks + `</tbody>`;
    }
    async function setCCore(symbol, enable, operationId=0) {
      const msg = enable
        ? `确认把 ${symbol} 设为唯一 C 做T核心？\\n其他 C 做T标记会自动关闭，同代码 B 候选买入也会禁用。`
        : `确认关闭 ${symbol} 的 C 做T？\\n不会删除观察记录，也不会卖出持仓。`;
      if (!confirm(msg)) return;
      const result = await postJson('/api/c_core/set', {symbol, enable, operation_id: operationId});
      if (!result.ok) {
        alert(result.error || 'C 做T核心更新失败');
        return;
      }
      await loadAll();
    }
    async function deleteStockPoolCandidate(operationId) {
      const row = latestHoldings.find(item => Number(item.operation_id || 0) === Number(operationId || 0));
      const symbol = row?.symbol || '这只股票';
      if (!confirm(`确认从入选池删除 ${symbol}？\\n只会删除 stock_operations 里的候选记录。`)) return;
      const result = await postJson('/api/stock_pool/delete', {operation_id: operationId});
      if (!result.ok) {
        alert(result.error || '删除失败');
        return;
      }
      await loadAll();
    }
    function isDSectionHolding(value=currentHolding) {
      return value === 'D' || value === 'Q';
    }
    function renderDSection() {
      const intraday = dSection === 'intraday';
      const intradayPanel = document.getElementById('dIntradayPanel');
      if (intradayPanel) intradayPanel.hidden = !intraday;
    }
    function renderManualTradeTabs() {
      const stock = manualTradeTab !== 'option';
      document.getElementById('manualStockPanel')?.toggleAttribute('hidden', !stock);
      document.getElementById('manualOptionPanel')?.toggleAttribute('hidden', stock);
      document.getElementById('manualStockTradeTab')?.classList.toggle('active', stock);
      document.getElementById('manualOptionTradeTab')?.classList.toggle('active', !stock);
      const trade = document.body.classList.contains('trade-focus');
      document.getElementById('stockTradeFocusBtn')?.classList.toggle('active', trade && stock);
      document.getElementById('optionTradeFocusBtn')?.classList.toggle('active', trade && !stock);
    }
    function setManualTradeTab(tab) {
      manualTradeTab = tab === 'option' ? 'option' : 'stock';
      renderManualTradeTabs();
      if (manualTradeTab === 'option') {
        if (!dOptionSymbol) {
          loadDTactical({centerOptionPreview: true});
        } else {
          loadDOptionPreview({center: true});
        }
      }
    }
    function scheduleStatusLabel(status, task) {
      if (status === 'ok') return task.running ? '运行中' : '正常';
      if (status === 'warn') return '需检查';
      if (status === 'off') return '未启用';
      return '未知';
    }
    function scheduleDateText(task) {
      const raw = String(task.latest_date || '').trim();
      if (!raw) return '最近数据 --';
      const dateText = raw.length > 10 ? raw.slice(0, 19).replace('T', ' ') : raw;
      const rows = Number(task.latest_rows || 0);
      return rows > 0 ? `最近数据 ${dateText} · ${rows} 行` : `最近心跳 ${dateText}`;
    }
    function renderSchedules(payload) {
      const grid = document.getElementById('scheduleGrid');
      const meta = document.getElementById('scheduleMeta');
      if (!grid) return;
      if (!payload?.ok) {
        grid.innerHTML = `<div class="schedule-empty">${esc(payload?.error || '定时任务读取失败')}</div>`;
        if (meta) meta.textContent = '读取失败';
        return;
      }
      const tasks = [...(payload.tasks || []), ...(payload.bot_tasks || [])];
      if (meta) meta.textContent = `更新时间 ${String(payload.generated_at || '--').slice(0, 19).replace('T', ' ')}`;
      if (!tasks.length) {
        grid.innerHTML = '<div class="schedule-empty">暂无定时任务记录</div>';
        return;
      }
      grid.innerHTML = tasks.map(task => {
        const status = String(task.status || 'unknown');
        const clsName = status === 'warn' ? 'warn' : status === 'off' ? 'off' : '';
        const source = String(task.source || task.target || '--');
        const message = String(task.message || task.target || '');
        return `
          <div class="schedule-card ${clsName}">
            <div class="schedule-top">
              <div class="schedule-name" title="${esc(task.name || '')}">${esc(task.name || '--')}</div>
              <span class="schedule-pill">${esc(scheduleStatusLabel(status, task))}</span>
            </div>
            <div class="schedule-line" title="${esc(task.schedule || '')}">${esc(task.schedule || '--')}</div>
            <div class="schedule-line">${esc(scheduleDateText(task))}</div>
            <div class="schedule-sub" title="${esc(source)}">${esc(source)}</div>
            ${message ? `<div class="schedule-sub" title="${esc(message)}">${esc(message)}</div>` : ''}
          </div>
        `;
      }).join('');
    }
    async function loadSchedules() {
      const payload = await api('/api/schedules');
      renderSchedules(payload);
    }
    function bCfgPct(value) {
      const n = Number(value || 0);
      return `${n >= 0 ? '+' : ''}${(n * 100).toFixed(Math.abs(n) < 0.1 ? 1 : 0)}%`;
    }
    function bCfgMoney(value) {
      const n = Number(value || 0);
      return n > 0 ? money(n) : '--';
    }
    function bConfigRow(label, value, cls='') {
      return `<div class="b-config-row"><div class="b-config-label">${esc(label)}</div><div class="b-config-value ${cls}">${esc(value)}</div></div>`;
    }
    function renderStrategyBConfig(payload) {
      const grid = document.getElementById('strategyBConfigGrid');
      const meta = document.getElementById('strategyBConfigMeta');
      if (!grid) return;
      if (!payload?.ok) {
        grid.innerHTML = `<div class="schedule-empty">${esc(payload?.error || 'B 策略配置读取失败')}</div>`;
        if (meta) meta.textContent = '读取失败';
        return;
      }
      latestStrategyBConfig = payload.config || null;
      updateManualBStopNotice();
      const cfg = payload.config || {};
      const candidate = cfg.candidate || {};
      const score = cfg.score || {};
      const market = cfg.market || {};
      const buy = cfg.buy || {};
      const sell = cfg.sell || {};
      if (meta) {
        meta.textContent = `Alpaca ${buy.price_source || '--'} · 入选当天量 > ${compactNumber(candidate.min_day_volume || 0)} · 买前昨量 > ${compactNumber(buy.min_prev_day_volume || 0)}`;
      }
      const peakRules = (sell.peak_giveback_rules || [])
        .map(r => `最高涨 ${bCfgPct(r.min_peak_gain_pct)} / 回撤 ${(Number(r.giveback_pct || 0) * 100).toFixed(1)}%`)
        .join('；');
      const stageRules = (sell.stage_sell_rules || [])
        .map(r => `${bCfgPct(r.profit_pct)} 卖 ${(Number(r.sell_ratio || 0) * 100).toFixed(0)}%`)
        .join('；');
      const peakRuleRows = (sell.peak_giveback_rules || [])
        .map((r, i) => bConfigRow(`高点回撤 ${i + 1}`, `最高涨 ${bCfgPct(r.min_peak_gain_pct)} / 回撤 ${(Number(r.giveback_pct || 0) * 100).toFixed(1)}%`, 'warn'))
        .join('');
      const stageRuleRows = (sell.stage_sell_rules || [])
        .map(r => bConfigRow(`止盈 Stage ${Number(r.stage || 0)}`, `盈利 ${bCfgPct(r.profit_pct)} 卖出 ${(Number(r.sell_ratio || 0) * 100).toFixed(0)}%`, 'good'))
        .join('');
      const flashWaitRows = (sell.flash_wait_rules || [])
        .map(r => bConfigRow(`闪崩等待 ${bCfgPct(r.min_profit_pct)}`, `跌破止损后等待 ${Number(r.wait_minutes || 0)} 分钟`))
        .join('');
      grid.innerHTML = `
        <div class="b-config-card">
          <h4>入选池条件</h4>
          <div class="b-config-list">
            ${bConfigRow('数据流向', candidate.source || 'stock_prices_pool -> stock_operations')}
            ${bConfigRow('涨幅范围', `${bCfgPct(candidate.min_gain_pct)} 到 ${bCfgPct(candidate.max_gain_pct)}`, 'good')}
            ${bConfigRow('最低股价', bCfgMoney(candidate.min_price))}
            ${bConfigRow('当天成交量', `> ${compactNumber(candidate.min_day_volume || 0)}`, 'good')}
            ${bConfigRow('均量过滤', `> ${compactNumber(candidate.min_avg_volume || 0)} / ${Number(candidate.avg_volume_days || 0)}日`)}
            ${bConfigRow('最低成交额', bCfgMoney(candidate.min_dollar_volume))}
            ${bConfigRow('连涨天数', `${Number(candidate.min_up_streak || 0)}-${Number(candidate.max_up_streak || 0)} 天`)}
            ${bConfigRow('收盘位置', `>= ${(Number(candidate.min_close_position || 0) * 100).toFixed(0)}%`)}
            ${bConfigRow('阳线要求', candidate.require_green_day ? '需要' : '不需要', candidate.require_green_day ? 'good' : '')}
            ${bConfigRow('保留窗口', `最近 ${Number(candidate.window_trading_days || 0)} 个交易日`)}
            ${bConfigRow('最多入选', Number(candidate.limit || 0) > 0 ? `${Number(candidate.limit || 0)} 只` : '不限')}
          </div>
        </div>
        <div class="b-config-card">
          <h4>买入确认</h4>
          <div class="b-config-list">
            ${bConfigRow('行情来源', buy.price_source || 'alpaca')}
            ${bConfigRow('买入时间', buy.window || '--')}
            ${bConfigRow('候选状态', 'can_buy=1 / 未买入 / 无冷却')}
            ${bConfigRow('触发价格', '实时价 > trigger_price')}
            ${bConfigRow('日涨幅区间', `${bCfgPct(buy.min_day_up_pct)} 到 ${bCfgPct(buy.max_buy_day_up_pct)}`)}
            ${bConfigRow('最高入场涨幅', bCfgPct(buy.max_entry_up_pct))}
            ${bConfigRow('低于开盘容忍', (Number(buy.max_below_open_pct || 0) * 100).toFixed(1) + '%')}
            ${bConfigRow('高点回落容忍', (Number(buy.max_pullback_from_high_pct || 0) * 100).toFixed(1) + '%')}
            ${bConfigRow('Top排名确认', `Top ${Number(score.top_n || 0)} / ${Number(score.lookback_minutes || 0)}分钟内 ${Number(score.confirmations || 0)} 次`)}
            ${bConfigRow('排名间隔', `${Number(score.interval_minutes || 0)} 分钟`)}
            ${bConfigRow('昨天成交量', `> ${compactNumber(buy.min_prev_day_volume || 0)}`, 'good')}
            ${bConfigRow('分时/昨量比例', '未启用此规则', 'warn')}
          </div>
        </div>
        <div class="b-config-card">
          <h4>市场环境过滤</h4>
          <div class="b-config-list">
            ${bConfigRow('开关', market.enabled ? '开启' : '关闭', market.enabled ? 'good' : 'warn')}
            ${bConfigRow('过滤方式', '风险评分闸门，不是简单看涨跌')}
            ${bConfigRow('最低评分', `>= ${Number(market.score_min || 0).toFixed(0)}`)}
            ${bConfigRow('趋势过滤', `QQQ 向下且日内弱于 ${(Number(market.max_downtrend_qqq_drop_pct || 0) * 100).toFixed(1)}% 时禁买`, 'warn')}
            ${bConfigRow('大跌过滤', `QQQ 当日 <= ${(Number(market.max_qqq_drop_pct || 0) * 100).toFixed(1)}% 扣分/禁买`, 'warn')}
            ${bConfigRow('VIX 警戒', `>= ${Number(market.warn_vix || 0).toFixed(1)} 扣分`)}
            ${bConfigRow('VIX 恐慌', `>= ${Number(market.max_vix || 0).toFixed(1)} 禁买`, 'warn')}
            ${bConfigRow('允许情况', '向上趋势优先；健康回踩不一刀切')}
          </div>
        </div>
        <div class="b-config-card">
          <h4>买入前检查</h4>
          <div class="b-config-list">
            ${bConfigRow('账户', 'B 策略资金池 / 原保证金账户')}
            ${bConfigRow('买入时间', buy.window || '--')}
            ${bConfigRow('基础状态', 'can_buy=1 / is_bought=0 / 无冷却')}
            ${bConfigRow('最多持仓', `${Number(buy.max_active_positions || 0)} 只`)}
            ${bConfigRow('最低股价', bCfgMoney(buy.min_price))}
            ${bConfigRow('最低购买力', `>= ${bCfgMoney(buy.min_buying_power || 0)}`, 'good')}
            ${bConfigRow('开盘购买力', bCfgMoney(buy.min_open_buying_power || 0))}
            ${bConfigRow('购买力使用', `${(Number(buy.bp_use_ratio || 1) * 100).toFixed(0)}% 可用购买力`)}
            ${bConfigRow('单笔目标', `${bCfgMoney(buy.target_notional_usd)} / 上限 ${bCfgMoney(buy.dynamic_max_trade_notional || buy.max_notional_usd)}`)}
            ${bConfigRow('额度不足处理', `可用购买力不足单笔目标时，使用全部可用购买力`, 'good')}
            ${bConfigRow('最低下单额', `实际下单额需 >= ${bCfgMoney(buy.dynamic_min_trade_notional || buy.min_buying_power || 0)}`)}
            ${bConfigRow('动态仓位', buy.dynamic_sizing ? '开启' : '关闭', buy.dynamic_sizing ? 'good' : '')}
            ${bConfigRow('额度计算', buy.notional_rule || '按目标额度和购买力取较小值')}
            ${bConfigRow('数量计算', 'floor(实际下单额 / 实时价)，数量必须 > 0')}
            ${bConfigRow('限价保护', '买入限价不超过最高买入价')}
            ${bConfigRow('下单前清理', '先取消同股票残留买单')}
            ${bConfigRow('成交确认', '无成交/拒单则写冷却，不记为已买入')}
            ${bConfigRow('成交落库', '成交后写 qty、成本、初始止损、Stage=0')}
            ${bConfigRow('冷却时间', `${Number(buy.cooldown_minutes || 0)} 分钟`)}
            ${bConfigRow('订单模式', buy.limit_mode || '--')}
            ${bConfigRow('盘中量能过滤', buy.require_intraday_volume ? '开启' : '关闭', buy.require_intraday_volume ? 'good' : 'warn')}
            ${buy.require_intraday_volume
              ? bConfigRow('盘中量能口径', `20日均量/RVOL；非昨量比例`)
              : bConfigRow('当前量能口径', '只用历史/昨日成交量，不用实时盘中量', 'warn')}
            ${buy.require_intraday_volume
              ? bConfigRow('最低盘中量', compactNumber(buy.min_realtime_volume || 0))
              : bConfigRow('入选量过滤', `入选当天量 > ${compactNumber(candidate.min_day_volume || 0)}`, 'good')}
            ${buy.require_intraday_volume
              ? bConfigRow('最低盘中成交额', bCfgMoney(buy.min_realtime_dollar_volume))
              : bConfigRow('买前量过滤', `昨天成交量 > ${compactNumber(buy.min_prev_day_volume || 0)}`, 'good')}
            ${buy.require_intraday_volume
              ? bConfigRow('RVOL阈值', `早 ${Number(buy.rvol_early || 0).toFixed(2)} / 中 ${Number(buy.rvol_mid || 0).toFixed(2)} / 后 ${Number(buy.rvol_late || 0).toFixed(2)}`)
              : bConfigRow('RVOL阈值', '未启用')}
          </div>
        </div>
        <div class="b-config-card">
          <h4>卖出配置</h4>
          <div class="b-config-list">
            ${bConfigRow('卖出订单', '实时价限价卖出，不走市价', 'good')}
            ${bConfigRow('初始止损', bCfgPct(sell.initial_stop_pct), 'warn')}
            ${bConfigRow('锁盈启动', `${bCfgPct(sell.trail_lock_start_pct)} 后止损抬到 ${bCfgPct(sell.trail_lock_profit_pct)}`, 'good')}
            ${bConfigRow('买入后宽限', `${Number(sell.initial_stop_grace_seconds || 0)} 秒`)}
            ${bConfigRow('灾难止损', bCfgPct(sell.catastrophic_stop_loss_pct), 'warn')}
            ${flashWaitRows || ''}
            ${peakRuleRows || bConfigRow('高点回撤保护', peakRules || '--')}
            ${stageRuleRows || bConfigRow('阶梯止盈', stageRules || '--')}
            ${bConfigRow('结构退出', `Stage ${Number(sell.structure_exit_stage || 0)} 后启用`)}
            ${bConfigRow('买入当天卖出', sell.same_day_sell_allowed ? '允许' : '禁止', sell.same_day_sell_allowed ? 'good' : '')}
            ${bConfigRow('加仓', sell.add_position ? '允许' : '不加仓')}
          </div>
        </div>
      `;
    }
    async function loadStrategyBConfig() {
      const payload = await api('/api/strategy_b_config');
      renderStrategyBConfig(payload);
    }
    function dGridSymbolRow(row={}) {
      const state = String(row.state || 'IDLE');
      const active = ['BUY_WORKING','SELL_WORKING','CLOSING'].includes(state);
      const detail = state === 'BUY_WORKING'
        ? `买单中 · ${money(row.buy_limit || 0)}`
        : state === 'SELL_WORKING' || state === 'CLOSING'
          ? `${state === 'CLOSING' ? '收盘处理中' : '卖单中'} · ${Number(row.buy_filled_qty || 0).toFixed(0)}股 · ${money(row.sell_limit || 0)}`
          : state === 'COOLDOWN'
            ? `冷却中 · 本轮毛收益 ${money(row.realized_pnl || 0)}`
            : row.last_error ? `${state} · ${row.last_error}` : state;
      return `<div class="d-grid-symbol-row" data-d-grid-symbol>
        <label class="d-grid-symbol-switch"><input type="checkbox" data-field="enabled" ${row.enabled ? 'checked' : ''}>启用</label>
        <div class="d-grid-field"><label>股票代码</label><input data-field="symbol" value="${esc(row.symbol || '')}" placeholder="例如 SMR"></div>
        <div class="d-grid-field"><label>单轮资金 $</label><input data-field="lot_notional" type="number" min="1" step="1" value="${Number(row.lot_notional || 250)}"></div>
        <input data-field="entry_offset" type="hidden" value="0.03">
        <input data-field="profit_offset" type="hidden" value="0.06">
        <input data-field="max_spread" type="hidden" value="999">
        <div class="d-grid-symbol-state ${active ? 'active' : ''}">${esc(detail)}<br>完成轮次 ${Number(row.cycle_no || 0)}</div>
        <button class="d-grid-remove" title="移除股票" onclick="removeDGridSymbol(this)" ${active ? 'disabled' : ''}>×</button>
      </div>`;
    }
    function renderDGridConfig(payload) {
      const runtime = document.getElementById('dGridRuntime');
      const symbols = document.getElementById('dGridSymbols');
      const meta = document.getElementById('dGridConfigMeta');
      if (!runtime || !symbols) return;
      if (!payload?.ok) {
        symbols.innerHTML = `<div class="schedule-empty">${esc(payload?.error || 'D 策略配置读取失败')}</div>`;
        if (meta) meta.textContent = '读取失败';
        return;
      }
      if (meta) meta.textContent = `${payload.dry_run ? '模拟模式' : '实盘模式'} · 候选 ${Number(payload.candidate_count || 0)} 只 · 当前 ${payload.auto_selected_symbol || '待选择'}`;
      runtime.innerHTML = `
        <div class="d-grid-field"><label>策略开关</label><select id="dGridEnabled"><option value="0" ${!payload.enabled ? 'selected' : ''}>关闭新循环</option><option value="1" ${payload.enabled ? 'selected' : ''}>允许新循环</option></select></div>
        <div class="d-grid-field"><label>执行模式</label><select id="dGridDryRun"><option value="1" ${payload.dry_run ? 'selected' : ''}>模拟，不提交订单</option><option value="0" ${!payload.dry_run ? 'selected' : ''}>实盘，提交 Alpaca</option></select></div>
        <div class="d-grid-field"><label>自动选股</label><select id="dAutoSelectEnabled"><option value="1" ${payload.auto_select_enabled ? 'selected' : ''}>开启</option><option value="0" ${!payload.auto_select_enabled ? 'selected' : ''}>关闭</option></select></div>
        <div class="d-grid-field"><label>重新检查</label><select id="dAutoSelectInterval"><option value="3600" selected>每 1 小时</option></select></div>
        <div class="d-grid-field"><label>限价买入回落</label><input id="dGridEntryPct" type="number" min="0.01" max="5" step="0.01" value="${(Number(payload.entry_pct || 0.0025) * 100).toFixed(2)}"><span class="small-muted">当前价下方百分比</span></div>
        <div class="d-grid-field"><label>成交后止盈</label><input id="dGridProfitPct" type="number" min="0.01" max="20" step="0.1" value="${(Number(payload.profit_pct || 0.01) * 100).toFixed(2)}"><span class="small-muted">按实际成交价计算</span></div>
        <div class="d-grid-field"><label>开始交易</label><input id="dGridOpenTime" type="time" value="${esc(payload.open_time || '06:35')}"></div>
        <div class="d-grid-field"><label>停止开仓</label><input id="dGridLastEntry" type="time" value="${esc(payload.last_entry_time || '12:30')}"></div>
        <div class="d-grid-field"><label>收盘平仓</label><input id="dGridFlattenTime" type="time" value="${esc(payload.flatten_time || '12:50')}"></div>
        <div class="d-grid-field"><label>买单等待秒数</label><input id="dGridBuyTimeout" type="number" min="5" step="1" value="${Number(payload.buy_timeout_seconds || 45)}"></div>
        <div class="d-grid-field"><label>每轮冷却秒数</label><input id="dGridCooldown" type="number" min="1" step="1" value="${Number(payload.cooldown_seconds || 5)}"></div>
        <div class="d-grid-field"><label>机器人状态</label><div class="d-grid-symbol-state ${payload.bot_enabled ? 'active' : ''}">${payload.bot_enabled ? '运行开关已开启' : '机器人关闭，保存配置不会自动启动'}</div></div>`;
      symbols.innerHTML = (payload.symbols || []).map(dGridSymbolRow).join('') || '<div class="schedule-empty">尚未配置股票。点击“添加股票”，基础版最多 2 只。</div>';
    }
    async function loadDGridConfig() { renderDGridConfig(await api('/api/d_grid_config')); }
    function addDGridSymbol() {
      const box = document.getElementById('dGridSymbols');
      if (!box) return;
      const count = box.querySelectorAll('[data-d-grid-symbol]').length;
      if (count >= 2) { alert('基础版最多配置 2 只股票'); return; }
      if (!count) box.innerHTML = '';
      box.insertAdjacentHTML('beforeend', dGridSymbolRow({enabled:false}));
    }
    function removeDGridSymbol(button) {
      button.closest('[data-d-grid-symbol]')?.remove();
      const box = document.getElementById('dGridSymbols');
      if (box && !box.querySelector('[data-d-grid-symbol]')) box.innerHTML = '<div class="schedule-empty">尚未配置股票。</div>';
    }
    function collectDGridConfig() {
      const symbols = [...document.querySelectorAll('[data-d-grid-symbol]')].map(row => {
        const read = name => row.querySelector(`[data-field="${name}"]`);
        return {enabled:!!read('enabled')?.checked, symbol:String(read('symbol')?.value || '').trim().toUpperCase(), lot_notional:Number(read('lot_notional')?.value || 0), entry_offset:Number(read('entry_offset')?.value || 0), profit_offset:Number(read('profit_offset')?.value || 0), max_spread:Number(read('max_spread')?.value || 0)};
      }).filter(row => row.symbol);
      return {enabled:document.getElementById('dGridEnabled')?.value === '1', dry_run:document.getElementById('dGridDryRun')?.value !== '0', auto_select_enabled:document.getElementById('dAutoSelectEnabled')?.value !== '0', auto_select_interval_seconds:Number(document.getElementById('dAutoSelectInterval')?.value || 3600), entry_pct:Number(document.getElementById('dGridEntryPct')?.value || 0.25)/100, profit_pct:Number(document.getElementById('dGridProfitPct')?.value || 1)/100, open_time:document.getElementById('dGridOpenTime')?.value || '06:35', last_entry_time:document.getElementById('dGridLastEntry')?.value || '12:30', flatten_time:document.getElementById('dGridFlattenTime')?.value || '12:50', buy_timeout_seconds:Number(document.getElementById('dGridBuyTimeout')?.value || 45), cooldown_seconds:Number(document.getElementById('dGridCooldown')?.value || 5), symbols};
    }
    async function saveDGridConfig() {
      const config = collectDGridConfig();
      if (!config.dry_run && config.enabled && !confirm('你正在启用 D 实盘新循环。确认保存实盘配置？')) return;
      const result = await postJson('/api/d_grid_config', config);
      if (!result?.ok) { alert(result?.error || 'D 配置保存失败'); return; }
      renderDGridConfig(result);
    }
    function renderAccountConfig(payload) {
      accountConfig = payload || null;
      const grid = document.getElementById('accountConfigGrid');
      const mapGrid = document.getElementById('poolMapGrid');
      const monthlyGrid = document.getElementById('monthlyConfigGrid');
      const meta = document.getElementById('accountConfigMeta');
      if (!grid || !mapGrid || !monthlyGrid) return;
      if (!payload?.ok) {
        grid.innerHTML = `<div class="schedule-empty">${esc(payload?.error || '账户配置读取失败')}</div>`;
        if (meta) meta.textContent = '读取失败';
        return;
      }
      const profiles = payload.profiles || {};
      const profileKeys = ['retirement','trading'].filter(key => profiles[key]);
      if (meta) meta.textContent = 'A 独立养老金账户；B/C/D 共用原保证金账户';
      grid.innerHTML = profileKeys.map(key => {
        const p = profiles[key] || {};
        const title = key === 'retirement' ? 'A 养老金账户' : 'B/C/D 原保证金账户';
        return `<div class="account-card" data-account-profile="${esc(key)}">
          <h4>${esc(title)}</h4>
          <div class="account-mask">Key ${esc(p.key_id_mask || '--')} · Secret ${p.has_secret ? '已配置' : '未配置'} · ${esc(p.mode || '--')}</div>
          <div class="account-fields">
            <label>名称</label><input class="account-label" value="${esc(p.label || key)}" />
            <label>当前模式</label><div class="account-mode-readonly">${esc(p.mode || '--')}（在 env/代码里切换）</div>
            <label>API Key</label><input class="account-key" placeholder="${esc(p.key_id_mask ? '留空不修改 ' + p.key_id_mask : '留空使用环境变量')}" />
            <label>Secret</label><input class="account-secret" type="password" placeholder="${p.has_secret ? '留空不修改' : '留空使用环境变量'}" />
          </div>
        </div>`;
      }).join('');
      const poolLabels = {A:'A 月投/养老金', B:'B 自动策略', C:'C 自动建仓/做T', D:'D 日内交易'};
      const profileLabels = {retirement:'养老金账户', trading:'原保证金账户'};
      const fixedPools = {A:'retirement', B:'trading', C:'trading', D:'trading'};
      mapGrid.innerHTML = ['A','B','C','D'].map(group => `
        <div class="pool-map-card" data-pool-map="${group}">
          <label>${esc(poolLabels[group])}</label>
          <div class="pool-map-value">${esc(profileLabels[fixedPools[group]])}</div>
        </div>
      `).join('');
      const monthly = payload.monthly_invest || {};
      const groups = monthly.groups || {};
      monthlyGrid.innerHTML = `
        <div class="monthly-card"><label>月投开关</label><select id="monthlyEnabled"><option value="1" ${monthly.enabled !== false ? 'selected' : ''}>启用</option><option value="0" ${monthly.enabled === false ? 'selected' : ''}>关闭</option></select></div>
        <div class="monthly-card"><label>每月日期</label><input id="monthlyDay" type="number" min="1" max="28" value="${esc(monthly.day || 15)}" /></div>
        <div class="monthly-card"><label>自动执行</label><select id="monthlyAutoExecute"><option value="0" ${monthly.auto_execute ? '' : 'selected'}>只预览</option><option value="1" ${monthly.auto_execute ? 'selected' : ''}>自动下单</option></select></div>
        <div class="monthly-card"><label>A 使用比例</label><input id="monthlyAFraction" type="number" step="0.05" min="0" max="1" value="${esc(groups.A?.budget_fraction ?? 1)}" /></div>
        <div class="monthly-card"><label>A 最大标的</label><input id="monthlyAMax" type="number" min="1" value="${esc(groups.A?.max_symbols || 20)}" /></div>
        <div class="monthly-card"><label>C 买入</label><div class="account-mode-readonly">不参与月投 · 有可用资金自动补仓</div></div>
        <div class="monthly-card"><label>订单类型</label><select id="monthlyOrderType"><option value="limit" selected>实时价限价</option></select></div>
      `;
    }
    async function loadAccountConfig() {
      const meta = document.getElementById('accountConfigMeta');
      if (meta) meta.textContent = '加载中...';
      const payload = await api('/api/account_config');
      renderAccountConfig(payload);
    }
    function collectAccountConfig() {
      const profiles = {};
      document.querySelectorAll('[data-account-profile]').forEach(card => {
        const key = card.getAttribute('data-account-profile');
        profiles[key] = {
          label: card.querySelector('.account-label')?.value || key,
          key_id: card.querySelector('.account-key')?.value || '',
          secret_key: card.querySelector('.account-secret')?.value || '',
        };
      });
      const orderType = document.getElementById('monthlyOrderType')?.value || 'limit';
      return {
        config: {
          active_profile: 'trading',
          pool_profiles: {A:'retirement', B:'trading', C:'trading', D:'trading'},
          profiles,
        },
        monthly_invest: {
          enabled: document.getElementById('monthlyEnabled')?.value !== '0',
          auto_execute: document.getElementById('monthlyAutoExecute')?.value === '1',
          day: Number(document.getElementById('monthlyDay')?.value || 15),
          groups: {
            A: {enabled: true, budget_fraction: Number(document.getElementById('monthlyAFraction')?.value || 0), max_symbols: Number(document.getElementById('monthlyAMax')?.value || 20), order_type: orderType},
            C: {enabled: false, budget_fraction: 0, max_symbols: 1, order_type: orderType},
          },
        },
      };
    }
    async function saveAccountConfig() {
      const meta = document.getElementById('accountConfigMeta');
      if (meta) meta.textContent = '保存中...';
      const result = await postJson('/api/account_config', collectAccountConfig());
      renderAccountConfig(result);
      if (meta) meta.textContent = result.ok ? '已保存' : (result.error || '保存失败');
      await loadAll();
    }
    function monthlyResultText(result) {
      if (!result?.ok) return result?.error || '月投预览失败';
      if (result.skipped) return `跳过：今天 ${result.today}，月投日 ${result.config?.day || 15}，上次执行 ${result.last_run_month || '--'}`;
      return (result.groups || []).map(g => {
        const orders = (g.orders || []).slice(0, 8).map(o => `${o.symbol} ${o.qty}股 ${money(o.target_notional || 0)} ${o.status}${o.error ? ' ' + o.error : ''}`).join('\\n');
        return `${g.group} 预算 ${money(g.budget || 0)} / 标的 ${g.target_count || 0}\\n${orders || '无订单'}`;
      }).join('\\n\\n');
    }
    async function previewMonthlyInvest() {
      const box = document.getElementById('monthlyInvestResult');
      if (box) box.textContent = '生成预览中...';
      await postJson('/api/account_config', collectAccountConfig());
      const result = await postJson('/api/monthly_invest', {force:true, execute:false});
      if (box) box.textContent = monthlyResultText(result);
    }
    function ruleInput(strategyKey, section, rule) {
      const value = rule.value ?? '';
      const type = typeof value === 'number' ? 'number' : 'text';
      return `
        <div class="rule-row" data-strategy="${strategyKey}" data-section="${section}" data-key="${esc(rule.key)}">
          <input type="checkbox" class="rule-enabled" ${rule.enabled === false ? '' : 'checked'} />
          <div class="rule-label" title="${esc(rule.label)}">${esc(rule.label)}</div>
          <input class="rule-value" type="${type}" value="${esc(value)}" />
          <div class="rule-unit">${esc(rule.unit || '')}</div>
        </div>`;
    }
    const CONFIG_TAB_COPY = {
      account:['账户与资金映射','管理养老金账户、原保证金账户、资金池映射和公共系统设置。'],
      A:['A · 养老金长期账户','独立养老金账户、每月定投与 A 长期持仓规则。'],
      B:['B · 动量策略','查看入选池、买入确认、市场过滤、仓位和卖出规则。'],
      C:['C · 长期核心仓','管理长期候选、自动建仓与核心仓日内做 T 规则。'],
      D:['D · 单循环日内交易','配置 1-2 只股票，上一轮完整卖出后才开启下一轮。'],
      robots:['机器人与自动化','查看定时任务、交易机器人开关和最近运行状态。'],
      logs:['日志与交易记录','快速进入机器人日志或真实交易记录。'],
    };
    function applyConfigTab() {
      const page = document.getElementById('strategy2Page');
      if (page) page.dataset.activeConfigTab = configTab;
      document.querySelectorAll('[data-config-tab]').forEach(button => button.classList.toggle('active', button.dataset.configTab === configTab));
      document.querySelectorAll('.config-module').forEach(module => {
        const tabs = String(module.dataset.configModules || '').split(/\s+/).filter(Boolean);
        module.hidden = !tabs.includes(configTab);
      });
      document.querySelectorAll('[data-strategy-card]').forEach(card => {
        card.hidden = card.dataset.strategyCard !== configTab;
      });
      const intro = document.getElementById('configTabIntro');
      const copy = CONFIG_TAB_COPY[configTab] || CONFIG_TAB_COPY.account;
      if (intro) intro.innerHTML = `<strong>${esc(copy[0])}</strong><span>${esc(copy[1])}</span>`;
    }
    function setConfigTab(tab) {
      configTab = CONFIG_TAB_COPY[tab] ? tab : 'account';
      applyConfigTab();
      if (configTab === 'account' || configTab === 'A') loadAccountConfig();
      if (configTab === 'B') loadStrategyBConfig();
      if (configTab === 'D') loadDGridConfig();
      if (configTab === 'robots') {
        loadSchedules();
        renderConfigBots(latestBotHeartbeats, latestBotControls);
      }
    }
    function openConfigLogs(view) {
      toggleLogFocus();
      setLogView(view === 'trades' ? 'trades' : 'bots');
    }
    function renderStrategy2Config(config) {
      strategy2Config = config;
      const desc = document.getElementById('strategy2Desc');
      if (desc) desc.textContent = config?.capital?.desc || 'A 月投，B 自动策略，C 自动建仓并做 T，D 日内交易；配置页只展示和保存当前系统规则。';
      const status = document.getElementById('strategy2Status');
      if (status) status.textContent = '已加载';
      const capital = document.getElementById('strategy2Capital');
      if (capital) {
        const rules = config?.capital?.rules || [];
        capital.innerHTML = rules.map(rule => `
          <div class="strategy2-capital-card" data-capital-key="${esc(rule.key)}">
            <div class="strategy2-capital-label">${esc(rule.label)}</div>
            <div class="strategy2-capital-value">${esc(rule.value)}${esc(rule.unit || '')}</div>
            <label class="small-muted"><input type="checkbox" class="capital-enabled" ${rule.enabled === false ? '' : 'checked'} /> 启用</label>
          </div>
        `).join('');
      }
      const grid = document.getElementById('strategy2Grid');
      if (!grid) return;
      const badgeLabel = key => ({A:'长期', B:'股票', C:'长期', D:'日内/期权'}[key] || '策略');
      grid.innerHTML = (config?.strategies || []).map(strategy => `
        <div class="strategy-card" data-strategy-card="${esc(strategy.key)}">
          <div class="strategy-card-head">
            <div>
              <h3>${esc(strategy.key)} · ${esc(strategy.name)}</h3>
              <div class="strategy-card-meta">${esc(strategy.broker)} · 资金 ${esc(strategy.capital)}</div>
            </div>
            <span class="strategy-badge">${esc(badgeLabel(strategy.key))}</span>
          </div>
          <p class="strategy-mission">${esc(strategy.mission)}</p>
          <div class="rule-section">
            <div class="rule-section-title">选股/候选条件</div>
            ${(strategy.select_rules || []).map(rule => ruleInput(strategy.key, 'select_rules', rule)).join('')}
          </div>
          <div class="rule-section">
            <div class="rule-section-title">买入/开仓规则</div>
            ${(strategy.buy_rules || []).map(rule => ruleInput(strategy.key, 'buy_rules', rule)).join('')}
          </div>
          <div class="rule-section">
            <div class="rule-section-title">卖出/止损规则</div>
            ${(strategy.sell_rules || []).map(rule => ruleInput(strategy.key, 'sell_rules', rule)).join('')}
          </div>
        </div>
      `).join('');
      applyConfigTab();
    }
    async function loadStrategy2Config() {
      const status = document.getElementById('strategy2Status');
      if (status) status.textContent = '加载中...';
      const result = await api('/api/strategy_2_config');
      if (!result.ok) {
        if (status) status.textContent = result.error || '加载失败';
        return;
      }
      renderStrategy2Config(result.config);
    }
    function collectStrategy2Config() {
      const config = JSON.parse(JSON.stringify(strategy2Config || {version:'2.0', capital:{rules:[]}, strategies:[]}));
      const capitalMap = Object.fromEntries((config.capital?.rules || []).map(rule => [rule.key, rule]));
      document.querySelectorAll('[data-capital-key]').forEach(card => {
        const key = card.getAttribute('data-capital-key');
        if (capitalMap[key]) capitalMap[key].enabled = Boolean(card.querySelector('.capital-enabled')?.checked);
      });
      const strategyMap = Object.fromEntries((config.strategies || []).map(strategy => [strategy.key, strategy]));
      document.querySelectorAll('.rule-row[data-strategy]').forEach(row => {
        const strategy = strategyMap[row.getAttribute('data-strategy') || ''];
        const section = row.getAttribute('data-section') || '';
        const key = row.getAttribute('data-key') || '';
        const rule = (strategy?.[section] || []).find(item => item.key === key);
        if (!rule) return;
        const input = row.querySelector('.rule-value');
        const raw = input?.value ?? '';
        rule.value = input?.type === 'number' ? Number(raw || 0) : raw;
        rule.enabled = Boolean(row.querySelector('.rule-enabled')?.checked);
      });
      return config;
    }
    async function saveStrategy2Config() {
      if (!strategy2Config) await loadStrategy2Config();
      const config = collectStrategy2Config();
      const status = document.getElementById('strategy2Status');
      if (status) status.textContent = '保存中...';
      const result = await postJson('/api/strategy_2_config', {config});
      if (!result.ok) {
        if (status) status.textContent = result.error || '保存失败';
        return;
      }
      renderStrategy2Config(result.config);
      if (status) status.textContent = '已保存';
    }
    function renderLowerView() {
      const tradeFocus = document.body.classList.contains('trade-focus');
      const configFocus = document.body.classList.contains('config-focus');
      if (tradeFocus) lowerView = 'holdings';
      if (configFocus && lowerView !== 'strategy') {
        lowerView = 'strategy';
      }
      if (!isHoldingsFocus() && !tradeFocus && !configFocus) {
        lowerView = 'holdings';
        currentHolding = 'ALL';
      }
      const holdingsMode = lowerView === 'holdings';
      const marketMode = lowerView === 'market';
      const dMode = lowerView === 'd';
      const strategyMode = lowerView === 'strategy';
      document.getElementById('lowerPanelTitle').textContent = strategyMode ? '系统配置' : dMode ? (dSection === 'intraday' ? 'D 日内交易' : 'Q 期权交易') : (marketMode ? '行情分析' : '持仓');
      document.getElementById('viewToggleBtn').textContent = strategyMode ? '看持仓' : marketMode ? (isDSectionHolding() ? '看D' : '看持仓') : '看行情';
      document.querySelector('.holdings-panel').classList.toggle('market-view', marketMode);
      document.querySelector('.holdings-panel').classList.toggle('d-view', dMode);
      document.querySelector('.holdings-panel').classList.remove('trades-view');
      document.querySelector('.holdings-panel').classList.toggle('strategy-view', strategyMode);
      const track = document.getElementById('lowerTrack');
      track.classList.toggle('market', marketMode);
      track.classList.toggle('d', dMode);
      track.classList.remove('trades');
      track.classList.toggle('strategy', strategyMode);
      document.getElementById('dotHoldings').classList.toggle('active', holdingsMode);
      document.getElementById('dotMarket').classList.toggle('active', marketMode);
      document.getElementById('dotD')?.classList.toggle('active', dMode);
      renderDSection();
    }
    function setLowerView(view) {
      lowerView = view === 'market' ? 'market' : view === 'd' ? 'd' : view === 'strategy' ? 'strategy' : 'holdings';
      renderHoldings();
      renderLowerView();
      if (lowerView === 'market') loadMarketCategories(currentCategory);
      if (lowerView === 'd') loadDTactical();
      if (lowerView === 'strategy' && !strategy2Config) loadStrategy2Config();
      if (lowerView === 'strategy') loadSchedules();
      if (lowerView === 'strategy') loadStrategyBConfig();
      if (lowerView === 'strategy') loadAccountConfig();
      if (lowerView === 'strategy') loadDGridConfig();
      if (lowerView === 'strategy') applyConfigTab();
    }
    function toggleLowerView() {
      if (lowerView === 'strategy') setLowerView('holdings');
      else
      if (lowerView === 'market') setLowerView('holdings');
      else setLowerView('market');
    }
    function renderMarketCategories(payload) {
      latestMarketMeta = payload.meta || [];
      currentCategory = payload.selected_key || currentCategory || '';
      if (!latestMarketMeta.length) {
        document.getElementById('marketMeta').innerHTML = '<span class="market-pill">暂无分类快照</span>';
        document.getElementById('marketCategorySelect').innerHTML = '<option>暂无数据</option>';
        document.getElementById('marketTable').innerHTML = `<tbody><tr><td><div class="empty-state">暂无行情分析数据，等待分类脚本生成快照</div></td></tr></tbody>`;
        return;
      }
      const current = latestMarketMeta.find(r => String(r.category_key || '') === currentCategory) || latestMarketMeta[0];
      const options = [];
      let lastGroup = '';
      latestMarketMeta.forEach(r => {
        const group = String(r.category_group_label || '');
        if (group !== lastGroup) {
          if (lastGroup) options.push('</optgroup>');
          options.push(`<optgroup label="${group}">`);
          lastGroup = group;
        }
        const key = String(r.category_key || '');
        options.push(`<option value="${key}" ${key === currentCategory ? 'selected' : ''}>${r.category_label} (${Number(r.symbol_count || 0)})</option>`);
      });
      if (lastGroup) options.push('</optgroup>');
      document.getElementById('marketCategorySelect').innerHTML = options.join('');
      document.getElementById('marketMeta').innerHTML = [
        `快照交易日 ${payload.snapshot_date || '--'}`,
        `更新时间 ${current.snapshot_updated_at || '--'}`,
        `当前分类 ${current.category_label || '--'} / ${Number(current.symbol_count || 0)}`
      ].map(x => `<span class="market-pill">${x}</span>`).join('');
      const rows = payload.rows || [];
      const blanks = Array.from({length: Math.max(0, 10 - rows.length)}, () => `<tr><td>&nbsp;</td><td></td><td></td><td></td><td></td><td></td><td></td></tr>`).join('');
      document.getElementById('marketTable').innerHTML = `<thead><tr>${['代码','涨跌','开','高','低','收','量'].map(h=>`<th>${h}</th>`).join('')}</tr></thead><tbody>` +
        rows.map(r => {
          const change = Number(r.change_pct || 0);
          return `<tr><td><button class="symbol-fill-btn" onclick="fillManualSymbol('${r.symbol}')">${r.symbol}</button></td><td class="${cls(change)}">${pct(change)}</td><td>${money(r.open)}</td><td>${money(r.high)}</td><td>${money(r.low)}</td><td>${money(r.close)}</td><td>${compactNumber(r.volume)}</td></tr>`;
        }).join('') + blanks + `</tbody>`;
    }
    async function loadMarketCategories(category=currentCategory) {
      const payload = await api(`/api/market_categories?category=${encodeURIComponent(category || '')}`);
      if (!payload.ok) return;
      renderMarketCategories(payload);
    }
    async function refreshMarketCategories() {
      const btn = document.getElementById('marketRefreshBtn');
      const select = document.getElementById('marketCategorySelect');
      const category = select ? select.value : currentCategory;
      const oldText = btn ? btn.textContent : '';
      if (btn) { btn.classList.add('loading'); btn.textContent = '刷新中'; }
      try {
        const payload = await postJson('/api/refresh_market_categories', {category});
        if (!payload.ok) {
          alert(payload.error || '行情分类刷新失败');
          return;
        }
        renderMarketCategories(payload);
      } finally {
        if (btn) { btn.classList.remove('loading'); btn.textContent = oldText || '刷新分类'; }
      }
    }
    function riskChip(label, value, tone='info') {
      return `<span class="risk-chip ${tone}"><span class="risk-chip-label">${label}</span><span class="risk-chip-value">${value}</span></span>`;
    }
    function qqqRiskValue(risk) {
      const price = Number(risk.qqq_price || 0);
      const change = Number(risk.qqq_change_pct || 0);
      const priceText = price > 0 ? price.toFixed(2) : '--';
      const sign = change > 0 ? '+' : '';
      return `${priceText} / ${sign}${change.toFixed(2)}%`;
    }
    function riskTone(risk) {
      if (risk.block_all_new || Number(risk.risk_multiplier || 0) <= 0 || risk.market_trend === '向下' || Number(risk.vix || 0) > 28) return 'danger';
      if (risk.suggest_mode || risk.market_trend === '横盘' || Number(risk.vix || 0) >= 20 || Number(risk.recommended_exposure || 0) < 0.5) return 'warn';
      return 'ok';
    }
    function renderRebalanceAdvice(exposureState, risk) {
      const el = document.getElementById('rebalanceAdvice');
      if (!el) return;
      const cap = window.latestCapitalPayload || {};
      const leverage = Number(cap.margin_usage_percent || 1);
      const leverageLabel = `${leverage.toFixed(2)}x`;
      const leverageText = cap.margin_usage_mode === 'AUTO'
        ? `自动杠杆 ${leverageLabel}`
        : `固定杠杆 ${leverageLabel}`;
      if (!exposureState) {
        const target = Number(risk?.recommended_exposure || 0);
        el.innerHTML = `
          <span class="rebalance-icon">调</span>
          <span class="rebalance-title">自动调仓 <span class="risk-chip info">等待建议</span></span>
          <span class="rebalance-detail"><span>${leverageText}</span><span>目标仓位 ${target ? (target * 100).toFixed(0) + '%' : '--'}</span><span>rebalance_bot 未生成</span></span>
        `;
        return;
      }
      const cur = Number(exposureState.current_exposure_pct || 0);
      const target = Number(exposureState.target_exposure_pct || 0);
      const gap = Number(exposureState.exposure_gap_value || 0);
      const action = String(exposureState.action || 'HOLD').toUpperCase();
      const mode = String(exposureState.mode || 'SUGGEST').toUpperCase();
      const tone = action === 'SELL' ? 'danger' : action === 'BUY' ? 'ok' : 'info';
      const label = action === 'SELL' ? '建议减仓' : action === 'BUY' ? '建议加仓' : '保持仓位';
      el.innerHTML = `
        <span class="rebalance-icon">调</span>
        <span class="rebalance-title">自动调仓 <span class="risk-chip ${tone}">${label}</span></span>
        <span class="rebalance-detail">
          <span>${leverageText}</span>
          <span>当前 ${(cur * 100).toFixed(1)}%</span>
          <span>目标 ${(target * 100).toFixed(1)}%</span>
          <span>差额 ${money(Math.abs(gap))}</span>
          <span>${mode}</span>
        </span>
      `;
    }
    function renderDailyActionPanel(cap, risk, holdingsPayload, state, dTactical) {
      const el = document.getElementById('brokerBalances');
      if (!el) return;
      const available = cap?.available || {};
      const enabled = cap?.pool_enabled || {};
      const rows = holdingsPayload?.rows || latestHoldings || [];
      const activePositions = rows.filter(r => Number(r.qty || 0) > 0);
      const bCandidates = rows.filter(r => String(r.strategy_group || '').toUpperCase() === 'B' && String(r.status || '').toLowerCase() === 'candidate').length;
      const dSignals = Number(dTactical?.signals?.length || dTactical?.candidates?.length || 0);
      const poolStatus = ['A','B','C','D'].map(g => {
        const open = enabled[g] !== false;
        const amt = Number(available[g] || 0);
        const blocked = risk?.block_all_new || risk?.[`block_${g.toLowerCase()}`];
        const cls = !open ? 'off' : blocked ? 'danger' : amt > 0 ? '' : 'warn';
        const label = !open ? `${g} 关闭` : blocked ? `${g} 风控` : `${g} ${money(amt)}`;
        return `<span class="daily-pill ${cls}">${label}</span>`;
      }).join('');
      const leverage = Number(cap?.margin_usage_percent || cap?.total_risk_percent || 1);
      const leverageMode = cap?.margin_usage_mode === 'AUTO' ? '自动杠杆' : '固定杠杆';
      const targetExposure = Number(cap?.total_risk_percent || 0);
      const usedTotal = Number(cap?.used_total || 0);
      const usableTotal = Number(cap?.usable_total || 0);
      const availableTotal = Object.values(available || {}).reduce((sum, value) => sum + Number(value || 0), 0);
      const usedPct = usableTotal > 0 ? usedTotal / usableTotal : 0;
      const riskBlocked = risk?.block_all_new;
      const advice = riskBlocked
        ? '风控已阻止新开仓，今天以复盘持仓、检查止损和等待风险解除为主。'
        : bCandidates > 0 && enabled.B !== false && Number(available.B || 0) > 0
          ? `先复盘 B 策略候选 ${bCandidates} 只，再按额度和买点分批处理。`
          : enabled.D !== false && Number(available.D || 0) > 0
            ? '没有明显 B 候选时，D 只做日内/期权短打，收盘前降低隔夜风险。'
            : enabled.C !== false && Number(available.C || 0) > 0
              ? '今日偏观察，可把 C 的长期低吸名单排一遍，等回撤或突破确认。'
              : '暂无明确开仓动作，保留现金，重点复盘涨幅榜和异常波动。';
      const actionTone = riskBlocked ? 'danger' : availableTotal > 0 ? 'ok' : 'warn';
      el.innerHTML = `
        <div class="daily-action-section">
          <div class="daily-action-title">今日可用行动</div>
          <div class="daily-action-main ${actionTone}">${availableTotal > 0 ? money(availableTotal) : '暂停开仓'}</div>
          <div class="daily-pill-row">${poolStatus}</div>
        </div>
        <div class="daily-action-section">
          <div class="daily-action-title">风险预算</div>
          <div class="daily-action-main">${leverage.toFixed(2)}x</div>
          <div class="daily-action-note">${leverageMode} · 目标 ${(targetExposure * 100).toFixed(0)}% · 已用 ${(usedPct * 100).toFixed(1)}%</div>
          <div class="daily-action-note">${cap?.margin_usage_reason || '跟随当前风控设置'}</div>
        </div>
        <div class="daily-action-section">
          <div class="daily-action-title">下一步建议</div>
          <div class="daily-action-advice">${advice}</div>
          <div class="daily-action-note">持仓 ${activePositions.length} 只 · B候选 ${bCandidates} · D信号 ${dSignals}</div>
        </div>
      `;
    }
    async function updateRiskPreference(value) {
      const result = await postJson('/api/risk_settings', {risk_preference:value});
      if (!result.ok) { alert(result.error || '风险偏好更新失败'); return; }
      await loadAll();
    }
    async function updateMarginUsage(value) {
      const body = value === 'auto' ? {margin_mode:'AUTO'} : {margin_usage:value, margin_mode:'MANUAL'};
      const result = await postJson('/api/risk_settings', body);
      if (!result.ok) { alert(result.error || '保证金额度更新失败'); return; }
      await loadAll();
    }
    async function updatePoolEnabled(group, enabled) {
      const result = await postJson('/api/risk_settings', {pool_enabled:{[group]: enabled}});
      if (!result.ok) { alert(result.error || '资金池开关更新失败'); await loadAll(); return; }
      await loadAll();
    }
    async function loadAll() {
      const refreshBtn = document.querySelector('.refresh-btn');
      if (refreshBtn) refreshBtn.classList.add('loading');
      try {
      const [cap, risk, holdings, state, phase, dTactical] = await Promise.all([api('/api/capital'), api('/api/risk'), api('/api/holdings'), api('/api/state'), api('/api/trade_phase'), api('/api/d_tactical')]);
      if (cap.ok) {
        window.latestCapitalPayload = cap;
        document.getElementById('modeValue').textContent = cap.mode_label || cap.mode;
        renderAnnualGoals(cap.annual_goals || []);
        renderMobileRealAssets(cap);
        document.getElementById('heroPools').innerHTML = ['A','B','C','D'].map(g => poolRow(g, cap)).join('');
        const marginSelect = document.getElementById('marginUsageSelect');
        if (marginSelect) {
          marginSelect.value = cap.margin_usage_mode === 'AUTO' ? 'auto' : String((Number(cap.margin_usage_percent || cap.total_risk_percent || 1)).toFixed(1));
          marginSelect.title = cap.margin_usage_mode === 'AUTO'
            ? `自动额度：当前 ${(Number(cap.margin_usage_percent || 1) * 100).toFixed(0)}%。${cap.margin_usage_reason || ''}`
            : 'A/B/C 保证金使用额度';
        }
        renderPoolSwitches(cap);
        updateManualPoolAvailable();
        drawDonut(cap);
        renderCapitalAllocation(cap);
        renderDailyActionPanel(cap, risk, holdings, state, dTactical);
      } else {
        document.getElementById('modeValue').textContent = 'ERROR';
      }
      const tone = riskTone(risk);
      window.latestRiskPayload = risk;
      const riskSelect = document.getElementById('riskPreferenceSelect');
      if (riskSelect) riskSelect.value = risk.risk_preference || '中性';
      const marketExposure = Number(risk.recommended_exposure || 0);
      const rebalanceTarget = Number(state?.exposure_state?.target_exposure_pct ?? marketExposure);
      const targetTone = rebalanceTarget <= 0.1 ? 'danger' : rebalanceTarget < 0.5 ? 'warn' : 'ok';
      document.getElementById('marketRisk').innerHTML = [
        riskChip('趋势', risk.market_trend || '--', risk.market_trend === '向上' ? 'ok' : risk.market_trend === '向下' ? 'danger' : 'warn'),
        riskChip('QQQ', qqqRiskValue(risk), Number(risk.qqq_change_pct || 0) < 0 ? 'warn' : 'ok'),
        riskChip('VIX', Number(risk.vix || 0).toFixed(1), Number(risk.vix || 0) > 28 ? 'danger' : Number(risk.vix || 0) >= 20 ? 'warn' : 'ok')
      ].join('');
      const marketRisk = document.getElementById('marketRisk');
      marketRisk.classList.remove('fresh');
      void marketRisk.offsetWidth;
      marketRisk.classList.add('fresh');
      renderRebalanceAdvice(state.exposure_state, risk);
      window.latestBotProcesses = state.bot_processes || [];
      latestBotHeartbeats = state.bot_heartbeats || [];
      latestBotControls = state.bot_controls || [];
      renderBots(latestBotHeartbeats, latestBotControls);
      renderPhase(phase);
      if (dTactical.ok) renderDTactical(dTactical);
      latestHoldings = holdings.rows || [];
      updateManualHeldQty();
      updateManualTradePreviews();
      renderHoldings();
      renderLowerView();
      if (lowerView === 'market') await loadMarketCategories(currentCategory);
      if (lowerView === 'strategy') {
        await Promise.all([loadStrategy2Config(), loadSchedules(), loadStrategyBConfig(), loadAccountConfig(), loadDGridConfig()]);
      }
      if (document.body.classList.contains('stock-focus')) await loadStockSelection();
      if (document.body.classList.contains('log-focus')) {
        if (logView === 'trades') await loadTradeRecords();
        else await loadBotLogs();
      }
      await loadCurve(currentPeriod);
      } finally {
        if (refreshBtn) refreshBtn.classList.remove('loading');
      }
    }
    document.querySelectorAll('.tab').forEach(b => b.addEventListener('click', () => loadCurve(b.dataset.period)));
    document.querySelectorAll('.holding-tab').forEach(b => b.addEventListener('click', () => {
      const nextHolding = b.dataset.holding;
      currentHolding = nextHolding;
      if (currentHolding === 'D') dSection = 'intraday';
      if (currentHolding === 'Q') dSection = 'options';
      renderHoldings();
      setLowerView('holdings');
    }));
    document.addEventListener('click', (e) => {
      const pop = document.getElementById('phasePopover');
      const chip = document.getElementById('phaseChip');
      if (pop && chip && !pop.contains(e.target) && !chip.contains(e.target)) pop.classList.remove('show');
      if (!e.target.closest?.('.stock-action-wrap')) closeStockPoolMenus();
    });
    let lowerTouchX = null;
    document.getElementById('lowerSlider').addEventListener('touchstart', (e) => {
      if (isMobileView() || isHoldingsFocus() || document.body.classList.contains('config-focus')) return;
      lowerTouchX = e.touches?.[0]?.clientX ?? null;
    }, {passive:true});
    document.getElementById('lowerSlider').addEventListener('touchend', (e) => {
      if (isMobileView()) { lowerTouchX = null; return; }
      if (lowerTouchX === null) return;
      const endX = e.changedTouches?.[0]?.clientX ?? lowerTouchX;
      const dx = endX - lowerTouchX;
      lowerTouchX = null;
      if (Math.abs(dx) < 48) return;
      if (dx < 0) setLowerView(lowerView === 'holdings' ? 'market' : lowerView === 'market' ? 'd' : 'strategy');
      else setLowerView(lowerView === 'strategy' ? 'd' : lowerView === 'd' ? 'market' : 'holdings');
    }, {passive:true});
    function openClearModal() {
      document.getElementById('clearPassword').value = '';
      document.getElementById('clearModal').classList.add('show');
      setTimeout(() => document.getElementById('clearPassword').focus(), 50);
    }
    function closeClearModal() { document.getElementById('clearModal').classList.remove('show'); }
    function clearResultText(result) {
      const rows = (result.results || []).slice(0, 12).map(r => `${r.symbol} qty=${Number(r.qty || 0).toFixed(6)} limit=${money(r.limit_price || r.current_price || 0)} ${r.status || ''}${r.error ? ' ' + r.error : ''}`);
      const suffix = (result.results || []).length > rows.length ? `\n... 另有 ${(result.results || []).length - rows.length} 条` : '';
      return `${result.message || '清仓限价卖单处理完成'}${rows.length ? '\n\n' + rows.join('\n') + suffix : ''}`;
    }
    async function submitClearPosition(dryRun=false) {
      const password = document.getElementById('clearPassword').value;
      if (!dryRun && !confirm('确认按当前价限价卖出全部股票持仓？')) return;
      const result = await postJson('/api/clear_position', {password, dry_run:dryRun});
      if (!result.ok) { alert(result.error || '清仓命令失败'); return; }
      if (!dryRun) closeClearModal();
      alert(clearResultText(result));
      await loadAll();
    }
    async function toggleBot(botName, enabled) {
      const result = await postJson('/api/bot_control', {bot_name:botName, enabled});
      if (!result.ok) { alert(result.error || '开关失败'); return; }
      await loadAll();
    }
    async function syncPositions() {
      const btn = document.querySelector('.sync-positions-btn');
      const oldText = btn ? btn.textContent : '';
      if (btn) { btn.classList.add('loading'); btn.textContent = '同步中'; }
      try {
        const result = await postJson('/api/sync_positions', {});
        if (!result.ok) { alert(result.error || '同步仓位失败'); return; }
        await loadAll();
      } finally {
        if (btn) { btn.classList.remove('loading'); btn.textContent = oldText || '同步仓位'; }
      }
    }
    updateLifeDate();
    loadJournal();
    loadFiveYearPlan();
    document.getElementById('fiveYearPlanText')?.addEventListener('input', () => {
      const status = document.getElementById('fiveYearStatus');
      if (status) status.textContent = '未保存';
    });
    document.getElementById('journalText')?.addEventListener('input', () => {
      const status = document.getElementById('journalStatus');
      if (status) status.textContent = '未保存';
    });
    restoreManualSymbolLock();
    ['buy', 'sell', 'short'].forEach(updateManualOrderType);
    loadAll();
    setInterval(loadAll, 30000);
  </script>
</body>
</html>"""


LOGIN_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
  <title>CSZY Ultimate V1 登录</title>
  <style>
    :root { --bg:#f4f7fb; --ink:#111827; --muted:#667085; --line:#d8dee8; --blue:#2563eb; --red:#c62828; }
    * { box-sizing:border-box; }
    body { margin:0; min-height:100vh; display:flex; align-items:center; justify-content:center; padding:22px; background:radial-gradient(circle at top left, #e8f1ff, transparent 36%), var(--bg); color:var(--ink); font-family:ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    .login-card { width:min(420px, 100%); background:#fff; border:1px solid var(--line); border-radius:16px; padding:26px; box-shadow:0 24px 70px rgba(15,23,42,.12); }
    .brand { display:flex; align-items:center; gap:14px; margin-bottom:22px; }
    .brand img { width:54px; height:54px; border-radius:12px; object-fit:contain; box-shadow:0 10px 24px rgba(15,23,42,.08); }
    h1 { margin:0; font-size:28px; line-height:1.05; letter-spacing:0; }
    p { margin:8px 0 0; color:var(--muted); font-size:14px; }
    label { display:block; color:var(--muted); font-size:13px; font-weight:750; margin-bottom:8px; }
    input { width:100%; height:46px; border:1px solid var(--line); border-radius:10px; padding:0 13px; font-size:16px; outline:none; }
    input:focus { border-color:var(--blue); box-shadow:0 0 0 4px rgba(37,99,235,.12); }
    button { width:100%; height:46px; border:0; border-radius:10px; background:var(--blue); color:#fff; font-size:16px; font-weight:850; margin-top:14px; cursor:pointer; box-shadow:0 12px 26px rgba(37,99,235,.22); }
    button:active { transform:scale(.98); }
    .error { min-height:22px; margin-top:12px; color:var(--red); font-size:13px; font-weight:750; }
    @media (max-width:480px) {
      body { align-items:flex-start; padding:54px 18px 18px; }
      .login-card { border-radius:14px; padding:22px; }
      h1 { font-size:25px; }
    }
  </style>
</head>
<body>
  <form class="login-card" id="loginForm">
    <div class="brand">
      <img src="/assets/cszy_ultimate_logo.png" alt="CSZY Ultimate logo" />
      <div><h1>CSZY Ultimate V1</h1><p>请输入看板登录密码</p></div>
    </div>
    <label for="password">登录密码</label>
    <input id="password" name="password" type="password" autocomplete="current-password" autofocus />
    <button id="loginBtn" type="submit">进入看板</button>
    <div class="error" id="error"></div>
  </form>
  <script>
    document.getElementById('loginForm').addEventListener('submit', async (e) => {
      e.preventDefault();
      const btn = document.getElementById('loginBtn');
      const err = document.getElementById('error');
      btn.disabled = true;
      btn.textContent = '验证中';
      err.textContent = '';
      try {
        const r = await fetch('/api/login', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({password:document.getElementById('password').value})});
        const data = await r.json();
        if (!data.ok) { err.textContent = data.error || '密码错误'; return; }
        location.reload();
      } catch (ex) {
        err.textContent = '网络异常，请稍后再试';
      } finally {
        btn.disabled = false;
        btn.textContent = '进入看板';
      }
    });
  </script>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
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
        raw = self.rfile.read(length).decode("utf-8")
        return json.loads(raw or "{}")

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
        """判断当前浏览器是否已经登录；未配置密码时默认放行。"""
        expected = _auth_token()
        if not expected:
            return True
        actual = self._cookie_value(AUTH_COOKIE_NAME)
        return bool(actual and hmac.compare_digest(actual, expected))

    def _handle_login(self, payload: dict) -> None:
        expected = _login_password()
        if not expected:
            self._send_json({"ok": True, "message": "未配置登录密码，已放行"})
            return
        password = str(payload.get("password") or "")
        if not hmac.compare_digest(password, expected):
            self._send_json({"ok": False, "error": "登录密码错误"}, 403)
            return
        cookie = f"{AUTH_COOKIE_NAME}={_auth_token()}; Path=/; HttpOnly; SameSite=Lax; Max-Age=604800"
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
            elif path == "/api/capital":
                self._send_json(_allocation_payload())
            elif path == "/api/risk":
                self._send_json(_risk_payload())
            elif path == "/api/holdings":
                self._send_json(_holdings_payload())
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
                    headers={"Set-Cookie": f"{AUTH_COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"},
                )
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
    s = settings()
    from .main import startup

    startup()
    if env_str("ULTIMATE_SKIP_BOT_SYNC_ON_START", "0").strip().lower() not in {"1", "true", "yes"}:
        sync_from_controls()
        start_watchdog()
    server = ThreadingHTTPServer((s.web_host, s.web_port), Handler)
    print(f"[WEB] http://127.0.0.1:{s.web_port}", flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        shutdown_supervisor()


if __name__ == "__main__":
    run()
