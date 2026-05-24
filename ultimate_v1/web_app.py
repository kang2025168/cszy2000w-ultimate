from __future__ import annotations

"""轻量网页看板：展示资金池、风控状态和 position_holdings 持仓。"""

import json
import hashlib
import hmac
import contextlib
import csv
import importlib.util
import io
import socket
import time
from datetime import date, datetime, time as dt_time
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from . import alpaca_gateway
from .bot_supervisor import managed_bot_names, process_status, set_bot_runtime, sync_from_controls
from .capital_manager import get_capital_allocation, get_strategy_used_capital
from .config import env_str, settings
from .db import db_conn, fetch_all
from .d_tactical import d_tactical_payload, option_preview, submit_option_combo
from .exposure_manager import latest_exposure_state, latest_rebalance_actions, refresh_exposure_plan
from .rebalance_monthly import generate_rebalance_report
from .risk_controller import CAPITAL_MODE_LABELS, get_risk_state
from .schema import ensure_schema
from .state_store import bot_controls, bot_heartbeats, capital_state_rows, equity_curve, get_app_setting, latest_risk_state, set_app_setting
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
    margin_usage = _parse_margin_usage_setting()
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
        "base_total": base_total,
        "usable_total": usable_total,
        "used_total": used_total,
        "total_risk_percent": allocation.total_risk_percent,
        "margin_usage_percent": margin_usage,
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

    weekly_fitness_target = _setting_float("WEEKLY_FITNESS_TARGET", 4.0)
    weekly_fitness_current = _setting_float("WEEKLY_FITNESS_CURRENT", 0.0)
    weekly_words_target = _setting_float("WEEKLY_WORDS_TARGET", 50.0)
    weekly_words_current = _setting_float("WEEKLY_WORDS_CURRENT", 0.0)

    return [
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
            "key": "stock_growth",
            "name": "股票账户跃迁",
            "desc": "年度回报目标 30%",
            "current": return_current,
            "target": return_target,
            "unit": "percent",
            "start_equity": start_equity,
            "equity": equity,
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
    raw = get_app_setting("RISK_TOTAL_CAPITAL_PCT", "1.0")
    try:
        value = float(raw)
        if value > 10:
            value = value / 100.0
    except Exception:
        value = 1.0
    return max(1.0, min(1.5, value))


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
    """本地日线没有价格时，优先用 Alpaca snapshot 补现价/昨收，再用 yfinance 兜底。"""
    global _QUOTE_REFRESH_TS
    if time.time() - _QUOTE_REFRESH_TS < 120:
        return

    _QUOTE_REFRESH_TS = time.time()
    max_refresh = int(env_str("HOLDINGS_QUOTE_REFRESH_LIMIT", "12") or "12")
    targets = [s for s in sorted({v.strip().upper() for v in symbols if v})][:max_refresh]
    if not targets:
        return

    unresolved: list[str] = []
    with db_conn() as conn:
        with conn.cursor() as cur:
            for symbol in targets:
                try:
                    from app.strategy_b import get_snapshot_realtime

                    current, prev, feed = get_snapshot_realtime(symbol)
                    if float(current or 0) > 0:
                        _write_quote_cache(cur, symbol, float(current), float(prev or 0), f"alpaca_snapshot_{feed}")
                        continue
                except Exception as exc:
                    print(f"[HOLDINGS QUOTE] {symbol} alpaca snapshot failed: {exc}", flush=True)
                unresolved.append(symbol)

            if env_str("HOLDINGS_ENABLE_YFINANCE_QUOTE", "1").strip() not in {"1", "true", "TRUE", "yes", "YES"}:
                return
            if not importlib.util.find_spec("yfinance"):
                return
            try:
                import yfinance as yf
            except Exception:
                return

            for symbol in unresolved:
                old_timeout = socket.getdefaulttimeout()
                try:
                    socket.setdefaulttimeout(float(env_str("HOLDINGS_QUOTE_TIMEOUT_SEC", "3") or "3"))
                    ticker = yf.Ticker(symbol)
                    fast_info = getattr(ticker, "fast_info", {}) or {}

                    def _fast_get(key: str):
                        try:
                            return fast_info.get(key) if hasattr(fast_info, "get") else getattr(fast_info, key, None)
                        except Exception:
                            return None

                    current = (
                        _safe_float(_fast_get("last_price"))
                        or _safe_float(_fast_get("regular_market_price"))
                        or _safe_float(_fast_get("lastPrice"))
                    )
                    prev = (
                        _safe_float(_fast_get("previous_close"))
                        or _safe_float(_fast_get("regular_market_previous_close"))
                        or _safe_float(_fast_get("previousClose"))
                    )
                    if current <= 0:
                        hist = ticker.history(period="5d")
                        if hist is not None and not hist.empty:
                            closes = [float(v) for v in hist["Close"].dropna().tolist() if float(v) > 0]
                            if closes:
                                current = closes[-1]
                            if len(closes) >= 2 and prev <= 0:
                                prev = closes[-2]
                    if current <= 0:
                        continue
                    _write_quote_cache(cur, symbol, current, prev, "yfinance")
                except Exception as exc:
                    print(f"[HOLDINGS QUOTE] {symbol} quote refresh failed: {exc}", flush=True)
                finally:
                    socket.setdefaulttimeout(old_timeout)


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
    missing = [symbol for symbol in symbols if _safe_float((out.get(symbol) or {}).get("latest_close")) <= 0]
    cached = _quote_cache(missing)
    stale_missing = [symbol for symbol in missing if symbol not in cached]
    _refresh_missing_quotes(stale_missing)
    if stale_missing:
        cached = _quote_cache(missing)
    for symbol, row in cached.items():
        if _safe_float((out.get(symbol) or {}).get("latest_close")) > 0:
            continue
        current = _safe_float(row.get("current_price"))
        prev = _safe_float(row.get("prev_close"))
        out[symbol] = {
            "latest_close": current,
            "prev_close": prev,
            "day_change_pct": row.get("day_change_pct") if row.get("day_change_pct") is not None else ((current - prev) / prev if current > 0 and prev > 0 else None),
            "latest_date": row.get("fetched_at"),
        }
    return out


def _enrich_holdings_rows(rows: list[dict]) -> list[dict]:
    """给持仓/观察票补日涨跌和现价；未买入股票也能看到行情状态。"""
    symbols = [str(row.get("symbol") or "").strip().upper() for row in rows]
    price_meta = _latest_price_meta(symbols)

    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        meta = price_meta.get(symbol) or {}
        latest = _safe_float(meta.get("latest_close"))
        current = _safe_float(row.get("current_price")) or latest
        prev = _safe_float(meta.get("prev_close"))
        day_change_pct = (current - prev) / prev if current > 0 and prev > 0 else meta.get("day_change_pct")
        qty = _safe_float(row.get("qty"))

        row["symbol"] = symbol
        row["current_price"] = current
        row["day_change_pct"] = day_change_pct
        row["price_as_of"] = meta.get("latest_date")
        if _safe_float(row.get("market_value")) <= 0 and qty > 0 and current > 0:
            row["market_value"] = qty * current
    return rows


def _holdings_payload() -> dict:
    """读取持仓展示表，供前端表格渲染。"""
    rows = fetch_all(
        """
        SELECT symbol, normalized_group AS strategy_group, stock_type, status, qty,
               initial_entry_price, avg_entry_price,
               current_price, market_value, cost_basis, unrealized_pnl,
               unrealized_pnl_pct, realized_pnl, entry_time, exit_time,
               holding_days, stop_loss_price, take_profit_price, b_stage,
               capital_pool, margin_used, last_order_side, last_update_time
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
        ORDER BY FIELD(status, 'open', 'needs_review', 'closed'), strategy_group, symbol
        LIMIT 500
        """
    )
    return {"ok": True, "rows": _enrich_holdings_rows(list(rows or []))}


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
    """读取当天买卖机器人记录，限制在面板内滚动展示。"""
    rows: list[dict] = []
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
                WHERE DATE(created_at)=CURDATE()
                  AND UPPER(side) IN ('BUY','SELL')
                ORDER BY created_at DESC, order_id DESC
                LIMIT 200
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
                WHERE DATE(last_order_time)=CURDATE()
                  AND LOWER(last_order_side) IN ('buy','sell')
                ORDER BY last_order_time DESC, id DESC
                LIMIT 200
                """
            )
        )
    except Exception as exc:
        print(f"[WEB TRADE RECORDS] stock_operations unavailable: {exc}", flush=True)

    try:
        rows.extend(
            fetch_all(
                """
                SELECT
                    created_at AS event_time,
                    bot_name AS symbol,
                    action AS side,
                    'BOT' AS strategy_group,
                    0 AS qty,
                    0 AS price,
                    status,
                    CONCAT(message, IF(pid IS NULL, '', CONCAT(' pid=', pid))) AS note,
                    CAST(id AS CHAR) AS order_id,
                    'bot_lifecycle_events' AS source
                FROM bot_lifecycle_events
                WHERE DATE(created_at)=CURDATE()
                ORDER BY created_at DESC, id DESC
                LIMIT 200
                """
            )
        )
    except Exception as exc:
        print(f"[WEB TRADE RECORDS] bot_lifecycle_events unavailable: {exc}", flush=True)

    def key(row: dict) -> str:
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
    for row in rows:
        k = key(row)
        if k in seen:
            continue
        seen.add(k)
        cleaned.append(row)
    cleaned.sort(key=lambda r: str(r.get("event_time") or ""), reverse=True)
    return {"ok": True, "rows": cleaned[:200]}


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
    .left-titlebar { min-height:58px; display:flex; align-items:center; justify-content:space-between; gap:14px; padding:8px 10px 10px 18px; border:1px solid #263852; border-radius:8px; background:linear-gradient(135deg,#0b1220 0%,#111c2e 52%,#1f2f46 100%); box-shadow:0 22px 52px rgba(15,23,42,.18); backdrop-filter:blur(10px); }
    .brand-lockup { display:flex; align-items:center; gap:12px; min-width:0; }
    .brand-logo { width:42px; height:42px; border-radius:8px; object-fit:contain; background:#fff; box-shadow:0 10px 24px rgba(15,23,42,.10); border:1px solid #edf2f7; }
    .brand-copy { min-width:0; display:flex; flex-direction:column; gap:7px; }
    .left-titlebar h1 { color:#f8fafc; text-shadow:0 1px 0 rgba(0,0,0,.18); }
    .dashboard-motto { color:#b8c5d8; font-size:13px; font-weight:750; letter-spacing:0; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .title-actions { display:flex; align-items:center; gap:10px; flex:0 0 auto; }
    .phase-chip { min-width:144px; height:38px; border:1px solid rgba(226,232,240,.24); border-radius:999px; background:rgba(255,255,255,.94); display:flex; align-items:center; justify-content:center; gap:7px; padding:0 13px; font-size:12px; font-weight:850; color:var(--ink); box-shadow:0 10px 24px rgba(0,0,0,.18); }
    .phase-chip .phase-dot { width:9px; height:9px; border-radius:50%; background:var(--muted); box-shadow:0 0 0 4px rgba(102,112,133,.1); }
    .phase-chip.ok .phase-dot { background:var(--green); box-shadow:0 0 0 4px rgba(21,147,106,.12); }
    .phase-chip.blue .phase-dot { background:var(--blue); box-shadow:0 0 0 4px rgba(37,99,235,.12); }
    .phase-chip.warn .phase-dot { background:var(--amber); box-shadow:0 0 0 4px rgba(183,110,0,.14); }
    .phase-chip.sleep .phase-dot { background:var(--red); box-shadow:0 0 0 4px rgba(198,40,40,.12); }
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
    .refresh-btn { height:38px; padding:0 18px; border:0; border-radius:9px; background:#2563eb; color:#fff; font-weight:850; box-shadow:0 9px 22px rgba(37,99,235,.22); transition:transform .12s ease, background .12s ease, opacity .12s ease; }
    .refresh-btn:hover { background:#1d4ed8; }
    .refresh-btn:active { transform:scale(.96); }
    .refresh-btn.loading { opacity:.72; pointer-events:none; }
    .dash { display:grid; grid-template-columns:minmax(560px, 1.08fr) minmax(520px, .92fr); gap:18px; align-items:stretch; }
    .panel { background:linear-gradient(180deg, #fff 0%, #fbfdff 100%); border:1px solid var(--line); border-radius:8px; padding:16px; box-shadow:var(--shadow-soft); }
    .mobile-collapse-toggle { display:none; }
    .left-stack, .right-stack { display:flex; flex-direction:column; gap:18px; min-width:0; }
    .capital-hero { flex:1; }
    .hero-top { display:grid; grid-template-columns:minmax(340px,1fr) minmax(300px,.78fr); gap:12px; align-items:start; padding:14px; border:1px solid #263852; border-radius:8px; background:linear-gradient(145deg,#0b1220 0%,#111827 46%,#1e293b 100%); box-shadow:inset 0 1px 0 rgba(255,255,255,.07), 0 22px 50px rgba(15,23,42,.16); }
    .hero-donut { border:1px solid rgba(191,219,254,.26); border-radius:8px; min-height:164px; padding:14px; overflow:hidden; background:linear-gradient(135deg,#eef6ff 0%,#f8fbff 100%); box-shadow:inset 0 0 0 1px rgba(255,255,255,.82), 0 16px 34px rgba(2,6,23,.18); }
    .hero-donut-head { display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:4px; }
    .mode-pill { display:inline-flex; align-items:center; justify-content:center; min-width:48px; height:26px; padding:0 10px; border-radius:999px; background:#101828; color:#fff; font-size:12px; font-weight:900; }
    .hero-carousel-viewport { overflow:hidden; width:100%; }
    .hero-carousel-track { width:200%; display:flex; transition:transform .28s ease; }
    .hero-carousel-track.bots { transform:translateX(-50%); }
    .hero-carousel-page { width:50%; flex:0 0 50%; min-width:0; display:flex; flex-direction:column; }
    .hero-carousel-page .donut-wrap { min-height:112px; }
    .hero-metrics-column { display:grid; gap:12px; align-self:start; min-width:0; }
    .metric-grid { display:grid; grid-template-columns:repeat(2, minmax(0,1fr)); gap:12px; align-self:start; }
    .metric { border:1px solid rgba(226,232,240,.88); border-radius:8px; padding:13px 14px; min-height:84px; background:linear-gradient(180deg,#fff,#f8fafc); box-shadow:0 14px 30px rgba(2,6,23,.16); }
    .metric-label, .pool-meta, .small-muted { color:var(--muted); font-size:12px; }
    .metric-value { font-size:20px; font-weight:850; margin-top:6px; line-height:1.1; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .risk-strip { margin-top:14px; border:1px solid #d5e6f8; border-radius:8px; padding:14px; display:grid; gap:12px; background:linear-gradient(135deg, #fff 0%, #f4fbff 100%); box-shadow:inset 0 0 0 1px rgba(255,255,255,.72); }
    .risk-topbar { display:flex; align-items:center; justify-content:space-between; gap:14px; }
    .risk-main { min-width:0; display:grid; gap:10px; }
    .risk-head { display:flex; align-items:center; gap:12px; flex-wrap:wrap; min-width:0; }
    .risk-head h2 { white-space:nowrap; }
    .risk-body { min-width:0; }
    .risk-line { display:flex; gap:10px; flex-wrap:wrap; align-content:flex-start; color:var(--muted); font-size:12px; }
    .risk-chip { min-height:30px; display:inline-flex; align-items:center; border-radius:999px; padding:6px 12px; background:#eef2f6; color:#475467; font-weight:850; white-space:nowrap; border:1px solid rgba(255,255,255,.72); box-shadow:0 5px 12px rgba(15,23,42,.04); }
    .risk-chip.ok { background:#e7f6ef; color:#08734f; }
    .risk-chip.warn { background:#fff3d6; color:#9a5b00; }
    .risk-chip.danger { background:#fee2e2; color:#b42318; }
    .risk-chip.info { background:#e0f2fe; color:#075985; }
    .market-risk-inline { display:flex; gap:8px; flex-wrap:wrap; align-items:center; min-width:0; }
    .market-risk-inline .risk-chip { padding:7px 13px; font-size:13px; }
    .market-risk-inline.fresh .risk-chip { animation:freshPulse .85s ease-out 1; }
    @keyframes freshPulse {
      0% { transform:scale(1); box-shadow:0 0 0 0 rgba(21,147,106,.24); filter:brightness(1); }
      42% { transform:scale(1.035); box-shadow:0 0 0 7px rgba(21,147,106,.10); filter:brightness(1.04); }
      100% { transform:scale(1); box-shadow:0 0 0 0 rgba(21,147,106,0); filter:brightness(1); }
    }
    .risk-actions { display:flex; align-items:center; gap:10px; flex:0 0 auto; }
    .risk-badge { font-size:13px; font-weight:700; padding:5px 9px; border-radius:999px; background:#e7f6ef; color:var(--green); white-space:nowrap; }
    .risk-badge.warn { background:#fff3d6; color:#9a5b00; }
    .risk-badge.danger { background:#fee2e2; color:#b42318; }
    .risk-control-select { height:34px; border:1px solid var(--line); border-radius:7px; padding:0 10px; background:#fff; color:var(--ink); font-weight:800; box-shadow:0 5px 14px rgba(15,23,42,.035); }
    .clear-btn { height:30px; padding:0 14px; border:0; border-radius:7px; background:#fee2e2; color:#b42318; font-weight:850; }
    .clear-btn:hover { background:#fecaca; }
    .rebalance-advice { min-height:74px; display:grid; grid-template-columns:auto 1fr; grid-template-areas:"icon title" "icon detail"; align-items:center; column-gap:10px; row-gap:4px; padding:11px 12px; border:1px solid rgba(191,219,254,.7); border-radius:8px; background:linear-gradient(135deg,#eef8ff,#f8fbff); color:var(--muted); font-size:12px; font-weight:750; box-shadow:0 14px 30px rgba(2,6,23,.15); }
    .rebalance-icon { grid-area:icon; width:34px; height:34px; border-radius:8px; display:grid; place-items:center; background:#fff; color:#075985; font-weight:950; box-shadow:inset 0 0 0 1px #bfdbfe; }
    .rebalance-title { grid-area:title; display:flex; align-items:center; gap:8px; flex-wrap:wrap; color:var(--ink); font-weight:900; }
    .rebalance-detail { grid-area:detail; display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
    .exposure-card { margin-top:14px; border:1px solid var(--line); border-radius:8px; padding:12px 14px; background:linear-gradient(180deg,#fff,#f8fbff); box-shadow:0 8px 20px rgba(15,23,42,.035); }
    .exposure-head { display:flex; align-items:center; justify-content:space-between; gap:12px; font-size:13px; font-weight:800; }
    .exposure-value { color:var(--muted); font-size:12px; font-weight:700; }
    .exposure-bar { height:12px; border-radius:999px; overflow:hidden; background:#e9edf3; margin-top:10px; }
    .exposure-fill { height:100%; width:0%; background:linear-gradient(90deg, #15936a, #d97706); }
    .pool-grid { margin-top:26px; display:grid; grid-template-columns:repeat(2, minmax(0,1fr)); gap:12px; }
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
    .events-panel { min-height:164px; background:linear-gradient(180deg,#fff 0%,#fbfcff 100%); }
    .events-head { display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:8px; }
    .events-kicker { color:var(--muted); font-size:12px; font-weight:800; }
    .event-list { display:grid; gap:7px; }
    .event-row { display:grid; grid-template-columns:64px 42px minmax(0,1fr) 30px; align-items:center; gap:8px; min-height:30px; color:var(--ink); font-size:12px; font-weight:750; padding:2px 4px; border-radius:7px; }
    .event-row:hover { background:#f5f8fc; }
    .event-date { color:var(--muted); font-weight:850; }
    .event-type { display:inline-flex; align-items:center; justify-content:center; height:22px; border-radius:999px; background:#eef2f6; color:#475467; font-size:11px; font-weight:900; }
    .event-type.macro { background:#fee2e2; color:#b42318; }
    .event-type.ipo { background:#fff3d6; color:#9a5b00; }
    .event-type.earnings { background:#e7f6ef; color:#08734f; }
    .event-title { min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .event-impact { color:var(--muted); text-align:right; font-size:11px; font-weight:850; }
    .event-empty { color:var(--muted); font-size:12px; font-weight:750; padding:8px 0; }
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
    .status { display:inline-block; min-width:64px; text-align:center; padding:3px 8px; border-radius:999px; background:#eef2f6; }
    .open { color:var(--green); background:#e7f6ef; }
    .closed { color:var(--muted); }
    .needs_review { color:var(--amber); background:#fff3d6; }
    .neg { color:var(--red); }
    .pos { color:var(--green); }
    .scroll { overflow:auto; border-radius:8px; }
    .holdings-panel { margin-top:18px; min-height:430px; overflow:hidden; box-shadow:var(--shadow); }
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
    .lower-track.trades { transform:translateX(-75%); }
    .lower-page { width:25%; flex:0 0 25%; padding:0 2px; }
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
    .modal p { margin:10px 0 14px; color:var(--muted); font-size:13px; }
    .modal input { width:100%; height:38px; border:1px solid var(--line); border-radius:7px; padding:0 10px; }
    .modal-actions { margin-top:14px; display:flex; justify-content:flex-end; gap:8px; }
    .danger-action { border:0; background:#b42318; color:#fff; font-weight:800; }
    @media (max-width: 1180px) { .dash { grid-template-columns:1fr; } .capital-hero { flex:none; } .chart-panel { min-height:324px; } }
    @media (max-width: 760px) {
      body { background:#f7f9fc; }
      main { padding:10px 10px 28px; max-width:none; display:flex; flex-direction:column; gap:12px; }
      h1 { font-size:23px; line-height:1.05; max-width:128px; }
      h2 { font-size:15px; }
      .dash, .left-stack, .right-stack { display:contents; }
      .left-titlebar { order:0; }
      .chart-panel { order:1; }
      .holdings-panel { order:2; }
      .capital-hero { order:3; }
      .annual-panel { order:4; }
      .events-panel { order:5; }
      .left-titlebar, .chart-panel, .holdings-panel, .capital-hero, .annual-panel, .events-panel { width:100%; }
      .left-titlebar { height:auto; min-height:48px; padding:6px 2px 10px; gap:8px; align-items:center; }
      .brand-lockup { gap:8px; flex:1 1 auto; }
      .brand-logo { width:38px; height:38px; border-radius:8px; }
      .brand-copy { gap:5px; }
      .dashboard-motto { font-size:11px; max-width:190px; }
      .title-actions { gap:7px; flex:0 0 auto; }
      .phase-chip { min-width:88px; height:34px; padding:0 10px; font-size:12px; }
      .phase-chip .phase-dot { width:8px; height:8px; }
      .refresh-btn { height:36px; padding:0 14px; border-radius:8px; }
      .phase-popover { top:64px; left:10px; width:calc(100vw - 20px); padding:12px; }
      .panel { padding:12px; border-radius:10px; }
      .mobile-collapsible { padding:0; overflow:hidden; }
      .mobile-collapsible:not(.mobile-open) { min-height:0 !important; }
      .mobile-collapse-toggle { width:100%; height:48px; border:0; border-radius:0; background:#fff; display:flex; align-items:center; justify-content:space-between; padding:0 14px; font-size:15px; font-weight:850; color:var(--ink); }
      .mobile-collapse-toggle span:last-child { color:var(--blue); font-size:12px; font-weight:850; }
      .mobile-collapse-body { display:none; padding:12px; border-top:1px solid #eef2f6; }
      .mobile-collapsible:not(.mobile-open) > .mobile-collapse-body { display:none !important; }
      .mobile-collapsible.mobile-open .mobile-collapse-body { display:block; }
      .mobile-collapsible.mobile-open .mobile-collapse-toggle span:last-child::before { content:"收起"; }
      .mobile-collapsible:not(.mobile-open) .mobile-collapse-toggle span:last-child::before { content:"展开"; }
      .hero-top, .pool-grid { grid-template-columns:1fr; gap:10px; }
      .hero-donut { min-height:auto; padding:13px; }
      .hero-metrics-column { gap:10px; }
      .metric-grid { grid-template-columns:repeat(2, minmax(0,1fr)); gap:10px; }
      .metric { min-height:70px; padding:12px; }
      .metric-label, .pool-meta, .small-muted { font-size:11px; }
      .metric-value { font-size:17px; margin-top:7px; }
      .risk-strip { padding:12px; }
      .risk-topbar, .risk-head { align-items:flex-start; }
      .risk-topbar { display:grid; grid-template-columns:1fr; gap:10px; }
      .risk-line { gap:10px; line-height:1.5; }
      .risk-actions { width:100%; justify-content:flex-end; }
      .rebalance-advice { min-height:0; }
      .clear-btn { height:32px; }
      .exposure-card { margin-top:12px; padding:12px; }
      .exposure-head { align-items:flex-start; flex-direction:column; gap:4px; }
      .exposure-value { line-height:1.35; }
      .pool-grid { margin-top:14px; }
      .pool-card { min-height:112px; padding:12px; }
      .pool-value { font-size:25px; }
      .pool-amounts { font-size:11px; gap:6px; }
      .annual-panel { min-height:auto; }
      .annual-panel .mobile-collapse-body { display:none; }
      .annual-panel.mobile-open .mobile-collapse-body { display:block; }
      .events-panel { min-height:auto; }
      .events-panel .mobile-collapse-body { display:none; }
      .events-panel.mobile-open .mobile-collapse-body { display:block; }
      .annual-head { margin-bottom:10px; }
      .annual-grid { grid-template-columns:1fr; gap:9px; }
      .annual-goal { min-height:84px; padding:11px; }
      .donut-wrap { min-height:188px; justify-content:center; gap:14px; }
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
      .holdings-panel { min-height:520px; margin-top:12px; }
      .holding-head { flex-direction:column; align-items:stretch; gap:10px; margin:0 0 12px; }
      .holding-left-tools { flex-wrap:wrap; gap:8px; align-items:center; }
      .holding-left-tools h2 { flex-basis:6em; width:6em; min-width:6em; }
      .sync-positions-btn { order:2; height:32px; padding:0 11px; }
      .holding-tabs { order:3; width:100%; flex-wrap:nowrap; overflow-x:auto; justify-content:flex-start; padding:4px; }
      .holding-tab { min-width:52px; height:32px; }
      .holding-right-tools { width:100%; justify-content:flex-end; gap:9px; }
      .page-dots { margin-right:auto; min-width:42px; }
      .view-toggle-btn { height:34px; min-width:82px; }
      .scroll { overflow:auto; -webkit-overflow-scrolling:touch; }
      table { min-width:980px; }
      th, td { padding:9px 10px; font-size:12px; }
      th:first-child, td:first-child { position:sticky; left:0; z-index:1; background:#fff; }
      th:first-child { background:#eef2f6; }
      .market-toolbar { grid-template-columns:1fr; }
      .market-meta { gap:6px; }
      .market-pill { font-size:11px; }
      .d-panel { margin-top:12px; }
      .d-grid, .d-option-layout, .d-preview-grid, .d-help-grid { grid-template-columns:1fr; }
      .d-subpanel { padding:12px; }
      .d-option-scroll { max-height:520px; }
    }
  </style>
</head>
<body>
  <header>
  </header>
  <main>
    <section class="dash">
      <div class="left-stack">
        <div class="left-titlebar">
          <div class="brand-lockup"><img class="brand-logo" src="/assets/cszy_ultimate_logo.png" alt="CSZY Ultimate logo" /><div class="brand-copy"><h1>CSZY Ultimate V1</h1><div class="dashboard-motto">把每一次回撤，都变成下一次出手的纪律。上升启动趋势，龙头回调完毕上升趋势。</div></div></div>
          <div class="title-actions">
            <button class="phase-chip sleep" id="phaseChip" onclick="togglePhasePopover()"><span class="phase-dot"></span><span id="phaseChipText">阶段 --</span></button>
            <button class="refresh-btn" onclick="loadAll()">刷新</button>
          </div>
        </div>
        <div class="phase-popover" id="phasePopover"></div>
        <div class="panel capital-hero mobile-collapsible" id="capitalPanel">
          <button class="mobile-collapse-toggle" onclick="toggleMobilePanel('capitalPanel')"><span>账户资金</span><span></span></button>
          <div class="mobile-collapse-body">
            <div class="hero-top">
              <div class="hero-donut">
                <div class="hero-donut-head">
                  <h2 id="toolsPanelTitle">资金比例</h2>
                  <div class="carousel-actions">
                    <span class="mode-pill" id="modeValue">--</span>
                    <button class="carousel-tab active" id="toolTabDonut" onclick="setToolsPage('donut')">资金</button>
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
                      <div class="bot-grid" id="botLights"></div>
                      <div class="bot-pager" id="botPager"></div>
                    </div>
                  </div>
                </div>
              </div>
              <div class="hero-metrics-column">
                <div class="metric-grid" id="metrics"></div>
                <div class="rebalance-advice" id="rebalanceAdvice"></div>
              </div>
            </div>
            <div class="risk-strip">
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
                    <option value="1.0">额度 100%</option>
                    <option value="1.1">额度 110%</option>
                    <option value="1.2">额度 120%</option>
                    <option value="1.3">额度 130%</option>
                    <option value="1.4">额度 140%</option>
                    <option value="1.5">额度 150%</option>
                  </select>
                </div>
              </div>
              <div class="risk-body">
                <div class="risk-line" id="risk"></div>
              </div>
            </div>
            <div class="exposure-card">
              <div class="exposure-head">
                <span>资金池使用率</span>
                <span class="exposure-value" id="exposureValue">--</span>
              </div>
              <div class="exposure-bar"><div class="exposure-fill" id="exposureFill"></div></div>
            </div>
            <div class="pool-grid" id="pools"></div>
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
        <div class="panel events-panel mobile-collapsible mobile-open" id="eventsPanel">
          <button class="mobile-collapse-toggle" onclick="toggleMobilePanel('eventsPanel')"><span>重大事件提醒</span><span></span></button>
          <div class="mobile-collapse-body">
            <div class="events-head">
              <h2>重大事件提醒</h2>
              <span class="events-kicker">Next 10</span>
            </div>
            <div class="event-list" id="majorEvents"></div>
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
    <section class="panel holdings-panel">
      <div class="section-head holding-head">
        <div class="holding-left-tools">
          <h2 id="lowerPanelTitle">持仓</h2>
          <button class="sync-positions-btn" id="syncPositionsBtn" onclick="syncPositions()">同步仓位</button>
          <div class="holding-tabs" id="holdingTabs">
            <button class="holding-tab active" data-holding="ALL">总</button>
            <button class="holding-tab" data-holding="A">A</button>
            <button class="holding-tab" data-holding="C">C</button>
            <button class="holding-tab" data-holding="B">B</button>
            <button class="holding-tab" data-holding="D">D</button>
            <button class="holding-tab" data-holding="Q">Q</button>
            <button class="holding-tab" data-holding="TRADES">交易</button>
          </div>
        </div>
        <div class="holding-right-tools">
          <div class="page-dots">
            <button class="page-dot active" id="dotHoldings" onclick="setLowerView('holdings')" title="持仓"></button>
            <button class="page-dot" id="dotMarket" onclick="setLowerView('market')" title="行情分析"></button>
            <button class="page-dot" id="dotD" onclick="setLowerView('d')" title="D 战术仓"></button>
            <button class="page-dot" id="dotTrades" onclick="setLowerView('trades')" title="交易记录"></button>
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
              <div class="d-subpanel" id="dOptionPanel">
                <div class="d-subhead">
                  <div>
                    <div class="d-subtitle">期权手动开仓</div>
                    <div class="d-submeta"><span class="d-code-pill">Q</span><span id="dOptionMeta">选择标的和类型</span></div>
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
          </div>
          <div class="lower-page">
            <div class="trade-records">
              <div class="trade-records-head">
                <span class="trade-records-title">今日交易记录</span>
                <span class="trade-records-count" id="tradeRecordsCount">--</span>
              </div>
              <div class="trade-records-scroll">
                <table id="tradeRecords"></table>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  </main>
  <div class="modal-backdrop" id="clearModal">
    <div class="modal">
      <h2>确认清仓</h2>
      <p>该操作会按 Alpaca 当前价提交 DAY + extended hours 限价卖单。可先预检价格，不会下单。</p>
      <input id="clearPassword" type="password" placeholder="操作密码" />
      <div class="modal-actions">
        <button onclick="closeClearModal()">取消</button>
        <button onclick="submitClearPosition(true)">预检</button>
        <button class="danger-action" onclick="submitClearPosition(false)">确认清仓</button>
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
    let latestBotHeartbeats = [];
    let latestBotControls = [];
    let dOptionSymbol = '';
    let dOptionMode = 'BULL_CALL';
    let dOptionWidth = 10;
    let dOptionQty = 1;
    let selectedDCombo = null;
    let dOptionScrollMode = 'preserve';
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
        return `
          <div class="annual-goal ${goal.key || ''}">
            <div class="annual-goal-top">
              <div><div class="annual-name">${goal.name || '--'}</div><div class="annual-desc">${goal.desc || ''}</div></div>
              <div class="annual-actions">${goal.step ? `<button class="annual-step-btn" onclick="advanceAnnualGoal('${goal.key}')">${goal.action_label || '+'}</button>` : ''}<span class="annual-pct">${pctText}</span></div>
            </div>
            <div class="annual-bar"><div class="annual-fill" style="width:${donePct}%"></div></div>
            <div class="annual-foot"><span>${extra}</span><span>${rawPct >= 100 ? '已达成' : '推进中'}</span></div>
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
    function renderMajorEvents(payload) {
      const box = document.getElementById('majorEvents');
      if (!box) return;
      if (!payload || !payload.ok) {
        box.innerHTML = `<div class="event-empty">事件日历暂不可用</div>`;
        return;
      }
      const rows = payload.rows || [];
      if (!rows.length) {
        box.innerHTML = `<div class="event-empty">${payload.message || '未来 45 天暂无重点事件'}</div>`;
        return;
      }
      const typeClass = {宏观:'macro', IPO:'ipo', 财报:'earnings'};
      box.innerHTML = rows.map(e => {
        const d = String(e.date || '').slice(5) || '--';
        const type = e.type || '--';
        const title = [e.symbol, e.title].filter(Boolean).join(' · ');
        return `<div class="event-row" title="${title}"><span class="event-date">${d}</span><span class="event-type ${typeClass[type] || ''}">${type}</span><span class="event-title">${title || '--'}</span><span class="event-impact">${e.impact || ''}</span></div>`;
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
      const riskPct = Number(cap.total_risk_percent || 0) * Number(cap.pool_risk_percents?.[g] || 0) * 100;
      return `<div class="pool-card"><div class="pool-head"><div><div class="pool-name">${g} 资金池</div><div class="small-muted">月度 ${basePct.toFixed(1)}% · 可开 ${riskPct.toFixed(0)}%</div></div><div class="small-muted">${w.toFixed(1)}%</div></div><div class="pool-value">${money(used)}</div><div class="pool-amounts"><span>月度目标 ${money(displayTarget)}</span><span>可开仓 ${money(av)}</span></div><div class="bar"><div class="fill" style="width:${w}%;background:${colors[g]}"></div></div></div>`;
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
        ['rebalance_bot','b_buy_bot','b_sell_bot','d_buy_bot','d_sell_bot'],
        ['dashboard_bot','risk_bot','ac_bot','q_sell_bot'],
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
    }
    function setBotPage(page) {
      botPage = Math.max(0, Math.min(Number(page || 0), 2));
      renderBots(latestBotHeartbeats, latestBotControls);
    }
    function setToolsPage(page) {
      toolsPage = page === 'bots' ? 'bots' : 'donut';
      const track = document.getElementById('toolsTrack');
      if (track) track.classList.toggle('bots', toolsPage === 'bots');
      document.getElementById('toolTabDonut')?.classList.toggle('active', toolsPage === 'donut');
      document.getElementById('toolTabBots')?.classList.toggle('active', toolsPage === 'bots');
      const title = document.getElementById('toolsPanelTitle');
      if (title) title.textContent = toolsPage === 'bots' ? '机器人' : '资金比例';
      if (toolsPage === 'donut' && window.latestCapitalPayload) setTimeout(() => drawDonut(window.latestCapitalPayload), 50);
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
      const rows = (payload && payload.ok ? payload.rows : []) || [];
      const countEl = document.getElementById('tradeRecordsCount');
      const tableEl = document.getElementById('tradeRecords');
      countEl.textContent = `${rows.length} 条`;
      if (!rows.length) {
        tableEl.innerHTML = `<tbody><tr><td class="small-muted" style="padding:18px;text-align:center;">今日暂无买卖机器人交易记录</td></tr></tbody>`;
        return;
      }
      const widths = [92, 82, 90, 160, 92, 108, 128, 248];
      const colgroup = `<colgroup>${widths.map(w => `<col style="width:${w}px">`).join('')}</colgroup>`;
      tableEl.innerHTML = `${colgroup}<thead><tr>${['时间','方向','策略','代码','数量','价格','状态','说明'].map(h=>`<th>${h}</th>`).join('')}</tr></thead><tbody>` +
        rows.map(r => {
          const side = String(r.side || '').toUpperCase();
          const isBotEvent = r.source === 'bot_lifecycle_events';
          const sideClass = side === 'SELL' || side === 'STOP' ? 'sell' : 'buy';
          const sideLabel = isBotEvent ? (side === 'STOP' ? '关闭' : '开启') : (side === 'SELL' ? '卖出' : '买入');
          const timeText = String(r.event_time || '').slice(11,19) || String(r.event_time || '').slice(0,16);
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
      drawChart(curve);
      renderTodayPnl(curve);
      renderTradeRecords(trades);
    }
    function renderDTactical(payload, options={}) {
      const underlyings = payload.option_underlyings || [];
      const modes = payload.option_modes || [];
      const candidates = payload.intraday_candidates || [];
      const hadOptionSymbol = !!dOptionSymbol;
      if (!dOptionSymbol && underlyings.length) dOptionSymbol = underlyings[0].symbol;
      document.getElementById('dIntradayCount').textContent = `${candidates.length} 条`;
      document.getElementById('dIntradayTable').innerHTML = candidates.length
        ? `<thead><tr><th>日期</th><th>代码</th><th>分数</th><th>确认</th><th>原因</th></tr></thead><tbody>${candidates.map(r => `<tr><td>${r.snapshot_date || ''}</td><td><b>${r.symbol}</b></td><td>${Number(r.score || 0).toFixed(1)}</td><td>${Number(r.confirmed || 0) ? '是' : '否'}</td><td>${r.reason || ''}</td></tr>`).join('')}</tbody>`
        : `<tbody><tr><td class="small-muted" style="padding:14px;text-align:center;">暂无 D 日内股票候选</td></tr></tbody>`;
      document.getElementById('dOptionSymbols').innerHTML = underlyings.map(r => `<button class="d-symbol-btn ${r.symbol === dOptionSymbol ? 'active' : ''}" onclick="selectDOptionSymbol('${r.symbol}')">${r.symbol}</button>`).join('');
      document.getElementById('dOptionModes').innerHTML = modes.map(r => `<button class="d-mode-btn ${r.mode === dOptionMode ? 'active' : ''}" title="${r.desc || ''}" onclick="selectDOptionMode('${r.mode}')">${r.label}</button>`).join('');
      renderDSection();
      if (lowerView === 'd' && dSection === 'options' && dOptionSymbol) loadDOptionPreview({center: options.centerOptionPreview || !hadOptionSymbol});
    }
    function renderDOptionPreview(payload) {
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
        return `<div class="d-preview-card"><div class="d-preview-top"><div><div class="d-preview-title">${idx === 0 ? '下周五' : '下下周五'} ${p.expiry}</div><div class="small-muted">${p.mode} · width ${Number(payload.width || p.width || 0).toFixed(2)}</div></div><div class="d-preview-actions"><label class="d-qty-control">× <input type="number" min="1" max="99" step="1" value="${dOptionQty}" onchange="changeDOptionQty(this.value)" oninput="changeDOptionQty(this.value)"></label><button class="d-confirm-btn" onclick="confirmDOptionBuy()" ${selectedDCombo ? '' : 'disabled'}>确认买入</button></div></div>${priceLine}${legs}</div>`;
      }).join('') || `<div class="d-note">暂无预览</div>`;
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
    }
    async function confirmDOptionBuy() {
      if (!selectedDCombo) { alert('请先选择一组期权组合'); return; }
      const r = selectedDCombo.row;
      const qty = Math.max(1, Math.min(99, Math.floor(Number(dOptionQty || 1) || 1)));
      selectedDCombo.qty = qty;
      const msg = `确认买入 ${qty} 组 ${selectedDCombo.symbol} ${selectedDCombo.mode} ${selectedDCombo.expiry}？\n限价 ${Number(r.alpaca_limit_price || 0).toFixed(2)}\n单组最大亏损 ${money(r.max_loss_per_spread)}\n总最大亏损 ${money(Number(r.max_loss_per_spread || 0) * qty)}`;
      if (!confirm(msg)) return;
      const result = await postJson('/api/d_option_buy', selectedDCombo);
      if (!result.ok) { alert(result.error || '期权买入失败'); return; }
      alert(`期权买入已提交\n张数 ${result.qty || qty}\n订单 ${result.order_id || '--'}\n状态 ${result.status || '--'}`);
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
    function renderHoldings() {
      const holdingGroup = currentHolding === 'Q' ? 'D' : currentHolding;
      const rows = holdingGroup === 'ALL'
        ? latestHoldings
        : latestHoldings.filter(r => String(r.strategy_group || '').toUpperCase() === holdingGroup);
      document.querySelectorAll('.holding-tab').forEach(b => b.classList.toggle('active', b.dataset.holding === currentHolding));
      const colCount = 14;
      const blanks = Array.from({length: Math.max(0, 10 - rows.length)}, () => `<tr>${Array.from({length: colCount}, (_, i) => `<td>${i === 0 ? '&nbsp;' : ''}</td>`).join('')}</tr>`).join('');
      document.getElementById('holdings').innerHTML = `<thead><tr>${['代码','策略组','状态','日涨跌','现价','数量','初始成本','均价','持仓市值','浮盈亏','浮盈亏%','已实现','持仓天数','更新时间'].map(h=>`<th>${h}</th>`).join('')}</tr></thead><tbody>` +
        rows.map(r => {
          const day = Number(r.day_change_pct || 0);
          return `<tr><td><b>${r.symbol}</b></td><td>${r.strategy_group}</td><td><span class="status ${r.status}">${r.status}</span></td><td class="${cls(day)}">${pct(day)}</td><td>${maybeMoney(r.current_price)}</td><td>${Number(r.qty||0).toFixed(4)}</td><td>${maybeMoney(r.initial_entry_price || r.avg_entry_price)}</td><td>${money(r.avg_entry_price)}</td><td>${money(r.market_value)}</td><td class="${cls(r.unrealized_pnl)}">${money(r.unrealized_pnl)}</td><td class="${cls(r.unrealized_pnl_pct)}">${pct(r.unrealized_pnl_pct)}</td><td class="${cls(r.realized_pnl)}">${money(r.realized_pnl)}</td><td>${r.holding_days || 0}</td><td>${r.last_update_time || ''}</td></tr>`;
        }).join('') +
        blanks + `</tbody>`;
    }
    function isDSectionHolding(value=currentHolding) {
      return value === 'D' || value === 'Q';
    }
    function isTradesHolding(value=currentHolding) {
      return value === 'TRADES';
    }
    function renderDSection() {
      const intraday = dSection === 'intraday';
      const intradayPanel = document.getElementById('dIntradayPanel');
      const optionPanel = document.getElementById('dOptionPanel');
      if (intradayPanel) intradayPanel.hidden = !intraday;
      if (optionPanel) optionPanel.hidden = intraday;
    }
    function renderLowerView() {
      const holdingsMode = lowerView === 'holdings';
      const marketMode = lowerView === 'market';
      const dMode = lowerView === 'd';
      const tradesMode = lowerView === 'trades';
      document.getElementById('lowerPanelTitle').textContent = tradesMode ? '今日交易记录' : dMode ? (dSection === 'intraday' ? 'D 日内交易' : 'Q 期权交易') : (marketMode ? '行情分析' : '持仓');
      document.getElementById('viewToggleBtn').textContent = marketMode ? (isDSectionHolding() ? '看D' : isTradesHolding() ? '看交易' : '看持仓') : '看行情';
      document.querySelector('.holdings-panel').classList.toggle('market-view', marketMode);
      document.querySelector('.holdings-panel').classList.toggle('d-view', dMode);
      document.querySelector('.holdings-panel').classList.toggle('trades-view', tradesMode);
      const track = document.getElementById('lowerTrack');
      track.classList.toggle('market', marketMode);
      track.classList.toggle('d', dMode);
      track.classList.toggle('trades', tradesMode);
      document.getElementById('dotHoldings').classList.toggle('active', holdingsMode);
      document.getElementById('dotMarket').classList.toggle('active', marketMode);
      document.getElementById('dotD').classList.toggle('active', dMode);
      document.getElementById('dotTrades').classList.toggle('active', tradesMode);
      renderDSection();
    }
    function setLowerView(view) {
      lowerView = view === 'market' ? 'market' : view === 'd' ? 'd' : view === 'trades' ? 'trades' : 'holdings';
      if (lowerView === 'trades') currentHolding = 'TRADES';
      if (lowerView === 'holdings' && isTradesHolding()) currentHolding = 'ALL';
      renderHoldings();
      renderLowerView();
      if (lowerView === 'market') loadMarketCategories(currentCategory);
      if (lowerView === 'd') loadDTactical();
      if (lowerView === 'trades') loadTradeRecords();
    }
    function toggleLowerView() {
      if (lowerView === 'market') setLowerView(isDSectionHolding() ? 'd' : 'holdings');
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
          return `<tr><td><b>${r.symbol}</b></td><td class="${cls(change)}">${pct(change)}</td><td>${money(r.open)}</td><td>${money(r.high)}</td><td>${money(r.low)}</td><td>${money(r.close)}</td><td>${compactNumber(r.volume)}</td></tr>`;
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
      return `<span class="risk-chip ${tone}">${label}=${value}</span>`;
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
      if (!exposureState) {
        const target = Number(risk?.recommended_exposure || 0);
        el.innerHTML = `
          <span class="rebalance-icon">调</span>
          <span class="rebalance-title">自动调仓 <span class="risk-chip info">等待建议</span></span>
          <span class="rebalance-detail"><span>目标仓位 ${target ? (target * 100).toFixed(0) + '%' : '--'}</span><span>rebalance_bot 未生成</span></span>
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
          <span>当前 ${(cur * 100).toFixed(1)}%</span>
          <span>目标 ${(target * 100).toFixed(1)}%</span>
          <span>差额 ${money(Math.abs(gap))}</span>
          <span>${mode}</span>
        </span>
      `;
    }
    async function updateRiskPreference(value) {
      const result = await postJson('/api/risk_settings', {risk_preference:value});
      if (!result.ok) { alert(result.error || '风险偏好更新失败'); return; }
      await loadAll();
    }
    async function updateMarginUsage(value) {
      const result = await postJson('/api/risk_settings', {margin_usage:value});
      if (!result.ok) { alert(result.error || '保证金额度更新失败'); return; }
      await loadAll();
    }
    async function loadAll() {
      const refreshBtn = document.querySelector('.refresh-btn');
      if (refreshBtn) refreshBtn.classList.add('loading');
      try {
      const [cap, risk, holdings, state, phase, dTactical, majorEvents] = await Promise.all([api('/api/capital'), api('/api/risk'), api('/api/holdings'), api('/api/state'), api('/api/trade_phase'), api('/api/d_tactical'), api('/api/major_events')]);
      if (cap.ok) {
        window.latestCapitalPayload = cap;
        document.getElementById('modeValue').textContent = cap.mode_label || cap.mode;
        document.getElementById('metrics').innerHTML = [
          metric('Equity', money(cap.equity)), metric('Buying Power', money(cap.buying_power)), metric('Portfolio', money(cap.portfolio_value)), metric('Cash', money(cap.cash))
        ].join('');
        renderAnnualGoals(cap.annual_goals || []);
        document.getElementById('pools').innerHTML = ['A','B','C','D','X','Z'].map(g => poolCard(g, cap)).join('');
        const usedTotal = Number(cap.used_total || 0);
        const usableTotal = Number(cap.usable_total || 0);
        const baseTotal = Number(cap.base_total || 0);
        const exposureBase = usableTotal > 0 ? usableTotal : baseTotal;
        const exposurePct = exposureBase > 0 ? Math.min(999, usedTotal / exposureBase * 100) : 0;
        const totalRiskPct = Number(cap.total_risk_percent || 0) * 100;
        document.getElementById('exposureValue').textContent = `${exposurePct.toFixed(1)}% / 可用${totalRiskPct.toFixed(0)}% / ${money(usedTotal)}`;
        document.getElementById('exposureFill').style.width = `${Math.min(100, exposurePct)}%`;
        const marginSelect = document.getElementById('marginUsageSelect');
        if (marginSelect) marginSelect.value = String((Number(cap.margin_usage_percent || cap.total_risk_percent || 1)).toFixed(1));
        drawDonut(cap);
      } else {
        document.getElementById('modeValue').textContent = 'ERROR';
        document.getElementById('metrics').innerHTML = metric('账户', cap.error || '不可用');
      }
      const tone = riskTone(risk);
      const riskSelect = document.getElementById('riskPreferenceSelect');
      if (riskSelect) riskSelect.value = risk.risk_preference || '中性';
      const marketExposure = Number(risk.recommended_exposure || 0);
      const rebalanceTarget = Number(state?.exposure_state?.target_exposure_pct ?? marketExposure);
      const targetTone = rebalanceTarget <= 0.1 ? 'danger' : rebalanceTarget < 0.5 ? 'warn' : 'ok';
      document.getElementById('risk').innerHTML = [
        riskChip('风险', Number(risk.risk_multiplier || 0).toFixed(2), tone),
        riskChip('日亏', pct(risk.daily_pnl_pct), Number(risk.daily_pnl_pct || 0) < 0 ? 'danger' : 'ok'),
        riskChip('连亏', risk.loss_days || 0, Number(risk.loss_days || 0) > 0 ? 'warn' : 'ok'),
        riskChip('回撤', pct(risk.max_drawdown), Number(risk.max_drawdown || 0) > 0.04 ? 'danger' : 'ok'),
        riskChip('市场仓位', `${(marketExposure * 100).toFixed(0)}%`, marketExposure < 0.5 ? 'warn' : 'ok'),
        riskChip('调仓目标', `${(rebalanceTarget * 100).toFixed(0)}%`, targetTone),
        riskChip('AC', (risk.block_a || risk.block_c) ? '警' : '正常', (risk.block_a || risk.block_c) ? 'danger' : 'ok'),
        riskChip('B', risk.block_b ? '警' : '正常', risk.block_b ? 'danger' : 'ok'),
        riskChip('D', risk.block_d ? '警' : '正常', risk.block_d ? 'danger' : 'ok')
      ].join('');
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
      renderMajorEvents(majorEvents);
      window.latestBotProcesses = state.bot_processes || [];
      latestBotHeartbeats = state.bot_heartbeats || [];
      latestBotControls = state.bot_controls || [];
      renderBots(latestBotHeartbeats, latestBotControls);
      renderPhase(phase);
      if (dTactical.ok) renderDTactical(dTactical);
      latestHoldings = holdings.rows || [];
      renderHoldings();
      renderLowerView();
      if (lowerView === 'market') await loadMarketCategories(currentCategory);
      await loadCurve(currentPeriod);
      } finally {
        if (refreshBtn) refreshBtn.classList.remove('loading');
      }
    }
    document.querySelectorAll('.tab').forEach(b => b.addEventListener('click', () => loadCurve(b.dataset.period)));
    document.querySelectorAll('.holding-tab').forEach(b => b.addEventListener('click', () => {
      currentHolding = b.dataset.holding;
      if (currentHolding === 'D') dSection = 'intraday';
      if (currentHolding === 'Q') dSection = 'options';
      renderHoldings();
      setLowerView(isTradesHolding() ? 'trades' : isDSectionHolding() ? 'd' : 'holdings');
    }));
    document.addEventListener('click', (e) => {
      const pop = document.getElementById('phasePopover');
      const chip = document.getElementById('phaseChip');
      if (pop && chip && !pop.contains(e.target) && !chip.contains(e.target)) pop.classList.remove('show');
    });
    let lowerTouchX = null;
    document.getElementById('lowerSlider').addEventListener('touchstart', (e) => {
      if (isMobileView()) return;
      lowerTouchX = e.touches?.[0]?.clientX ?? null;
    }, {passive:true});
    document.getElementById('lowerSlider').addEventListener('touchend', (e) => {
      if (isMobileView()) { lowerTouchX = null; return; }
      if (lowerTouchX === null) return;
      const endX = e.changedTouches?.[0]?.clientX ?? lowerTouchX;
      const dx = endX - lowerTouchX;
      lowerTouchX = null;
      if (Math.abs(dx) < 48) return;
      if (dx < 0) setLowerView(lowerView === 'market' ? 'd' : lowerView === 'd' ? 'trades' : 'market');
      else setLowerView('holdings');
    }, {passive:true});
    function openClearModal() {
      document.getElementById('clearPassword').value = '';
      document.getElementById('clearModal').classList.add('show');
      setTimeout(() => document.getElementById('clearPassword').focus(), 50);
    }
    function closeClearModal() { document.getElementById('clearModal').classList.remove('show'); }
    function clearResultText(result) {
      const rows = (result.results || []).slice(0, 12).map(r => `${r.symbol} qty=${Number(r.qty || 0).toFixed(6)} limit=${money(r.limit_price)} ${r.status || ''}${r.error ? ' ' + r.error : ''}`);
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
                result["message"] = f"清仓限价卖单{action}完成：成功={result.get('ok_count', 0)} 失败={result.get('error_count', 0)} 总数={result.get('count', 0)}"
                self._send_json(result)
            elif path == "/api/d_option_buy":
                self._send_json(submit_option_combo(payload))
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
                self._send_json({"ok": True, "bot_name": bot_name, "enabled": enabled, "running": running})
            elif path == "/api/risk_settings":
                risk_preference = str(payload.get("risk_preference") or "").strip()
                margin_usage = payload.get("margin_usage")
                response = {"ok": True}
                if risk_preference:
                    if risk_preference not in {"保守", "中性", "激进"}:
                        self._send_json({"ok": False, "error": "不支持的风险偏好"}, 400)
                        return
                    set_app_setting("RISK_PREFERENCE", risk_preference)
                    response["risk_preference"] = risk_preference
                if margin_usage is not None:
                    try:
                        margin_value = float(margin_usage)
                    except Exception:
                        margin_value = 0.0
                    if margin_value not in {1.0, 1.1, 1.2, 1.3, 1.4, 1.5}:
                        self._send_json({"ok": False, "error": "不支持的保证金额度"}, 400)
                        return
                    set_app_setting("RISK_TOTAL_CAPITAL_PCT", f"{margin_value:.1f}")
                    response["margin_usage"] = margin_value
                if "risk_preference" not in response and "margin_usage" not in response:
                    self._send_json({"ok": False, "error": "没有可更新的设置"}, 400)
                    return
                try:
                    refresh_exposure_plan(mode="SUGGEST", execute=True)
                except Exception as exc:
                    response["exposure_refresh_error"] = str(exc)[:180]
                self._send_json(response)
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
    sync_from_controls()
    server = ThreadingHTTPServer((s.web_host, s.web_port), Handler)
    print(f"[WEB] http://127.0.0.1:{s.web_port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    run()
