from __future__ import annotations

"""Monthly scheduled buys for long-term pools.

A uses monthly retirement-account investing. C is a core holding / intraday T
pool, so it is intentionally excluded from automatic monthly buying by default.
"""

import json
import time
from datetime import date, datetime
from zoneinfo import ZoneInfo

from . import alpaca_gateway
from .capital_manager import get_capital_allocation
from .config import env_bool, env_float, env_int, env_str, settings
from .db import db_conn, fetch_all
from .schema import ensure_schema
from .state_store import get_app_setting, set_app_setting


CONFIG_KEY = "MONTHLY_INVEST_CONFIG"

DEFAULT_CONFIG = {
    "enabled": True,
    "auto_execute": False,
    "day": 15,
    "timezone": "America/Los_Angeles",
    "groups": {
        "A": {"enabled": True, "budget_fraction": 1.0, "max_symbols": 20, "order_type": "limit"},
        "C": {"enabled": False, "budget_fraction": 0.0, "max_symbols": 1, "order_type": "limit"},
    },
}


def _merge(default: dict, saved: dict) -> dict:
    merged = json.loads(json.dumps(default, ensure_ascii=False))
    if not isinstance(saved, dict):
        return merged
    for key in ("enabled", "auto_execute", "day", "timezone"):
        if key in saved:
            merged[key] = saved[key]
    if isinstance(saved.get("groups"), dict):
        for group in ("A", "C"):
            if isinstance(saved["groups"].get(group), dict):
                merged["groups"][group].update(saved["groups"][group])
    return merged


def load_monthly_invest_config() -> dict:
    raw = get_app_setting(CONFIG_KEY, "")
    try:
        saved = json.loads(raw) if raw else {}
    except Exception:
        saved = {}
    config = _merge(DEFAULT_CONFIG, saved)
    config["enabled"] = env_bool("MONTHLY_INVEST_ENABLED", bool(config.get("enabled")))
    config["auto_execute"] = env_bool("MONTHLY_INVEST_AUTO_EXECUTE", bool(config.get("auto_execute")))
    config["day"] = env_int("MONTHLY_INVEST_DAY", int(config.get("day") or 15))
    return config


def save_monthly_invest_config(config: dict) -> dict:
    merged = _merge(load_monthly_invest_config(), config if isinstance(config, dict) else {})
    merged["day"] = max(1, min(28, int(float(merged.get("day") or 15))))
    for group in ("A", "C"):
        rule = merged["groups"][group]
        rule["budget_fraction"] = max(0.0, min(1.0, float(rule.get("budget_fraction") or 0)))
        rule["max_symbols"] = max(1, int(float(rule.get("max_symbols") or 20)))
        rule["order_type"] = "limit"
    set_app_setting(CONFIG_KEY, json.dumps(merged, ensure_ascii=False))
    return merged


def _today(config: dict) -> date:
    tz_name = str(config.get("timezone") or env_str("TIMEZONE", "America/Los_Angeles"))
    try:
        return datetime.now(ZoneInfo(tz_name)).date()
    except Exception:
        return date.today()


def _columns(table: str) -> set[str]:
    rows = fetch_all(
        """
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s
        """,
        (table,),
    )
    return {str(row.get("COLUMN_NAME") or "") for row in rows}


def _load_targets(group: str, limit: int) -> list[dict]:
    s = settings()
    cols = _columns(s.ops_table)
    weight_expr = "1.0"
    for name in ("monthly_buy_weight", "target_weight", "weight", "target_percent"):
        if name in cols:
            weight_expr = f"COALESCE(`{name}`, 0)"
            break
    name_expr = "`stock_name`" if "stock_name" in cols else "''"
    current_expr = "`current_price`" if "current_price" in cols else "0"
    close_expr = "`close_price`" if "close_price" in cols else "0"
    can_buy_expr = "COALESCE(can_buy,1)=1" if "can_buy" in cols else "1=1"
    sql = f"""
    SELECT stock_code AS symbol,
           {name_expr} AS name,
           {weight_expr} AS raw_weight,
           {current_expr} AS current_price,
           {close_expr} AS close_price
    FROM `{s.ops_table}`
    WHERE UPPER(COALESCE(NULLIF(strategy_group,''), stock_type))=%s
      AND {can_buy_expr}
      AND stock_code IS NOT NULL
      AND stock_code <> ''
    ORDER BY COALESCE(is_bought,0) ASC, stock_code ASC
    LIMIT %s
    """
    rows = fetch_all(sql, (group, limit))
    return [row for row in rows if str(row.get("symbol") or "").strip()]


def _plan_group(group: str, rule: dict, execute: bool) -> dict:
    allocation = get_capital_allocation()
    if allocation is None:
        return {"group": group, "ok": False, "error": "capital_allocation_failed", "orders": []}
    available = float(allocation.available.get(group, 0.0) or 0.0)
    budget = max(0.0, available * float(rule.get("budget_fraction") or 0.0))
    targets = _load_targets(group, int(rule.get("max_symbols") or 20))
    positive_weights = [max(0.0, float(row.get("raw_weight") or 0.0)) for row in targets]
    total_weight = sum(positive_weights)
    if total_weight <= 0 and targets:
        positive_weights = [1.0 for _ in targets]
        total_weight = float(len(targets))

    client = None
    if execute and targets:
        client = alpaca_gateway.trading_client(pool=group)

    orders = []
    for row, weight in zip(targets, positive_weights):
        symbol = str(row.get("symbol") or "").strip().upper()
        price = alpaca_gateway.get_latest_stock_price(symbol, pool=group)
        notional = budget * (weight / total_weight) if total_weight > 0 else 0.0
        qty = int(notional / price) if price > 0 else 0
        order_row = {
            "symbol": symbol,
            "group": group,
            "weight": weight,
            "price": round(price, 4),
            "target_notional": round(notional, 2),
            "qty": qty,
            "status": "planned",
            "order_id": "",
            "error": "",
        }
        if qty <= 0:
            order_row["status"] = "skipped"
            order_row["error"] = "qty<=0"
        elif execute and client is not None:
            try:
                from alpaca.trading.enums import OrderSide, TimeInForce
                from alpaca.trading.requests import LimitOrderRequest

                req = LimitOrderRequest(
                    symbol=symbol,
                    qty=qty,
                    side=OrderSide.BUY,
                    limit_price=alpaca_gateway.stock_limit_price(price),
                    time_in_force=TimeInForce.DAY,
                )
                order = client.submit_order(order_data=req)
                order_row["status"] = str(getattr(order, "status", "") or "submitted")
                order_row["order_id"] = str(getattr(order, "id", "") or "")
            except Exception as exc:
                order_row["status"] = "error"
                order_row["error"] = str(exc)[:180]
        orders.append(order_row)
    return {
        "group": group,
        "ok": True,
        "available": round(available, 2),
        "budget": round(budget, 2),
        "target_count": len(targets),
        "execute": execute,
        "orders": orders,
    }


def run_monthly_investment(force: bool = False, execute: bool | None = None) -> dict:
    ensure_schema()
    config = load_monthly_invest_config()
    today = _today(config)
    execute = bool(config.get("auto_execute")) if execute is None else bool(execute)
    month_key = today.strftime("%Y-%m")
    last_key = get_app_setting("MONTHLY_INVEST_LAST_RUN_MONTH", "")
    due = bool(config.get("enabled")) and today.day == int(config.get("day") or 15)
    if not force and (not due or last_key == month_key):
        return {
            "ok": True,
            "skipped": True,
            "due": due,
            "today": today.isoformat(),
            "last_run_month": last_key,
            "config": config,
            "groups": [],
        }
    groups = []
    for group in ("A", "C"):
        rule = config.get("groups", {}).get(group, {})
        if not rule.get("enabled", True):
            groups.append({"group": group, "ok": True, "skipped": True, "orders": []})
            continue
        groups.append(_plan_group(group, rule, execute=execute))
    if execute and not any(not g.get("ok") for g in groups):
        set_app_setting("MONTHLY_INVEST_LAST_RUN_MONTH", month_key)
        set_app_setting("MONTHLY_INVEST_LAST_RUN_AT", datetime.now().isoformat(timespec="seconds"))
    return {
        "ok": not any(not g.get("ok") for g in groups),
        "skipped": False,
        "due": due,
        "today": today.isoformat(),
        "execute": execute,
        "config": config,
        "groups": groups,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run A/C monthly proportional buys.")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=int, default=21600)
    args = parser.parse_args()
    while True:
        result = run_monthly_investment(force=args.force, execute=False if args.dry_run else args.execute)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str), flush=True)
        if not args.loop:
            break
        time.sleep(max(int(args.interval or 21600), 60))
