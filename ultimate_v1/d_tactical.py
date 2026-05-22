from __future__ import annotations

"""D 战术仓：日内候选和手动期权计划预览。"""

from datetime import date, timedelta
import os
from typing import Any

from . import alpaca_gateway
from .db import db_conn, fetch_all


D_OPTION_UNDERLYINGS_TABLE = "d_option_underlyings"
D_INTRADAY_CANDIDATES_TABLE = "d_intraday_candidates"
D_OPTION_PREVIEW_SIDE_LIMIT = int(os.getenv("D_OPTION_PREVIEW_SIDE_LIMIT", "24"))

OPTION_MODES = [
    {"mode": "BULL_CALL", "label": "看涨进攻", "desc": "Bull Call 借方价差"},
    {"mode": "BEAR_PUT", "label": "看跌进攻", "desc": "Bear Put 借方价差"},
    {"mode": "BULL_PUT", "label": "看涨收租", "desc": "Bull Put 信用价差"},
    {"mode": "BEAR_CALL", "label": "看跌收租", "desc": "Bear Call 信用价差"},
]

DEFAULT_UNDERLYINGS = [
    ("QQQ", "纳指 ETF"),
    ("SPY", "标普 ETF"),
    ("NVDA", "英伟达"),
    ("TSLA", "特斯拉"),
    ("AMD", "AMD"),
    ("AAPL", "苹果"),
    ("MSFT", "微软"),
    ("META", "Meta"),
]


def ensure_d_tactical_schema() -> None:
    """创建 D 战术仓需要的轻量表，并写入默认期权标的。"""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{D_OPTION_UNDERLYINGS_TABLE}` (
                  symbol VARCHAR(16) NOT NULL PRIMARY KEY,
                  label VARCHAR(64) NULL,
                  enabled TINYINT NOT NULL DEFAULT 1,
                  sort_order INT NOT NULL DEFAULT 100,
                  notes VARCHAR(255) NULL,
                  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{D_INTRADAY_CANDIDATES_TABLE}` (
                  id BIGINT AUTO_INCREMENT PRIMARY KEY,
                  snapshot_date DATE NOT NULL,
                  symbol VARCHAR(16) NOT NULL,
                  score DECIMAL(10,4) NOT NULL DEFAULT 0,
                  reason VARCHAR(500) NULL,
                  confirmed TINYINT NOT NULL DEFAULT 0,
                  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  UNIQUE KEY uniq_d_intraday_candidate (snapshot_date, symbol),
                  KEY idx_confirmed_score (confirmed, score)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            for i, (symbol, label) in enumerate(DEFAULT_UNDERLYINGS, start=1):
                cur.execute(
                    f"""
                    INSERT IGNORE INTO `{D_OPTION_UNDERLYINGS_TABLE}` (symbol, label, sort_order, notes)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (symbol, label, i * 10, "default D option watchlist"),
                )


def option_underlyings() -> list[dict]:
    ensure_d_tactical_schema()
    return fetch_all(
        f"""
        SELECT symbol, label, enabled, sort_order, notes, updated_at
        FROM `{D_OPTION_UNDERLYINGS_TABLE}`
        WHERE enabled=1
        ORDER BY sort_order, symbol
        """
    )


def intraday_candidates(limit: int = 80) -> list[dict]:
    ensure_d_tactical_schema()
    return fetch_all(
        f"""
        SELECT snapshot_date, symbol, score, reason, confirmed, updated_at
        FROM `{D_INTRADAY_CANDIDATES_TABLE}`
        ORDER BY snapshot_date DESC, confirmed DESC, score DESC, symbol
        LIMIT %s
        """,
        (int(limit),),
    )


def d_tactical_payload() -> dict:
    return {
        "ok": True,
        "option_underlyings": option_underlyings(),
        "option_modes": OPTION_MODES,
        "intraday_candidates": intraday_candidates(),
    }


def _next_two_target_fridays(today: date | None = None) -> list[date]:
    today = today or date.today()
    current_week_friday = today + timedelta(days=(4 - today.weekday()) % 7)
    next_week_friday = current_week_friday + timedelta(days=7)
    return [next_week_friday, next_week_friday + timedelta(days=7)]


def _latest_local_close(symbol: str) -> float:
    rows = fetch_all(
        """
        SELECT `close`
        FROM stock_prices_pool
        WHERE UPPER(symbol)=%s AND `close` IS NOT NULL
        ORDER BY `date` DESC
        LIMIT 1
        """,
        (symbol.upper(),),
    )
    if not rows:
        return 0.0
    try:
        return float(rows[0].get("close") or 0.0)
    except Exception:
        return 0.0


def _underlying_price(symbol: str) -> tuple[float, str]:
    try:
        from app.strategy_b import get_snapshot_realtime

        price, _prev_close, feed = get_snapshot_realtime(symbol)
        if float(price or 0) > 0:
            return float(price), f"alpaca_snapshot_{feed}"
    except Exception:
        pass
    price = alpaca_gateway.get_latest_stock_price(symbol)
    if price > 0:
        return price, "alpaca_realtime"
    price = _latest_local_close(symbol)
    if price > 0:
        return price, "local_close"
    closes = alpaca_gateway.get_daily_closes(symbol, days=3)
    if closes:
        return float(closes[-1]), "alpaca_daily"
    return 0.0, "missing"


def _mode_cp(strategy_c, mode: str) -> str:
    if mode in {strategy_c.MODE_BULL_CALL, strategy_c.MODE_BEAR_CALL}:
        return "C"
    return "P"


def _option_rows_around_price(strategy_c, symbol: str, mode: str, expiry: date, price: float, width: float) -> list[dict]:
    cp = _mode_cp(strategy_c, mode)
    width = max(float(width or strategy_c.C_SPREAD_WIDTH or 1.0), 0.01)
    strike_range = max(strategy_c.C_OPTION_CHAIN_STRIKE_RANGE, width * 8.0, 80.0)
    chain_quotes = strategy_c._get_option_chain_quotes(symbol, expiry, cp, price, strike_range)
    strikes = sorted(chain_quotes.keys())
    side_limit = max(int(D_OPTION_PREVIEW_SIDE_LIMIT or 24), 3)
    below = [s for s in strikes if s < price][-side_limit:]
    above = [s for s in strikes if s > price][:side_limit]
    selected = sorted(below + above, reverse=True)
    rows = []
    for strike in selected:
        if mode == strategy_c.MODE_BULL_CALL:
            buy_strike = strike
            sell_strike = min(strikes, key=lambda s: abs(s - (buy_strike + width)), default=None)
            buy_label, sell_label = "BUY C", "SELL C"
        elif mode == strategy_c.MODE_BEAR_PUT:
            buy_strike = strike
            sell_strike = min(strikes, key=lambda s: abs(s - (buy_strike - width)), default=None)
            buy_label, sell_label = "BUY P", "SELL P"
        elif mode == strategy_c.MODE_BULL_PUT:
            sell_strike = strike
            buy_strike = min(strikes, key=lambda s: abs(s - (sell_strike - width)), default=None)
            buy_label, sell_label = "BUY P", "SELL P"
        elif mode == strategy_c.MODE_BEAR_CALL:
            sell_strike = strike
            buy_strike = min(strikes, key=lambda s: abs(s - (sell_strike + width)), default=None)
            buy_label, sell_label = "BUY C", "SELL C"
        else:
            continue
        if buy_strike is None or sell_strike is None:
            continue
        actual_width = abs(float(sell_strike) - float(buy_strike))
        if actual_width <= 0:
            continue
        buy_q = chain_quotes[buy_strike]
        sell_q = chain_quotes[sell_strike]

        def quote_dict(q):
            bid = float(q.bid or 0)
            ask = float(q.ask or 0)
            mid = round((bid + ask) / 2.0, 2) if bid > 0 and ask > 0 else 0.0
            return {
                "option_symbol": q.option_symbol,
                "bid": bid,
                "ask": ask,
                "mid": mid,
            }

        buy_quote = quote_dict(buy_q)
        sell_quote = quote_dict(sell_q)
        if mode in strategy_c.DEBIT_MODES:
            spread_mid = round(max(buy_quote["mid"] - sell_quote["mid"], 0.0), 2)
            alpaca_limit_price = spread_mid
            max_loss_per_spread = round(spread_mid * 100.0, 2)
            price_label = "预估成本"
        else:
            spread_mid = round(max(sell_quote["mid"] - buy_quote["mid"], 0.0), 2)
            alpaca_limit_price = -spread_mid
            max_loss_per_spread = round(max(actual_width - spread_mid, 0.0) * 100.0, 2)
            price_label = "预估收款"
        rows.append(
            {
                "cp": cp,
                "strike": float(strike),
                "width": round(actual_width, 2),
                "spread_mid": spread_mid,
                "alpaca_limit_price": alpaca_limit_price,
                "max_loss_per_spread": max_loss_per_spread,
                "price_label": price_label,
                "buy": {
                    "label": buy_label,
                    "strike": float(buy_strike),
                    **buy_quote,
                },
                "sell": {
                    "label": sell_label,
                    "strike": float(sell_strike),
                    **sell_quote,
                },
                "side": "below" if strike < price else "above",
                "distance": round(float(strike) - float(price), 2),
            }
        )
    return rows


def submit_option_combo(payload: dict) -> dict:
    """按页面选中的 D 期权组合提交 1 组 MLEG 限价开仓单。"""
    from app import strategy_c

    symbol = str(payload.get("symbol") or "").strip().upper()
    mode = str(payload.get("mode") or "").strip().upper()
    expiry = date.fromisoformat(str(payload.get("expiry") or "")[:10])
    row = payload.get("row") or {}
    buy = row.get("buy") or {}
    sell = row.get("sell") or {}
    if not symbol or mode not in {m["mode"] for m in OPTION_MODES}:
        raise RuntimeError("invalid symbol or mode")

    price, price_source = _underlying_price(symbol)
    buy_leg = strategy_c.OptionLeg(
        "BUY",
        str(buy.get("label") or "").split()[-1] or _mode_cp(strategy_c, mode),
        float(buy.get("strike") or 0),
        option_symbol=str(buy.get("option_symbol") or ""),
    )
    sell_leg = strategy_c.OptionLeg(
        "SELL",
        str(sell.get("label") or "").split()[-1] or _mode_cp(strategy_c, mode),
        float(sell.get("strike") or 0),
        option_symbol=str(sell.get("option_symbol") or ""),
    )
    if not buy_leg.option_symbol or not sell_leg.option_symbol:
        raise RuntimeError("missing option symbol")

    plan = strategy_c.SpreadPlan(
        underlying=symbol,
        mode=mode,
        expiry=expiry,
        underlying_price=round(float(price or 0), 2),
        width=abs(float(buy_leg.strike) - float(sell_leg.strike)),
        legs=[buy_leg, sell_leg],
        signal_score=0.0,
        signal_reason=f"D manual option buy price_source={price_source}",
        status="PLANNED",
    )
    limit_price = float(row.get("alpaca_limit_price") or 0)
    if limit_price == 0:
        raise RuntimeError("invalid limit price")
    pricing = strategy_c.SpreadPricing(
        entry_price=abs(float(row.get("spread_mid") or limit_price)),
        alpaca_limit_price=round(limit_price, 2),
        max_loss_per_spread=float(row.get("max_loss_per_spread") or 0),
        qty=1,
        buying_power=0.0,
        reason="D manual selected combo qty=1",
    )
    order = strategy_c.submit_open_spread_order(plan, pricing)
    return {
        "ok": True,
        "symbol": symbol,
        "mode": mode,
        "expiry": expiry.isoformat(),
        "limit_price": pricing.alpaca_limit_price,
        "qty": 1,
        "order_id": str(getattr(order, "id", "") or ""),
        "status": str(getattr(order, "status", "") or ""),
    }


def _plan_to_dict(plan: Any, pricing: Any | None, error: str = "") -> dict:
    legs = []
    for leg in getattr(plan, "legs", []) or []:
        legs.append(
            {
                "side": leg.side,
                "cp": leg.cp,
                "strike": float(leg.strike),
                "qty": int(getattr(leg, "qty", 1) or 1),
                "option_symbol": getattr(leg, "option_symbol", "") or "",
            }
        )
    return {
        "underlying": plan.underlying,
        "mode": plan.mode,
        "expiry": plan.expiry.isoformat(),
        "underlying_price": float(plan.underlying_price or 0),
        "width": float(plan.width or 0),
        "legs": legs,
        "entry_price": float(getattr(pricing, "entry_price", 0) or 0) if pricing else 0,
        "alpaca_limit_price": float(getattr(pricing, "alpaca_limit_price", 0) or 0) if pricing else 0,
        "max_loss_per_spread": float(getattr(pricing, "max_loss_per_spread", 0) or 0) if pricing else 0,
        "qty": int(getattr(pricing, "qty", 0) or 0) if pricing else 0,
        "pricing_reason": str(getattr(pricing, "reason", "") or "") if pricing else "",
        "option_rows": getattr(plan, "_d_option_rows", []),
        "error": error,
    }


def _price_exact_expiry_plan(strategy_c, plan, expiry: date):
    plan.expiry = expiry
    buy_leg = next((leg for leg in plan.legs if leg.side.upper() == "BUY"), None)
    sell_leg = next((leg for leg in plan.legs if leg.side.upper() == "SELL"), None)
    if not buy_leg or not sell_leg:
        raise RuntimeError("spread plan missing BUY/SELL legs")

    cp = buy_leg.cp
    center = (float(buy_leg.strike) + float(sell_leg.strike)) / 2.0
    strike_range = max(strategy_c.C_OPTION_CHAIN_STRIKE_RANGE, abs(float(sell_leg.strike) - float(buy_leg.strike)) * 2.5)
    chain_quotes = strategy_c._get_option_chain_quotes(plan.underlying, expiry, cp, center, strike_range)
    if len(chain_quotes) < 2:
        raise RuntimeError(f"chain empty expiry={expiry} cp={cp}")

    pairs = []
    strikes = sorted(chain_quotes.keys())
    for s1 in strikes:
        for s2 in strikes:
            if abs(s1 - s2) < 0.01:
                continue
            if plan.mode == strategy_c.MODE_BULL_CALL and not (s1 < s2):
                continue
            if plan.mode == strategy_c.MODE_BEAR_PUT and not (s1 > s2):
                continue
            if plan.mode == strategy_c.MODE_BULL_PUT and not (s1 < s2):
                continue
            if plan.mode == strategy_c.MODE_BEAR_CALL and not (s1 > s2):
                continue
            width = abs(float(s2) - float(s1))
            if width <= 0 or width > max(strategy_c.C_SPREAD_WIDTH * 2.0, strategy_c.C_SPREAD_WIDTH + 5.0):
                continue
            score = abs(s1 - float(buy_leg.strike)) + abs(s2 - float(sell_leg.strike))
            score += abs(width - strategy_c.C_SPREAD_WIDTH) * 0.25
            pairs.append((score, s1, s2))

    last_error = ""
    for _score, buy_strike, sell_strike in sorted(pairs)[:40]:
        buy_q = chain_quotes[buy_strike]
        sell_q = chain_quotes[sell_strike]
        test_plan = strategy_c._clone_plan_with_chain_strikes(
            plan,
            strategy_c.OptionLeg("BUY", cp, buy_strike, option_symbol=buy_q.option_symbol),
            strategy_c.OptionLeg("SELL", cp, sell_strike, option_symbol=sell_q.option_symbol),
            expiry,
        )
        try:
            old_max_risk = strategy_c.C_MAX_RISK_PER_TRADE
            old_max_usage = strategy_c.C_MAX_OPTIONS_BP_USAGE
            strategy_c.C_MAX_RISK_PER_TRADE = 100000.0
            strategy_c.C_MAX_OPTIONS_BP_USAGE = 100000.0
            pricing = strategy_c._price_spread_from_quotes(
                test_plan,
                {buy_q.option_symbol: buy_q, sell_q.option_symbol: sell_q},
                active_options_usage=0.0,
                options_buying_power_override=100000.0,
            )
            strategy_c.C_MAX_RISK_PER_TRADE = old_max_risk
            strategy_c.C_MAX_OPTIONS_BP_USAGE = old_max_usage
            return test_plan, pricing
        except Exception as exc:
            strategy_c.C_MAX_RISK_PER_TRADE = old_max_risk
            strategy_c.C_MAX_OPTIONS_BP_USAGE = old_max_usage
            last_error = str(exc)
    raise RuntimeError(last_error or "no tradable option pair")


def option_preview(symbol: str, mode: str, width: float | None = None) -> dict:
    ensure_d_tactical_schema()
    symbol = (symbol or "").strip().upper()
    mode = (mode or "").strip().upper()
    valid_modes = {row["mode"] for row in OPTION_MODES}
    if mode not in valid_modes:
        raise RuntimeError(f"unsupported D option mode: {mode}")

    from app import strategy_c

    width = float(width or strategy_c.C_SPREAD_WIDTH or 10.0)
    price, price_source = _underlying_price(symbol)
    if price <= 0:
        raise RuntimeError(f"cannot resolve underlying price for {symbol}")
    market = {"score": 0.0, "reason": f"D manual option preview price_source={price_source}", "price": price}
    previews = []
    for expiry in _next_two_target_fridays():
        old_width = strategy_c.C_SPREAD_WIDTH
        strategy_c.C_SPREAD_WIDTH = width
        base_plan = strategy_c.build_spread_plan(symbol, mode, price, market)
        strategy_c.C_SPREAD_WIDTH = old_width
        if base_plan is None:
            raise RuntimeError(f"failed to build spread plan for {symbol} {mode}")
        option_rows = []
        try:
            option_rows = _option_rows_around_price(strategy_c, symbol, mode, expiry, price, width)
        except Exception:
            option_rows = []
        try:
            plan, pricing = _price_exact_expiry_plan(strategy_c, base_plan, expiry)
            plan._d_option_rows = option_rows
            previews.append(_plan_to_dict(plan, pricing))
        except Exception as exc:
            base_plan.expiry = expiry
            strategy_c._attach_option_symbols(base_plan)
            base_plan._d_option_rows = option_rows
            previews.append(_plan_to_dict(base_plan, None, str(exc)))

    return {
        "ok": True,
        "symbol": symbol,
        "mode": mode,
        "width": width,
        "price": price,
        "price_source": price_source,
        "expiries": [d.isoformat() for d in _next_two_target_fridays()],
        "previews": previews,
    }
