from __future__ import annotations

"""Alpaca 访问封装：账户、持仓和下单接口都集中在这里。"""

from dataclasses import dataclass
from datetime import date, timedelta

from .account_config import credentials_for_profile
from .config import env_str, settings
from .yahoo_market_data import get_yahoo_latest_stock_price


@dataclass
class AccountSnapshot:
    equity: float
    buying_power: float
    cash: float
    portfolio_value: float
    non_marginable_buying_power: float = 0.0
    options_buying_power: float = 0.0
    regt_buying_power: float = 0.0
    daytrading_buying_power: float = 0.0
    multiplier: float = 0.0
    trading_blocked: bool = False
    account_blocked: bool = False
    trade_suspended_by_user: bool = False
    pattern_day_trader: bool = False
    daytrade_count: int = 0


def _float_attr(obj, name: str, default: float = 0.0) -> float:
    try:
        return float(getattr(obj, name, default) or default)
    except Exception:
        return default


def _int_attr(obj, name: str, default: int = 0) -> int:
    try:
        return int(float(getattr(obj, name, default) or default))
    except Exception:
        return default


def _bool_attr(obj, name: str, default: bool = False) -> bool:
    try:
        return bool(getattr(obj, name, default))
    except Exception:
        return default


def trading_client(pool: str | None = None, profile: str | None = None):
    from alpaca.trading.client import TradingClient

    key, secret, paper = credentials_for_profile(profile, pool)
    if not key or not secret:
        raise RuntimeError("缺少 Alpaca API 密钥")
    return TradingClient(key, secret, paper=paper)


def stock_data_client(pool: str | None = None, profile: str | None = None):
    from alpaca.data.historical import StockHistoricalDataClient

    key, secret, _paper = credentials_for_profile(profile, pool)
    if not key or not secret:
        raise RuntimeError("缺少 Alpaca API 密钥")
    return StockHistoricalDataClient(key, secret)


def get_daily_closes(symbol: str, days: int = 60, feed: str | None = None) -> list[float]:
    """读取 Alpaca 日线收盘价，用于风险机器人判断 QQQ 趋势。"""
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame

    end = date.today() + timedelta(days=1)
    start = end - timedelta(days=max(days * 2, 90))
    req = StockBarsRequest(
        symbol_or_symbols=[symbol],
        timeframe=TimeFrame.Day,
        start=start,
        end=end,
        feed=feed or env_str("ALPACA_DATA_FEED", "iex"),
        adjustment="all",
    )
    bars = stock_data_client().get_stock_bars(req)
    if bars is None or bars.df is None or bars.df.empty:
        return []
    df = bars.df.reset_index()
    df = df[df["symbol"] == symbol].sort_values("timestamp")
    closes = [float(v) for v in df["close"].tail(days).tolist() if float(v) > 0]
    return closes


def get_latest_stock_price(symbol: str, feed: str | None = None, pool: str | None = None, profile: str | None = None) -> float:
    """读取最新股票价格。

    默认使用 Alpaca，保持策略主流程只依赖券商接口。
    如需临时走 Yahoo，可设置 STOCK_PRICE_PROVIDER=yahoo。
    """
    provider = env_str("STOCK_PRICE_PROVIDER", "alpaca").lower()
    if provider == "yahoo":
        try:
            return get_yahoo_latest_stock_price(symbol)
        except Exception as exc:
            print(f"[PRICE] Yahoo latest price failed {symbol}: {exc}", flush=True)
            if env_str("STOCK_PRICE_FALLBACK_ALPACA", "0").lower() not in {"1", "true", "yes", "on"}:
                return 0.0

    from alpaca.data.requests import StockLatestQuoteRequest, StockLatestTradeRequest

    symbol = (symbol or "").strip().upper()
    if not symbol:
        return 0.0
    client = stock_data_client(pool=pool, profile=profile)
    feed_name = feed or env_str("ALPACA_DATA_FEED", "iex")

    try:
        trade_resp = client.get_stock_latest_trade(
            StockLatestTradeRequest(symbol_or_symbols=[symbol], feed=feed_name)
        )
        trade = trade_resp.get(symbol) if isinstance(trade_resp, dict) else getattr(trade_resp, symbol, None)
        price = float(getattr(trade, "price", 0) or 0)
        if price > 0:
            return price
    except Exception:
        pass

    try:
        quote_resp = client.get_stock_latest_quote(
            StockLatestQuoteRequest(symbol_or_symbols=[symbol], feed=feed_name)
        )
        quote = quote_resp.get(symbol) if isinstance(quote_resp, dict) else getattr(quote_resp, symbol, None)
        bid = float(getattr(quote, "bid_price", 0) or 0)
        ask = float(getattr(quote, "ask_price", 0) or 0)
        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
    except Exception:
        pass

    return 0.0


def get_account_snapshot(pool: str | None = None, profile: str | None = None) -> AccountSnapshot | None:
    """读取账户资金快照；失败时返回 None，调用方必须禁止新开仓。"""
    try:
        acct = trading_client(pool=pool, profile=profile).get_account()
        snap = AccountSnapshot(
            equity=_float_attr(acct, "equity"),
            buying_power=_float_attr(acct, "buying_power"),
            cash=_float_attr(acct, "cash"),
            portfolio_value=_float_attr(acct, "portfolio_value"),
            non_marginable_buying_power=_float_attr(acct, "non_marginable_buying_power"),
            options_buying_power=_float_attr(acct, "options_buying_power"),
            regt_buying_power=_float_attr(acct, "regt_buying_power"),
            daytrading_buying_power=_float_attr(acct, "daytrading_buying_power"),
            multiplier=_float_attr(acct, "multiplier"),
            trading_blocked=_bool_attr(acct, "trading_blocked"),
            account_blocked=_bool_attr(acct, "account_blocked"),
            trade_suspended_by_user=_bool_attr(acct, "trade_suspended_by_user"),
            pattern_day_trader=_bool_attr(acct, "pattern_day_trader"),
            daytrade_count=_int_attr(acct, "daytrade_count"),
        )
        return snap
    except Exception as exc:
        print(f"[ACCOUNT ERROR] cannot fetch Alpaca account: {exc}", flush=True)
        return None


def account_trade_block_reason(snap: AccountSnapshot | None) -> str:
    """返回账户级交易阻断原因；PDT/daytrade 字段已废弃，不再作为阻断条件。"""
    if snap is None:
        return "account_snapshot_unavailable"
    if snap.account_blocked:
        return "account_blocked"
    if snap.trading_blocked:
        return "trading_blocked"
    if snap.trade_suspended_by_user:
        return "trade_suspended_by_user"
    return ""


def list_positions(pool: str | None = None, profile: str | None = None) -> list:
    return list(trading_client(pool=pool, profile=profile).get_all_positions())


def submit_market_sell(symbol: str, qty: float, pool: str | None = None, profile: str | None = None):
    """兼容旧调用名：按当前实时价提交 DAY 限价卖单。"""
    from alpaca.trading.enums import OrderSide, TimeInForce
    from alpaca.trading.requests import LimitOrderRequest

    current_price = get_latest_stock_price(symbol, pool=pool, profile=profile)
    limit_price = stock_limit_price(current_price)
    if limit_price <= 0:
        raise RuntimeError(f"current price unavailable for {symbol}")
    request = LimitOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.SELL,
        limit_price=limit_price,
        time_in_force=TimeInForce.DAY,
    )
    return trading_client(pool=pool, profile=profile).submit_order(order_data=request)


def stock_limit_price(price: float) -> float:
    """股票限价精度：>=1 美元保留 2 位，低价股保留 4 位。"""
    price = float(price or 0.0)
    if price <= 0:
        return 0.0
    return round(price, 4 if price < 1 else 2)


def _is_equity_position(position) -> bool:
    asset_class = str(getattr(position, "asset_class", "") or "").upper()
    return "EQUITY" in asset_class or asset_class in {"US_EQUITY", "US_EQUITIES"}


def submit_current_price_limit_sell_all(dry_run: bool = False) -> dict:
    """对所有股票持仓按当前实时价提交 DAY 限价卖单。"""
    from alpaca.trading.enums import OrderSide, TimeInForce
    from alpaca.trading.requests import LimitOrderRequest

    client = trading_client()
    positions = client.get_all_positions() or []
    results = []

    for pos in positions:
        if not _is_equity_position(pos):
            continue
        symbol = str(getattr(pos, "symbol", "") or "").strip().upper()
        qty = float(getattr(pos, "qty", 0) or 0)
        current_price = get_latest_stock_price(symbol) or float(getattr(pos, "current_price", 0) or 0)
        row = {
            "symbol": symbol,
            "qty": qty,
            "current_price": current_price,
            "limit_price": 0.0,
            "status": "DRY_RUN" if dry_run else "",
            "order_id": "",
            "error": "",
        }
        if not symbol or qty <= 0:
            row["status"] = "SKIPPED"
            row["error"] = "qty<=0 or empty symbol"
            results.append(row)
            continue
        if current_price <= 0:
            row["status"] = "ERROR"
            row["error"] = "current_price missing"
            results.append(row)
            continue
        if not dry_run:
            try:
                req = LimitOrderRequest(
                    symbol=symbol,
                    qty=str(getattr(pos, "qty", qty)),
                    side=OrderSide.SELL,
                    limit_price=stock_limit_price(current_price),
                    time_in_force=TimeInForce.DAY,
                )
                order = client.submit_order(order_data=req)
                row["status"] = str(getattr(order, "status", "") or "")
                row["order_id"] = str(getattr(order, "id", "") or "")
            except Exception as exc:
                row["status"] = "ERROR"
                row["error"] = str(exc)
        results.append(row)

    ok_count = sum(1 for r in results if r.get("order_id") or r.get("status") == "DRY_RUN")
    error_count = sum(1 for r in results if r.get("error"))
    return {
        "dry_run": dry_run,
        "count": len(results),
        "ok_count": ok_count,
        "error_count": error_count,
        "results": results,
    }
