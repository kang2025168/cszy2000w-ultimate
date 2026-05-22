from __future__ import annotations

"""Alpaca 访问封装：账户、持仓和下单接口都集中在这里。"""

from dataclasses import dataclass
from datetime import date, timedelta

from .config import alpaca_credentials, env_str, settings


@dataclass
class AccountSnapshot:
    equity: float
    buying_power: float
    cash: float
    portfolio_value: float
    daytrade_buying_power: float | None = None


def trading_client():
    from alpaca.trading.client import TradingClient

    s = settings()
    key, secret, paper = alpaca_credentials(s)
    if not key or not secret:
        raise RuntimeError("缺少 Alpaca API 密钥")
    return TradingClient(key, secret, paper=paper)


def stock_data_client():
    from alpaca.data.historical import StockHistoricalDataClient

    s = settings()
    key, secret, _paper = alpaca_credentials(s)
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


def get_latest_stock_price(symbol: str, feed: str | None = None) -> float:
    """读取 Alpaca 最新股票成交价；没有成交价时用 bid/ask 中间价。"""
    from alpaca.data.requests import StockLatestQuoteRequest, StockLatestTradeRequest

    symbol = (symbol or "").strip().upper()
    if not symbol:
        return 0.0
    client = stock_data_client()
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


def get_account_snapshot() -> AccountSnapshot | None:
    """读取账户资金快照；失败时返回 None，调用方必须禁止新开仓。"""
    try:
        acct = trading_client().get_account()
        snap = AccountSnapshot(
            equity=float(getattr(acct, "equity", 0) or 0),
            buying_power=float(getattr(acct, "buying_power", 0) or 0),
            cash=float(getattr(acct, "cash", 0) or 0),
            portfolio_value=float(getattr(acct, "portfolio_value", 0) or 0),
            daytrade_buying_power=(
                float(getattr(acct, "daytrading_buying_power", 0) or 0)
                if getattr(acct, "daytrading_buying_power", None) is not None
                else None
            ),
        )
        return snap
    except Exception as exc:
        print(f"[ACCOUNT ERROR] cannot fetch Alpaca account: {exc}", flush=True)
        return None


def list_positions() -> list:
    return list(trading_client().get_all_positions())


def submit_market_sell(symbol: str, qty: float):
    """提交市价卖单，主要给 D 类收盘强平使用。"""
    from alpaca.trading.enums import OrderSide, TimeInForce
    from alpaca.trading.requests import MarketOrderRequest

    request = MarketOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.SELL,
        time_in_force=TimeInForce.DAY,
    )
    return trading_client().submit_order(order_data=request)


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
    """按 Alpaca 持仓 current_price 对所有股票持仓提交限价卖单。"""
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
        current_price = float(getattr(pos, "current_price", 0) or 0)
        limit_price = stock_limit_price(current_price)
        row = {
            "symbol": symbol,
            "qty": qty,
            "current_price": current_price,
            "limit_price": limit_price,
            "status": "DRY_RUN" if dry_run else "",
            "order_id": "",
            "error": "",
        }
        if not symbol or qty <= 0:
            row["status"] = "SKIPPED"
            row["error"] = "qty<=0 or empty symbol"
            results.append(row)
            continue
        if limit_price <= 0:
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
                    time_in_force=TimeInForce.DAY,
                    limit_price=limit_price,
                    extended_hours=True,
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
