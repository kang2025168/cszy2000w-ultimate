from __future__ import annotations

"""Alpaca 访问封装：账户、持仓和下单接口都集中在这里。"""

from dataclasses import dataclass

from .config import alpaca_credentials, settings


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
