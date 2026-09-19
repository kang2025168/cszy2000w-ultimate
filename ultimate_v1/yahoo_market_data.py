from __future__ import annotations

"""Yahoo Finance 股票行情封装。

交易账户和下单仍然使用 Alpaca；这里仅负责股票价格/quote。
"""

import time
from dataclasses import dataclass, replace
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

from .config import env_float, env_int


@dataclass(frozen=True)
class YahooStockQuote:
    symbol: str
    last: float
    day_volume: int = 0
    bid: float = 0.0
    ask: float = 0.0
    day_open: float = 0.0
    day_high: float = 0.0
    day_low: float = 0.0
    regular_close: float = 0.0
    prev_close: float = 0.0
    source: str = "yahoo"
    as_of: str = ""


YAHOO_QUOTE_CACHE_SEC = env_int("YAHOO_QUOTE_CACHE_SEC", 15)
YAHOO_QUOTE_STALE_SEC = env_int("YAHOO_QUOTE_STALE_SEC", 300)
YAHOO_QUOTE_MIN_INTERVAL = env_float("YAHOO_QUOTE_MIN_INTERVAL", 0.8)
YAHOO_QUOTE_ERROR_COOLDOWN_SEC = env_float("YAHOO_QUOTE_ERROR_COOLDOWN_SEC", 45.0)
YAHOO_QUOTE_TIMEOUT_SEC = env_float("YAHOO_QUOTE_TIMEOUT_SEC", 5.0)
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
_last_fetch_ts = 0.0
_quote_cache: dict[str, tuple[float, YahooStockQuote]] = {}
_quote_error_until: dict[str, float] = {}


def _yahoo_symbol(symbol: str) -> str:
    """Translate broker symbols to Yahoo's class-share notation."""
    return (symbol or "").strip().upper().replace(".", "-")


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _fast_value(fast_info, *names: str) -> float:
    for name in names:
        try:
            value = fast_info[name]
        except Exception:
            try:
                value = getattr(fast_info, name, None)
            except Exception:
                value = None
        number = _safe_float(value)
        if number > 0:
            return number
    return 0.0


def _sleep_for_yahoo_rate_limit() -> None:
    global _last_fetch_ts
    now = time.time()
    gap = now - _last_fetch_ts
    if gap < YAHOO_QUOTE_MIN_INTERVAL:
        time.sleep(YAHOO_QUOTE_MIN_INTERVAL - gap)
    _last_fetch_ts = time.time()


def _is_rate_or_network_error(exc: Exception | None) -> bool:
    text = str(exc or "").lower()
    return any(
        token in text
        for token in (
            "too many requests",
            "rate limit",
            "rate_limited",
            "network is unreachable",
            "failed to establish a new connection",
            "max retries exceeded",
            "connection aborted",
            "connection reset",
            "timed out",
        )
    )


def _stale_quote(symbol: str, now: float, max_age: int = YAHOO_QUOTE_STALE_SEC) -> YahooStockQuote | None:
    cached = _quote_cache.get(symbol)
    if not cached:
        return None
    ts, quote = cached
    if (now - ts) > max_age:
        return None
    return replace(quote, source=f"{quote.source}_stale")


def _remember_yahoo_error(symbol: str, exc: Exception | None) -> None:
    if _is_rate_or_network_error(exc):
        _quote_error_until[symbol] = time.time() + YAHOO_QUOTE_ERROR_COOLDOWN_SEC


def _history_quote(ticker, symbol: str) -> YahooStockQuote | None:
    hist = ticker.history(period="5d", interval="1m", prepost=True)
    if hist is None or hist.empty:
        hist = ticker.history(period="5d")
    if hist is None or hist.empty:
        return None

    closes = [float(v) for v in hist["Close"].dropna().tolist() if float(v) > 0]
    if not closes:
        return None
    last = closes[-1]
    prev_close = closes[-2] if len(closes) >= 2 else 0.0
    day_open = _safe_float(hist["Open"].dropna().iloc[-1] if "Open" in hist else 0)
    day_high = _safe_float(hist["High"].dropna().max() if "High" in hist else 0)
    day_low = _safe_float(hist["Low"].dropna().min() if "Low" in hist else 0)
    as_of = ""
    try:
        as_of = str(hist.index[-1])
    except Exception:
        pass
    return YahooStockQuote(
        symbol=symbol,
        last=last,
        day_open=day_open,
        day_high=max(day_high, last),
        day_low=min(day_low if day_low > 0 else last, last),
        regular_close=last,
        prev_close=prev_close,
        source="yahoo_history",
        as_of=as_of,
    )


def _last_positive(values: list | None) -> float:
    for value in reversed(values or []):
        number = _safe_float(value)
        if number > 0:
            return number
    return 0.0


def _today_chart_volume(timestamps: list | None, volumes: list | None) -> int:
    market_tz = ZoneInfo("America/Los_Angeles")
    today = datetime.now(market_tz).date()
    total = 0
    for timestamp, volume in zip(timestamps or [], volumes or []):
        volume_int = int(_safe_float(volume))
        if volume_int <= 0:
            continue
        try:
            bar_date = datetime.fromtimestamp(int(timestamp), market_tz).date()
        except Exception:
            continue
        if bar_date == today:
            total += volume_int
    return total


def _chart_quote(symbol: str) -> YahooStockQuote | None:
    resp = requests.get(
        YAHOO_CHART_URL.format(symbol=symbol),
        params={"range": "1d", "interval": "1m", "includePrePost": "true"},
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=YAHOO_QUOTE_TIMEOUT_SEC,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"yahoo chart http {resp.status_code}: {resp.text[:120]}")
    js = resp.json()
    result = (((js.get("chart") or {}).get("result") or []) or [None])[0] or {}
    meta = result.get("meta") or {}
    quote = (((result.get("indicators") or {}).get("quote") or []) or [None])[0] or {}
    closes = quote.get("close") or []
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    volumes = quote.get("volume") or []
    timestamps = result.get("timestamp") or []

    # The metadata regularMarketPrice can stay pinned to the regular-session
    # price during pre/post-market. The 1m chart series carries the latest
    # extended-hours print, so prefer it when available.
    last = _last_positive(closes) or _safe_float(meta.get("regularMarketPrice"))
    prev_close = (
        _safe_float(meta.get("chartPreviousClose"))
        or _safe_float(meta.get("previousClose"))
        or _safe_float(meta.get("regularMarketPreviousClose"))
    )
    day_open = _last_positive(opens[:1]) or _safe_float(meta.get("regularMarketOpen")) or last
    day_high = max([_safe_float(v) for v in highs if _safe_float(v) > 0] or [last])
    day_low = min([_safe_float(v) for v in lows if _safe_float(v) > 0] or [last])
    day_volume = _today_chart_volume(timestamps, volumes)
    as_of = ""
    if timestamps:
        try:
            as_of = datetime.fromtimestamp(int(timestamps[-1])).isoformat(timespec="seconds")
        except Exception:
            pass

    if last <= 0:
        return None
    if prev_close <= 0:
        prev_close = last
    return YahooStockQuote(
        symbol=symbol,
        last=last,
        day_volume=day_volume,
        day_open=day_open,
        day_high=max(day_high, last),
        day_low=min(day_low, last),
        regular_close=prev_close,
        prev_close=prev_close,
        source="yahoo",
        as_of=as_of,
    )


def get_yahoo_stock_quote(symbol: str) -> YahooStockQuote:
    """读取 Yahoo 最新股票行情。

    返回字段尽量兼容原 Alpaca snapshot 使用方式。Yahoo 的 bid/ask 并非所有标的都提供；
    没有 bid/ask 时调用方应使用 last 作为限价基础。
    """
    symbol = (symbol or "").strip().upper()
    if not symbol:
        raise RuntimeError("empty symbol")
    yahoo_symbol = _yahoo_symbol(symbol)

    now = time.time()
    cached = _quote_cache.get(symbol)
    if cached and (now - cached[0]) <= YAHOO_QUOTE_CACHE_SEC:
        return cached[1]
    if now < _quote_error_until.get(symbol, 0):
        stale = _stale_quote(symbol, now)
        if stale:
            return stale

    _sleep_for_yahoo_rate_limit()
    try:
        quote = _chart_quote(yahoo_symbol)
        if quote:
            quote = replace(quote, symbol=symbol)
            _quote_cache[symbol] = (time.time(), quote)
            return quote
    except Exception as chart_exc:
        last_chart_exc = chart_exc
        _remember_yahoo_error(symbol, chart_exc)
        if _is_rate_or_network_error(chart_exc):
            stale = _stale_quote(symbol, time.time())
            if stale:
                return stale
            raise RuntimeError(f"Yahoo chart unavailable: {symbol}; {chart_exc}") from chart_exc
    else:
        last_chart_exc = None

    try:
        import yfinance as yf
    except Exception as exc:
        raise RuntimeError(f"Yahoo chart failed: {last_chart_exc}; yfinance unavailable: {exc}") from exc

    try:
        ticker = yf.Ticker(yahoo_symbol)
        fast_info = getattr(ticker, "fast_info", None)
        last = _fast_value(fast_info, "last_price", "lastPrice", "regular_market_price", "regularMarketPrice")
        prev_close = _fast_value(fast_info, "previous_close", "previousClose", "regular_market_previous_close", "regularMarketPreviousClose")
        day_open = _fast_value(fast_info, "open", "regular_market_open", "regularMarketOpen")
        day_high = _fast_value(fast_info, "day_high", "dayHigh", "regular_market_day_high", "regularMarketDayHigh")
        day_low = _fast_value(fast_info, "day_low", "dayLow", "regular_market_day_low", "regularMarketDayLow")
    except Exception as exc:
        _remember_yahoo_error(symbol, exc)
        stale = _stale_quote(symbol, time.time())
        if stale:
            return stale
        raise

    bid = 0.0
    ask = 0.0
    if last <= 0 or prev_close <= 0 or day_open <= 0 or day_high <= 0 or day_low <= 0:
        try:
            hist_quote = _history_quote(ticker, symbol)
            if hist_quote:
                last = last or hist_quote.last
                prev_close = prev_close or hist_quote.prev_close
                day_open = day_open or hist_quote.day_open
                day_high = day_high or hist_quote.day_high
                day_low = day_low or hist_quote.day_low
        except Exception as exc:
            _remember_yahoo_error(symbol, exc)
            stale = _stale_quote(symbol, time.time())
            if stale:
                return stale
            raise

    if last <= 0:
        stale = _stale_quote(symbol, time.time())
        if stale:
            return stale
        raise RuntimeError(f"yahoo quote missing price: {symbol}; chart_error={last_chart_exc}")
    if prev_close <= 0:
        prev_close = last
    if day_open <= 0:
        day_open = last
    if day_high <= 0:
        day_high = last
    if day_low <= 0:
        day_low = last

    quote = YahooStockQuote(
        symbol=symbol,
        last=last,
        bid=bid,
        ask=ask,
        day_open=day_open,
        day_high=max(day_high, last),
        day_low=min(day_low, last),
        regular_close=prev_close,
        prev_close=prev_close,
        source="yahoo",
        as_of=datetime.now().isoformat(timespec="seconds"),
    )
    _quote_cache[symbol] = (time.time(), quote)
    return quote


def get_yahoo_latest_stock_price(symbol: str) -> float:
    return float(get_yahoo_stock_quote(symbol).last or 0.0)


def get_yahoo_intraday_volume(symbol: str) -> int:
    return int(get_yahoo_stock_quote(symbol).day_volume or 0)
