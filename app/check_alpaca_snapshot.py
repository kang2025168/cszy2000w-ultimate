# -*- coding: utf-8 -*-
"""
手动检查 Alpaca snapshot 是否能拿到策略F需要的实时字段。

用法：
    python app/check_alpaca_snapshot.py CAR
    python app/check_alpaca_snapshot.py CAR QQQ TSLA

只读取行情，不下单，不写数据库。
"""

import json
import sys

from app.strategy_b import B_DATA_FEED, _snapshot_http


def _safe_float(v, default=0.0):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def check_symbol(code: str):
    code = (code or "").strip().upper()
    if not code:
        return

    print("=" * 72, flush=True)
    print(f"[CHECK] symbol={code} feed={B_DATA_FEED}", flush=True)

    r = _snapshot_http(code, B_DATA_FEED)
    print(f"[HTTP] status={r.status_code}", flush=True)
    if r.status_code != 200:
        print(r.text[:1000], flush=True)
        return

    js = r.json()
    latest_trade = js.get("latestTrade") or {}
    latest_quote = js.get("latestQuote") or {}
    daily_bar = js.get("dailyBar") or {}
    prev_daily_bar = js.get("prevDailyBar") or {}

    last = _safe_float(latest_trade.get("p"))
    day_open = _safe_float(daily_bar.get("o"))
    day_high = _safe_float(daily_bar.get("h"))
    day_low = _safe_float(daily_bar.get("l"))
    day_close = _safe_float(daily_bar.get("c"))
    prev_close = _safe_float(prev_daily_bar.get("c"))
    bid = _safe_float(latest_quote.get("bp"))
    ask = _safe_float(latest_quote.get("ap"))

    price = last or day_close
    day_up = (price - prev_close) / prev_close if price > 0 and prev_close > 0 else 0.0
    day_range = day_high - day_low
    intraday_pos = (price - day_low) / day_range if price > 0 and day_range > 0 else 0.0

    print("[FIELDS]", flush=True)
    print(f"latestTrade.p  当前价     = {last}", flush=True)
    print(f"latestQuote.bp bid        = {bid}", flush=True)
    print(f"latestQuote.ap ask        = {ask}", flush=True)
    print(f"dailyBar.o     今日开盘   = {day_open}", flush=True)
    print(f"dailyBar.h     今日最高   = {day_high}", flush=True)
    print(f"dailyBar.l     今日最低   = {day_low}", flush=True)
    print(f"dailyBar.c     daily close= {day_close}", flush=True)
    print(f"prevDailyBar.c 昨收       = {prev_close}", flush=True)
    print("[CALC]", flush=True)
    print(f"price_used     = {price}", flush=True)
    print(f"day_up         = {day_up:.2%}", flush=True)
    print(f"intraday_pos   = {intraday_pos:.2f}", flush=True)

    missing = []
    if price <= 0:
        missing.append("price/latestTrade.p")
    if day_high <= 0:
        missing.append("dailyBar.h")
    if day_low <= 0:
        missing.append("dailyBar.l")
    if prev_close <= 0:
        missing.append("prevDailyBar.c")

    if missing:
        print(f"[RESULT] 缺字段: {', '.join(missing)}", flush=True)
    else:
        print("[RESULT] OK: 策略F需要的价格/高低点/昨收字段齐全", flush=True)

    if "--raw" in sys.argv:
        print("[RAW JSON]", flush=True)
        print(json.dumps(js, ensure_ascii=False, indent=2)[:5000], flush=True)


def main():
    symbols = [x for x in sys.argv[1:] if not x.startswith("--")]
    if not symbols:
        symbols = ["QQQ"]
    for sym in symbols:
        check_symbol(sym)


if __name__ == "__main__":
    main()


# python app/check_alpaca_snapshot.py CAR
