# -*- coding: utf-8 -*-
"""
手动检查 Alpaca 是否能拉到真实期权 snapshot。

这个脚本只读取行情：
- 不下单
- 不写数据库
- 不依赖策略 C 的数据库表

用法：
    # 方式1：直接给 OCC 期权代码
    python app/check_alpaca_option_snapshot.py --symbol QQQ260619C00680000

    # 方式2：用 标的 + 到期日 + C/P + 行权价 生成 OCC 代码
    python app/check_alpaca_option_snapshot.py --underlying QQQ --expiry 2026-06-19 --cp C --strike 680

    # 如果你有 OPRA 订阅，可以试：
    C_OPTION_DATA_FEED=opra python app/check_alpaca_option_snapshot.py --underlying QQQ --expiry 2026-06-19 --cp C --strike 680

环境变量：
    APCA_API_KEY_ID / APCA_API_SECRET_KEY
    或 ALPACA_KEY / ALPACA_SECRET
    C_OPTION_DATA_FEED=indicative/opra，默认 indicative
"""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime


def _safe_float(v, default=0.0):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _safe_int(v, default=0):
    try:
        if v is None:
            return default
        return int(float(v))
    except Exception:
        return default


def _occ_option_symbol(underlying: str, expiry: str, cp: str, strike: float) -> str:
    root = (underlying or "").strip().upper()
    expiry_d = datetime.strptime(str(expiry)[:10], "%Y-%m-%d").date()
    yymmdd = expiry_d.strftime("%y%m%d")
    cp = (cp or "").strip().upper()[0]
    strike_int = int(round(float(strike) * 1000))
    return f"{root}{yymmdd}{cp}{strike_int:08d}"


def _parse_occ_symbol(symbol: str):
    m = re.match(r"^([A-Z]+)(\d{6})([CP])(\d{8})$", (symbol or "").strip().upper())
    if not m:
        return None
    root, yymmdd, cp, strike_raw = m.groups()
    expiry = datetime.strptime(yymmdd, "%y%m%d").date().strftime("%Y-%m-%d")
    strike = int(strike_raw) / 1000.0
    return {
        "underlying": root,
        "expiry": expiry,
        "cp": cp,
        "strike": strike,
    }


def _get_attr(obj, *names, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        for name in names:
            if name in obj:
                return obj.get(name)
        return default
    for name in names:
        if hasattr(obj, name):
            return getattr(obj, name)
    return default


def _to_jsonable(obj):
    try:
        if hasattr(obj, "model_dump"):
            return obj.model_dump(mode="json")
        if hasattr(obj, "dict"):
            return obj.dict()
        if isinstance(obj, dict):
            return obj
        return str(obj)
    except Exception:
        return str(obj)


def _quote_fields_from_snapshot(snap):
    latest_quote = _get_attr(snap, "latest_quote", "latestQuote")
    latest_trade = _get_attr(snap, "latest_trade", "latestTrade")
    daily_bar = _get_attr(snap, "daily_bar", "dailyBar")

    bid = _safe_float(_get_attr(latest_quote, "bid_price", "bp"))
    ask = _safe_float(_get_attr(latest_quote, "ask_price", "ap"))
    mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0
    spread = ask - bid if ask > 0 and bid > 0 else 0.0
    spread_pct = spread / mid if mid > 0 else 0.0

    last = _safe_float(_get_attr(latest_trade, "price", "p"))
    volume = _safe_int(_get_attr(daily_bar, "volume", "v"))
    open_interest = _safe_int(_get_attr(snap, "open_interest", "openInterest", "oi"))

    return {
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread": spread,
        "spread_pct": spread_pct,
        "last": last,
        "volume": volume,
        "open_interest": open_interest,
    }


def _option_client_and_feed():
    from alpaca.data import OptionHistoricalDataClient

    feed_name = os.getenv("C_OPTION_DATA_FEED", "indicative").strip().lower()
    feed = None
    try:
        from alpaca.data.enums import OptionsFeed
        feed = OptionsFeed.OPRA if feed_name == "opra" else OptionsFeed.INDICATIVE
    except Exception:
        pass

    key = os.getenv("APCA_API_KEY_ID", "") or os.getenv("ALPACA_KEY", "")
    secret = os.getenv("APCA_API_SECRET_KEY", "") or os.getenv("ALPACA_SECRET", "")
    if not key or not secret:
        raise RuntimeError("缺少 Alpaca key: APCA_API_KEY_ID / APCA_API_SECRET_KEY")

    return OptionHistoricalDataClient(key, secret), feed, feed_name


def fetch_option_snapshot(option_symbol: str, raw: bool = False):
    from alpaca.data.requests import OptionSnapshotRequest

    client, feed, feed_name = _option_client_and_feed()
    req_kwargs = {"symbol_or_symbols": [option_symbol]}
    if feed is not None:
        req_kwargs["feed"] = feed

    snapshots = client.get_option_snapshot(OptionSnapshotRequest(**req_kwargs))
    snap = snapshots.get(option_symbol) if isinstance(snapshots, dict) else None
    if not snap:
        print(f"[RESULT] 没有拿到 snapshot: {option_symbol}", flush=True)
        print(f"[DEBUG] response_type={type(snapshots)} response={snapshots}", flush=True)
        return

    fields = _quote_fields_from_snapshot(snap)

    print("=" * 72, flush=True)
    print(f"[OPTION CHECK] symbol={option_symbol} feed={feed_name}", flush=True)
    print(f"bid           = {fields['bid']}", flush=True)
    print(f"ask           = {fields['ask']}", flush=True)
    print(f"mid           = {fields['mid']}", flush=True)
    print(f"spread        = {fields['spread']}", flush=True)
    print(f"spread_pct    = {fields['spread_pct']:.2%}", flush=True)
    print(f"last_trade    = {fields['last']}", flush=True)
    print(f"daily_volume  = {fields['volume']}", flush=True)
    print(f"open_interest = {fields['open_interest']}", flush=True)

    missing = []
    if fields["bid"] <= 0:
        missing.append("bid")
    if fields["ask"] <= 0:
        missing.append("ask")
    strict_volume_oi = (feed_name == "opra")
    if strict_volume_oi and fields["volume"] <= 0:
        missing.append("daily_volume")
    if strict_volume_oi and fields["open_interest"] <= 0:
        missing.append("open_interest")

    if missing:
        print(f"[RESULT] 拿到了 snapshot，但这些字段缺失/为0: {', '.join(missing)}", flush=True)
    else:
        if strict_volume_oi:
            print("[RESULT] OK: bid/ask/volume/open_interest 都拿到了", flush=True)
        else:
            print("[RESULT] OK: indicative 可用于 bid/ask/spread；volume/open_interest 为0时不硬过滤", flush=True)

    if raw:
        print("[RAW]", flush=True)
        print(json.dumps(_to_jsonable(snap), ensure_ascii=False, indent=2, default=str)[:8000], flush=True)


def fetch_option_chain(underlying: str, expiry: str, cp: str, strike: float, strike_range: float, raw: bool = False):
    from alpaca.data.requests import OptionChainRequest

    client, feed, feed_name = _option_client_and_feed()

    opt_type = "call" if (cp or "").upper().startswith("C") else "put"
    try:
        from alpaca.data.enums import ContractType
        opt_type = ContractType.CALL if opt_type == "call" else ContractType.PUT
    except Exception:
        pass

    req_kwargs = {
        "underlying_symbol": underlying.upper(),
        "expiration_date": expiry,
        "type": opt_type,
        "strike_price_gte": max(float(strike) - float(strike_range), 0.0),
        "strike_price_lte": float(strike) + float(strike_range),
    }
    if feed is not None:
        req_kwargs["feed"] = feed

    chain = client.get_option_chain(OptionChainRequest(**req_kwargs))
    if not chain:
        print(
            f"[CHAIN] 没有返回合约: underlying={underlying} expiry={expiry} cp={cp} "
            f"strike≈{strike} range=±{strike_range} feed={feed_name}",
            flush=True,
        )
        return

    rows = []
    for sym, snap in chain.items():
        parsed = _parse_occ_symbol(sym)
        sym_strike = parsed["strike"] if parsed else 0.0
        fields = _quote_fields_from_snapshot(snap)
        rows.append((abs(sym_strike - float(strike)), sym, sym_strike, fields, snap))

    rows.sort(key=lambda x: x[0])

    print("=" * 72, flush=True)
    print(
        f"[CHAIN] underlying={underlying.upper()} expiry={expiry} cp={cp.upper()} "
        f"strike≈{strike} range=±{strike_range} feed={feed_name} returned={len(rows)}",
        flush=True,
    )
    for _, sym, sym_strike, f, _snap in rows[:30]:
        print(
            f"{sym:22s} strike={sym_strike:8.2f} "
            f"bid={f['bid']:.2f} ask={f['ask']:.2f} mid={f['mid']:.2f} "
            f"spr={f['spread_pct']:.1%} vol={f['volume']} oi={f['open_interest']}",
            flush=True,
        )

    if raw and rows:
        print("[RAW FIRST]", flush=True)
        print(json.dumps(_to_jsonable(rows[0][4]), ensure_ascii=False, indent=2, default=str)[:8000], flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", help="OCC option symbol，例如 QQQ260619C00680000")
    parser.add_argument("--underlying", help="标的，例如 QQQ")
    parser.add_argument("--expiry", help="到期日，例如 2026-06-19")
    parser.add_argument("--cp", choices=["C", "P", "c", "p"], help="C 或 P")
    parser.add_argument("--strike", type=float, help="行权价，例如 680")
    parser.add_argument("--chain", action="store_true", help="查询附近期权链，列出 Alpaca 实际返回的合约")
    parser.add_argument("--range", type=float, default=20.0, help="查询期权链时的行权价范围，默认 ±20")
    parser.add_argument("--raw", action="store_true", help="打印原始 snapshot")
    args = parser.parse_args()

    option_symbol = args.symbol
    if not option_symbol:
        if not (args.underlying and args.expiry and args.cp and args.strike):
            raise SystemExit("请提供 --symbol，或同时提供 --underlying --expiry --cp --strike")
        option_symbol = _occ_option_symbol(args.underlying, args.expiry, args.cp, args.strike)

    fetch_option_snapshot(option_symbol.strip().upper(), raw=args.raw)

    if args.chain:
        if args.underlying and args.expiry and args.cp and args.strike:
            chain_underlying = args.underlying
            chain_expiry = args.expiry
            chain_cp = args.cp
            chain_strike = args.strike
        else:
            parsed = _parse_occ_symbol(option_symbol)
            if not parsed:
                raise SystemExit("--chain 需要 --underlying/--expiry/--cp/--strike，或者可解析的 OCC --symbol")
            chain_underlying = parsed["underlying"]
            chain_expiry = parsed["expiry"]
            chain_cp = parsed["cp"]
            chain_strike = parsed["strike"]

        fetch_option_chain(
            chain_underlying,
            chain_expiry,
            chain_cp,
            chain_strike,
            args.range,
            raw=args.raw,
        )


if __name__ == "__main__":
    main()
