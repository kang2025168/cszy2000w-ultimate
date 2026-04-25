# -*- coding: utf-8 -*-
"""
app/strategy_c.py
策略C候选筛选：
从 stock_operations 所有 B 类股票中，筛选最近 N 个交易日内曾经出现
放量突破 / 加速上涨 / 接近新高 的股票。

只筛选，不买入，不写库。
"""

import os
import pymysql
import pandas as pd


# =========================
# DB
# =========================
OPS_TABLE = os.getenv("OPS_TABLE", "stock_operations")
PRICES_TABLE = os.getenv("B_PRICES_TABLE", "stock_prices_pool")

DB = dict(
    host=os.getenv("DB_HOST", "mysql"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASS", "mlp009988"),
    database=os.getenv("DB_NAME", "cszy2000"),
    charset="utf8mb4",
    autocommit=True,
    cursorclass=pymysql.cursors.DictCursor,
)


# =========================
# 策略C参数
# =========================
LOOKBACK_DAYS = int(os.getenv("C_LOOKBACK_DAYS", "120"))

# 最近几个交易日内曾经符合
SCAN_RECENT_DAYS = int(os.getenv("C_SCAN_RECENT_DAYS", "5"))

MIN_PRICE = float(os.getenv("C_MIN_PRICE", "3"))
MAX_PRICE = float(os.getenv("C_MAX_PRICE", "300"))

MIN_RET3 = float(os.getenv("C_MIN_RET3", "0.08"))
MIN_RET5 = float(os.getenv("C_MIN_RET5", "0.12"))
MIN_RET10 = float(os.getenv("C_MIN_RET10", "0.18"))

MIN_VOL_RATIO = float(os.getenv("C_MIN_VOL_RATIO", "1.5"))

MAX_DIST_HIGH20 = float(os.getenv("C_MAX_DIST_HIGH20", "0.08"))
MAX_DIST_HIGH60 = float(os.getenv("C_MAX_DIST_HIGH60", "0.12"))

TOP_N = int(os.getenv("C_TOP_N", "50"))


def connect():
    return pymysql.connect(**DB)


def load_b_symbols(conn):
    sql = f"""
    SELECT
        stock_code,
        trigger_price,
        is_bought,
        can_buy,
        can_sell,
        qty,
        cost_price,
        stop_loss_price,
        last_order_side,
        last_order_time
    FROM `{OPS_TABLE}`
    WHERE stock_type='B'
      AND stock_code IS NOT NULL
      AND stock_code <> ''
    ORDER BY stock_code;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall() or []

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["stock_code"] = df["stock_code"].astype(str).str.upper().str.strip()
    df = df[df["stock_code"] != ""].copy()
    return df


def debug_price_table(conn, symbols):
    print(f"[C] 使用价格表: {PRICES_TABLE}")

    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS n FROM `{PRICES_TABLE}`;")
        total = cur.fetchone() or {}
        print(f"[C] 价格表总行数: {int(total.get('n') or 0)}")

    if not symbols:
        return

    sample = symbols[:10]
    placeholders = ",".join(["%s"] * len(sample))

    sql = f"""
    SELECT symbol, COUNT(*) AS n, MAX(`date`) AS max_date
    FROM `{PRICES_TABLE}`
    WHERE UPPER(TRIM(symbol)) IN ({placeholders})
    GROUP BY symbol
    ORDER BY n DESC
    LIMIT 20;
    """
    with conn.cursor() as cur:
        cur.execute(sql, sample)
        rows = cur.fetchall() or []

    print(f"[C] 抽样检查前10个B股票是否有价格数据: {sample}")
    if rows:
        for r in rows:
            print(f"[C] price_match {r.get('symbol')} rows={r.get('n')} max_date={r.get('max_date')}")
    else:
        print("[C] 抽样没有匹配到价格数据，可能 stock_operations.stock_code 和 stock_prices_pool.symbol 不一致")


def load_price_data(conn, symbols):
    if not symbols:
        return pd.DataFrame()

    placeholders = ",".join(["%s"] * len(symbols))

    sql = f"""
    SELECT
        UPPER(TRIM(symbol)) AS symbol,
        CAST(`date` AS CHAR) AS date_str,
        open,
        high,
        low,
        close,
        volume
    FROM `{PRICES_TABLE}`
    WHERE UPPER(TRIM(symbol)) IN ({placeholders})
      AND `date` IS NOT NULL
      AND CAST(`date` AS CHAR) <> ''
      AND CAST(`date` AS CHAR) <> 'date'
    ORDER BY symbol, `date`;
    """

    with conn.cursor() as cur:
        cur.execute(sql, list(symbols))
        rows = cur.fetchall() or []

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["date"] = pd.to_datetime(df["date_str"], errors="coerce")

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["symbol", "date", "open", "high", "low", "close", "volume"])

    if df.empty:
        return df

    latest_day = df["date"].max()
    start_day = latest_day - pd.Timedelta(days=LOOKBACK_DAYS)

    df = df[df["date"] >= start_day].copy()
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

    print(f"[C] 价格数据范围: {df['date'].min().date()} ~ {df['date'].max().date()} rows={len(df)}")
    return df


def calc_features_for_one_day(symbol, g, idx):
    if idx < 30:
        return None

    sub = g.iloc[: idx + 1].copy()

    last = sub.iloc[-1]
    prev1 = sub.iloc[-2]
    prev3 = sub.iloc[-4] if len(sub) >= 4 else None
    prev5 = sub.iloc[-6] if len(sub) >= 6 else None
    prev10 = sub.iloc[-11] if len(sub) >= 11 else None
    prev20 = sub.iloc[-21] if len(sub) >= 21 else None

    close = float(last["close"])
    high = float(last["high"])
    low = float(last["low"])
    volume = float(last["volume"])

    ma3 = float(sub["close"].tail(3).mean())
    ma5 = float(sub["close"].tail(5).mean())
    ma10 = float(sub["close"].tail(10).mean())
    ma20 = float(sub["close"].tail(20).mean())
    ma60 = float(sub["close"].tail(60).mean()) if len(sub) >= 60 else 0

    vol5 = float(sub["volume"].tail(5).mean())
    vol20 = float(sub["volume"].tail(20).mean())

    high20 = float(sub["high"].tail(20).max())
    high60 = float(sub["high"].tail(60).max()) if len(sub) >= 60 else high20

    if close <= 0 or ma5 <= 0 or ma10 <= 0 or ma20 <= 0 or vol20 <= 0 or high20 <= 0:
        return None

    ret1 = close / float(prev1["close"]) - 1 if float(prev1["close"]) > 0 else 0
    ret3 = close / float(prev3["close"]) - 1 if prev3 is not None and float(prev3["close"]) > 0 else 0
    ret5 = close / float(prev5["close"]) - 1 if prev5 is not None and float(prev5["close"]) > 0 else 0
    ret10 = close / float(prev10["close"]) - 1 if prev10 is not None and float(prev10["close"]) > 0 else 0
    ret20 = close / float(prev20["close"]) - 1 if prev20 is not None and float(prev20["close"]) > 0 else 0

    vol_ratio = volume / vol20 if vol20 > 0 else 0
    vol5_ratio = vol5 / vol20 if vol20 > 0 else 0

    dist_high20 = close / high20 - 1 if high20 > 0 else 0
    dist_high60 = close / high60 - 1 if high60 > 0 else 0

    closes3 = list(sub["close"].tail(3))
    close_up_3 = len(closes3) == 3 and closes3[2] > closes3[1] > closes3[0]

    ma_bull = ma5 > ma10 > ma20
    above_ma20 = close > ma20
    near_high20 = dist_high20 >= -MAX_DIST_HIGH20
    near_high60 = dist_high60 >= -MAX_DIST_HIGH60

    # 起飞形态1：短期强加速
    strong_fast = (
        ret3 >= MIN_RET3
        and ret5 >= MIN_RET5
        and vol_ratio >= MIN_VOL_RATIO
        and near_high20
        and above_ma20
    )

    # 起飞形态2：10日趋势很强
    strong_trend = (
        ret10 >= MIN_RET10
        and vol5_ratio >= 1.2
        and near_high20
        and ma_bull
        and above_ma20
    )

    # 起飞形态3：突破新高附近，成交量明显放大
    breakout = (
        near_high20
        and near_high60
        and vol_ratio >= MIN_VOL_RATIO
        and ret5 >= 0.08
        and close > ma5 > ma10
    )

    passed = strong_fast or strong_trend or breakout

    if not passed:
        return None

    score = 0
    score += min(ret3 * 100, 25) * 2.0
    score += min(ret5 * 100, 35) * 2.0
    score += min(ret10 * 100, 60) * 1.2
    score += min(vol_ratio, 6) * 10
    score += min(vol5_ratio, 4) * 8
    score += 15 if ma_bull else 0
    score += 10 if close_up_3 else 0
    score += 10 if near_high20 else 0
    score += 8 if near_high60 else 0
    score += 8 if close > ma3 > ma5 else 0

    passed_type = "C_STRONG"
    if breakout and not strong_fast:
        passed_type = "C_BREAK"
    elif strong_trend and not strong_fast:
        passed_type = "C_TREND"

    return {
        "symbol": symbol,
        "signal_date": last["date"].strftime("%Y-%m-%d"),
        "close": close,
        "high": high,
        "low": low,
        "volume": int(volume),
        "ret1_pct": ret1 * 100,
        "ret3_pct": ret3 * 100,
        "ret5_pct": ret5 * 100,
        "ret10_pct": ret10 * 100,
        "ret20_pct": ret20 * 100,
        "ma3": ma3,
        "ma5": ma5,
        "ma10": ma10,
        "ma20": ma20,
        "ma60": ma60,
        "vol20": vol20,
        "vol_ratio": vol_ratio,
        "vol5_ratio": vol5_ratio,
        "high20": high20,
        "high60": high60,
        "dist_high20_pct": dist_high20 * 100,
        "dist_high60_pct": dist_high60 * 100,
        "ma_bull": int(ma_bull),
        "close_up_3": int(close_up_3),
        "strong_fast": int(strong_fast),
        "strong_trend": int(strong_trend),
        "breakout": int(breakout),
        "passed_type": passed_type,
        "score": score,
    }


def scan_candidates(conn):
    ops_df = load_b_symbols(conn)

    if ops_df.empty:
        print("[C] no B symbols found in stock_operations")
        return pd.DataFrame()

    symbols = sorted(ops_df["stock_code"].dropna().unique().tolist())
    print(f"[C] B股票池数量: {len(symbols)}")

    debug_price_table(conn, symbols)

    price_df = load_price_data(conn, symbols)
    if price_df.empty:
        print("[C] no price data found")
        return pd.DataFrame()

    all_hits = []

    for symbol, g in price_df.groupby("symbol"):
        g = g.sort_values("date").reset_index(drop=True)

        # 最近 N 个交易日里，只要某一天符合就选出来
        start_idx = max(30, len(g) - SCAN_RECENT_DAYS)

        for idx in range(start_idx, len(g)):
            item = calc_features_for_one_day(symbol, g, idx)
            if item:
                all_hits.append(item)

    if not all_hits:
        return pd.DataFrame()

    hits = pd.DataFrame(all_hits)

    # 同一股票最近5天可能多次命中，只保留 score 最高的一天
    hits = hits.sort_values(
        by=["symbol", "score", "signal_date"],
        ascending=[True, False, False],
    )
    hits = hits.drop_duplicates(subset=["symbol"], keep="first")

    out = hits.merge(
        ops_df,
        left_on="symbol",
        right_on="stock_code",
        how="left",
    )

    out = out.sort_values(
        by=["score", "ret5_pct", "ret10_pct", "vol_ratio"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    return out


def print_result(df):
    if df.empty:
        print("\n[C] 最近几天没有筛选出符合走势的股票")
        return

    print("\n" + "=" * 140)
    print(f"策略C候选：最近 {SCAN_RECENT_DAYS} 个交易日内，从所有 B 类股票中筛选起飞走势")
    print("=" * 140)

    show_cols = [
        "symbol",
        "passed_type",
        "score",
        "signal_date",
        "close",
        "ret1_pct",
        "ret3_pct",
        "ret5_pct",
        "ret10_pct",
        "ret20_pct",
        "vol_ratio",
        "vol5_ratio",
        "dist_high20_pct",
        "dist_high60_pct",
        "ma5",
        "ma10",
        "ma20",
        "is_bought",
        "can_buy",
    ]

    df2 = df.copy()

    for col in [
        "score",
        "close",
        "ret1_pct",
        "ret3_pct",
        "ret5_pct",
        "ret10_pct",
        "ret20_pct",
        "vol_ratio",
        "vol5_ratio",
        "dist_high20_pct",
        "dist_high60_pct",
        "ma5",
        "ma10",
        "ma20",
    ]:
        if col in df2.columns:
            df2[col] = df2[col].astype(float).round(2)

    print(df2[show_cols].head(TOP_N).to_string(index=False))

    print("\n" + "=" * 140)
    print("简版名单")
    print("=" * 140)

    for _, r in df2.head(TOP_N).iterrows():
        print(
            f"{r['symbol']:6s} "
            f"{r['passed_type']:9s} "
            f"date={r['signal_date']} "
            f"score={float(r['score']):6.2f} "
            f"close={float(r['close']):8.2f} "
            f"ret3={float(r['ret3_pct']):7.2f}% "
            f"ret5={float(r['ret5_pct']):7.2f}% "
            f"ret10={float(r['ret10_pct']):7.2f}% "
            f"volx={float(r['vol_ratio']):5.2f} "
            f"high20_dist={float(r['dist_high20_pct']):7.2f}% "
            f"is_bought={int(r.get('is_bought') or 0)} "
            f"can_buy={int(r.get('can_buy') or 0)}"
        )


def main():
    conn = None
    try:
        print("=" * 100)
        print("[C] strategy_c scan start")
        print(f"[C] DB_HOST={DB['host']} DB_NAME={DB['database']} OPS_TABLE={OPS_TABLE} PRICES_TABLE={PRICES_TABLE}")
        print(f"[C] LOOKBACK_DAYS={LOOKBACK_DAYS} SCAN_RECENT_DAYS={SCAN_RECENT_DAYS}")
        print("=" * 100)

        conn = connect()
        df = scan_candidates(conn)
        print_result(df)

        if not df.empty:
            out_path = "/app/data/strategy_c_candidates.csv"
            df.to_csv(out_path, index=False)
            print(f"\n[C] 已导出: {out_path}")

    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()