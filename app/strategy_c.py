# -*- coding: utf-8 -*-
"""
app/strategy_c.py
策略C候选筛选：从 stock_operations 所有 B 类股票中，筛选放量突破/加速上涨走势

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
# 策略C筛选参数
# =========================
LOOKBACK_DAYS = int(os.getenv("C_LOOKBACK_DAYS", "80"))

MIN_PRICE = float(os.getenv("C_MIN_PRICE", "3"))
MAX_PRICE = float(os.getenv("C_MAX_PRICE", "300"))

MIN_RET5 = float(os.getenv("C_MIN_RET5", "0.15"))          # 最近5日涨幅 > 15%
MIN_RET10 = float(os.getenv("C_MIN_RET10", "0.20"))        # 最近10日涨幅 > 20%
MIN_VOL_RATIO = float(os.getenv("C_MIN_VOL_RATIO", "2.0")) # 最新成交量 > 20日均量 x2
MAX_DIST_HIGH20 = float(os.getenv("C_MAX_DIST_HIGH20", "0.05"))  # 距离20日新高不超过5%

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
        return cur.fetchall() or []


def load_price_data(conn, symbols):
    if not symbols:
        return pd.DataFrame()

    placeholders = ",".join(["%s"] * len(symbols))

    # 注意：这里不要写 `date` <> ''，MySQL 严格模式会报 Incorrect DATE value
    sql = f"""
    SELECT
        symbol,
        `date`,
        open,
        high,
        low,
        close,
        volume
    FROM `{PRICES_TABLE}`
    WHERE symbol IN ({placeholders})
      AND `date` IS NOT NULL
    ORDER BY symbol, `date`;
    """

    df = pd.read_sql(sql, conn, params=list(symbols))

    if df.empty:
        return df

    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()

    # Python 里清洗日期，坏数据直接变 NaT
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["symbol", "date", "open", "high", "low", "close", "volume"])

    if df.empty:
        return df

    latest_day = df["date"].max()
    start_day = latest_day - pd.Timedelta(days=LOOKBACK_DAYS)

    df = df[df["date"] >= start_day].copy()
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

    return df


def calc_one_symbol(symbol, g):
    g = g.sort_values("date").copy()

    if len(g) < 30:
        return None

    g["ma3"] = g["close"].rolling(3).mean()
    g["ma5"] = g["close"].rolling(5).mean()
    g["ma10"] = g["close"].rolling(10).mean()
    g["ma20"] = g["close"].rolling(20).mean()
    g["ma60"] = g["close"].rolling(60).mean()
    g["vol20"] = g["volume"].rolling(20).mean()
    g["high20"] = g["high"].rolling(20).max()
    g["high60"] = g["high"].rolling(60).max()

    last = g.iloc[-1]
    prev1 = g.iloc[-2]
    prev5 = g.iloc[-6] if len(g) >= 6 else None
    prev10 = g.iloc[-11] if len(g) >= 11 else None
    prev20 = g.iloc[-21] if len(g) >= 21 else None

    close = float(last["close"])
    volume = float(last["volume"])
    ma3 = float(last["ma3"]) if pd.notna(last["ma3"]) else 0
    ma5 = float(last["ma5"]) if pd.notna(last["ma5"]) else 0
    ma10 = float(last["ma10"]) if pd.notna(last["ma10"]) else 0
    ma20 = float(last["ma20"]) if pd.notna(last["ma20"]) else 0
    ma60 = float(last["ma60"]) if pd.notna(last["ma60"]) else 0
    vol20 = float(last["vol20"]) if pd.notna(last["vol20"]) else 0
    high20 = float(last["high20"]) if pd.notna(last["high20"]) else 0
    high60 = float(last["high60"]) if pd.notna(last["high60"]) else 0

    if close <= 0 or vol20 <= 0 or ma5 <= 0 or ma10 <= 0 or ma20 <= 0 or high20 <= 0:
        return None

    ret1 = close / float(prev1["close"]) - 1 if float(prev1["close"]) > 0 else 0
    ret5 = close / float(prev5["close"]) - 1 if prev5 is not None and float(prev5["close"]) > 0 else 0
    ret10 = close / float(prev10["close"]) - 1 if prev10 is not None and float(prev10["close"]) > 0 else 0
    ret20 = close / float(prev20["close"]) - 1 if prev20 is not None and float(prev20["close"]) > 0 else 0

    vol_ratio = volume / vol20 if vol20 > 0 else 0
    dist_high20 = close / high20 - 1 if high20 > 0 else 0
    dist_high60 = close / high60 - 1 if high60 > 0 else 0

    # 最近3天收盘是否抬高
    closes_tail = list(g["close"].tail(3))
    close_up_3 = len(closes_tail) == 3 and closes_tail[2] > closes_tail[1] > closes_tail[0]

    # 均线多头排列
    ma_bull = ma5 > ma10 > ma20

    # 突破状态
    above_ma20 = close > ma20
    near_high20 = dist_high20 >= -MAX_DIST_HIGH20
    near_high60 = dist_high60 >= -0.08 if high60 > 0 else False

    # 加速状态
    strong_ret5 = ret5 >= MIN_RET5
    strong_ret10 = ret10 >= MIN_RET10
    volume_breakout = vol_ratio >= MIN_VOL_RATIO

    # 评分：方便排序
    score = 0
    score += min(ret5 * 100, 30) * 2
    score += min(ret10 * 100, 50) * 1.2
    score += min(vol_ratio, 5) * 10
    score += 15 if ma_bull else 0
    score += 10 if close_up_3 else 0
    score += 10 if near_high20 else 0
    score += 8 if near_high60 else 0
    score += 5 if close > ma3 > ma5 else 0

    passed = (
        close >= MIN_PRICE
        and close <= MAX_PRICE
        and above_ma20
        and ma_bull
        and strong_ret5
        and volume_breakout
        and near_high20
    )

    # 稍微宽松一点：10日强趋势 + 接近新高 + 均线多头，也放进观察
    watch_passed = (
        close >= MIN_PRICE
        and close <= MAX_PRICE
        and above_ma20
        and ma_bull
        and strong_ret10
        and vol_ratio >= 1.5
        and near_high20
    )

    if not passed and not watch_passed:
        return None

    return {
        "symbol": symbol,
        "date": last["date"].strftime("%Y-%m-%d"),
        "close": close,
        "volume": int(volume),
        "ret1_pct": ret1 * 100,
        "ret5_pct": ret5 * 100,
        "ret10_pct": ret10 * 100,
        "ret20_pct": ret20 * 100,
        "ma5": ma5,
        "ma10": ma10,
        "ma20": ma20,
        "ma60": ma60,
        "vol20": vol20,
        "vol_ratio": vol_ratio,
        "high20": high20,
        "dist_high20_pct": dist_high20 * 100,
        "high60": high60,
        "dist_high60_pct": dist_high60 * 100,
        "ma_bull": int(ma_bull),
        "close_up_3": int(close_up_3),
        "passed_type": "C_STRONG" if passed else "C_WATCH",
        "score": score,
    }


def scan_candidates(conn):
    rows = load_b_symbols(conn)
    if not rows:
        print("[C] no B symbols found in stock_operations")
        return pd.DataFrame()

    ops_df = pd.DataFrame(rows)
    ops_df["stock_code"] = ops_df["stock_code"].astype(str).str.upper().str.strip()
    symbols = sorted(ops_df["stock_code"].dropna().unique().tolist())

    print(f"[C] B股票池数量: {len(symbols)}")

    price_df = load_price_data(conn, symbols)
    if price_df.empty:
        print("[C] no price data found")
        return pd.DataFrame()

    result = []
    for symbol, g in price_df.groupby("symbol"):
        item = calc_one_symbol(symbol, g)
        if item:
            result.append(item)

    if not result:
        return pd.DataFrame()

    out = pd.DataFrame(result)

    out = out.merge(
        ops_df,
        left_on="symbol",
        right_on="stock_code",
        how="left",
    )

    out = out.sort_values(
        by=["passed_type", "score", "ret5_pct", "vol_ratio"],
        ascending=[True, False, False, False],
    )

    return out


def print_result(df):
    if df.empty:
        print("\n[C] 没有筛选出符合走势的股票")
        return

    print("\n" + "=" * 120)
    print("策略C候选：从所有 B 类股票中筛选放量突破/加速上涨")
    print("=" * 120)

    show_cols = [
        "symbol",
        "passed_type",
        "score",
        "date",
        "close",
        "ret1_pct",
        "ret5_pct",
        "ret10_pct",
        "ret20_pct",
        "vol_ratio",
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
        "ret5_pct",
        "ret10_pct",
        "ret20_pct",
        "vol_ratio",
        "dist_high20_pct",
        "dist_high60_pct",
        "ma5",
        "ma10",
        "ma20",
    ]:
        if col in df2.columns:
            df2[col] = df2[col].astype(float).round(2)

    print(df2[show_cols].head(TOP_N).to_string(index=False))

    print("\n" + "=" * 120)
    print("简版名单")
    print("=" * 120)
    for _, r in df2.head(TOP_N).iterrows():
        print(
            f"{r['symbol']:6s} "
            f"{r['passed_type']:9s} "
            f"score={float(r['score']):6.2f} "
            f"close={float(r['close']):8.2f} "
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