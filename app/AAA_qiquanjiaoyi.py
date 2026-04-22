# -*- coding: utf-8 -*-
"""
买入窗口回测脚本（云服务器数据库版）

目标：
1) 找出“可买窗口 buy_window”
   - 前面跌过
   - 已经站上 MA5
   - 没再创新低
   - 当天还没有暴涨（避免追高）
   - 一旦出现 breakout_day，后续不再允许首次买入窗口

2) 找出“爆发日 breakout_day”
   - 前面跌得更深
   - 已经站上 MA5
   - 当天明显拉升
   - 这种日子不适合第一次买

3) 输出指定日期附近上下文，方便你对照图看
"""

import warnings
from typing import Optional

import mysql.connector
import pandas as pd

warnings.filterwarnings("ignore", category=UserWarning)


# =========================
# 1. 数据库连接（云服务器）
# =========================
def get_conn(
    host="127.0.0.1",
    port=13307,
    user="tradebot",
    password="TradeBot#2026!",
    database="cszy2000"
):
    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )


# =========================
# 2. 参数区
# =========================
TABLE_NAME = "stock_prices_pool"

SYMBOL = "QQQ"
START_DATE = "2025-10-01"
END_DATE = "2026-04-30"
CHECK_DATE = "2026-02-05"

LOOKBACK_DAYS = 10

# “前面跌过”的定义
BUY_PULLBACK_PCT = -0.05        # 可买窗口：最近10天最大回撤达到 -5%
BREAKOUT_PULLBACK_PCT = -0.08   # 爆发日：最近10天最大回撤达到 -8%

# “不要追高”的定义
MAX_BUY_DAY_CHG_PCT = 0.02      # buy_window 要求：当天涨幅 < 2%
BREAKOUT_DAY_CHG_PCT = 0.02     # breakout_day 要求：当天涨幅 >= 2%

# 信号去重
MIN_GAP_BETWEEN_WINDOWS = 1     # 先设成1，完整看出可买区间
MIN_GAP_BETWEEN_BREAKOUTS = 5


# =========================
# 3. 读取历史行情
# =========================
def load_price_data(
    conn,
    symbol: str,
    start_date: str,
    end_date: str,
    table_name: str = TABLE_NAME,
) -> pd.DataFrame:
    sql = f"""
    SELECT
        symbol,
        DATE(`date`) AS d,
        open,
        high,
        low,
        close,
        volume
    FROM {table_name}
    WHERE symbol = %s
      AND DATE(`date`) BETWEEN %s AND %s
    ORDER BY DATE(`date`)
    """

    df = pd.read_sql(sql, conn, params=[symbol, start_date, end_date])

    if df.empty:
        raise ValueError(
            f"没有查到数据: symbol={symbol}, start_date={start_date}, end_date={end_date}, table={table_name}"
        )

    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df["d"] = pd.to_datetime(df["d"])

    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["d", "open", "high", "low", "close"]).copy()
    df = df.sort_values("d").reset_index(drop=True)

    return df


# =========================
# 4. 计算指标与状态
# =========================
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # 前一日
    out["prev_close"] = out["close"].shift(1)
    out["prev_low"] = out["low"].shift(1)

    # 均线
    out["ma5"] = out["close"].rolling(5, min_periods=1).mean()
    out["ma10"] = out["close"].rolling(10, min_periods=1).mean()

    # 最近 LOOKBACK_DAYS 天窗口高低点
    out["recent_max_close"] = out["close"].rolling(
        LOOKBACK_DAYS, min_periods=LOOKBACK_DAYS
    ).max()

    out["recent_min_low"] = out["low"].rolling(
        LOOKBACK_DAYS, min_periods=LOOKBACK_DAYS
    ).min()

    # 最近 LOOKBACK_DAYS 天最大回撤
    out["pullback_pct"] = (out["recent_min_low"] / out["recent_max_close"]) - 1.0

    # 当天涨跌幅（相对昨收）
    out["day_chg_pct"] = (out["close"] / out["prev_close"]) - 1.0

    # 基础状态
    out["above_ma5"] = out["close"] > out["ma5"]
    out["above_ma10"] = out["close"] > out["ma10"]
    out["is_green_candle"] = out["close"] > out["open"]

    # 是否没有再创新低
    out["no_new_low"] = out["low"] > out["recent_min_low"]

    # 是否前面跌过
    out["is_pullback_for_buy"] = out["pullback_pct"] <= BUY_PULLBACK_PCT
    out["is_pullback_for_breakout"] = out["pullback_pct"] <= BREAKOUT_PULLBACK_PCT

    # 先定义爆发日
    out["breakout_day"] = (
        out["is_pullback_for_breakout"] &
        out["above_ma5"] &
        (out["day_chg_pct"] >= BREAKOUT_DAY_CHG_PCT)
    )

    # 一旦在前一天或更早出现过 breakout，后面就不再允许 buy_window
    out["breakout_happened"] = (
        out["breakout_day"]
        .shift(1)
        .fillna(False)
        .astype(bool)
        .cummax()
    )

    # 可买窗口：爆发前的修复区间
    out["buy_window"] = (
        out["is_pullback_for_buy"] &
        out["above_ma5"] &
        out["no_new_low"] &
        (out["day_chg_pct"] < MAX_BUY_DAY_CHG_PCT) &
        (~out["breakout_happened"])
    )

    # 危险区：跌过，但还没站上 MA5
    out["danger"] = (
        out["is_pullback_for_buy"] &
        (~out["above_ma5"])
    )

    return out


# =========================
# 5. 信号去重
# =========================
def filter_signals_with_gap(
    df: pd.DataFrame,
    signal_col: str,
    min_gap: int
) -> pd.DataFrame:
    signal_idx = df.index[df[signal_col].fillna(False)].tolist()
    if not signal_idx:
        return df.iloc[0:0].copy()

    kept = []
    last_keep = -10**9

    for idx in signal_idx:
        if idx - last_keep >= min_gap:
            kept.append(idx)
            last_keep = idx

    return df.loc[kept].copy()


# =========================
# 6. 买入后表现
# =========================
def forward_return(df: pd.DataFrame, entry_idx: int, hold_days: int) -> Optional[float]:
    exit_idx = entry_idx + hold_days
    if exit_idx >= len(df):
        return None

    entry_price = df.loc[entry_idx, "close"]
    exit_price = df.loc[exit_idx, "close"]

    if pd.isna(entry_price) or pd.isna(exit_price) or entry_price == 0:
        return None

    return (exit_price / entry_price) - 1.0


def attach_forward_returns(df_all: pd.DataFrame, signals: pd.DataFrame) -> pd.DataFrame:
    out = signals.copy()

    out["ret_3d"] = [forward_return(df_all, idx, 3) for idx in out.index]
    out["ret_5d"] = [forward_return(df_all, idx, 5) for idx in out.index]
    out["ret_10d"] = [forward_return(df_all, idx, 10) for idx in out.index]

    return out


# =========================
# 7. 打印表
# =========================
def print_signal_table(signals: pd.DataFrame, title: str):
    print("\n" + "=" * 150)
    print(title)
    print("=" * 150)

    if signals.empty:
        print("没有信号")
        return

    show_cols = [
        "d",
        "open",
        "high",
        "low",
        "close",
        "ma5",
        "ma10",
        "pullback_pct",
        "day_chg_pct",
        "breakout_happened",
        "buy_window",
        "breakout_day",
        "danger",
        "ret_3d",
        "ret_5d",
        "ret_10d",
    ]

    tmp = signals[show_cols].copy()

    for col in ["open", "high", "low", "close", "ma5", "ma10"]:
        tmp[col] = tmp[col].map(lambda x: round(x, 2) if pd.notna(x) else None)

    for col in ["pullback_pct", "day_chg_pct", "ret_3d", "ret_5d", "ret_10d"]:
        tmp[col] = tmp[col].map(lambda x: f"{x:.2%}" if pd.notna(x) else None)

    print(tmp.to_string(index=False))


# =========================
# 8. 查看指定日期附近
# =========================
def print_one_day_context(
    df: pd.DataFrame,
    target_date: str,
    days_before: int = 6,
    days_after: int = 6
):
    target_date = pd.to_datetime(target_date)
    hit = df[df["d"] == target_date]

    print("\n" + "=" * 170)
    print(f"指定日期检查: {target_date.date()}")
    print("=" * 170)

    if hit.empty:
        print("这一天没有数据")
        return

    idx = hit.index[0]
    left = max(0, idx - days_before)
    right = min(len(df) - 1, idx + days_after)

    cols = [
        "d",
        "open",
        "high",
        "low",
        "close",
        "ma5",
        "ma10",
        "recent_max_close",
        "recent_min_low",
        "pullback_pct",
        "day_chg_pct",
        "is_pullback_for_buy",
        "is_pullback_for_breakout",
        "above_ma5",
        "no_new_low",
        "breakout_happened",
        "buy_window",
        "breakout_day",
        "danger",
    ]

    tmp = df.loc[left:right, cols].copy()

    for col in [
        "open", "high", "low", "close", "ma5", "ma10",
        "recent_max_close", "recent_min_low"
    ]:
        tmp[col] = tmp[col].map(lambda x: round(x, 2) if pd.notna(x) else None)

    for col in ["pullback_pct", "day_chg_pct"]:
        tmp[col] = tmp[col].map(lambda x: f"{x:.2%}" if pd.notna(x) else None)

    print(tmp.to_string(index=False))


# =========================
# 9. 统计
# =========================
def print_summary(
    df: pd.DataFrame,
    buy_signals: pd.DataFrame,
    breakout_signals: pd.DataFrame
):
    print("\n" + "=" * 150)
    print("统计汇总")
    print("=" * 150)

    print(f"总样本天数: {len(df)}")
    print(f"buy_window 信号数: {len(buy_signals)}")
    print(f"breakout_day 信号数: {len(breakout_signals)}")

    if not buy_signals.empty:
        for col in ["ret_3d", "ret_5d", "ret_10d"]:
            valid = buy_signals[col].dropna()
            if not valid.empty:
                print(f"buy_window {col} | 平均收益: {valid.mean():.2%} | 胜率: {(valid > 0).mean():.2%}")
            else:
                print(f"buy_window {col} | 无足够数据")

    if not breakout_signals.empty:
        for col in ["ret_3d", "ret_5d", "ret_10d"]:
            valid = breakout_signals[col].dropna()
            if not valid.empty:
                print(f"breakout_day {col} | 平均收益: {valid.mean():.2%} | 胜率: {(valid > 0).mean():.2%}")
            else:
                print(f"breakout_day {col} | 无足够数据")


# =========================
# 10. 主回测函数
# =========================
def run_backtest(
    symbol: str,
    start_date: str,
    end_date: str,
    check_date: Optional[str] = None,
    table_name: str = TABLE_NAME,
):
    conn = None
    try:
        conn = get_conn(
            host="127.0.0.1",
            port=13307,
            user="tradebot",
            password="TradeBot#2026!",
            database="cszy2000"
        )

        df = load_price_data(
            conn=conn,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            table_name=table_name
        )

        df = add_indicators(df)

        buy_signals = filter_signals_with_gap(df, "buy_window", MIN_GAP_BETWEEN_WINDOWS)
        breakout_signals = filter_signals_with_gap(df, "breakout_day", MIN_GAP_BETWEEN_BREAKOUTS)

        buy_signals = attach_forward_returns(df, buy_signals)
        breakout_signals = attach_forward_returns(df, breakout_signals)

        print_signal_table(buy_signals, f"{symbol} - 可买窗口（buy_window）")
        print_signal_table(breakout_signals, f"{symbol} - 爆发日（breakout_day）")

        if check_date:
            print_one_day_context(df, check_date, days_before=6, days_after=6)

        print_summary(df, buy_signals, breakout_signals)

    except Exception as e:
        print(f"运行出错: {e}")
        raise
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


# =========================
# 11. 直接运行
# =========================
if __name__ == "__main__":
    run_backtest(
        symbol=SYMBOL,
        start_date=START_DATE,
        end_date=END_DATE,
        check_date=CHECK_DATE,
        table_name=TABLE_NAME,
    )