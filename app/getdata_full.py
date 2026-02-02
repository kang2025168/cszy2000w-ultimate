# -*- coding: utf-8 -*-
import os
import time
import random
from pathlib import Path

import yfinance as yf
import pymysql
import pandas as pd


# =========================
# 路径 & 文件
# =========================
BASE_DIR = Path(__file__).resolve().parent
# 容器内默认 CSV 放在 /app/data；也允许用 env 覆盖
# CSV_FILE = Path(os.getenv("SYMBOLS_CSV", "/app/data/low_price_symbols.csv")).resolve()
BASE_DIR = "/app/data/mysql"
CSV_FILE = os.path.join(BASE_DIR, "low_price_symbols.csv")
OUT_FAILED_CSV = os.getenv("OUT_FAILED_CSV", "/app/data/failed_symbols_today.csv")

# =========================
# 参数（env 可覆盖）
# =========================
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "120"))
INTERVAL = os.getenv("INTERVAL", "1d")
LOOKBACK_PERIOD = os.getenv("LOOKBACK_PERIOD", "7d")
MAX_TICKERS = int(os.getenv("MAX_TICKERS", "0"))  # 0 表示不限制

# =========================
# MySQL（env 可覆盖）
# =========================
MYSQL_CFG = dict(
    host=os.getenv("DB_HOST", "127.0.0.1"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "tradebot"),
    password=os.getenv("DB_PASS", ""),
    database=os.getenv("DB_NAME", "cszy2000"),
    charset="utf8mb4",
    autocommit=False,
    cursorclass=pymysql.cursors.Cursor,
)

TABLE = os.getenv("GETDATA_TABLE", "stock_prices_pool")

INSERT_SQL = f"""
REPLACE INTO `{TABLE}` (symbol, `date`, open, high, low, close, volume)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

# =========================
# 稳定性相关配置
# =========================
BULK_RETRIES = int(os.getenv("BULK_RETRIES", "3"))
SINGLE_RETRIES = int(os.getenv("SINGLE_RETRIES", "3"))
BATCH_SLEEP_RANGE = (
    float(os.getenv("BATCH_SLEEP_MIN", "0.5")),
    float(os.getenv("BATCH_SLEEP_MAX", "1.6")),
)
FALLBACK_SLEEP = float(os.getenv("FALLBACK_SLEEP", "0.12"))


def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def should_retry(e: Exception) -> bool:
    s = (str(e) or "").lower()

    no_retry_keys = [
        "429", "too many requests", "crumb",
        "jsondecode", "forbidden", "captcha",
        "yf ratelimit", "ratelimited", "rate limited",
    ]
    if any(k in s for k in no_retry_keys):
        return False

    retry_keys = [
        "timed out", "timeout",
        "temporarily", "connection", "reset",
        "remote end closed connection",
        "name or service not known", "getaddrinfo", "dns",
        "502", "503", "504"
    ]
    return any(k in s for k in retry_keys)


def last_valid_row_to_tuple(sym: str, subdf: pd.DataFrame):
    if subdf is None or subdf.empty:
        return None

    subdf = subdf.dropna(how="any")
    if subdf.empty:
        return None

    dt = subdf.index[-1]
    row = subdf.iloc[-1]
    date_str = pd.to_datetime(dt).strftime("%Y-%m-%d")

    return (
        sym,
        date_str,
        float(row["Open"]),
        float(row["High"]),
        float(row["Low"]),
        float(row["Close"]),
        int(row["Volume"]) if pd.notna(row["Volume"]) else 0,
    )


def extract_last_rows(df: pd.DataFrame, symbols: list[str]):
    rows, missing = [], []
    if df is None or df.empty:
        return rows, symbols[:]

    is_multi = isinstance(df.columns, pd.MultiIndex)

    for sym in symbols:
        try:
            if is_multi:
                lvl0 = df.columns.get_level_values(0)
                lvl1 = df.columns.get_level_values(1)

                if "Open" in lvl0:
                    if sym not in lvl1:
                        missing.append(sym)
                        continue
                    sub = df.xs(sym, axis=1, level=1, drop_level=True)
                else:
                    if sym not in lvl0:
                        missing.append(sym)
                        continue
                    sub = df[sym]
            else:
                sub = df

            t = last_valid_row_to_tuple(sym, sub)
            if t is None:
                missing.append(sym)
            else:
                rows.append(t)

        except Exception as e:
            print(f"  [EXTRACT FAIL] {sym} | {type(e).__name__}: {e}", flush=True)
            missing.append(sym)

    return rows, missing


def bulk_download(batch: list[str]) -> pd.DataFrame | None:
    last_e = None
    for attempt in range(1, BULK_RETRIES + 1):
        try:
            return yf.download(
                tickers=" ".join(batch),
                period=LOOKBACK_PERIOD,
                interval=INTERVAL,
                group_by="ticker",
                auto_adjust=False,
                threads=False,
                progress=False,
            )
        except Exception as e:
            last_e = e
            print(f"[BULK RETRY {attempt}/{BULK_RETRIES}] {type(e).__name__}: {e}", flush=True)
            if should_retry(e) and attempt < BULK_RETRIES:
                time.sleep(1.2 + attempt * 1.8)
                continue
            break

    print(f"❌ 批量下载最终失败：{type(last_e).__name__}: {last_e}", flush=True)
    return None


def single_symbol_fallback(sym: str):
    last_e = None
    for attempt in range(1, SINGLE_RETRIES + 1):
        try:
            tk = yf.Ticker(sym)
            sdf = tk.history(period=LOOKBACK_PERIOD, interval=INTERVAL, auto_adjust=False)
            t = last_valid_row_to_tuple(sym, sdf)
            return [t] if t else []
        except Exception as e:
            last_e = e
            print(f"  [FB RETRY {attempt}/{SINGLE_RETRIES}] {sym} | {type(e).__name__}: {e}", flush=True)
            if should_retry(e) and attempt < SINGLE_RETRIES:
                time.sleep(0.8 + attempt * 1.6)
                continue
            break
        finally:
            time.sleep(FALLBACK_SLEEP)

    print(f"  × fallback 最终失败: {sym} | {type(last_e).__name__}: {last_e}", flush=True)
    return []


def main():
    if not CSV_FILE.exists():
        raise FileNotFoundError(f"CSV 不存在：{CSV_FILE}")

    df_symbols = pd.read_csv(CSV_FILE)
    if "symbol" not in df_symbols.columns:
        raise KeyError(f"CSV 必须包含 symbol 列：{CSV_FILE}")

    symbols = (
        df_symbols["symbol"]
        .astype(str).str.strip().str.upper()
        .dropna().drop_duplicates().tolist()
    )

    if MAX_TICKERS and MAX_TICKERS > 0:
        symbols = symbols[:MAX_TICKERS]

    print(f"计划处理 {len(symbols)} 个 symbol（lookback={LOOKBACK_PERIOD}, batch={BATCH_SIZE}）", flush=True)
    print(f"写入表：{MYSQL_CFG['database']}.{TABLE}", flush=True)

    conn = pymysql.connect(**MYSQL_CFG)
    cursor = conn.cursor()

    t0 = time.perf_counter()
    total_rows, total_missing = 0, 0
    failed_all = []

    try:
        batch_idx = 0
        for batch in chunked(symbols, BATCH_SIZE):
            batch_idx += 1
            print(f"\n===== 处理第 {batch_idx} 批，共 {len(batch)} symbols =====", flush=True)
            t_batch0 = time.perf_counter()

            df = bulk_download(batch)
            rows, missing = extract_last_rows(df, batch)

            # fallback 补救（注意：被限流时会慢，这是正常的）
            recovered = []
            for sym in missing:
                r = single_symbol_fallback(sym)
                if r:
                    recovered.extend(r)
                # 不刷屏了，只输出失败
                else:
                    print(f"× fallback 失败: {sym}", flush=True)

            rows.extend(recovered)

            if rows:
                cursor.executemany(INSERT_SQL, rows)
                conn.commit()
            else:
                conn.rollback()

            recovered_syms = {r[0] for r in recovered}
            still_missing = [m for m in missing if m not in recovered_syms]
            failed_all.extend(still_missing)

            t_batch1 = time.perf_counter()
            print(f"本批写入 {len(rows)} 行，失败 {len(still_missing)}，用时 {t_batch1 - t_batch0:.2f}s", flush=True)

            total_rows += len(rows)
            total_missing += len(still_missing)

            time.sleep(random.uniform(*BATCH_SLEEP_RANGE))

        t1 = time.perf_counter()
        print(f"\n========== 全部完成 ==========", flush=True)
        print(f"写入 {total_rows} 行，失败 {total_missing} 个，耗时 {t1 - t0:.2f}s", flush=True)

    except Exception as e:
        conn.rollback()
        print("❌ 异常:", type(e).__name__, e, flush=True)
        raise
    finally:
        cursor.close()
        conn.close()

    if failed_all:
        pd.DataFrame({"failed": sorted(set(failed_all))}).to_csv(OUT_FAILED_CSV, index=False)
        print(f"⚠ 已导出失败 symbol 至 {OUT_FAILED_CSV}", flush=True)


if __name__ == "__main__":
    main()