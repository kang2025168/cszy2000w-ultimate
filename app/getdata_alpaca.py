# -*- coding: utf-8 -*-
"""
getdata_alpaca.py
- 从 Alpaca 拉取历史 OHLCV
- 写入 MySQL 表（匹配你的 stock_prices_pool 或 stock_prices 结构）:
  symbol varchar(10) NOT NULL,
  date date NOT NULL,
  open float,
  high float,
  low float,
  close float,
  volume bigint,
  PRIMARY KEY (symbol, date)

支持：
- 命令行传参 tickers：python -u app/getdata_alpaca.py QQQ AAPL
- 或从 CSV 读取（SYMBOLS_CSV=/app/data/symbols/low_price_symbols.csv）
- 支持区间：START_DATE=YYYY-MM-DD END_DATE=YYYY-MM-DD（end inclusive）
- 批量参数：BATCH_SIZE / MAX_TICKERS
- 失败导出：/app/data/symbols/failed_symbols_today.csv
"""

import os
import sys
import time
import csv
import traceback
from datetime import datetime, timedelta, date as dt_date

import pandas as pd
import pymysql

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.timeframe import TimeFrame
from alpaca.data.requests import StockBarsRequest


# =========================
# 0) 环境参数
# =========================
TABLE_NAME = os.getenv("GETDATA_TABLE", "stock_prices_pool").strip()

SYMBOLS_CSV = os.getenv("SYMBOLS_CSV", "/app/data/symbols/low_price_symbols.csv").strip()
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
MAX_TICKERS = int(os.getenv("MAX_TICKERS", "0"))  # 0 表示不限制

INTERVAL = os.getenv("INTERVAL", "1d").strip()   # 目前只实现 1d -> Alpaca TimeFrame.Day

# ✅ 两种模式：
# 1) DAILY=1：自动拉最近 N 天（默认 2 天，兼容周末/节假日）
# 2) DAILY=0：必须提供 START_DATE/END_DATE
DAILY = os.getenv("DAILY", "0").strip()  # "1" or "0"
DAILY_DAYS = int(os.getenv("DAILY_DAYS", "2"))  # DAILY=1 时生效：拉最近几天（建议 2 或 3）

START_DATE = os.getenv("START_DATE", "").strip()  # YYYY-MM-DD
END_DATE = os.getenv("END_DATE", "").strip()      # YYYY-MM-DD (inclusive)

ALPACA_KEY = os.getenv("ALPACA_KEY", "").strip()
ALPACA_SECRET = os.getenv("ALPACA_SECRET", "").strip()
ALPACA_DATA_FEED = os.getenv("ALPACA_DATA_FEED", "iex").strip()  # iex / sip（sip通常要权限）
# =========================
# 1) MySQL 配置
# =========================
DB_HOST = os.getenv("DB_HOST", "mysql").strip()
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "tradebot").strip()
DB_PASS = os.getenv("DB_PASS", "").strip()
DB_NAME = os.getenv("DB_NAME", "cszy2000").strip()

# 输出文件
FAILED_OUT = os.getenv("FAILED_OUT", "/app/data/symbols/failed_symbols_today.csv").strip()


def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def die(msg: str, code: int = 1):
    print(f"[{now_ts()}] ❌ {msg}", flush=True)
    sys.exit(code)


def parse_date(s: str) -> dt_date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def resolve_range() -> tuple[dt_date, dt_date]:
    """
    返回 (start_date, end_date_exclusive)
    - DAILY=1：自动拉最近 DAILY_DAYS 天（默认2天），覆盖周末/节假日
    - DAILY=0：用 START_DATE/END_DATE（END_DATE inclusive）
    """
    if DAILY == "1":
        # 用本地时间即可（你跑在美国，日线对日期敏感）
        today = datetime.now().date()
        # end_exclusive：+1 保证包含“今天”这根K线（如果当天已出）
        end_dt = today + timedelta(days=1)
        start_dt = today - timedelta(days=max(1, DAILY_DAYS))
        return start_dt, end_dt

    # 非 DAILY：必须给区间
    if not START_DATE or not END_DATE:
        die("必须提供 START_DATE 和 END_DATE，或设置 DAILY=1")

    s = parse_date(START_DATE)
    e = parse_date(END_DATE) + timedelta(days=1)
    return s, e

def mysql_conn():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def ensure_table(conn, table: str):
    """
    如果表不存在就创建（字段匹配：symbol,date,open,high,low,close,volume）
    若你已经建好了也没事
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{table}` (
      `symbol` varchar(10) NOT NULL,
      `date` date NOT NULL,
      `open` float DEFAULT NULL,
      `high` float DEFAULT NULL,
      `low` float DEFAULT NULL,
      `close` float DEFAULT NULL,
      `volume` bigint DEFAULT NULL,
      PRIMARY KEY (`symbol`,`date`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def read_symbols_from_csv(path: str) -> list[str]:
    if not os.path.exists(path):
        die(f"找不到 SYMBOLS_CSV 文件：{path}")

    syms = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            s = str(row[0]).strip().upper()
            if not s or s.startswith("#"):
                continue
            # 过滤明显不合法
            if len(s) > 10:
                continue
            syms.append(s)

    # 去重保持顺序
    seen = set()
    out = []
    for s in syms:
        if s not in seen:
            out.append(s)
            seen.add(s)

    return out


def get_tickers() -> list[str]:
    # 1) 命令行优先
    if len(sys.argv) > 1:
        t = [x.strip().upper() for x in sys.argv[1:] if x.strip()]
        return t

    # 2) 否则读 CSV
    t = read_symbols_from_csv(SYMBOLS_CSV)
    if MAX_TICKERS and MAX_TICKERS > 0:
        t = t[:MAX_TICKERS]
    return t


def alpaca_client():
    if not ALPACA_KEY or not ALPACA_SECRET:
        die("缺少 ALPACA_KEY / ALPACA_SECRET")
    return StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)


def upsert_rows(conn, table: str, symbol: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0

    sql = f"""
    INSERT INTO `{table}` (`symbol`,`date`,`open`,`high`,`low`,`close`,`volume`)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      `open`=VALUES(`open`),
      `high`=VALUES(`high`),
      `low`=VALUES(`low`),
      `close`=VALUES(`close`),
      `volume`=VALUES(`volume`);
    """

    rows = []
    for _, r in df.iterrows():
        rows.append((
            symbol,
            r["date"],
            None if pd.isna(r["open"]) else float(r["open"]),
            None if pd.isna(r["high"]) else float(r["high"]),
            None if pd.isna(r["low"]) else float(r["low"]),
            None if pd.isna(r["close"]) else float(r["close"]),
            None if pd.isna(r["volume"]) else int(r["volume"]),
        ))

    with conn.cursor() as cur:
        cur.executemany(sql, rows)

    return len(rows)


def fetch_bars_batch(client: StockHistoricalDataClient, symbols: list[str], start_dt: dt_date, end_dt: dt_date) -> dict[str, pd.DataFrame]:
    """
    Alpaca 批量拉 bars，返回 {symbol: df(date,open,high,low,close,volume)}
    """
    if INTERVAL != "1d":
        die(f"当前只支持 INTERVAL=1d，你给的是 {INTERVAL}")

    req = StockBarsRequest(
        symbol_or_symbols=symbols,
        timeframe=TimeFrame.Day,
        start=pd.Timestamp(start_dt),
        end=pd.Timestamp(end_dt),
        feed=ALPACA_DATA_FEED,
        adjustment="all",  # ✅关键：不做拆股/分红复权，volume更接近“原始成交量口径”
    )

    bars = client.get_stock_bars(req)

    # bars.df: MultiIndex (symbol, timestamp)
    if bars is None or bars.df is None or bars.df.empty:
        return {s: pd.DataFrame() for s in symbols}

    df_all = bars.df.reset_index()

    out = {}
    for s in symbols:
        d = df_all[df_all["symbol"] == s].copy()
        if d.empty:
            out[s] = pd.DataFrame()
            continue

        # timestamp -> date
        d["date"] = pd.to_datetime(d["timestamp"]).dt.date

        # 标准列名
        d.rename(columns={
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        }, inplace=True)

        keep = ["date", "open", "high", "low", "close", "volume"]
        d = d[keep].dropna(subset=["date"]).sort_values("date")

        # 过滤到区间内（end_exclusive）
        d = d[(d["date"] >= start_dt) & (d["date"] < end_dt)]
        out[s] = d

    return out


def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def main():
    start_dt, end_dt = resolve_range()
    tickers = get_tickers()

    print(f"[{now_ts()}] ===== getdata_alpaca.py start =====", flush=True)
    print(f"[{now_ts()}] DB={DB_HOST}:{DB_PORT}/{DB_NAME}  TABLE={TABLE_NAME}", flush=True)
    print(f"[{now_ts()}] tickers={len(tickers)} timeframe=1Day feed={ALPACA_DATA_FEED}", flush=True)
    print(f"[{now_ts()}] range: {start_dt} -> {end_dt} (end_exclusive)", flush=True)
    print(f"[{now_ts()}] csv: {SYMBOLS_CSV}", flush=True)

    conn = mysql_conn()
    ensure_table(conn, TABLE_NAME)

    client = alpaca_client()

    ok = 0
    failed = 0
    total_rows = 0
    failed_syms = []

    # 批量拉取，减少请求次数
    for batch in chunked(tickers, BATCH_SIZE):
        try:
            data_map = fetch_bars_batch(client, batch, start_dt, end_dt)
        except Exception as e:
            # 整个 batch 挂了
            print(f"[{now_ts()}] ❌ batch 拉取失败: {batch} err={e}", flush=True)
            traceback.print_exc()
            failed += len(batch)
            failed_syms.extend(batch)
            continue

        for sym in batch:
            try:
                df = data_map.get(sym, pd.DataFrame())
                if df is None or df.empty:
                    # DAILY 模式下，周末/节假日空数据是正常的，不算失败
                    if DAILY == "1":
                        print(f"[{now_ts()}] ⚠ {sym}: 空数据（可能非交易日/当天无bar）", flush=True)
                        continue
                    else:
                        print(f"[{now_ts()}] ⚠ {sym}: 空数据", flush=True)
                        failed += 1
                        failed_syms.append(sym)
                        continue
                n = upsert_rows(conn, TABLE_NAME, sym, df)
                total_rows += n
                ok += 1
                print(f"[{now_ts()}] ✅ {sym}: upsert {n} rows ({df['date'].min()} -> {df['date'].max()})", flush=True)

            except Exception as e:
                print(f"[{now_ts()}] ❌ {sym} 写入失败: {e}", flush=True)
                traceback.print_exc()
                failed += 1
                failed_syms.append(sym)

        # 控制节奏（别太激进）
        time.sleep(0.2)

    conn.close()

    # 导出失败列表
    if failed_syms:
        os.makedirs(os.path.dirname(FAILED_OUT), exist_ok=True)
        with open(FAILED_OUT, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for s in failed_syms:
                w.writerow([s])
        print(f"[{now_ts()}] ⚠ 已导出失败 symbol 至 {FAILED_OUT}", flush=True)

    print(f"[{now_ts()}] ===== done. ok={ok} failed={failed} total_upsert_rows={total_rows} =====", flush=True)


if __name__ == "__main__":
    main()