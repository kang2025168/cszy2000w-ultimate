# -*- coding: utf-8 -*-
"""
Build daily stock price category snapshots from stock_prices_pool.

The web UI reads the snapshot table instead of recalculating on every request.
Run it after the daily bars have been written to stock_prices_pool; before the
next close, the UI keeps showing the previous completed snapshot.

涨跌幅口径：
    (`close` - previous trading day's `close`) / previous trading day's `close`

Example:
    DB_HOST=138.197.75.51 DB_PORT=3307 DB_USER=tradebot DB_PASS='***' DB_NAME=cszy2000 \
    .venv/bin/python scripts/refresh_stock_price_categories.py
"""

from __future__ import annotations

import argparse
import os
import time
from collections import defaultdict
from datetime import date, datetime, timedelta

import pymysql

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


DB = dict(
    host=os.getenv("DB_HOST", "mysql"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "tradebot"),
    password=os.getenv("DB_PASS", ""),
    database=os.getenv("DB_NAME", "cszy2000"),
    charset="utf8mb4",
    autocommit=True,
    cursorclass=pymysql.cursors.DictCursor,
)

SRC_TABLE = os.getenv("SRC_TABLE", "stock_prices_pool")
DST_TABLE = os.getenv("PRICE_CATEGORY_TABLE", "stock_price_category_snapshots")
RUN_TZ_NAME = os.getenv("PRICE_CATEGORY_RUN_TZ", os.getenv("TZ", "America/Los_Angeles"))
RUN_TIME = os.getenv("PRICE_CATEGORY_RUN_TIME", "18:15")
MIN_CLOSE_PRICE = float(os.getenv("PRICE_CATEGORY_MIN_CLOSE_PRICE", "3"))


CATEGORIES = [
    ("gain_pct", "按涨幅来分类", "gain_gt_5", "涨幅>5%", 10),
    ("gain_pct", "按涨幅来分类", "gain_gt_10", "涨幅>10%", 20),
    ("gain_pct", "按涨幅来分类", "gain_gt_15", "涨幅>15%", 30),
    ("gain_pct", "按涨幅来分类", "gain_gt_20", "涨幅>20%", 40),
    ("loss_pct", "按跌幅来分类", "loss_gt_5", "跌幅>5%", 50),
    ("loss_pct", "按跌幅来分类", "loss_gt_10", "跌幅>10%", 60),
    ("loss_pct", "按跌幅来分类", "loss_gt_15", "跌幅>15%", 70),
    ("loss_pct", "按跌幅来分类", "loss_gt_20", "跌幅>20%", 80),
    ("up_streak", "按连涨天数分类", "up_streak_2", "连涨2天", 90),
    ("up_streak", "按连涨天数分类", "up_streak_3", "连涨3天", 100),
    ("up_streak", "按连涨天数分类", "up_streak_4", "连涨4天", 110),
    ("up_streak", "按连涨天数分类", "up_streak_5", "连涨5天", 120),
    ("up_streak", "按连涨天数分类", "up_streak_6", "连涨6天", 130),
    ("up_streak", "按连涨天数分类", "up_streak_7", "连涨7天", 140),
    ("down_streak", "按连跌天数分类", "down_streak_2", "连跌2天", 150),
    ("down_streak", "按连跌天数分类", "down_streak_3", "连跌3天", 160),
    ("down_streak", "按连跌天数分类", "down_streak_4", "连跌4天", 170),
    ("down_streak", "按连跌天数分类", "down_streak_5", "连跌5天", 180),
    ("down_streak", "按连跌天数分类", "down_streak_6", "连跌6天", 190),
    ("down_streak", "按连跌天数分类", "down_streak_7", "连跌7天", 200),
]

CATEGORY_META = {key: (group_key, group_label, label, sort_order) for group_key, group_label, key, label, sort_order in CATEGORIES}


def _connect():
    return pymysql.connect(**DB)


def _ensure_table(conn):
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{DST_TABLE}` (
        snapshot_date DATE NOT NULL,
        category_group VARCHAR(32) NOT NULL,
        category_group_label VARCHAR(64) NOT NULL,
        category_key VARCHAR(64) NOT NULL,
        category_label VARCHAR(64) NOT NULL,
        category_order INT NOT NULL,
        symbol VARCHAR(16) NOT NULL,
        open DOUBLE NULL,
        high DOUBLE NULL,
        low DOUBLE NULL,
        close DOUBLE NULL,
        volume BIGINT NULL,
        change_pct DOUBLE NULL,
        up_streak INT NOT NULL DEFAULT 0,
        down_streak INT NOT NULL DEFAULT 0,
        up_days_2 INT NULL,
        up_days_3 INT NULL,
        up_days_4 INT NULL,
        up_days_5 INT NULL,
        down_days_2 INT NULL,
        down_days_3 INT NULL,
        down_days_4 INT NULL,
        down_days_5 INT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (snapshot_date, category_key, symbol),
        KEY idx_snapshot_order (snapshot_date, category_order),
        KEY idx_symbol_date (symbol, snapshot_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def _latest_dates(conn, limit: int = 9):
    sql = f"""
    SELECT DISTINCT DATE(`date`) AS d
    FROM `{SRC_TABLE}`
    WHERE `date` IS NOT NULL
    ORDER BY d DESC
    LIMIT %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall() or []
    dates = [r["d"] for r in rows if r.get("d")]
    return sorted(dates)


def _fetch_prices(conn, dates):
    placeholders = ",".join(["%s"] * len(dates))
    sql = f"""
    SELECT UPPER(symbol) AS symbol, DATE(`date`) AS trade_date, `open`, high, low, `close`, volume
    FROM `{SRC_TABLE}`
    WHERE DATE(`date`) IN ({placeholders})
      AND symbol IS NOT NULL
      AND symbol <> ''
      AND `close` IS NOT NULL;
    """
    with conn.cursor() as cur:
        cur.execute(sql, tuple(dates))
        return cur.fetchall() or []


def _as_float(v):
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _count_direction(rows, window: int, direction: str) -> int | None:
    if len(rows) < window + 1:
        return None
    tail = rows[-(window + 1):]
    count = 0
    for prev, cur in zip(tail, tail[1:]):
        prev_close = _as_float(prev.get("close"))
        cur_close = _as_float(cur.get("close"))
        if prev_close is None or cur_close is None:
            continue
        if direction == "up" and cur_close > prev_close:
            count += 1
        if direction == "down" and cur_close < prev_close:
            count += 1
    return count


def _streak(rows, direction: str) -> int:
    count = 0
    for i in range(len(rows) - 1, 0, -1):
        cur_close = _as_float(rows[i].get("close"))
        prev_close = _as_float(rows[i - 1].get("close"))
        if cur_close is None or prev_close is None:
            break
        if direction == "up" and cur_close > prev_close:
            count += 1
            continue
        if direction == "down" and cur_close < prev_close:
            count += 1
            continue
        break
    return count


def _add(out, category_key: str, latest: dict, metrics: dict):
    group_key, group_label, label, sort_order = CATEGORY_META[category_key]
    out.append({
        "snapshot_date": latest["trade_date"],
        "category_group": group_key,
        "category_group_label": group_label,
        "category_key": category_key,
        "category_label": label,
        "category_order": sort_order,
        "symbol": latest["symbol"],
        "open": latest.get("open"),
        "high": latest.get("high"),
        "low": latest.get("low"),
        "close": latest.get("close"),
        "volume": latest.get("volume"),
        **metrics,
    })


def _build_snapshot(rows, snapshot_date: date):
    by_symbol = defaultdict(list)
    for row in rows:
        sym = str(row.get("symbol") or "").strip().upper()
        if not sym:
            continue
        by_symbol[sym].append(row)

    out = []
    for sym, sym_rows in by_symbol.items():
        sym_rows.sort(key=lambda r: r["trade_date"])
        latest = sym_rows[-1]
        if latest.get("trade_date") != snapshot_date:
            continue

        close_price = _as_float(latest.get("close"))
        if close_price is None:
            continue
        if close_price <= MIN_CLOSE_PRICE:
            continue
        if len(sym_rows) < 2:
            continue
        prev_close = _as_float(sym_rows[-2].get("close"))
        if not prev_close or prev_close <= 0:
            continue

        change_pct = (close_price - prev_close) / prev_close
        up_streak = _streak(sym_rows, "up")
        down_streak = _streak(sym_rows, "down")
        up_days = {n: _count_direction(sym_rows, n, "up") for n in (2, 3, 4, 5)}
        down_days = {n: _count_direction(sym_rows, n, "down") for n in (2, 3, 4, 5)}
        metrics = {
            "change_pct": change_pct,
            "up_streak": up_streak,
            "down_streak": down_streak,
            "up_days_2": up_days[2],
            "up_days_3": up_days[3],
            "up_days_4": up_days[4],
            "up_days_5": up_days[5],
            "down_days_2": down_days[2],
            "down_days_3": down_days[3],
            "down_days_4": down_days[4],
            "down_days_5": down_days[5],
        }

        for threshold in (5, 10, 15, 20):
            if change_pct > threshold / 100:
                _add(out, f"gain_gt_{threshold}", latest, metrics)
            if change_pct < -threshold / 100:
                _add(out, f"loss_gt_{threshold}", latest, metrics)

        for days in range(2, 8):
            if up_streak >= days:
                _add(out, f"up_streak_{days}", latest, metrics)
            if down_streak >= days:
                _add(out, f"down_streak_{days}", latest, metrics)

    out.sort(key=lambda r: (r["category_order"], -(r["change_pct"] or 0), r["symbol"]))
    return out


def _replace_snapshot(conn, snapshot_date, rows):
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM `{DST_TABLE}` WHERE snapshot_date=%s;", (snapshot_date,))
        if not rows:
            return
        sql = f"""
        INSERT INTO `{DST_TABLE}` (
            snapshot_date, category_group, category_group_label, category_key,
            category_label, category_order, symbol, `open`, high, low, `close`,
            volume, change_pct, up_streak, down_streak, up_days_2, up_days_3,
            up_days_4, up_days_5, down_days_2, down_days_3, down_days_4,
            down_days_5
        )
        VALUES (
            %(snapshot_date)s, %(category_group)s, %(category_group_label)s,
            %(category_key)s, %(category_label)s, %(category_order)s, %(symbol)s,
            %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(change_pct)s,
            %(up_streak)s, %(down_streak)s, %(up_days_2)s, %(up_days_3)s,
            %(up_days_4)s, %(up_days_5)s, %(down_days_2)s, %(down_days_3)s,
            %(down_days_4)s, %(down_days_5)s
        );
        """
        cur.executemany(sql, rows)


def _run_once(snapshot_date_arg: str | None = None, dry_run: bool = False):
    with _connect() as conn:
        _ensure_table(conn)
        dates = _latest_dates(conn, 9)
        if not dates:
            raise RuntimeError(f"{SRC_TABLE} has no data")
        snapshot_date = date.fromisoformat(snapshot_date_arg) if snapshot_date_arg else dates[-1]
        if snapshot_date not in dates:
            dates = sorted(set(dates + [snapshot_date]))
        rows = _fetch_prices(conn, dates)
        snapshot_rows = _build_snapshot(rows, snapshot_date)
        counts = defaultdict(int)
        for row in snapshot_rows:
            counts[row["category_label"]] += 1

        if not dry_run:
            _replace_snapshot(conn, snapshot_date, snapshot_rows)

    mode = "DRY" if dry_run else "OK"
    print(
        f"[{mode}] snapshot_date={snapshot_date} rows={len(snapshot_rows)} "
        f"table={DST_TABLE} min_close>{MIN_CLOSE_PRICE:g}",
        flush=True,
    )
    for _, _, key, label, _ in CATEGORIES:
        print(f"  {label}: {counts.get(label, 0)}", flush=True)


def _parse_run_time():
    try:
        hh, mm = RUN_TIME.split(":", 1)
        return int(hh), int(mm)
    except Exception:
        return 13, 20


def _now_run_tz():
    if ZoneInfo:
        return datetime.now(ZoneInfo(RUN_TZ_NAME))
    return datetime.now()


def _seconds_until_next_run() -> float:
    now = _now_run_tz()
    hh, mm = _parse_run_time()
    target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=1)
    return max(1.0, target.timestamp() - now.timestamp())


def _run_loop(dry_run: bool = False):
    print(f"[LOOP] daily refresh at {RUN_TIME} {RUN_TZ_NAME}", flush=True)
    while True:
        sleep_s = _seconds_until_next_run()
        next_at = _now_run_tz() + timedelta(seconds=sleep_s)
        print(f"[LOOP] next_run={next_at.strftime('%Y-%m-%d %H:%M:%S %Z')} sleep={int(sleep_s)}s", flush=True)
        time.sleep(sleep_s)
        try:
            _run_once(dry_run=dry_run)
        except Exception as e:
            print(f"[ERROR] refresh failed: {e}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Snapshot date, YYYY-MM-DD. Defaults to latest date in stock_prices_pool.")
    parser.add_argument("--dry-run", action="store_true", help="Calculate and print counts without writing.")
    parser.add_argument("--loop", action="store_true", help="Run every day after market close. Defaults to 13:20 America/Los_Angeles.")
    args = parser.parse_args()

    if args.loop:
        _run_loop(dry_run=args.dry_run)
        return
    _run_once(snapshot_date_arg=args.date, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
