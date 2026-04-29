# -*- coding: utf-8 -*-
"""
策略B：筛选入池 -> 写入 stock_operations（去掉 trigger 动态跟随版）

本版规则：
A) 不再动态修改 trigger_price
   - 不再执行 trigger_price = GREATEST(entry_close, today_close)
   - trigger_price 仅在首次入池时写入，值 = strategy_b_levels.pressure_price

B) 旧池子（仅 B 且未买入）只做淘汰删除：
   - 如果 today_close < entry_open，则删除该 B 记录（仅未买入）

C) 新进入入选池：
   - 如果已在 stock_operations（无论A/B）且 is_bought=1：不更新任何字段
   - 如果已在 stock_operations 且 stock_type='A'：不更新
   - 如果已在 stock_operations（B 且未买入）：不改变其现有数据
   - 只有“完全不存在”的股票，才新增一条 B 记录，并写入：
       trigger_price = strategy_b_levels.pressure_price
       entry_date    = 入选日日期(as_of)
       entry_open    = 入选日当天 open
       entry_close   = 入选日当天 close
       created_at    = pressure_date 00:00:00（保持你之前逻辑）

D) 入池筛选条件：
   1) last_close 在 pressure_price * [0.95, 1.10]
   2) 价格 > 2（close_today）
   3) vol_today > 1,000,000
   4) MA3 > MA10
   5) vol_today > avg(vol_prev_20) * 1.5
   6) up_pct_today > 2%
   7) (ma3-ma8)_today > (ma3-ma8)_yesterday > (ma3-ma8)_daybefore
   8) ma3 > ma8
   9) 从近20日低点涨幅 < 35%
  10) close_today > open_today，过滤高开低走/收阴线
  11) 上影线占收盘价比例 <= 5%，过滤冲高回落太明显的票
"""

import os
import pymysql

# =========================
# DB
# =========================
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

SRC_TABLE    = os.getenv("SRC_TABLE", "stock_prices_pool")
LEVELS_TABLE = os.getenv("LEVELS_TABLE", "strategy_b_levels")
OPS_TABLE    = os.getenv("OPS_TABLE", "stock_operations")

LOW_PCT  = float(os.getenv("B_LOW_PCT", "0.95"))
HIGH_PCT = float(os.getenv("B_HIGH_PCT", "1.10"))

VOL_MULT      = float(os.getenv("B_VOL_MULT", "1.5"))
UP_PCT_MIN    = float(os.getenv("B_UP_PCT_MIN", "0.02"))
MIN_PRICE     = float(os.getenv("B_MIN_PRICE", "2.0"))
MIN_VOL_TODAY = float(os.getenv("B_MIN_VOL_TODAY", "1000000"))

PRINT_LIMIT = int(os.getenv("B_PRINT_LIMIT", "300"))
MAX_RISE_FROM_LOW_PCT = float(os.getenv("B_MAX_RISE_FROM_LOW_PCT", "0.35"))
MAX_UPPER_SHADOW_PCT = float(os.getenv("B_MAX_UPPER_SHADOW_PCT", "0.05"))


def _connect():
    return pymysql.connect(**DB)


def _safe_float(x, default=0.0):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _fetch_as_of_date(cur):
    cur.execute(f"SELECT MAX(DATE(`date`)) AS d FROM `{SRC_TABLE}`;")
    row = cur.fetchone()
    as_of = (row or {}).get("d")
    if not as_of:
        raise RuntimeError(f"[FATAL] {SRC_TABLE} 没数据")
    return as_of


# =========================
# 维护逻辑：旧池子（B且未买入）
# =========================
def _maintain_old_unbought_b(cur, as_of):
    """
    仅做淘汰删除，不再动态修改 trigger_price。
    删除条件：
      today_close < entry_open
    仅删除：
      stock_type='B' 且 is_bought=0/NULL 的未买入记录
    """
    sql = f"""
    DELETE op
    FROM `{OPS_TABLE}` op
    JOIN (
        SELECT p.symbol, p.`close` AS today_close
        FROM `{SRC_TABLE}` p
        JOIN (
            SELECT symbol, MAX(DATE(`date`)) AS last_date
            FROM `{SRC_TABLE}`
            WHERE DATE(`date`) <= DATE(%s)
            GROUP BY symbol
        ) t
          ON p.symbol = t.symbol
         AND DATE(p.`date`) = t.last_date
    ) px
      ON px.symbol = op.stock_code
    WHERE op.stock_type = 'B'
      AND (op.is_bought IS NULL OR op.is_bought = 0)
      AND op.entry_open IS NOT NULL
      AND px.today_close IS NOT NULL
      AND px.today_close < op.entry_open;
    """
    n = cur.execute(sql, (as_of,))
    print(
        f"[OK] maintain old B(unbought): keep trigger_price unchanged, "
        f"delete(today_close<entry_open) rows={n}",
        flush=True,
    )


# =========================
# 候选阶段：提前过滤价格>2 和 成交量>100万
# =========================
def _load_candidates(cur, as_of):
    sql = f"""
    SELECT
        lv.symbol,
        lv.pressure_price,
        lv.pressure_date,
        lb.last_date,
        lb.last_close,
        lb.last_vol
    FROM `{LEVELS_TABLE}` lv
    JOIN (
        SELECT p.symbol,
               DATE(p.`date`)  AS last_date,
               p.`close`       AS last_close,
               p.`volume`      AS last_vol
        FROM `{SRC_TABLE}` p
        JOIN (
            SELECT symbol, MAX(DATE(`date`)) AS last_date
            FROM `{SRC_TABLE}`
            WHERE DATE(`date`) <= DATE(%s)
            GROUP BY symbol
        ) t
          ON p.symbol = t.symbol
         AND DATE(p.`date`) = t.last_date
        WHERE p.`close`  IS NOT NULL
          AND p.`close`  > %s
          AND p.`volume` > %s
    ) lb
      ON lv.symbol = lb.symbol
    WHERE lb.last_close BETWEEN (lv.pressure_price * %s)
                           AND  (lv.pressure_price * %s)
    ;
    """
    cur.execute(sql, (as_of, MIN_PRICE, MIN_VOL_TODAY, LOW_PCT, HIGH_PCT))
    return cur.fetchall() or []


def _load_recent_bars(cur, symbol, as_of, limit=30):
    sql = f"""
    SELECT DATE(`date`) AS d, `open`, `high`, `low`, `close`, `volume`
    FROM `{SRC_TABLE}`
    WHERE symbol=%s
      AND DATE(`date`) <= DATE(%s)
    ORDER BY `date` DESC
    LIMIT %s;
    """
    cur.execute(sql, (symbol, as_of, int(limit)))
    return cur.fetchall() or []


def _compute_metrics(bars_desc):
    """
    bars_desc: 最近日期在前（DESC）
    需要至少 21 天
    """
    if len(bars_desc) < 21:
        return None

    closes = [_safe_float(r.get("close")) for r in bars_desc]
    opens  = [_safe_float(r.get("open")) for r in bars_desc]
    highs  = [_safe_float(r.get("high")) for r in bars_desc]
    lows   = [_safe_float(r.get("low")) for r in bars_desc]
    vols   = [_safe_float(r.get("volume")) for r in bars_desc]

    close_today = closes[0]
    close_prev  = closes[1] if len(closes) >= 2 else 0.0
    up_pct = (close_today - close_prev) / close_prev if close_prev > 0 else 0.0

    ma3_today  = sum(closes[0:3]) / 3.0
    ma8_today  = sum(closes[0:8]) / 8.0
    ma10_today = sum(closes[0:10]) / 10.0

    ma3_y = sum(closes[1:4]) / 3.0
    ma8_y = sum(closes[1:9]) / 8.0
    ma3_2 = sum(closes[2:5]) / 3.0
    ma8_2 = sum(closes[2:10]) / 8.0

    diff_today = ma3_today - ma8_today
    diff_y     = ma3_y - ma8_y
    diff_2     = ma3_2 - ma8_2

    vol_today      = vols[0]
    vol_avg20_prev = sum(vols[1:21]) / 20.0

    entry_open  = opens[0]
    entry_high  = highs[0]
    entry_low   = lows[0]
    entry_close = closes[0]

    min_close_20  = min(closes[1:21])
    rise_from_low = (close_today - min_close_20) / min_close_20 if min_close_20 > 0 else 0.0

    # 上影线过滤说明：
    # 用 high-close 衡量“冲高后回落”的幅度，占 close 的比例越大，说明抛压越明显。
    # 默认 5% 是温和过滤：不要求完美光头阳线，只排除明显长上影。
    upper_shadow_pct = (entry_high - entry_close) / entry_close if entry_close > 0 else 0.0
    intraday_range_pct = (entry_high - entry_low) / entry_close if entry_close > 0 else 0.0

    return {
        "close_today":    close_today,
        "close_prev":     close_prev,
        "up_pct":         up_pct,
        "ma3":            ma3_today,
        "ma8":            ma8_today,
        "ma10":           ma10_today,
        "diff_today":     diff_today,
        "diff_y":         diff_y,
        "diff_2":         diff_2,
        "vol_today":      vol_today,
        "vol_avg20_prev": vol_avg20_prev,
        "entry_open":     entry_open,
        "entry_high":     entry_high,
        "entry_low":      entry_low,
        "entry_close":    entry_close,
        "min_close_20":   min_close_20,
        "rise_from_low":  rise_from_low,
        "upper_shadow_pct": upper_shadow_pct,
        "intraday_range_pct": intraday_range_pct,
    }


# =========================
# 写入：只插入“全新”股票
# =========================
def _insert_new_ops_b_only(cur, rows):
    """
    只插入不存在的股票。
    entry_date / entry_open / entry_close / trigger_price 只在首次 INSERT 时写入。
    trigger_price = strategy_b_levels.pressure_price
    """
    if not rows:
        return 0

    sql = f"""
    INSERT INTO `{OPS_TABLE}` (
        stock_code,
        trigger_price,
        stock_type,
        is_bought,
        created_at,
        entry_open,
        entry_close,
        entry_date
    )
    VALUES (%s, %s, 'B', 0, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        stock_code = stock_code
    ;
    """
    args = []
    for r in rows:
        args.append((
            r["stock_code"],
            r["trigger_price"],
            r["created_at"],
            r["entry_open"],
            r["entry_close"],
            r["entry_date"],
        ))
    return cur.executemany(sql, args)


def _already_exists(cur, code: str) -> bool:
    """
    只要股票已存在于 stock_operations，就返回 True。
    包括：
      - 已买入
      - A类型
      - B未买入
    全部跳过，保护现有数据。
    """
    sql = f"SELECT 1 FROM `{OPS_TABLE}` WHERE stock_code=%s LIMIT 1;"
    cur.execute(sql, (code,))
    return cur.fetchone() is not None


def main():
    conn = _connect()
    try:
        with conn.cursor() as cur:
            as_of = _fetch_as_of_date(cur)
            print(
                f"[INFO] as_of_date={as_of} range=[{LOW_PCT},{HIGH_PCT}] "
                f"min_price={MIN_PRICE} min_vol={int(MIN_VOL_TODAY)} "
                f"vol_mult={VOL_MULT} min_up={UP_PCT_MIN} "
                f"max_rise_from_low={MAX_RISE_FROM_LOW_PCT} "
                f"max_upper_shadow={MAX_UPPER_SHADOW_PCT}",
                flush=True,
            )

            # A) 维护旧池子：只做淘汰删除，不再改 trigger_price
            _maintain_old_unbought_b(cur, as_of)

            # B) 候选（区间 + 价格>2 + 成交量>100万 已在 SQL 里过滤）
            candidates = _load_candidates(cur, as_of)
            print(f"[INFO] candidates(pre-filtered)={len(candidates)}", flush=True)

            to_insert = []
            printed = 0

            for c in candidates:
                sym = (c.get("symbol") or "").strip().upper()
                if not sym:
                    continue

                # 已存在于 ops 的全部跳过（保护现有数据）
                if _already_exists(cur, sym):
                    continue

                pressure_price = _safe_float(c.get("pressure_price"))
                pressure_date  = c.get("pressure_date")
                last_close     = _safe_float(c.get("last_close"))

                bars = _load_recent_bars(cur, sym, as_of, limit=30)
                m = _compute_metrics(bars)
                if not m:
                    continue

                # Python 层技术指标过滤
                if not (m["ma3"] > m["ma10"]):
                    continue

                if not (m["ma3"] > m["ma8"]):
                    continue

                if not (m["diff_today"] > m["diff_y"] > m["diff_2"]):
                    continue

                if not (m["vol_today"] > (m["vol_avg20_prev"] * VOL_MULT)):
                    continue

                if not (m["up_pct"] > UP_PCT_MIN):
                    continue

                if m["rise_from_low"] >= MAX_RISE_FROM_LOW_PCT:
                    continue

                # 收阳线过滤：
                # 只保留 close_today > open_today 的票，避免把高开低走、收阴线的票放进待买池。
                if not (m["entry_close"] > m["entry_open"]):
                    continue

                # 长上影线过滤：
                # high 有冲高但 close 离 high 太远，往往代表当日上方抛压重，次日突破质量较差。
                # 这里用 (high-close)/close <= 5% 做默认阈值，可用 B_MAX_UPPER_SHADOW_PCT 调整。
                if m["upper_shadow_pct"] > MAX_UPPER_SHADOW_PCT:
                    continue

                created_at = (
                    f"{pressure_date} 00:00:00"
                    if pressure_date
                    else f"{as_of} 00:00:00"
                )

                to_insert.append({
                    "stock_code":    sym,
                    "trigger_price": round(pressure_price, 2),   # 固定写入 pressure_price
                    "created_at":    created_at,
                    "entry_open":    round(float(m["entry_open"]), 2),   # 入选日开盘价
                    "entry_close":   round(float(m["entry_close"]), 2),  # 入选日收盘价
                    "entry_date":    str(as_of),                         # 入选日日期
                })

                if printed < PRINT_LIMIT:
                    printed += 1
                    print(
                        f"[PASS] {sym} last_close={last_close:.2f} pressure={pressure_price:.2f} "
                        f"close_today={m['close_today']:.2f} "
                        f"entry_open={m['entry_open']:.2f} entry_high={m['entry_high']:.2f} "
                        f"entry_low={m['entry_low']:.2f} entry_close={m['entry_close']:.2f} "
                        f"MA3={m['ma3']:.4f} MA8={m['ma8']:.4f} MA10={m['ma10']:.4f} "
                        f"diff(m3-m8) t={m['diff_today']:.4f} y={m['diff_y']:.4f} 2d={m['diff_2']:.4f} "
                        f"vol_today={m['vol_today']:.0f} vol_avg20={m['vol_avg20_prev']:.0f} "
                        f"up_pct={m['up_pct']*100:.2f}% rise_from_low={m['rise_from_low']*100:.1f}% "
                        f"upper_shadow={m['upper_shadow_pct']*100:.1f}% "
                        f"range={m['intraday_range_pct']*100:.1f}%",
                        flush=True,
                    )

            affected = _insert_new_ops_b_only(cur, to_insert)
            print(
                f"[OK] new_selected={len(to_insert)} insert_affected={affected}",
                flush=True,
            )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
