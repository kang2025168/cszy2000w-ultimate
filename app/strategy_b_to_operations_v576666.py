# -*- coding: utf-8 -*-
"""
策略B：筛选入池 -> 写入 stock_operations（定时任务用，最终版：entry_open/entry_close）

你的最终要求（全部实现）：
A) 不再“直接删掉之前入选池B(is_bought=0)”
   维护逻辑（只针对 stock_type='B' 且 is_bought=0 的旧记录）：
   1) trigger_price = entry_close   （把触发价锚定到“入选日收盘价”）
   2) 淘汰删除：如果 today_close < entry_open，则删除该 B 记录（仅未买入）
B) 新进入入选池：
   - 如果已在 stock_operations（无论A/B）且 is_bought=1：不更新任何字段（保持原状态）
   - 如果已在 stock_operations 且 stock_type='A'：不更新（A优先）
   - 如果已在 stock_operations（B 且未买入）：不改变其现有数据（包括 trigger_price / entry_open / entry_close 都不动）
   - 只有“完全不存在”的股票，才新增一条 B 记录，并写入：
       trigger_price = pressure_price（入选当天的触发价）
       entry_open / entry_close = 入选日当天的 open/close（只写一次）
       created_at = pressure_date 00:00:00（保持你之前逻辑）
C) 入池筛选条件（平衡版 + 你要求的增强）：
   1) last_close 在 pressure_price * [0.95, 1.15]
   2) 价格 > 2（用 close_today）
   3) vol_today > 1,000,000
   4) MA3 > MA10
   5) vol_today > avg(vol_prev_20) * 1.5
   6) up_pct_today > 2%（close_today vs close_prev）
   7) ✅ 新增：m3-m8 “三连增”：
       (ma3-ma8)_today > (ma3-ma8)_yesterday > (ma3-ma8)_daybefore
      且你补充：必须 ma3 在 ma8 上方（其实三连增里也会体现，但这里单独强制）

注意：
- 需要 stock_operations 表新增字段：
    entry_open DECIMAL(10,2) NULL,
    entry_close DECIMAL(10,2) NULL
  （你已决定先做这个）
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

SRC_TABLE = os.getenv("SRC_TABLE", "stock_prices_pool")
LEVELS_TABLE = os.getenv("LEVELS_TABLE", "strategy_b_levels")
OPS_TABLE = os.getenv("OPS_TABLE", "stock_operations")

# ✅ 平衡区间
LOW_PCT = float(os.getenv("B_LOW_PCT", "0.95"))
HIGH_PCT = float(os.getenv("B_HIGH_PCT", "1.15"))

# ✅ 量能/涨幅/价格过滤
VOL_MULT = float(os.getenv("B_VOL_MULT", "1.5"))                 # vol_today > avg20 * 1.5
UP_PCT_MIN = float(os.getenv("B_UP_PCT_MIN", "0.02"))            # >2%
MIN_PRICE = float(os.getenv("B_MIN_PRICE", "2.0"))               # >2
MIN_VOL_TODAY = float(os.getenv("B_MIN_VOL_TODAY", "1000000"))   # > 1,000,000

PRINT_LIMIT = int(os.getenv("B_PRINT_LIMIT", "300"))             # 最多打印多少只（防刷屏）


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
    1) 对旧 B(未买) : trigger_price = entry_close
    2) 淘汰删除：today_close < entry_open 则删除（仅未买）
    """
    # 1) 锚定 trigger_price
    sql1 = f"""
    UPDATE `{OPS_TABLE}`
    SET trigger_price = entry_close
    WHERE stock_type='B'
      AND (is_bought IS NULL OR is_bought=0)
      AND entry_close IS NOT NULL;
    """
    n1 = cur.execute(sql1)

    # 2) 淘汰：取 as_of 当天 close（today_close）来对比 entry_open
    # 只删除：B & 未买 & entry_open不为空 & today_close < entry_open
    sql2 = f"""
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
          ON p.symbol=t.symbol AND DATE(p.`date`)=t.last_date
    ) px
      ON px.symbol = op.stock_code
    WHERE op.stock_type='B'
      AND (op.is_bought IS NULL OR op.is_bought=0)
      AND op.entry_open IS NOT NULL
      AND px.today_close IS NOT NULL
      AND px.today_close < op.entry_open;
    """
    n2 = cur.execute(sql2, (as_of,))
    print(f"[OK] maintain old B(unbought): anchor_trigger=entry_close rows={n1}, delete(today_close<entry_open) rows={n2}", flush=True)


# =========================
# 候选：第一层区间过滤
# =========================
def _load_candidates(cur, as_of):
    """
    第一层：SQL 只做区间过滤（避免拉全市场）
    返回：symbol, pressure_price, pressure_date, last_close, last_date
    """
    sql = f"""
    SELECT
        lv.symbol,
        lv.pressure_price,
        lv.pressure_date,
        lb.last_date,
        lb.last_close
    FROM `{LEVELS_TABLE}` lv
    JOIN (
        SELECT p.symbol,
               DATE(p.`date`) AS last_date,
               p.`close` AS last_close
        FROM `{SRC_TABLE}` p
        JOIN (
            SELECT symbol, MAX(DATE(`date`)) AS last_date
            FROM `{SRC_TABLE}`
            WHERE DATE(`date`) <= DATE(%s)
            GROUP BY symbol
        ) t
          ON p.symbol = t.symbol AND DATE(p.`date`) = t.last_date
        WHERE p.`close` IS NOT NULL
    ) lb
      ON lv.symbol = lb.symbol
    WHERE lb.last_close BETWEEN (lv.pressure_price * %s)
                           AND (lv.pressure_price * %s)
    ;
    """
    cur.execute(sql, (as_of, LOW_PCT, HIGH_PCT))
    return cur.fetchall() or []


def _load_recent_bars(cur, symbol, as_of, limit=30):
    """
    拉最近 N 天（按 date DESC），需要 open/close/volume
    """
    sql = f"""
    SELECT DATE(`date`) AS d, `open`, `close`, `volume`
    FROM `{SRC_TABLE}`
    WHERE symbol=%s AND DATE(`date`) <= DATE(%s)
    ORDER BY `date` DESC
    LIMIT %s;
    """
    cur.execute(sql, (symbol, as_of, int(limit)))
    return cur.fetchall() or []


def _compute_metrics(bars_desc):
    """
    bars_desc: 最近日期在前（DESC）
    需要至少 21 天用于 avg20_prev（不含今天）
    需要至少 8 天用于 ma8 / diff(m3-m8) 三连增
    """
    if len(bars_desc) < 21:
        return None

    closes = [_safe_float(r.get("close")) for r in bars_desc]
    opens = [_safe_float(r.get("open")) for r in bars_desc]
    vols = [_safe_float(r.get("volume")) for r in bars_desc]

    close_today = closes[0]
    close_prev = closes[1] if len(closes) >= 2 else 0.0
    up_pct = (close_today - close_prev) / close_prev if close_prev > 0 else 0.0

    if len(closes) < 10:
        return None

    # MA3/MA10/MA8（用于 m3-m8）
    ma3_today = sum(closes[0:3]) / 3.0
    ma10_today = sum(closes[0:10]) / 10.0
    if len(closes) < 8:
        return None
    ma8_today = sum(closes[0:8]) / 8.0

    # m3-m8 三连增：今天>昨天>前天（用“滚动窗口”）
    # 今天: closes[0:3], closes[0:8]
    # 昨天: closes[1:4], closes[1:9]
    # 前天: closes[2:5], closes[2:10]
    if len(closes) < 10:
        return None

    ma3_y = sum(closes[1:4]) / 3.0
    ma8_y = sum(closes[1:9]) / 8.0
    ma3_2 = sum(closes[2:5]) / 3.0
    ma8_2 = sum(closes[2:10]) / 8.0

    diff_today = ma3_today - ma8_today
    diff_y = ma3_y - ma8_y
    diff_2 = ma3_2 - ma8_2

    vol_today = vols[0]
    vol_avg20_prev = sum(vols[1:21]) / 20.0

    entry_open = opens[0]   # 入选日的 open（这里用 as_of 当天，脚本每天跑就是“首次入选当日”）
    entry_close = closes[0] # 入选日的 close（同上）

    return {
        "close_today": close_today,
        "close_prev": close_prev,
        "up_pct": up_pct,
        "ma3": ma3_today,
        "ma10": ma10_today,
        "ma8": ma8_today,
        "diff_today": diff_today,
        "diff_y": diff_y,
        "diff_2": diff_2,
        "vol_today": vol_today,
        "vol_avg20_prev": vol_avg20_prev,
        "entry_open": entry_open,
        "entry_close": entry_close,
    }


# =========================
# 写入：只插入“全新”股票
# =========================
def _insert_new_ops_b_only(cur, rows):
    """
    只插入不存在的股票，不做更新（满足：
      - 已在入选池(B未买) 不改现有数据
      - 已买入(is_bought=1) 不动
      - 已是A 不动
    )
    """
    if not rows:
        return 0

    sql = f"""
    INSERT INTO `{OPS_TABLE}` (
      stock_code, trigger_price, stock_type, is_bought, created_at,
      entry_open, entry_close
    )
    VALUES (%s, %s, 'B', 0, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      stock_code = stock_code; -- 什么都不更新
    """
    args = []
    for r in rows:
        args.append((
            r["stock_code"],
            r["trigger_price"],
            r["created_at"],
            r["entry_open"],
            r["entry_close"],
        ))
    return cur.executemany(sql, args)


def _exists_and_bought_or_a(cur, code: str) -> bool:
    """
    True 表示：这个 code 不能被本脚本动（插入/更新都不应做）
    规则：
      - 只要 is_bought=1 （无论A/B） => True
      - 或者 stock_type='A' => True
      - 或者 已存在任何行（B未买）也不插入新行（因为你要求“已在入选池不改变其现有数据”）
    由于我们 INSERT ... ON DUPLICATE 不更新，因此这里只用于减少无效插入尝试（可选）。
    """
    sql = f"""
    SELECT stock_type, is_bought
    FROM `{OPS_TABLE}`
    WHERE stock_code=%s
    LIMIT 1;
    """
    cur.execute(sql, (code,))
    r = cur.fetchone()
    if not r:
        return False
    st = (r.get("stock_type") or "").strip().upper()
    ib = int(r.get("is_bought") or 0)
    if ib == 1:
        return True
    if st == "A":
        return True
    # 已存在（比如 B 未买）也不要插入新行
    return True


def main():
    conn = _connect()
    try:
        with conn.cursor() as cur:
            as_of = _fetch_as_of_date(cur)
            print(
                f"[INFO] as_of_date={as_of} range=[{LOW_PCT},{HIGH_PCT}] "
                f"min_price={MIN_PRICE} min_vol={int(MIN_VOL_TODAY)} vol_mult={VOL_MULT} min_up={UP_PCT_MIN}",
                flush=True,
            )

            # A) 维护旧池子（不直接删）：锚定 trigger_price，淘汰删除
            _maintain_old_unbought_b(cur, as_of)

            # B) 候选（区间）
            candidates = _load_candidates(cur, as_of)
            print(f"[INFO] candidates(in-range)={len(candidates)}", flush=True)

            to_insert = []
            printed = 0

            for c in candidates:
                sym = (c.get("symbol") or "").strip().upper()
                if not sym:
                    continue

                # ✅ 新进池子：如果已存在于 ops（任何类型/状态），都不动（你要“不改变其现有数据”）
                # 我们后面是 ON DUPLICATE 不更新，这里提前跳过只是节省 SQL 次数
                if _exists_and_bought_or_a(cur, sym):
                    continue

                pressure_price = _safe_float(c.get("pressure_price"))
                pressure_date = c.get("pressure_date")
                last_close = _safe_float(c.get("last_close"))

                bars = _load_recent_bars(cur, sym, as_of, limit=30)
                m = _compute_metrics(bars)
                if not m:
                    continue

                # 6) 价格>2（close_today）
                if not (m["close_today"] > MIN_PRICE):
                    continue

                # 7) 当日成交量 > 1,000,000
                if not (m["vol_today"] > MIN_VOL_TODAY):
                    continue

                # 3) MA3 > MA10
                if not (m["ma3"] > m["ma10"]):
                    continue

                # ✅ 你强调：必须 ma3 在 ma8 上面
                if not (m["ma3"] > m["ma8"]):
                    continue

                # ✅ 新增：diff(m3-m8) 三连增：today > yday > day-2
                if not (m["diff_today"] > m["diff_y"] > m["diff_2"]):
                    continue

                # 4) vol_today > avg20_prev * 1.5
                if not (m["vol_today"] > (m["vol_avg20_prev"] * VOL_MULT)):
                    continue

                # 5) up_pct > 2%
                if not (m["up_pct"] > UP_PCT_MIN):
                    continue

                created_at = f"{pressure_date} 00:00:00" if pressure_date else f"{as_of} 00:00:00"

                # ✅ 只对“全新”股票插入，并写入 entry_open/entry_close（只写一次）
                to_insert.append({
                    "stock_code": sym,
                    "trigger_price": round(pressure_price, 2),
                    "created_at": created_at,
                    "entry_open": round(float(m["entry_open"]), 2),
                    "entry_close": round(float(m["entry_close"]), 2),
                })

                if printed < PRINT_LIMIT:
                    printed += 1
                    print(
                        f"[PASS] {sym} last_close={last_close:.2f} pressure={pressure_price:.2f} "
                        f"close_today={m['close_today']:.2f} entry_open={m['entry_open']:.2f} entry_close={m['entry_close']:.2f} "
                        f"MA3={m['ma3']:.4f} MA8={m['ma8']:.4f} MA10={m['ma10']:.4f} "
                        f"diff(m3-m8) t={m['diff_today']:.4f} y={m['diff_y']:.4f} 2d={m['diff_2']:.4f} "
                        f"vol_today={m['vol_today']:.0f} vol_avg20_prev={m['vol_avg20_prev']:.0f} "
                        f"up_pct={m['up_pct']*100:.2f}%",
                        flush=True,
                    )

            affected = _insert_new_ops_b_only(cur, to_insert)
            print(f"[OK] new_selected={len(to_insert)} insert_affected={affected}", flush=True)

    finally:
        conn.close()


if __name__ == "__main__":
    main()