# -*- coding: utf-8 -*-
"""
策略B：筛选入池 -> 写入 stock_operations（定时任务用）

规则（全部实现）：
1) 执行时删除 stock_operations 中 stock_type='B' 且 is_bought=0 的记录
2) 入池区间（平衡）：last_close 在 pressure_price * [0.95, 1.15]
3) MA3 > MA10（打印 MA3/MA10 供核验）
4) vol_today > avg(vol_prev_20) * 1.5（打印）
5) up_pct_today > 2%（close_today vs close_prev，打印）
6) 价格 > 2 美元（用 close_today）
7) 当日成交量 > 1,000,000（用 vol_today）

保护：
- 如果 stock_operations 已存在 stock_type='A'，则不更新（A优先）
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
VOL_MULT = float(os.getenv("B_VOL_MULT", "1.5"))          # vol_today > avg20 * 1.5
UP_PCT_MIN = float(os.getenv("B_UP_PCT_MIN", "0.02"))     # >2%
MIN_PRICE = float(os.getenv("B_MIN_PRICE", "2.0"))        # >2
MIN_VOL_TODAY = float(os.getenv("B_MIN_VOL_TODAY", "1000000"))  # > 1,000,000

PRINT_LIMIT = int(os.getenv("B_PRINT_LIMIT", "300"))      # 最多打印多少只（防刷屏）


def _connect():
    return pymysql.connect(**DB)


def _fetch_as_of_date(cur):
    cur.execute(f"SELECT MAX(DATE(`date`)) AS d FROM `{SRC_TABLE}`;")
    row = cur.fetchone()
    as_of = (row or {}).get("d")
    if not as_of:
        raise RuntimeError(f"[FATAL] {SRC_TABLE} 没数据")
    return as_of


def _delete_ops_b_unbought(cur):
    sql = f"DELETE FROM `{OPS_TABLE}` WHERE stock_type='B' AND is_bought=0;"
    n = cur.execute(sql)
    print(f"[OK] deleted B(is_bought=0) rows={n}", flush=True)


def _load_candidates(cur, as_of):
    """
    第一层：SQL 只做区间过滤（避免拉全市场）
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


def _load_recent_bars(cur, symbol, as_of, limit=25):
    """
    拉最近 N 天（按 date DESC）
    """
    sql = f"""
    SELECT DATE(`date`) AS d, `close`, `volume`
    FROM `{SRC_TABLE}`
    WHERE symbol=%s AND DATE(`date`) <= DATE(%s)
    ORDER BY `date` DESC
    LIMIT %s;
    """
    cur.execute(sql, (symbol, as_of, int(limit)))
    return cur.fetchall() or []


def _safe_float(x, default=0.0):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _compute_metrics(bars_desc):
    """
    bars_desc: 最近日期在前（DESC）
    计算：
      close_today, close_prev, up_pct
      ma3, ma10
      vol_today, vol_avg20_prev（不含今天）
    """
    if len(bars_desc) < 21:
        return None

    closes = [_safe_float(r.get("close")) for r in bars_desc]
    vols = [_safe_float(r.get("volume")) for r in bars_desc]

    close_today = closes[0]
    close_prev = closes[1] if len(closes) >= 2 else 0.0
    up_pct = (close_today - close_prev) / close_prev if close_prev > 0 else 0.0

    if len(closes) < 10:
        return None
    ma3 = sum(closes[0:3]) / 3.0
    ma10 = sum(closes[0:10]) / 10.0

    vol_today = vols[0]
    # 过去20日均量：不含今天 => vols[1:21]
    vol_avg20_prev = sum(vols[1:21]) / 20.0

    return {
        "close_today": close_today,
        "close_prev": close_prev,
        "up_pct": up_pct,
        "ma3": ma3,
        "ma10": ma10,
        "vol_today": vol_today,
        "vol_avg20_prev": vol_avg20_prev,
    }


def _upsert_ops_b(cur, rows):
    """
    写入 ops：只写最核心字段（你之前的极简结构）
    """
    if not rows:
        return 0

    sql = f"""
    INSERT INTO `{OPS_TABLE}` (
      stock_code, trigger_price, stock_type, is_bought, created_at
    )
    VALUES (%s, %s, 'B', 0, %s)
    ON DUPLICATE KEY UPDATE
      trigger_price = IF(stock_type='A', trigger_price, VALUES(trigger_price)),
      created_at    = IF(stock_type='A', created_at,    VALUES(created_at)),
      stock_type    = IF(stock_type='A', stock_type,    VALUES(stock_type)),
      is_bought     = IF(stock_type='A', is_bought,     VALUES(is_bought));
    """
    args = [(r["stock_code"], r["trigger_price"], r["created_at"]) for r in rows]
    return cur.executemany(sql, args)


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

            # 1) 清理旧的未买入B
            _delete_ops_b_unbought(cur)

            # 2) 候选（区间）
            candidates = _load_candidates(cur, as_of)
            print(f"[INFO] candidates(in-range)={len(candidates)}", flush=True)

            selected = []
            printed = 0

            for c in candidates:
                sym = (c.get("symbol") or "").strip().upper()
                if not sym:
                    continue

                pressure_price = _safe_float(c.get("pressure_price"))
                pressure_date = c.get("pressure_date")
                last_close = _safe_float(c.get("last_close"))

                bars = _load_recent_bars(cur, sym, as_of, limit=25)
                m = _compute_metrics(bars)
                if not m:
                    continue

                # ✅ 新增优化条件：价格>2（用 close_today）
                if not (m["close_today"] > MIN_PRICE):
                    continue

                # ✅ 新增优化条件：当日成交量>1,000,000
                if not (m["vol_today"] > MIN_VOL_TODAY):
                    continue

                # 3) MA3 > MA10
                if not (m["ma3"] > m["ma10"]):
                    continue

                # 4) vol_today > avg20_prev * 1.5
                if not (m["vol_today"] > (m["vol_avg20_prev"] * VOL_MULT)):
                    continue

                # 5) up_pct > 2%
                if not (m["up_pct"] > UP_PCT_MIN):
                    continue

                created_at = f"{pressure_date} 00:00:00" if pressure_date else f"{as_of} 00:00:00"
                selected.append({
                    "stock_code": sym,
                    "trigger_price": round(pressure_price, 2),
                    "created_at": created_at,
                })

                if printed < PRINT_LIMIT:
                    printed += 1
                    print(
                        f"[PASS] {sym} last_close={last_close:.2f} pressure={pressure_price:.2f} "
                        f"close_today={m['close_today']:.2f} "
                        f"MA3={m['ma3']:.4f} MA10={m['ma10']:.4f} "
                        f"vol_today={m['vol_today']:.0f} vol_avg20_prev={m['vol_avg20_prev']:.0f} "
                        f"up_pct={m['up_pct']*100:.2f}%",
                        flush=True,
                    )

            affected = _upsert_ops_b(cur, selected)
            print(f"[OK] selected={len(selected)} upsert_affected={affected}", flush=True)

    finally:
        conn.close()


if __name__ == "__main__":
    main()