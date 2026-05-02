# -*- coding: utf-8 -*-
"""
策略 C 候选扫描器：全市场分类 -> 写入 strategy_c_candidates。

这个文件只做扫描和分类，不下单，不生成真实期权订单。
建议做成盘后/盘前定时任务，给 strategy_c.py 提供高确定性候选。

建表 SQL：

CREATE TABLE IF NOT EXISTS strategy_c_candidates (
    id BIGINT NOT NULL AUTO_INCREMENT,
    as_of DATE NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    category VARCHAR(32) NOT NULL,
    option_mode VARCHAR(32) NOT NULL,
    score DOUBLE NOT NULL DEFAULT 0,
    reason VARCHAR(800) NULL,
    close_price DOUBLE NULL,
    ma5 DOUBLE NULL,
    ma10 DOUBLE NULL,
    ma20 DOUBLE NULL,
    ma50 DOUBLE NULL,
    ret3 DOUBLE NULL,
    ret5 DOUBLE NULL,
    ret10 DOUBLE NULL,
    high20 DOUBLE NULL,
    low20 DOUBLE NULL,
    range20_pct DOUBLE NULL,
    dist_high20 DOUBLE NULL,
    dist_low20 DOUBLE NULL,
    vol_ratio DOUBLE NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_asof_symbol (as_of, symbol),
    KEY idx_asof_category_score (as_of, category, score),
    KEY idx_symbol (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

分类：
    STRONG_UP         -> BULL_CALL
    STRONG_DOWN       -> BEAR_PUT
    SIDEWAYS_BULLISH  -> BULL_PUT
    SIDEWAYS_BEARISH  -> BEAR_CALL
    NO_TRADE          -> NO_TRADE
"""

from __future__ import annotations

import os
import traceback
from collections import defaultdict
from datetime import date, datetime, timedelta

import pymysql


# =========================
# DB
# =========================
PRICES_TABLE = os.getenv("C_PRICES_TABLE", os.getenv("B_PRICES_TABLE", "stock_prices_pool"))
CANDIDATES_TABLE = os.getenv("C_CANDIDATES_TABLE", "strategy_c_candidates")

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
# 扫描参数
# =========================
C_SCAN_LOOKBACK_DAYS = int(os.getenv("C_SCAN_LOOKBACK_DAYS", "90"))
C_SCAN_MIN_BARS = int(os.getenv("C_SCAN_MIN_BARS", "60"))
C_SCAN_PRINT_LIMIT = int(os.getenv("C_SCAN_PRINT_LIMIT", "200"))
C_SCAN_WRITE_NO_TRADE = int(os.getenv("C_SCAN_WRITE_NO_TRADE", "0"))

C_SCAN_MIN_PRICE = float(os.getenv("C_SCAN_MIN_PRICE", "5"))
C_SCAN_MIN_AVG_VOL20 = float(os.getenv("C_SCAN_MIN_AVG_VOL20", "500000"))

C_UP_RET3 = float(os.getenv("C_UP_RET3", "0.015"))
C_DOWN_RET3 = float(os.getenv("C_DOWN_RET3", "-0.015"))
C_NEAR_HIGH20 = float(os.getenv("C_NEAR_HIGH20", "0.03"))
C_NEAR_LOW20 = float(os.getenv("C_NEAR_LOW20", "0.03"))

C_SIDEWAYS_MA20_BAND = float(os.getenv("C_SIDEWAYS_MA20_BAND", "0.025"))
C_SIDEWAYS_RANGE20_MAX = float(os.getenv("C_SIDEWAYS_RANGE20_MAX", "0.10"))
C_SIDEWAYS_RET10_MAX = float(os.getenv("C_SIDEWAYS_RET10_MAX", "0.04"))


CATEGORY_STRONG_UP = "STRONG_UP"
CATEGORY_STRONG_DOWN = "STRONG_DOWN"
CATEGORY_SIDEWAYS_BULLISH = "SIDEWAYS_BULLISH"
CATEGORY_SIDEWAYS_BEARISH = "SIDEWAYS_BEARISH"
CATEGORY_NO_TRADE = "NO_TRADE"

MODE_BULL_CALL = "BULL_CALL"
MODE_BEAR_PUT = "BEAR_PUT"
MODE_BULL_PUT = "BULL_PUT"
MODE_BEAR_CALL = "BEAR_CALL"
MODE_NO_TRADE = "NO_TRADE"


def _connect():
    return pymysql.connect(**DB)


def _safe_float(v, default=0.0):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _mean(xs):
    return sum(xs) / float(len(xs)) if xs else 0.0


def _fetch_as_of_date(cur) -> date:
    cur.execute(f"SELECT MAX(DATE(`date`)) AS d FROM `{PRICES_TABLE}`;")
    row = cur.fetchone() or {}
    as_of = row.get("d")
    if not as_of:
        raise RuntimeError(f"{PRICES_TABLE} 没有数据")
    return as_of


def _as_date(v) -> date:
    if isinstance(v, date):
        return v
    return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()


def _load_recent_bars_bulk(cur, as_of) -> dict[str, list[dict]]:
    """
    一次性加载全市场最近一段 K 线，避免每只股票单独查一次数据库。

    为什么不用 SQL 的 ROW_NUMBER() 做每只股票 LIMIT 90：
    - 有些 MySQL 版本不支持窗口函数。
    - 这里用“日历天回看”批量拉取，再在 Python 里截取每只股票最后 N 根。

    这会比 N+1 查询快很多；云数据库尤其明显，因为最大开销通常是网络往返。
    """
    as_of_d = _as_date(as_of)
    as_of_next = as_of_d + timedelta(days=1)
    calendar_days = max(int(C_SCAN_LOOKBACK_DAYS) * 2, int(C_SCAN_LOOKBACK_DAYS) + 30)
    cutoff = as_of_d - timedelta(days=calendar_days)

    sql = f"""
    SELECT
        p.symbol,
        DATE(p.`date`) AS d,
        p.`open`,
        p.`high`,
        p.`low`,
        p.`close`,
        p.`volume`
    FROM `{PRICES_TABLE}` p
    JOIN (
        SELECT DISTINCT symbol
        FROM `{PRICES_TABLE}`
        WHERE `date` >= %s
          AND `date` < %s
          AND symbol IS NOT NULL
          AND `close` IS NOT NULL
          AND `close` >= %s
          AND `volume` IS NOT NULL
          AND `volume` > 0
    ) s
      ON s.symbol = p.symbol
    WHERE p.`date` >= %s
      AND p.`date` < %s
      AND p.`close` IS NOT NULL
    ORDER BY p.symbol ASC, p.`date` ASC;
    """
    cur.execute(sql, (as_of_d, as_of_next, float(C_SCAN_MIN_PRICE), cutoff, as_of_next))
    rows = cur.fetchall() or []

    grouped = defaultdict(list)
    for r in rows:
        symbol = (r.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        grouped[symbol].append(r)

    # 每只股票只保留最后 C_SCAN_LOOKBACK_DAYS 根，保持 calc_metrics 的输入为升序。
    return {symbol: bars[-int(C_SCAN_LOOKBACK_DAYS):] for symbol, bars in grouped.items()}


def calc_metrics(bars: list[dict]) -> dict | None:
    if len(bars) < C_SCAN_MIN_BARS:
        return None

    closes = [_safe_float(r.get("close")) for r in bars]
    highs = [_safe_float(r.get("high")) for r in bars]
    lows = [_safe_float(r.get("low")) for r in bars]
    vols = [_safe_float(r.get("volume")) for r in bars]

    close = closes[-1]
    if close <= 0:
        return None

    ma5 = _mean(closes[-5:])
    ma10 = _mean(closes[-10:])
    ma20 = _mean(closes[-20:])
    ma50 = _mean(closes[-50:])

    ret3 = close / closes[-4] - 1 if len(closes) >= 4 and closes[-4] > 0 else 0.0
    ret5 = close / closes[-6] - 1 if len(closes) >= 6 and closes[-6] > 0 else 0.0
    ret10 = close / closes[-11] - 1 if len(closes) >= 11 and closes[-11] > 0 else 0.0

    high20 = max(highs[-20:])
    low20 = min(lows[-20:])
    range20_pct = (high20 - low20) / close if close > 0 else 0.0
    dist_high20 = close / high20 - 1 if high20 > 0 else 0.0
    dist_low20 = close / low20 - 1 if low20 > 0 else 0.0

    vol20 = _mean(vols[-20:])
    vol_ratio = vols[-1] / vol20 if vol20 > 0 else 0.0

    return {
        "close": close,
        "ma5": ma5,
        "ma10": ma10,
        "ma20": ma20,
        "ma50": ma50,
        "ret3": ret3,
        "ret5": ret5,
        "ret10": ret10,
        "high20": high20,
        "low20": low20,
        "range20_pct": range20_pct,
        "dist_high20": dist_high20,
        "dist_low20": dist_low20,
        "vol20": vol20,
        "vol_ratio": vol_ratio,
    }


def classify_symbol(m: dict) -> tuple[str, str, float, str]:
    """
    返回：(category, option_mode, score, reason)
    """
    close = m["close"]
    ma5 = m["ma5"]
    ma10 = m["ma10"]
    ma20 = m["ma20"]
    ma50 = m["ma50"]
    ret3 = m["ret3"]
    ret5 = m["ret5"]
    ret10 = m["ret10"]
    range20_pct = m["range20_pct"]
    dist_high20 = m["dist_high20"]
    dist_low20 = m["dist_low20"]
    vol20 = m["vol20"]
    vol_ratio = m["vol_ratio"]

    if vol20 < C_SCAN_MIN_AVG_VOL20:
        return (
            CATEGORY_NO_TRADE,
            MODE_NO_TRADE,
            0.0,
            f"NO_TRADE avg_vol20={vol20:.0f} < min={C_SCAN_MIN_AVG_VOL20:.0f}",
        )

    strong_up = (
        close > ma5 > ma10 > ma20
        and ma20 >= ma50
        and ret3 >= C_UP_RET3
        and dist_high20 >= -C_NEAR_HIGH20
    )
    strong_down = (
        close < ma5 < ma10 < ma20
        and ma20 <= ma50
        and ret3 <= C_DOWN_RET3
        and dist_low20 <= C_NEAR_LOW20
    )
    sideways = (
        abs(close / ma20 - 1) <= C_SIDEWAYS_MA20_BAND
        and range20_pct <= C_SIDEWAYS_RANGE20_MAX
        and abs(ret10) <= C_SIDEWAYS_RET10_MAX
    )

    if strong_up:
        score = 80 + min(ret5 * 100, 20) + min(vol_ratio, 3) * 3
        reason = (
            f"STRONG_UP close>MA5>MA10>MA20, ret3={ret3:.2%}, "
            f"ret5={ret5:.2%}, dist_high20={dist_high20:.2%}, volx={vol_ratio:.2f}"
        )
        return CATEGORY_STRONG_UP, MODE_BULL_CALL, round(score, 2), reason

    if strong_down:
        score = 80 + min(abs(ret5) * 100, 20) + min(vol_ratio, 3) * 3
        reason = (
            f"STRONG_DOWN close<MA5<MA10<MA20, ret3={ret3:.2%}, "
            f"ret5={ret5:.2%}, dist_low20={dist_low20:.2%}, volx={vol_ratio:.2f}"
        )
        return CATEGORY_STRONG_DOWN, MODE_BEAR_PUT, round(score, 2), reason

    if sideways:
        base = 55 + max(0.0, 1.0 - range20_pct / max(C_SIDEWAYS_RANGE20_MAX, 0.01)) * 20
        if close >= ma20 and ma5 >= ma10:
            reason = (
                f"SIDEWAYS_BULLISH close>=MA20 MA5>=MA10, "
                f"range20={range20_pct:.2%}, ret10={ret10:.2%}, volx={vol_ratio:.2f}"
            )
            return CATEGORY_SIDEWAYS_BULLISH, MODE_BULL_PUT, round(base, 2), reason

        if close <= ma20 and ma5 <= ma10:
            reason = (
                f"SIDEWAYS_BEARISH close<=MA20 MA5<=MA10, "
                f"range20={range20_pct:.2%}, ret10={ret10:.2%}, volx={vol_ratio:.2f}"
            )
            return CATEGORY_SIDEWAYS_BEARISH, MODE_BEAR_CALL, round(base, 2), reason

    reason = (
        f"NO_TRADE close={close:.2f}, MA5={ma5:.2f}, MA10={ma10:.2f}, "
        f"MA20={ma20:.2f}, ret3={ret3:.2%}, ret10={ret10:.2%}, range20={range20_pct:.2%}"
    )
    return CATEGORY_NO_TRADE, MODE_NO_TRADE, 0.0, reason


def save_candidate(cur, as_of, symbol: str, category: str, option_mode: str, score: float, reason: str, m: dict):
    if category == CATEGORY_NO_TRADE and C_SCAN_WRITE_NO_TRADE != 1:
        return

    sql = f"""
    INSERT INTO `{CANDIDATES_TABLE}` (
        as_of, symbol, category, option_mode, score, reason,
        close_price, ma5, ma10, ma20, ma50,
        ret3, ret5, ret10,
        high20, low20, range20_pct, dist_high20, dist_low20,
        vol_ratio,
        created_at, updated_at
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
    ON DUPLICATE KEY UPDATE
        category=VALUES(category),
        option_mode=VALUES(option_mode),
        score=VALUES(score),
        reason=VALUES(reason),
        close_price=VALUES(close_price),
        ma5=VALUES(ma5),
        ma10=VALUES(ma10),
        ma20=VALUES(ma20),
        ma50=VALUES(ma50),
        ret3=VALUES(ret3),
        ret5=VALUES(ret5),
        ret10=VALUES(ret10),
        high20=VALUES(high20),
        low20=VALUES(low20),
        range20_pct=VALUES(range20_pct),
        dist_high20=VALUES(dist_high20),
        dist_low20=VALUES(dist_low20),
        vol_ratio=VALUES(vol_ratio),
        updated_at=NOW();
    """
    cur.execute(
        sql,
        (
            as_of,
            symbol,
            category,
            option_mode,
            float(score),
            (reason or "")[:800],
            m["close"],
            m["ma5"],
            m["ma10"],
            m["ma20"],
            m["ma50"],
            m["ret3"],
            m["ret5"],
            m["ret10"],
            m["high20"],
            m["low20"],
            m["range20_pct"],
            m["dist_high20"],
            m["dist_low20"],
            m["vol_ratio"],
        ),
    )


def _candidate_args(as_of, symbol: str, category: str, option_mode: str, score: float, reason: str, m: dict):
    return (
        as_of,
        symbol,
        category,
        option_mode,
        float(score),
        (reason or "")[:800],
        m["close"],
        m["ma5"],
        m["ma10"],
        m["ma20"],
        m["ma50"],
        m["ret3"],
        m["ret5"],
        m["ret10"],
        m["high20"],
        m["low20"],
        m["range20_pct"],
        m["dist_high20"],
        m["dist_low20"],
        m["vol_ratio"],
    )


def save_candidates_bulk(cur, rows: list[tuple]) -> int:
    """
    批量写入候选结果。

    原来每个候选执行一次 INSERT；云数据库会被网络往返拖慢。
    这里改成 executemany，一批提交，分类结果不变。
    """
    if not rows:
        return 0

    sql = f"""
    INSERT INTO `{CANDIDATES_TABLE}` (
        as_of, symbol, category, option_mode, score, reason,
        close_price, ma5, ma10, ma20, ma50,
        ret3, ret5, ret10,
        high20, low20, range20_pct, dist_high20, dist_low20,
        vol_ratio,
        created_at, updated_at
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
    ON DUPLICATE KEY UPDATE
        category=VALUES(category),
        option_mode=VALUES(option_mode),
        score=VALUES(score),
        reason=VALUES(reason),
        close_price=VALUES(close_price),
        ma5=VALUES(ma5),
        ma10=VALUES(ma10),
        ma20=VALUES(ma20),
        ma50=VALUES(ma50),
        ret3=VALUES(ret3),
        ret5=VALUES(ret5),
        ret10=VALUES(ret10),
        high20=VALUES(high20),
        low20=VALUES(low20),
        range20_pct=VALUES(range20_pct),
        dist_high20=VALUES(dist_high20),
        dist_low20=VALUES(dist_low20),
        vol_ratio=VALUES(vol_ratio),
        updated_at=NOW();
    """
    return cur.executemany(sql, rows)


def scan_all():
    conn = _connect()
    counts = {}
    printed = 0
    saved = 0
    rows_to_save = []

    try:
        with conn.cursor() as cur:
            as_of = _fetch_as_of_date(cur)
            bars_by_symbol = _load_recent_bars_bulk(cur, as_of)
            print(
                f"[C SCAN] as_of={as_of} symbols={len(bars_by_symbol)} "
                f"lookback={C_SCAN_LOOKBACK_DAYS}",
                flush=True,
            )

            for symbol, bars in bars_by_symbol.items():
                try:
                    m = calc_metrics(bars)
                    if not m:
                        continue

                    category, option_mode, score, reason = classify_symbol(m)
                    counts[category] = counts.get(category, 0) + 1

                    if category != CATEGORY_NO_TRADE:
                        rows_to_save.append(_candidate_args(as_of, symbol, category, option_mode, score, reason, m))
                        saved += 1
                        if printed < C_SCAN_PRINT_LIMIT:
                            printed += 1
                            print(
                                f"[C PASS] {symbol} category={category} mode={option_mode} "
                                f"score={score:.2f} close={m['close']:.2f} reason={reason}",
                                flush=True,
                            )
                    elif C_SCAN_WRITE_NO_TRADE == 1:
                        rows_to_save.append(_candidate_args(as_of, symbol, category, option_mode, score, reason, m))

                except Exception as e:
                    print(f"[C SCAN] {symbol} error: {e}", flush=True)
                    traceback.print_exc()

            affected = save_candidates_bulk(cur, rows_to_save)

        print(f"[C SCAN OK] saved={saved} write_rows={len(rows_to_save)} affected={affected} counts={counts}", flush=True)
        return saved

    finally:
        conn.close()


def main():
    scan_all()


if __name__ == "__main__":
    main()


# python app/strategy_c_scan_candidates.py
