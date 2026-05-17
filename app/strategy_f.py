# -*- coding: utf-8 -*-
"""
app/strategy_f.py

策略F：B 卖出股二次拉回买入器。

核心逻辑：
1) 只扫描 monster_watchlist 中 B 卖出后的 WATCHING 股票。
2) 当前价相对 last_sell_price 涨回 5%~15%。
3) 当前价处于日内振幅上方区域 intraday_pos >= 0.55。
4) 每 2 分钟确认一次 Top3。
5) 同一只股票连续 3 个确认轮都进入 Top3，才写入 stock_operations，作为 F 候选等待买入。
6) 真正下单由 strategy_F_buy 执行。
"""

import os
import math
import traceback
from datetime import datetime, timedelta

import pymysql

from app.strategy_b import (
    B_DATA_FEED,
    _cancel_open_buy_orders,
    _get_extended_quote_realtime,
    _get_buying_power,
    _get_real_position_qty,
    _get_trading_client,
    _intent_short,
    _reconcile_fill,
    _reconcile_sell_fill,
    _sleep_for_rate_limit,
    _snapshot_http,
    _submit_limit_buy_qty,
    _submit_limit_qty_ext,
    _submit_market_qty,
    get_snapshot_realtime,
)


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

MONSTER_TABLE = os.getenv("MONSTER_TABLE", "monster_watchlist")
OPS_TABLE = os.getenv("OPS_TABLE", "stock_operations")
F_SCORE_TABLE = os.getenv("F_SCORE_TABLE", "strategy_f_buy_scores")


# =========================
# F 扫描参数
# =========================
F_SCORE_TOP_N = int(os.getenv("F_SCORE_TOP_N", "3"))
F_SCORE_CONFIRMATIONS = int(os.getenv("F_SCORE_CONFIRMATIONS", "3"))
F_CONFIRM_INTERVAL_SECONDS = int(os.getenv("F_CONFIRM_INTERVAL_SECONDS", "120"))

# 当前价相对 B 卖出价涨回 5%~15%
F_RECLAIM_MIN_PCT = float(os.getenv("F_RECLAIM_MIN_PCT", "0.05"))
F_RECLAIM_MAX_PCT = float(os.getenv("F_RECLAIM_MAX_PCT", "0.15"))

# 日内位置，默认 0.55
F_INTRADAY_POS_MIN = float(os.getenv("F_INTRADAY_POS_MIN", "0.55"))


# =========================
# F 仓位参数
# =========================
F_TARGET_NOTIONAL_USD = float(os.getenv("F_TARGET_NOTIONAL_USD", "1500"))
F_MAX_NOTIONAL_USD = float(os.getenv("F_MAX_NOTIONAL_USD", "1500"))
F_MIN_TRADE_NOTIONAL = float(os.getenv("F_MIN_TRADE_NOTIONAL", "300"))
F_BP_USE_RATIO = float(os.getenv("F_BP_USE_RATIO", "0.98"))
F_MARGIN_POOL_PCT = float(os.getenv("F_MARGIN_POOL_PCT", "0.15"))

# 初始止损 -3%
F_INIT_STOP_PCT = float(os.getenv("F_INIT_STOP_PCT", "0.03"))

# 盈利 5% 后抬到保本
F_LOCK_BREAKEVEN_PCT = float(os.getenv("F_LOCK_BREAKEVEN_PCT", "0.05"))

# 高点回撤保护
F_PEAK_GIVEBACK_RULES = [
    (0.60, 0.10),
    (0.30, 0.08),
    (0.15, 0.06),
    (0.05, 0.04),
]

# 分批止盈
F_STAGE_RULES = [
    (1, 0.15, 0.25),
    (2, 0.30, 0.25),
    (3, 0.50, 0.20),
]


def _connect():
    return pymysql.connect(**DB)


def _safe_float(v, default=0.0):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _bucket_time_now():
    """
    每 F_CONFIRM_INTERVAL_SECONDS 秒一个确认桶。
    默认 120 秒，也就是每 2 分钟算一轮。
    """
    now = datetime.now().replace(microsecond=0)
    seconds = int(now.timestamp())
    bucket_seconds = seconds - (seconds % int(F_CONFIRM_INTERVAL_SECONDS))
    return datetime.fromtimestamp(bucket_seconds)


def _ensure_monster_watchlist_table(conn):
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{MONSTER_TABLE}` (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        stock_code VARCHAR(20) NOT NULL,
        source_strategy VARCHAR(8) NOT NULL DEFAULT 'B',
        source_reason VARCHAR(255) NULL,
        last_sell_price DOUBLE NULL,
        last_sell_time DATETIME NULL,
        b_peak_price DOUBLE NULL,
        b_peak_profit DOUBLE NULL,
        watch_status VARCHAR(16) NOT NULL DEFAULT 'WATCHING',
        watch_since DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        last_checked_at DATETIME NULL,
        notes VARCHAR(500) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_symbol_source (stock_code, source_strategy),
        KEY idx_status_source (watch_status, source_strategy),
        KEY idx_watch_since (watch_since)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def _ensure_f_score_table(conn):
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{F_SCORE_TABLE}` (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        bucket_time DATETIME NOT NULL,
        symbol VARCHAR(20) NOT NULL,
        watch_id BIGINT NULL,
        rank_no INT NOT NULL,
        score DOUBLE NOT NULL,
        price DOUBLE NULL,
        last_sell_price DOUBLE NULL,
        reclaim_pct DOUBLE NULL,
        day_up_pct DOUBLE NULL,
        intraday_pos DOUBLE NULL,
        reason VARCHAR(255) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_bucket_symbol (bucket_time, symbol),
        KEY idx_symbol_bucket (symbol, bucket_time),
        KEY idx_bucket_rank (bucket_time, rank_no)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def _load_watch_rows(cur):
    sql = f"""
    SELECT
        id,
        stock_code,
        source_reason,
        last_sell_price,
        last_sell_time,
        b_peak_price,
        b_peak_profit,
        watch_since
    FROM `{MONSTER_TABLE}`
    WHERE watch_status='WATCHING'
      AND source_strategy='B'
    ORDER BY watch_since ASC, id ASC;
    """
    cur.execute(sql)
    return cur.fetchall() or []


def _load_watch_row_by_code(cur, code: str):
    sql = f"""
    SELECT *
    FROM `{MONSTER_TABLE}`
    WHERE stock_code=%s
      AND watch_status IN ('WATCHING', 'READY', 'BOUGHT')
    ORDER BY id DESC
    LIMIT 1;
    """
    cur.execute(sql, (code,))
    return cur.fetchone()


def _load_ops_row(cur, code: str):
    sql = f"""
    SELECT *
    FROM `{OPS_TABLE}`
    WHERE stock_code=%s
    LIMIT 1;
    """
    cur.execute(sql, (code,))
    return cur.fetchone()


def _load_one_f_row(conn, code: str):
    sql = f"""
    SELECT *
    FROM `{OPS_TABLE}`
    WHERE stock_code=%s
      AND stock_type='F'
    LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (code,))
        return cur.fetchone()


def _update_watch_note(cur, row_id: int, note: str):
    sql = f"""
    UPDATE `{MONSTER_TABLE}`
    SET last_checked_at=NOW(),
        notes=%s
    WHERE id=%s;
    """
    cur.execute(sql, ((note or "")[:500], int(row_id)))


def _mark_watch_status(cur, row_id: int, status: str, note: str):
    sql = f"""
    UPDATE `{MONSTER_TABLE}`
    SET watch_status=%s,
        last_checked_at=NOW(),
        notes=%s
    WHERE id=%s;
    """
    cur.execute(sql, (status, (note or "")[:500], int(row_id)))


def _update_ops_f_fields(conn, code: str, **kwargs):
    if not kwargs:
        return

    cols = []
    vals = []

    for k, v in kwargs.items():
        cols.append(f"`{k}`=%s")
        vals.append(v)

    vals.append(code)

    sql = f"""
    UPDATE `{OPS_TABLE}`
    SET {', '.join(cols)}
    WHERE stock_code=%s
      AND stock_type='F';
    """

    with conn.cursor() as cur:
        cur.execute(sql, tuple(vals))


def _get_realtime_daily_bar(code: str):
    code = (code or "").strip().upper()
    if not code:
        raise RuntimeError("empty symbol")

    _sleep_for_rate_limit()
    r = _snapshot_http(code, B_DATA_FEED)

    if r.status_code != 200:
        raise RuntimeError(f"snapshot http {r.status_code}: {r.text[:200]}")

    js = r.json()
    db = js.get("dailyBar") or {}
    lt = js.get("latestTrade") or {}
    lq = js.get("latestQuote") or {}
    pb = js.get("prevDailyBar") or {}

    bid = _safe_float(lq.get("bp"), 0.0)
    ask = _safe_float(lq.get("ap"), 0.0)

    last = _safe_float(lt.get("p"), 0.0)
    if last <= 0:
        last = _safe_float(db.get("c"), 0.0)
    if last <= 0 and bid > 0 and ask > 0:
        last = (bid + ask) / 2.0

    open_ = _safe_float(db.get("o"), last)
    high = _safe_float(db.get("h"), last)
    low = _safe_float(db.get("l"), last)
    prev_close = _safe_float(pb.get("c"), 0.0)

    if last <= 0 or prev_close <= 0:
        raise RuntimeError(f"snapshot missing fields: last={last} prev_close={prev_close}")

    high = max(high, last)
    low = min(low if low > 0 else last, last)

    return {
        "date": db.get("t") or "realtime",
        "open": open_,
        "high": high,
        "low": low,
        "close": last,
        "prev_close": prev_close,
        "bid": bid,
        "ask": ask,
        "feed": B_DATA_FEED,
    }


def _score_f_candidate(row: dict):
    """
    F 简化版买入评分：

    必须满足：
    1. price 比 last_sell_price 高 5%~15%
    2. intraday_pos >= 0.55

    不要求 day_up >= 5%。
    """
    code = (row.get("stock_code") or "").strip().upper()
    last_sell = _safe_float(row.get("last_sell_price"))

    if not code or last_sell <= 0:
        return None

    rt = _get_realtime_daily_bar(code)

    price = _safe_float(rt.get("close"))
    prev_close = _safe_float(rt.get("prev_close"))
    high = _safe_float(rt.get("high"))
    low = _safe_float(rt.get("low"))

    if price <= 0:
        return None

    reclaim_pct = (price - last_sell) / last_sell if last_sell > 0 else 0.0
    day_up = (price - prev_close) / prev_close if prev_close > 0 else 0.0
    intraday_pos = (price - low) / (high - low) if high > low else 1.0

    min_price = last_sell * (1.0 + F_RECLAIM_MIN_PCT)
    max_price = last_sell * (1.0 + F_RECLAIM_MAX_PCT)

    if not (
        reclaim_pct >= F_RECLAIM_MIN_PCT
        and reclaim_pct <= F_RECLAIM_MAX_PCT
        and intraday_pos >= F_INTRADAY_POS_MIN
    ):
        return None

    headroom_pct = max(0.0, F_RECLAIM_MAX_PCT - reclaim_pct)

    score = (
        reclaim_pct * 300.0
        + intraday_pos * 100.0
        + headroom_pct * 120.0
        + max(day_up, 0.0) * 80.0
    )

    reason = (
        f"price={price:.2f} last_sell={last_sell:.2f} "
        f"range={min_price:.2f}-{max_price:.2f} "
        f"reclaim={reclaim_pct:.2%} "
        f"day_up={day_up:.2%} "
        f"intraday_pos={intraday_pos:.2f}"
    )

    return {
        "symbol": code,
        "watch_id": int(row.get("id") or 0),
        "watch": row,
        "score": round(float(score), 4),
        "price": price,
        "last_sell_price": last_sell,
        "reclaim_pct": reclaim_pct,
        "day_up_pct": day_up,
        "intraday_pos": intraday_pos,
        "reason": reason[:255],
        "metrics": {
            "date": rt.get("date"),
            "open": _safe_float(rt.get("open")),
            "close": price,
            "high": high,
            "low": low,
            "day_up": day_up,
            "intraday_pos": intraday_pos,
            "bid": _safe_float(rt.get("bid")),
            "ask": _safe_float(rt.get("ask")),
            "feed": rt.get("feed"),
        },
        "detail": {
            "last_sell_price": last_sell,
            "min_reclaim_price": min_price,
            "max_reclaim_price": max_price,
            "reclaim_pct": reclaim_pct,
        },
    }


def _get_active_f_used_capital(conn) -> float:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT market_value, cost_basis, qty, current_price, avg_entry_price
                FROM position_holdings
                WHERE status='open'
                  AND UPPER(
                    CASE
                      WHEN strategy_group IN ('A','B','C','D','F') THEN strategy_group
                      WHEN stock_type IN ('A','B','C','D','F') THEN stock_type
                      ELSE strategy_group
                    END
                  )='F'
                """
            )
            rows = cur.fetchall() or []

        total = 0.0

        for row in rows:
            if row.get("market_value") is not None:
                total += abs(float(row.get("market_value") or 0.0))
                continue

            if row.get("cost_basis") is not None:
                total += abs(float(row.get("cost_basis") or 0.0))
                continue

            qty = float(row.get("qty") or 0.0)
            price = row.get("current_price")
            if price is None:
                price = row.get("avg_entry_price")

            total += abs(qty * float(price or 0.0))

        if total > 0:
            return total

    except Exception:
        pass

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT qty, current_price, close_price, cost_price
            FROM `{OPS_TABLE}`
            WHERE stock_type='F'
              AND is_bought=1;
            """
        )
        rows = cur.fetchall() or []

    total = 0.0

    for row in rows:
        qty = float(row.get("qty") or 0.0)
        price = row.get("current_price")

        if price is None:
            price = row.get("close_price")
        if price is None:
            price = row.get("cost_price")

        total += abs(qty * float(price or 0.0))

    return total


def _f_margin_buy_plan(conn, buying_power: float) -> dict:
    buying_power = max(0.0, float(buying_power or 0.0))
    pool_cap = buying_power * max(0.0, float(F_MARGIN_POOL_PCT))
    used = _get_active_f_used_capital(conn)
    available = max(0.0, pool_cap - used)

    target = min(
        float(F_TARGET_NOTIONAL_USD),
        float(F_MAX_NOTIONAL_USD),
        available,
        buying_power * float(F_BP_USE_RATIO),
    )

    return {
        "pool_pct": float(F_MARGIN_POOL_PCT),
        "pool_cap": pool_cap,
        "used": used,
        "available": available,
        "target": target,
    }


def _upsert_ready_f_ops(cur, code: str, row: dict, m: dict, detail: dict, note: str) -> bool:
    existing = _load_ops_row(cur, code)

    trigger_price = round(float(detail.get("last_sell_price") or 0), 2)
    close_price = round(float(m.get("close") or 0), 2)
    entry_open = round(float(m.get("open") or 0), 2)
    entry_close = round(float(m.get("close") or 0), 2)
    entry_date = str(m.get("date"))

    if existing:
        is_bought = int(existing.get("is_bought") or 0)
        old_type = str(existing.get("stock_type") or "").strip().upper()

        if is_bought == 1:
            print(f"[F READY] {code} skip: already bought stock_type={old_type}", flush=True)
            return False

        if old_type not in ("B", "F"):
            print(f"[F READY] {code} skip: protected old stock_type={old_type}", flush=True)
            return False

        sql = f"""
        UPDATE `{OPS_TABLE}`
        SET stock_type='F',
            is_bought=0,
            can_buy=1,
            can_sell=0,
            trigger_price=%s,
            close_price=%s,
            entry_open=%s,
            entry_close=%s,
            entry_date=%s,
            stop_loss_price=NULL,
            take_profit_price=0,
            b_stage=0,
            b_peak_price=NULL,
            b_peak_profit=0,
            b_last_profit=0,
            last_order_side=NULL,
            last_order_intent=%s,
            updated_at=CURRENT_TIMESTAMP
        WHERE stock_code=%s;
        """

        cur.execute(
            sql,
            (
                trigger_price,
                close_price,
                entry_open,
                entry_close,
                entry_date,
                _intent_short(note),
                code,
            ),
        )
        return True

    sql = f"""
    INSERT INTO `{OPS_TABLE}` (
        stock_code,
        stock_type,
        is_bought,
        can_buy,
        can_sell,
        trigger_price,
        close_price,
        entry_open,
        entry_close,
        entry_date,
        stop_loss_price,
        take_profit_price,
        b_stage,
        b_peak_price,
        b_peak_profit,
        b_last_profit,
        last_order_intent,
        created_at,
        updated_at
    )
    VALUES (%s, 'F', 0, 1, 0, %s, %s, %s, %s, %s, NULL, 0, 0, NULL, 0, 0, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
    """

    cur.execute(
        sql,
        (
            code,
            trigger_price,
            close_price,
            entry_open,
            entry_close,
            entry_date,
            _intent_short(note),
        ),
    )

    return True


def strategy_F_scan(prepare_buy: bool = False):
    """
    扫描 F 观察池。

    prepare_buy=False：
      只打印当前 Top3。

    prepare_buy=True：
      每 2 分钟确认一次。
      同一只股票连续 3 个确认桶都进入 Top3，才写入 F/can_buy=1。
    """
    conn = _connect()

    try:
        _ensure_monster_watchlist_table(conn)
        _ensure_f_score_table(conn)

        with conn.cursor() as cur:
            rows = _load_watch_rows(cur)

        scored = []

        for row in rows:
            code = (row.get("stock_code") or "").strip().upper()

            if not code:
                continue

            try:
                item = _score_f_candidate(row)
            except Exception as e:
                with conn.cursor() as cur:
                    _update_watch_note(cur, int(row["id"]), f"HOLD: F realtime score failed {e}")
                print(f"[F SCORE] {code} skip score error: {e}", flush=True)
                continue

            if item:
                scored.append(item)

        scored.sort(key=lambda x: x["score"], reverse=True)

        top_n = max(int(F_SCORE_TOP_N), 1)
        top = scored[:top_n]

        bucket_time = _bucket_time_now()

        if top:
            sql = f"""
            INSERT INTO `{F_SCORE_TABLE}` (
                bucket_time,
                symbol,
                watch_id,
                rank_no,
                score,
                price,
                last_sell_price,
                reclaim_pct,
                day_up_pct,
                intraday_pos,
                reason
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                watch_id=VALUES(watch_id),
                rank_no=VALUES(rank_no),
                score=VALUES(score),
                price=VALUES(price),
                last_sell_price=VALUES(last_sell_price),
                reclaim_pct=VALUES(reclaim_pct),
                day_up_pct=VALUES(day_up_pct),
                intraday_pos=VALUES(intraday_pos),
                reason=VALUES(reason);
            """

            args = []

            for idx, item in enumerate(top, start=1):
                args.append(
                    (
                        bucket_time,
                        item["symbol"],
                        item["watch_id"],
                        idx,
                        item["score"],
                        item["price"],
                        item["last_sell_price"],
                        item["reclaim_pct"],
                        item["day_up_pct"],
                        item["intraday_pos"],
                        item["reason"],
                    )
                )

            with conn.cursor() as cur:
                cur.executemany(sql, args)

        print(
            f"[F SCORE] bucket={bucket_time} watching={len(rows)} scored={len(scored)} "
            f"top={','.join([x['symbol'] for x in top]) or '-'}",
            flush=True,
        )

        selected = []

        if prepare_buy and top:
            confirm_sql = f"""
            SELECT
                s.symbol,
                COUNT(DISTINCT s.bucket_time) AS hits,
                MAX(CASE WHEN s.bucket_time=%s THEN s.rank_no END) AS latest_rank,
                AVG(s.score) AS avg_score
            FROM `{F_SCORE_TABLE}` s
            JOIN (
                SELECT DISTINCT bucket_time
                FROM `{F_SCORE_TABLE}`
                ORDER BY bucket_time DESC
                LIMIT %s
            ) b
              ON b.bucket_time = s.bucket_time
            GROUP BY s.symbol
            HAVING hits >= %s
               AND latest_rank IS NOT NULL
            ORDER BY latest_rank ASC, avg_score DESC
            LIMIT 1;
            """

            with conn.cursor() as cur:
                cur.execute(
                    confirm_sql,
                    (
                        bucket_time,
                        int(F_SCORE_CONFIRMATIONS),
                        int(F_SCORE_CONFIRMATIONS),
                    ),
                )
                confirmed = cur.fetchone()

            if confirmed:
                symbol = str(confirmed.get("symbol") or "").upper()
                item = next((x for x in top if x["symbol"] == symbol), None)

                if item:
                    note = (
                        f"PASS: F_TOP{F_SCORE_TOP_N}_CONFIRM "
                        f"hits={confirmed.get('hits')} {item['reason']}"
                    )

                    with conn.cursor() as cur:
                        ready_ok = _upsert_ready_f_ops(
                            cur,
                            symbol,
                            item["watch"],
                            item["metrics"],
                            item["detail"],
                            note,
                        )

                        if ready_ok:
                            _mark_watch_status(cur, int(item["watch_id"]), "READY", note)
                            selected.append(item)
                            print(f"[F READY] {symbol} {note}", flush=True)
                        else:
                            _update_watch_note(cur, int(item["watch_id"]), note)

        return selected if prepare_buy else top

    finally:
        conn.close()


def strategy_F_refresh_candidates():
    rows = strategy_F_scan(prepare_buy=True)
    return len(rows)


def strategy_F_buy(code: str) -> bool:
    code = (code or "").strip().upper()
    print(f"[F BUY] {code}", flush=True)

    conn = None
    order_id = None

    try:
        conn = _connect()

        row = _load_one_f_row(conn, code)

        if not row:
            print(f"[F BUY] {code} skip: no F row", flush=True)
            return False

        can_buy = int(row.get("can_buy") or 0)
        is_bought = int(row.get("is_bought") or 0)

        if can_buy != 1:
            print(f"[F BUY] {code} skip: can_buy={can_buy}", flush=True)
            return False

        if is_bought == 1:
            print(f"[F BUY] {code} skip: already bought", flush=True)
            return False

        with conn.cursor() as cur:
            watch = _load_watch_row_by_code(cur, code)

        if not watch:
            print(f"[F BUY] {code} skip: no watch row", flush=True)
            return False

        try:
            item = _score_f_candidate(watch)
        except Exception as e:
            print(f"[F BUY] {code} skip: realtime snapshot failed {e}", flush=True)
            return False

        if not item:
            print(f"[F BUY] {code} skip: signal faded simple reclaim rule", flush=True)

            _update_ops_f_fields(
                conn,
                code,
                can_buy=0,
                last_order_intent=_intent_short("F:BUY_SIGNAL_FADED simple_reclaim_rule"),
                updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )

            with conn.cursor() as cur:
                _mark_watch_status(cur, int(watch["id"]), "WATCHING", "BUY_SIGNAL_FADED: simple_reclaim_rule")

            return False

        m = item["metrics"]
        detail = item["detail"]

        price = float(m.get("close") or 0.0)
        bid = float(m.get("bid") or 0.0)
        ask = float(m.get("ask") or 0.0)

        if price <= 0:
            print(f"[F BUY] {code} skip: invalid realtime price={price:.2f}", flush=True)
            return False

        last_sell = float(detail.get("last_sell_price") or 0.0)
        max_reclaim_price = float(detail.get("max_reclaim_price") or 0.0)

        tc = _get_trading_client()
        buying_power = _get_buying_power(tc)
        margin_plan = _f_margin_buy_plan(conn, buying_power)

        print(
            f"[F BUY PLAN] {code} buying_power={buying_power:.2f} "
            f"pool_pct={margin_plan['pool_pct']:.2%} "
            f"pool_cap={margin_plan['pool_cap']:.2f} "
            f"used={margin_plan['used']:.2f} "
            f"available={margin_plan['available']:.2f} "
            f"target={margin_plan['target']:.2f}",
            flush=True,
        )

        if margin_plan["target"] < float(F_MIN_TRADE_NOTIONAL):
            print(
                f"[F BUY] {code} skip: target={margin_plan['target']:.2f} "
                f"< min_trade={float(F_MIN_TRADE_NOTIONAL):.2f}",
                flush=True,
            )
            return False

        required_bp = float(margin_plan["target"]) / max(float(F_BP_USE_RATIO), 0.01)

        if buying_power < required_bp:
            print(
                f"[F BUY] {code} skip: buying_power={buying_power:.2f} "
                f"< required_bp={required_bp:.2f}",
                flush=True,
            )
            return False

        target = float(margin_plan["target"])
        qty = int(math.floor(target / price)) if price > 0 else 0

        if qty <= 0:
            print(f"[F BUY] {code} skip: qty={qty} target={target:.2f} price={price:.2f}", flush=True)
            return False

        raw_limit = (ask * 1.003) if ask > 0 else (price * 1.005)
        raw_limit = max(raw_limit, price * 1.002)

        if max_reclaim_price > 0:
            raw_limit = min(raw_limit, max_reclaim_price)

        limit_price = round(float(raw_limit), 2)

        if limit_price < price:
            print(f"[F BUY] {code} skip: limit={limit_price:.2f} < price={price:.2f}", flush=True)
            return False

        intent = (
            f"F:BUY qty={qty} rt={price:.2f} bid={bid:.2f} ask={ask:.2f} "
            f"limit={limit_price:.2f} last_sell={last_sell:.2f} "
            f"max={max_reclaim_price:.2f} "
            f"reclaim={item['reclaim_pct']:.2%} "
            f"intraday_pos={m['intraday_pos']:.2f}"
        )

        _cancel_open_buy_orders(tc, code)

        order = _submit_limit_buy_qty(tc, code, qty, limit_price=limit_price)
        order_id = getattr(order, "id", None) or getattr(order, "order_id", None)
        order_status = str(getattr(order, "status", "") or "")

        print(f"[F BUY] {code} order submitted: id={order_id} status={order_status}", flush=True)

        if order_status.lower() in ("rejected", "expired"):
            _update_ops_f_fields(
                conn,
                code,
                can_buy=0,
                last_order_side="buy",
                last_order_intent=_intent_short(f"F:BUY_REJECT status={order_status}"),
                last_order_id=str(order_id or ""),
                last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )

            with conn.cursor() as cur:
                _mark_watch_status(cur, int(watch["id"]), "WATCHING", f"BUY_REJECT: status={order_status}")

            return False

        filled_qty, filled_avg = _reconcile_fill(tc, code, str(order_id), wait_sec=4.0)

        if filled_qty <= 0 or filled_avg <= 0:
            _update_ops_f_fields(
                conn,
                code,
                can_buy=0,
                last_order_side="buy",
                last_order_intent=_intent_short("F:BUY_NO_FILL"),
                last_order_id=str(order_id or ""),
                last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )

            with conn.cursor() as cur:
                _mark_watch_status(cur, int(watch["id"]), "WATCHING", "BUY_NO_FILL")

            return False

        cost_price = float(filled_avg)
        qty_to_write = int(filled_qty)
        init_sl = round(cost_price * (1.0 - float(F_INIT_STOP_PCT)), 2)
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        _update_ops_f_fields(
            conn,
            code,
            is_bought=1,
            qty=qty_to_write,
            base_qty=qty_to_write,
            cost_price=round(cost_price, 2),
            close_price=round(cost_price, 2),
            stop_loss_price=init_sl,
            take_profit_price=0,
            b_stage=0,
            b_peak_price=round(cost_price, 4),
            b_peak_profit=0,
            b_last_profit=0,
            can_sell=1,
            can_buy=0,
            strategy_group="F",
            capital_pool="F",
            margin_used=1,
            last_order_side="buy",
            last_order_intent=_intent_short(intent),
            last_order_id=str(order_id or ""),
            last_order_time=now_str,
            updated_at=now_str,
        )

        with conn.cursor() as cur:
            _mark_watch_status(
                cur,
                int(watch["id"]),
                "BOUGHT",
                f"BOUGHT: F qty={qty_to_write} cost={cost_price:.2f} sl={init_sl:.2f}",
            )

        print(f"[F BUY] {code} ✅ bought qty={qty_to_write} cost={cost_price:.2f} sl={init_sl:.2f}", flush=True)

        return True

    except Exception as e:
        print(f"[F BUY] {code} ❌ error: {e}", flush=True)
        traceback.print_exc()
        return False

    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def _f_giveback_pct_for_peak(peak_gain_pct: float):
    for min_gain, giveback_pct in F_PEAK_GIVEBACK_RULES:
        if peak_gain_pct >= min_gain:
            return giveback_pct
    return None


def _f_next_stage_rule(last_stage: int, up_pct: float):
    next_stage = int(last_stage or 0) + 1

    for stage, threshold, sell_ratio in F_STAGE_RULES:
        if stage == next_stage and up_pct >= threshold:
            return stage, threshold, sell_ratio

    return None


def strategy_F_extended_record(code: str, phase: str = "") -> bool:
    code = (code or "").strip().upper()
    conn = None

    try:
        conn = _connect()
        row = _load_one_f_row(conn, code)

        if not row or int(row.get("is_bought") or 0) != 1:
            return False

        q = _get_extended_quote_realtime(code)

        price = float(q["last"])
        regular_close = float(q["prev_close"] or q["regular_close"])
        qty = int(float(row.get("qty") or 0))
        cost = float(row.get("cost_price") or 0.0)
        old_peak = float(row.get("b_peak_price") or cost or regular_close)
        peak_price = max(old_peak, price)

        gain = (price - regular_close) / regular_close if regular_close > 0 else 0.0

        updates = {
            "b_last_profit": round((price - cost) * qty, 4) if cost > 0 and qty > 0 else 0,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        if peak_price > old_peak + 0.005:
            updates["b_peak_price"] = round(peak_price, 4)
            updates["b_peak_profit"] = round(max((peak_price - cost) * qty, 0.0), 4) if cost > 0 and qty > 0 else 0

        _update_ops_f_fields(conn, code, **updates)

        print(
            f"[F EXT RECORD] {code} phase={phase} price={price:.2f} "
            f"regular_close={regular_close:.2f} ext_gain={gain:.2%} peak={peak_price:.2f}",
            flush=True,
        )

        return False

    except Exception as e:
        print(f"[F EXT RECORD] {code} error: {e}", flush=True)
        return False

    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def _f_sell_qty_limit_ext(conn, code: str, qty: int, limit_price: float, reason: str) -> bool:
    qty = int(qty or 0)

    if qty <= 0 or float(limit_price or 0) <= 0:
        return False

    tc = _get_trading_client()
    real_qty = _get_real_position_qty(tc, code)

    if real_qty is None:
        return False

    if real_qty == 0:
        _update_ops_f_fields(
            conn,
            code,
            qty=0,
            is_bought=0,
            can_sell=0,
            can_buy=0,
            stop_loss_price=None,
            b_peak_price=None,
            b_peak_profit=0,
            b_last_profit=0,
            last_order_side="sell",
            last_order_intent="F:EXT_SELL_SKIP no_real_pos",
            last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        return False

    qty = min(qty, real_qty)

    row = _load_one_f_row(conn, code) or {}

    cost = float(row.get("cost_price") or 0.0)
    peak_price = float(row.get("b_peak_price") or cost)
    stage = int(float(row.get("b_stage") or 0))
    sl = row.get("stop_loss_price")

    order = _submit_limit_qty_ext(tc, code, qty, side="sell", limit_price=limit_price)
    order_id = getattr(order, "id", None) or getattr(order, "order_id", None)
    status = str(getattr(order, "status", "") or "")

    if status.lower() in ("rejected", "expired"):
        _update_ops_f_fields(
            conn,
            code,
            last_order_side="sell",
            last_order_intent=_intent_short(f"F:EXT_SELL_REJECT {reason} status={status}"),
            last_order_id=str(order_id or ""),
            last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        return False

    sold_qty = _reconcile_sell_fill(tc, code, str(order_id), expected_real_qty=real_qty, wait_sec=12.0)

    if sold_qty <= 0:
        _update_ops_f_fields(
            conn,
            code,
            last_order_side="sell",
            last_order_intent=_intent_short(f"F:EXT_SELL_NO_FILL {reason}"),
            last_order_id=str(order_id or ""),
            last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        return False

    sold_qty = min(sold_qty, qty)
    remaining_qty = max(real_qty - sold_qty, 0)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    _update_ops_f_fields(
        conn,
        code,
        qty=int(remaining_qty),
        is_bought=1 if remaining_qty > 0 else 0,
        can_sell=1 if remaining_qty > 0 else 0,
        can_buy=0,
        stop_loss_price=sl if remaining_qty > 0 else None,
        b_peak_price=peak_price if remaining_qty > 0 else None,
        b_peak_profit=max((peak_price - cost) * remaining_qty, 0.0) if remaining_qty > 0 else 0,
        b_last_profit=0,
        b_stage=stage if remaining_qty > 0 else 0,
        last_order_side="sell",
        last_order_intent=_intent_short(f"{reason} limit={limit_price:.2f} sold={sold_qty}"),
        last_order_id=str(order_id or ""),
        last_order_time=now_str,
        updated_at=now_str,
    )

    print(f"[F EXT SELL] {code} ✅ sold={sold_qty} remain={remaining_qty} reason={reason}", flush=True)

    return True


def strategy_F_premarket_manage(code: str) -> bool:
    code = (code or "").strip().upper()
    conn = None

    try:
        conn = _connect()
        row = _load_one_f_row(conn, code)

        if not row or int(row.get("is_bought") or 0) != 1:
            return False

        qty = int(float(row.get("qty") or 0))
        cost = float(row.get("cost_price") or 0.0)
        stage = int(float(row.get("b_stage") or 0))

        if qty <= 0:
            return False

        q = _get_extended_quote_realtime(code)

        price = float(q["last"])
        regular_close = float(q["prev_close"] or q["regular_close"])
        old_peak = float(row.get("b_peak_price") or cost or regular_close)
        peak_price = max(old_peak, price)

        gain = (price - regular_close) / regular_close if regular_close > 0 else 0.0
        peak_gain = (peak_price - regular_close) / regular_close if regular_close > 0 else 0.0

        if peak_price > old_peak + 0.005:
            _update_ops_f_fields(
                conn,
                code,
                b_peak_price=round(peak_price, 4),
                b_peak_profit=round(max((peak_price - cost) * qty, 0.0), 4) if cost > 0 else 0,
                updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )

        print(
            f"[F PRE] {code} price={price:.2f} regular_close={regular_close:.2f} "
            f"gain={gain:.2%} peak_gain={peak_gain:.2%} stage={stage}",
            flush=True,
        )

        if peak_gain >= 0.10:
            trigger = round(peak_price * 0.97, 2)

            if price <= trigger:
                reason = f"F_PREMARKET_GIVEBACK price={price:.2f} <= trigger={trigger:.2f} peak={peak_price:.2f}"
                return _f_sell_qty_limit_ext(conn, code, qty, trigger, reason)

            if stage < 1 and gain >= 0.10:
                sell_qty = max(int(math.floor(qty * 0.20)), 1)
                reason = f"F_PREMARKET_STAGE1_SELL20 price={price:.2f} gain={gain:.2%}"
                ok = _f_sell_qty_limit_ext(conn, code, sell_qty, round(price * 0.997, 2), reason)

                if ok:
                    _update_ops_f_fields(conn, code, b_stage=1, take_profit_price=1)

                return ok

        return False

    except Exception as e:
        print(f"[F PRE] {code} error: {e}", flush=True)
        traceback.print_exc()
        return False

    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def strategy_F_sell(code: str) -> bool:
    code = (code or "").strip().upper()
    print(f"[F SELL] {code}", flush=True)

    conn = None

    try:
        conn = _connect()
        row = _load_one_f_row(conn, code)

        if not row:
            print(f"[F SELL] {code} skip: no F row", flush=True)
            return False

        is_bought = int(row.get("is_bought") or 0)
        can_sell = int(row.get("can_sell") or 0)
        qty = int(float(row.get("qty") or 0))
        cost = float(row.get("cost_price") or 0.0)
        sl = float(row.get("stop_loss_price") or 0.0)
        last_stage = int(float(row.get("b_stage") or 0))

        if is_bought != 1 or can_sell != 1:
            print(f"[F SELL] {code} skip: is_bought={is_bought} can_sell={can_sell}", flush=True)
            return False

        if qty <= 0 or cost <= 0:
            print(f"[F SELL] {code} skip: invalid qty/cost qty={qty} cost={cost:.2f}", flush=True)
            return False

        tc = _get_trading_client()
        real_qty = _get_real_position_qty(tc, code)

        if real_qty is None:
            print(f"[F SELL] {code} skip: failed to query real position", flush=True)
            return False

        if real_qty == 0:
            _update_ops_f_fields(
                conn,
                code,
                qty=0,
                is_bought=0,
                can_sell=0,
                can_buy=0,
                stop_loss_price=None,
                b_peak_price=None,
                b_peak_profit=0,
                b_last_profit=0,
                last_order_side="sell",
                last_order_intent="F:SELL_SKIP no_real_pos",
                last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            return False

        qty = min(qty, real_qty)

        price, prev_close, feed = get_snapshot_realtime(code)
        price = float(price or 0.0)

        if price <= 0:
            print(f"[F SELL] {code} skip: invalid price={price:.2f}", flush=True)
            return False

        up_pct = (price - cost) / cost if cost > 0 else 0.0
        old_peak = float(row.get("b_peak_price") or cost)
        peak_price = max(old_peak, price)

        profit_now = (price - cost) * qty
        peak_profit = max(float(row.get("b_peak_profit") or 0.0), (peak_price - cost) * qty)

        updates = {}

        if peak_price > old_peak + 0.005:
            updates["b_peak_price"] = round(peak_price, 4)
            updates["b_peak_profit"] = round(peak_profit, 4)

        if abs(profit_now - float(row.get("b_last_profit") or 0.0)) >= 20:
            updates["b_last_profit"] = round(profit_now, 4)

        if sl <= 0:
            sl = round(cost * (1.0 - float(F_INIT_STOP_PCT)), 2)
            updates["stop_loss_price"] = sl

        new_sl = sl

        if up_pct >= F_LOCK_BREAKEVEN_PCT:
            new_sl = max(new_sl, cost)

        if new_sl > sl + 0.01:
            sl = round(new_sl, 2)
            updates["stop_loss_price"] = sl

        if updates:
            _update_ops_f_fields(conn, code, **updates)

        print(
            f"[F SELL] {code} price={price:.2f} cost={cost:.2f} up={up_pct:.2%} "
            f"qty={qty} sl={sl:.2f} peak={peak_price:.2f} peak_profit={peak_profit:.2f} feed={feed}",
            flush=True,
        )

        reason = None
        sell_qty = qty
        new_stage = last_stage

        if sl > 0 and price <= sl:
            reason = f"F_STOP price={price:.2f} <= sl={sl:.2f}"

        else:
            stage_rule = _f_next_stage_rule(last_stage, up_pct)

            if stage_rule is not None:
                stage, threshold, sell_ratio = stage_rule
                raw_sell_qty = int(math.floor(qty * float(sell_ratio)))
                sell_qty = max(raw_sell_qty, 1)
                sell_qty = min(sell_qty, qty)
                new_stage = stage

                reason = (
                    f"F_STAGE{stage}_SELL{int(sell_ratio * 100)} "
                    f"price={price:.2f} up={up_pct:.2%} threshold={threshold:.2%} qty={sell_qty}"
                )

            else:
                peak_gain_pct = (peak_price - cost) / cost if cost > 0 else 0.0
                giveback_pct = _f_giveback_pct_for_peak(peak_gain_pct)

                if giveback_pct is not None:
                    trigger = round(peak_price * (1.0 - giveback_pct), 2)

                    print(
                        f"[F SELL] {code} giveback watch peak_gain={peak_gain_pct:.2%} "
                        f"pullback={giveback_pct:.2%} trigger={trigger:.2f}",
                        flush=True,
                    )

                    if price <= trigger:
                        sell_qty = qty
                        reason = (
                            f"F_PEAK_GIVEBACK price={price:.2f} <= trigger={trigger:.2f} "
                            f"peak={peak_price:.2f} pullback={giveback_pct:.2%}"
                        )

        if not reason:
            return False

        order = _submit_market_qty(tc, code, sell_qty, side="sell")
        order_id = getattr(order, "id", None) or getattr(order, "order_id", None)
        status = str(getattr(order, "status", "") or "")

        if status.lower() in ("rejected", "expired"):
            _update_ops_f_fields(
                conn,
                code,
                last_order_side="sell",
                last_order_intent=_intent_short(f"F:SELL_REJECT {reason} status={status}"),
                last_order_id=str(order_id or ""),
                last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            return False

        sold_qty = _reconcile_sell_fill(tc, code, str(order_id), expected_real_qty=real_qty, wait_sec=4.0)

        if sold_qty <= 0:
            _update_ops_f_fields(
                conn,
                code,
                last_order_side="sell",
                last_order_intent=_intent_short(f"F:SELL_NO_FILL {reason}"),
                last_order_id=str(order_id or ""),
                last_order_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            return False

        sold_qty = min(sold_qty, sell_qty)
        remaining_qty = max(real_qty - sold_qty, 0)
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        _update_ops_f_fields(
            conn,
            code,
            qty=int(remaining_qty),
            is_bought=1 if remaining_qty > 0 else 0,
            can_sell=1 if remaining_qty > 0 else 0,
            can_buy=0,
            stop_loss_price=sl if remaining_qty > 0 else None,
            b_peak_price=peak_price if remaining_qty > 0 else None,
            b_peak_profit=max((peak_price - cost) * remaining_qty, 0.0) if remaining_qty > 0 else 0,
            b_last_profit=0,
            b_stage=new_stage if remaining_qty > 0 else 0,
            last_order_side="sell",
            last_order_intent=_intent_short(f"{reason} sold={sold_qty}"),
            last_order_id=str(order_id or ""),
            last_order_time=now_str,
            updated_at=now_str,
        )

        with conn.cursor() as cur:
            watch = _load_watch_row_by_code(cur, code)

            if watch and remaining_qty == 0:
                _mark_watch_status(cur, int(watch["id"]), "SOLD", f"SOLD: {reason}")

        print(f"[F SELL] {code} ✅ sold={sold_qty} remain={remaining_qty} reason={reason}", flush=True)

        return True

    except Exception as e:
        print(f"[F SELL] {code} ❌ error: {e}", flush=True)
        traceback.print_exc()
        return False

    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def main():
    strategy_F_scan(prepare_buy=False)


if __name__ == "__main__":
    main()