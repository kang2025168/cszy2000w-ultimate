# -*- coding: utf-8 -*-
"""
app/strategy_f.py

策略F：妖股二次启动观察池扫描器。

用途：
1) 策略B如果因为 PEAK_GIVEBACK 离场，股票会被写入 monster_watchlist。
2) 本文件只负责扫描这些 WATCHING 股票，判断是否出现“洗盘后重新启动”的迹象。
3) 当前版本只打印候选、写 last_checked_at/notes，不自动下单，也不接入主循环。

设计重点：
- 策略B止损跟得紧，用来保护利润。
- 策略F负责把“被洗出去但可能继续起飞”的股票捞回来观察。
- 先观察信号质量，稳定后再决定是否接入真实买入。
"""

import os
import math
import time
import traceback
from datetime import datetime

import pymysql

from app.strategy_b import (
    B_DATA_FEED,
    _cancel_open_buy_orders,
    _get_buying_power,
    _get_real_position_qty,
    _get_trading_client,
    _intent_short,
    _reconcile_fill,
    _reconcile_sell_fill,
    _sleep_for_rate_limit,
    _snapshot_http,
    _submit_limit_buy_qty,
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

PRICES_TABLE = os.getenv("F_PRICES_TABLE", os.getenv("B_PRICES_TABLE", "stock_prices_pool"))
MONSTER_TABLE = os.getenv("MONSTER_TABLE", "monster_watchlist")
OPS_TABLE = os.getenv("OPS_TABLE", "stock_operations")


# =========================
# 扫描参数
# =========================
F_SCAN_LIMIT = int(os.getenv("F_SCAN_LIMIT", "200"))
F_BAR_LIMIT = int(os.getenv("F_BAR_LIMIT", "30"))

# 二次启动当天至少上涨 5%，避免横盘小波动误判成重新启动
F_DAY_UP_MIN = float(os.getenv("F_DAY_UP_MIN", "0.05"))

# 最近几日内寻找“洗盘阴线前的最近一根阳线”
F_BULL_LOOKBACK_DAYS = int(os.getenv("F_BULL_LOOKBACK_DAYS", "5"))

# 当前价必须在日内振幅的上方区域，避免买到冲高回落
F_INTRADAY_POS_MIN = float(os.getenv("F_INTRADAY_POS_MIN", "0.65"))

# 动态追高上限：
# 普通二次启动最多离突破阳线高点 12%；强势放量允许 18%；爆量大涨允许 25%。
F_CHASE_BASE_MAX = float(os.getenv("F_CHASE_BASE_MAX", "0.12"))
F_CHASE_STRONG_MAX = float(os.getenv("F_CHASE_STRONG_MAX", "0.18"))
F_CHASE_EXPLOSIVE_MAX = float(os.getenv("F_CHASE_EXPLOSIVE_MAX", "0.25"))
F_STRONG_DAY_UP = float(os.getenv("F_STRONG_DAY_UP", "0.10"))
F_EXPLOSIVE_DAY_UP = float(os.getenv("F_EXPLOSIVE_DAY_UP", "0.15"))

# F 是 B 的二次启动版本，默认仓位先小一点，避免妖股波动把账户打疼。
F_TARGET_NOTIONAL_USD = float(os.getenv("F_TARGET_NOTIONAL_USD", "1050"))
F_MAX_NOTIONAL_USD = float(os.getenv("F_MAX_NOTIONAL_USD", "1050"))
F_MIN_BUYING_POWER = float(os.getenv("F_MIN_BUYING_POWER", "1050"))
F_BP_USE_RATIO = float(os.getenv("F_BP_USE_RATIO", "0.98"))

# F 的止损比 B 略宽：B 是 -2%，F 默认 -3%，给二次启动一点洗盘空间。
F_INIT_STOP_PCT = float(os.getenv("F_INIT_STOP_PCT", "0.03"))
F_LOCK_BREAKEVEN_PCT = float(os.getenv("F_LOCK_BREAKEVEN_PCT", "0.05"))

# F 的最高价回撤保护比 B 宽，目标是抓第二波，不是赚一点就跑。
F_PEAK_GIVEBACK_RULES = [
    (0.60, 0.10),  # 最高涨 >=60%，允许从最高价回撤 10%
    (0.30, 0.08),  # 最高涨 >=30%，允许回撤 8%
    (0.15, 0.06),  # 最高涨 >=15%，允许回撤 6%
    (0.05, 0.04),  # 最高涨 >=5%，允许回撤 4%
]

# F 分批止盈：先落袋一部分，剩余仓位继续跟妖股趋势。
F_STAGE_RULES = [
    (1, 0.15, 0.25),  # +15% 卖 25%
    (2, 0.30, 0.25),  # +30% 再卖 25%
    (3, 0.50, 0.20),  # +50% 再卖 20%，剩余 30% 交给回撤保护
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


def _load_watch_rows(cur):
    """
    只扫描 WATCHING 状态。
    READY/ARCHIVED 以后可以作为人工确认或停用状态，当前版本不主动处理。
    """
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
    ORDER BY watch_since ASC, id ASC
    LIMIT %s;
    """
    cur.execute(sql, (int(F_SCAN_LIMIT),))
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
    WHERE stock_code=%s AND stock_type='F'
    LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (code,))
        return cur.fetchone()


def _update_ops_f_fields(conn, code: str, **kwargs):
    if not kwargs:
        return
    cols = []
    vals = []
    for k, v in kwargs.items():
        cols.append(f"`{k}`=%s")
        vals.append(v)
    sql = f"UPDATE `{OPS_TABLE}` SET {', '.join(cols)} WHERE stock_code=%s AND stock_type='F';"
    vals.append(code)
    with conn.cursor() as cur:
        cur.execute(sql, tuple(vals))


def _load_recent_bars(cur, code: str, limit: int = F_BAR_LIMIT):
    """
    最近K线，按日期倒序返回。
    需要 high/low 是为了后面扩展“洗盘深度、重新突破前高”等判断。
    """
    sql = f"""
    SELECT DATE(`date`) AS d, `open`, `high`, `low`, `close`
    FROM `{PRICES_TABLE}`
    WHERE symbol=%s
    ORDER BY `date` DESC
    LIMIT %s;
    """
    cur.execute(sql, (code, int(limit)))
    return cur.fetchall() or []


def _get_realtime_daily_bar(code: str):
    """
    从 Alpaca snapshot 拉取今天的实时日内数据。

    stock_prices_pool 只当历史库用；F 的“今天涨幅、今天高低点、当前价”
    全部以这里的实时数据为准。
    """
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
    close = last
    prev_close = _safe_float(pb.get("c"), 0.0)

    if close <= 0 or prev_close <= 0:
        raise RuntimeError(f"snapshot missing realtime fields: close={close} prev_close={prev_close}")

    # dailyBar 可能轻微滞后，用 latestTrade 防御性修正高低点。
    high = max(high, close)
    low = min(low if low > 0 else close, close)

    return {
        "date": db.get("t") or "realtime",
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "prev_close": prev_close,
        "bid": bid,
        "ask": ask,
        "feed": B_DATA_FEED,
    }


def _calc_metrics(code: str, bars_desc):
    """
    根据历史K线 + Alpaca 实时日内数据计算二次启动信号。

    bars_desc: 历史K线，最新的已完成交易日在前，不依赖今天收盘后才入库的数据。
    最少需要20根历史K线，因为要计算过去20日均量。
    """
    if len(bars_desc) < 20:
        return None

    closes = [_safe_float(r.get("close")) for r in bars_desc]
    opens = [_safe_float(r.get("open")) for r in bars_desc]
    highs = [_safe_float(r.get("high")) for r in bars_desc]
    lows = [_safe_float(r.get("low")) for r in bars_desc]

    code = (code or "").strip().upper()
    rt = _get_realtime_daily_bar(code)

    open_today = _safe_float(rt.get("open"))
    close_today = _safe_float(rt.get("close"))
    high_today = _safe_float(rt.get("high"))
    low_today = _safe_float(rt.get("low"))
    close_prev = _safe_float(rt.get("prev_close"))

    closes_with_today = [close_today] + closes

    ma3 = sum(closes_with_today[0:3]) / 3.0
    ma8 = sum(closes_with_today[0:8]) / 8.0
    ma20 = sum(closes_with_today[0:20]) / 20.0

    day_up = (close_today - close_prev) / close_prev if close_prev > 0 else 0.0
    day_range = high_today - low_today
    intraday_pos = (close_today - low_today) / day_range if day_range > 0 else 0.0
    high20_prev = max(highs[0:20])
    low10_prev = min(lows[0:10])

    return {
        "date": rt.get("date"),
        "open": open_today,
        "close": close_today,
        "high": high_today,
        "low": low_today,
        "ma3": ma3,
        "ma8": ma8,
        "ma20": ma20,
        "day_up": day_up,
        "intraday_pos": intraday_pos,
        "high20_prev": high20_prev,
        "low10_prev": low10_prev,
        "bid": _safe_float(rt.get("bid")),
        "ask": _safe_float(rt.get("ask")),
        "feed": rt.get("feed"),
    }


def _find_recent_bull_reference(bars_desc):
    """
    寻找“洗盘阴线前的最近一根阳线”。

    逻辑：
    - bars_desc 是历史K线，最新一根就是昨天/最近完成交易日。
    - 最近如果连续阴线，全部跳过，视为洗盘。
    - 在最近 N 天内找到第一根阳线，取它的 high 作为突破位。

    返回：
      (阳线K线, 阳线high, 跳过的阴线数量)
    """
    lookback = max(int(F_BULL_LOOKBACK_DAYS), 1)
    skipped_bear = 0

    for r in bars_desc[0:lookback]:
        o = _safe_float(r.get("open"))
        c = _safe_float(r.get("close"))
        h = _safe_float(r.get("high"))

        if c > o and h > 0:
            return r, h, skipped_bear

        if c < o:
            skipped_bear += 1

    return None, 0.0, skipped_bear


def _max_chase_pct(day_up: float) -> float:
    """
    动态追高上限。

    妖股允许涨得多，但不能无脑追：
    - 普通二次启动：最多高出参考阳线 high 12%
    - 当天涨幅 >=10%：最多高出 18%
    - 当天涨幅 >=15%：最多高出 25%
    """
    if day_up >= F_EXPLOSIVE_DAY_UP:
        return F_CHASE_EXPLOSIVE_MAX
    if day_up >= F_STRONG_DAY_UP:
        return F_CHASE_STRONG_MAX
    return F_CHASE_BASE_MAX


def _is_restart_candidate(row, m, bars_desc):
    """
    二次启动候选规则。

    当天买入版核心：
    1) 最近5日内，跳过最近连续阴线，找到最近一根阳线。
    2) 当前价突破这根阳线 high，说明洗盘后重新启动。
    3) 当天涨幅 >= 5%。
    4) close > MA3 且 MA3 > MA8，短线趋势不能坏。
    5) 当前价处在日内高位，过滤冲高回落。
    6) 追高上限动态放宽，妖股强势爆量时允许追得更远。
    """
    close = _safe_float(m.get("close"))
    ma3 = _safe_float(m.get("ma3"))
    ma8 = _safe_float(m.get("ma8"))
    day_up = _safe_float(m.get("day_up"))
    intraday_pos = _safe_float(m.get("intraday_pos"))
    bull_bar, bull_high, skipped_bear = _find_recent_bull_reference(bars_desc)
    max_chase_pct = _max_chase_pct(day_up)
    max_chase_price = bull_high * (1.0 + max_chase_pct) if bull_high > 0 else 0.0

    checks = []
    checks.append(("bull_ref", bull_high > 0))
    checks.append(("break_bull_high", bull_high > 0 and close > bull_high))
    checks.append(("not_over_chase", bull_high > 0 and close <= max_chase_price))
    checks.append(("close>ma3", close > ma3 > 0))
    checks.append(("ma3>ma8", ma3 > ma8 > 0))
    checks.append(("day_up", day_up > F_DAY_UP_MIN))
    checks.append(("intraday_pos", intraday_pos >= F_INTRADAY_POS_MIN))

    passed = [name for name, ok in checks if ok]
    failed = [name for name, ok in checks if not ok]
    detail = {
        "bull_date": bull_bar.get("d") if bull_bar else None,
        "bull_high": bull_high,
        "skipped_bear": skipped_bear,
        "max_chase_pct": max_chase_pct,
        "max_chase_price": max_chase_price,
    }
    return len(failed) == 0, passed, failed, detail


def _update_watch_note(cur, row_id: int, note: str):
    sql = f"""
    UPDATE `{MONSTER_TABLE}`
    SET last_checked_at=NOW(), notes=%s
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


def _upsert_ready_f_ops(cur, code: str, row: dict, m: dict, detail: dict, note: str) -> bool:
    """
    把二次启动候选写入 stock_operations，作为 stock_type='F' 等待主程序买入。

    保护规则：
    - 如果这只股票已经有持仓，不抢、不改。
    - 如果它属于 A/C/D/E 等其它策略，不抢。
    - 只有不存在、B 未持仓、F 未持仓时，才改成 F 候选。
    """
    existing = _load_ops_row(cur, code)
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
                round(float(detail.get("bull_high") or 0), 2),
                round(float(m.get("close") or 0), 2),
                round(float(m.get("open") or 0), 2),
                round(float(m.get("close") or 0), 2),
                str(m.get("date")),
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
            round(float(detail.get("bull_high") or 0), 2),
            round(float(m.get("close") or 0), 2),
            round(float(m.get("open") or 0), 2),
            round(float(m.get("close") or 0), 2),
            str(m.get("date")),
            _intent_short(note),
        ),
    )
    return True


def strategy_F_scan(prepare_buy: bool = False):
    """
    扫描妖股观察池。

    prepare_buy=False：只扫描打印。
    prepare_buy=True ：满足条件后写入 stock_operations(stock_type='F', can_buy=1)，等待主程序买入。
    """
    conn = _connect()
    passed_rows = []

    try:
        with conn.cursor() as cur:
            rows = _load_watch_rows(cur)
            print(f"[F INFO] watching={len(rows)}", flush=True)

            for row in rows:
                code = (row.get("stock_code") or "").strip().upper()
                if not code:
                    continue

                bars = _load_recent_bars(cur, code)
                try:
                    m = _calc_metrics(code, bars)
                except Exception as e:
                    _update_watch_note(cur, int(row["id"]), f"HOLD: 实时行情获取失败 {e}")
                    print(f"[F HOLD] {code} realtime snapshot failed: {e}", flush=True)
                    continue
                if not m:
                    _update_watch_note(cur, int(row["id"]), "HOLD: K线不足，无法判断二次启动")
                    print(f"[F HOLD] {code} bars_not_enough", flush=True)
                    continue

                ok, passed, failed, detail = _is_restart_candidate(row, m, bars)
                last_sell = _safe_float(row.get("last_sell_price"))
                peak = _safe_float(row.get("b_peak_price"))
                bull_high = _safe_float(detail.get("bull_high"))
                max_chase_pct = _safe_float(detail.get("max_chase_pct"))
                max_chase_price = _safe_float(detail.get("max_chase_price"))

                if ok:
                    note = (
                        f"PASS: 二次启动候选 close={m['close']:.2f} "
                        f"bull_high={bull_high:.2f} max_chase={max_chase_price:.2f} "
                        f"day_up={m['day_up']:.2%} intraday_pos={m['intraday_pos']:.2f}"
                    )
                    if prepare_buy:
                        ready_ok = _upsert_ready_f_ops(cur, code, row, m, detail, note)
                        if ready_ok:
                            _mark_watch_status(cur, int(row["id"]), "READY", note)
                        else:
                            _update_watch_note(cur, int(row["id"]), note)
                    else:
                        _update_watch_note(cur, int(row["id"]), note)
                    passed_rows.append({"code": code, "metrics": m, "watch": row, "detail": detail})
                    print(
                        f"[F PASS] {code} close={m['close']:.2f} bull_high={bull_high:.2f} "
                        f"max_chase={max_chase_price:.2f} chase_pct={max_chase_pct:.1%} "
                        f"sell={last_sell:.2f} peak={peak:.2f} "
                        f"MA3={m['ma3']:.2f} MA8={m['ma8']:.2f} "
                        f"day_up={m['day_up']*100:.2f}% "
                        f"intraday_pos={m['intraday_pos']:.2f} "
                        f"bull_date={detail.get('bull_date')} skipped_bear={detail.get('skipped_bear')} "
                        f"passed={','.join(passed)}",
                        flush=True,
                    )
                else:
                    note = (
                        f"HOLD: failed={','.join(failed)} close={m['close']:.2f} "
                        f"bull_high={bull_high:.2f} max_chase={max_chase_price:.2f} "
                        f"day_up={m['day_up']:.2%} intraday_pos={m['intraday_pos']:.2f}"
                    )
                    _update_watch_note(cur, int(row["id"]), note)
                    print(
                        f"[F HOLD] {code} close={m['close']:.2f} bull_high={bull_high:.2f} "
                        f"max_chase={max_chase_price:.2f} chase_pct={max_chase_pct:.1%} "
                        f"sell={last_sell:.2f} peak={peak:.2f} failed={','.join(failed)}",
                        flush=True,
                    )

        print(f"[F OK] restart_candidates={len(passed_rows)}", flush=True)
        return passed_rows

    finally:
        conn.close()


def strategy_F_refresh_candidates():
    """
    给主程序调用：扫描观察池，把满足二次启动的股票写成 F 候选。

    注意：这里仍然不下单，只是让 stock_operations 出现 can_buy=1 的 F 记录。
    真正下单由 strategy_F_buy 执行，并继续受主程序资金开关/大盘开关控制。
    """
    rows = strategy_F_scan(prepare_buy=True)
    return len(rows)


def strategy_F_buy(code: str) -> bool:
    """
    策略F买入：只买 stock_operations 里已经被标记为 F/can_buy=1 的二次启动候选。

    和 B 的区别：
    - F 来自 monster_watchlist，是 B 被洗出去后的二次启动。
    - F 默认仓位更小。
    - F 初始止损更宽，默认 6%。
    """
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

        # 买前再确认一次 F 信号，避免 READY 后行情已经回落还继续买。
        with conn.cursor() as cur:
            bars = _load_recent_bars(cur, code)
        try:
            m = _calc_metrics(code, bars)
        except Exception as e:
            print(f"[F BUY] {code} skip: realtime snapshot failed {e}", flush=True)
            return False
        if not m:
            print(f"[F BUY] {code} skip: bars not enough", flush=True)
            return False
        ok, passed, failed, detail = _is_restart_candidate(watch or {}, m, bars)
        if not ok:
            print(f"[F BUY] {code} skip: signal faded failed={','.join(failed)}", flush=True)
            _update_ops_f_fields(
                conn,
                code,
                can_buy=0,
                last_order_intent=_intent_short(f"F:BUY_SIGNAL_FADED failed={','.join(failed)}"),
                updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            if watch:
                with conn.cursor() as cur:
                    _mark_watch_status(cur, int(watch["id"]), "WATCHING", f"BUY_SIGNAL_FADED: failed={','.join(failed)}")
            return False

        price = float(m.get("close") or 0.0)
        bid = float(m.get("bid") or 0.0)
        ask = float(m.get("ask") or 0.0)
        if price <= 0:
            print(f"[F BUY] {code} skip: invalid realtime price={price:.2f}", flush=True)
            return False

        # 盘中实时价也要继续站上突破阳线 high，避免数据库K线滞后。
        bull_high = float(detail.get("bull_high") or 0.0)
        max_chase_price = float(detail.get("max_chase_price") or 0.0)
        if bull_high > 0 and price <= bull_high:
            print(f"[F BUY] {code} skip: realtime price={price:.2f} <= bull_high={bull_high:.2f}", flush=True)
            return False
        if max_chase_price > 0 and price > max_chase_price:
            print(f"[F BUY] {code} skip: realtime price={price:.2f} > max_chase={max_chase_price:.2f}", flush=True)
            return False

        tc = _get_trading_client()
        buying_power = _get_buying_power(tc)
        required_bp = max(float(F_MIN_BUYING_POWER), float(F_TARGET_NOTIONAL_USD) / max(float(F_BP_USE_RATIO), 0.01))
        if buying_power < required_bp:
            print(f"[F BUY] {code} skip: buying_power={buying_power:.2f} < required_bp={required_bp:.2f}", flush=True)
            return False

        target = min(float(F_TARGET_NOTIONAL_USD), float(F_MAX_NOTIONAL_USD), buying_power * float(F_BP_USE_RATIO))
        qty = int(math.floor(target / price)) if price > 0 else 0
        if qty <= 0:
            print(f"[F BUY] {code} skip: qty={qty} target={target:.2f} price={price:.2f}", flush=True)
            return False

        # F 是突破追击，限价给一点成交空间，但仍然不超过动态追高上限。
        raw_limit = (ask * 1.003) if ask > 0 else (price * 1.005)
        raw_limit = max(raw_limit, price * 1.002)
        if max_chase_price > 0:
            raw_limit = min(raw_limit, max_chase_price)
        limit_price = round(float(raw_limit), 2)
        if limit_price < price:
            print(f"[F BUY] {code} skip: limit={limit_price:.2f} < price={price:.2f}", flush=True)
            return False

        intent = (
            f"F:BUY qty={qty} rt={price:.2f} bid={bid:.2f} ask={ask:.2f} "
            f"limit={limit_price:.2f} bull_high={bull_high:.2f} "
            f"day_up={m['day_up']:.2%} intraday_pos={m['intraday_pos']:.2f}"
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
            if watch:
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
            if watch:
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
            last_order_side="buy",
            last_order_intent=_intent_short(intent),
            last_order_id=str(order_id or ""),
            last_order_time=now_str,
            updated_at=now_str,
        )

        if watch:
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
    """
    F 分批止盈规则。

    只推进下一档，不跳级连续卖，避免一天暴涨时一次性卖太多。
    """
    next_stage = int(last_stage or 0) + 1
    for stage, threshold, sell_ratio in F_STAGE_RULES:
        if stage == next_stage and up_pct >= threshold:
            return stage, threshold, sell_ratio
    return None


def strategy_F_sell(code: str) -> bool:
    """
    策略F卖出：比 B 更宽的止损/回撤保护。

    规则：
    - 初始止损默认 -3%。
    - 盈利 >=5% 后抬到保本。
    - 最高涨幅 >=5% 且回撤 4% 时保护利润。
    - +15%/+30%/+50% 分批止盈，剩余仓位继续跟趋势。
    """
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
    strategy_F_scan()


if __name__ == "__main__":
    main()
