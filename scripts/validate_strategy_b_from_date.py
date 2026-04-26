# -*- coding: utf-8 -*-
"""
validate_strategy_b_from_date.py

用途：
1) 输入股票代码和日期
2) 从 MySQL 的 stock_prices_pool 读取历史日线
3) 近似验证该日是否符合买入条件
4) 用 app/strategy_b.py 当前“无加仓清爽版”的卖出逻辑做日线级回放
5) 输出从指定日期到今天的：
   - 是否触发买入
   - 买入价 / 初始止损
   - 每日持有过程
   - 是否被洗掉
   - Stage 触发情况
   - 已实现收益 / 未实现收益 / 总收益

注意：
- 这是“日线近似回放”，不是分钟级真实回放
- 盘中先冲高还是先下杀无法从日线完全还原
- 默认采用保守路径：先检查当日 low 是否打到旧 SL，再根据 high 更新动态止损 / Stage
- same-day lock / pending stop 也做了近似模拟，不可能和实盘 100% 一样
"""

import os
import sys
import math
import argparse
from dataclasses import dataclass
from datetime import datetime, date
from typing import List, Optional, Tuple

import pymysql
import pandas as pd

# ============================================================
# 为了复用你当前 strategy_b.py 的参数，优先导入 app.strategy_b
# ============================================================
try:
    from app import strategy_b as sb
except Exception as e:
    raise RuntimeError(
        f"无法导入 app.strategy_b，请在项目根目录运行，或把本文件放到项目根目录。错误: {e}"
    )

# ============================================================
# DB
# ============================================================
DB = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASS", "mlp009988"),
    database=os.getenv("DB_NAME", "cszy2000"),
    charset="utf8mb4",
    autocommit=True,
    cursorclass=pymysql.cursors.DictCursor,
)

PRICES_TABLE = getattr(sb, "PRICES_TABLE", "stock_prices_pool")
OPS_TABLE = getattr(sb, "OPS_TABLE", "stock_operations")

# ============================================================
# 复用你当前策略参数（来自 app.strategy_b）
# ============================================================
B_MIN_UP_PCT = float(getattr(sb, "B_MIN_UP_PCT", 0.03))
B_MAX_BUY_UP_PCT = float(getattr(sb, "B_MAX_BUY_UP_PCT", 0.10))
B_TARGET_NOTIONAL_USD = float(getattr(sb, "B_TARGET_NOTIONAL_USD", 2100.0))
B_MAX_NOTIONAL_USD = float(getattr(sb, "B_MAX_NOTIONAL_USD", 2100.0))

# 当前 strategy_B_sell 的关键参数（按你贴出来的最新版）
ENABLE_STRUCTURE_EXIT_STAGE = 3
DYNAMIC_TRAIL_START_PCT = 0.08
TRAIL_BREAKEVEN_PCT = 0.08
TRAIL_LOCK_LIGHT_PCT = 0.15
TRAIL_PRICE_TRACK_PCT = 0.25
TRAIL_BACKOFF_PCT = 0.07

SAME_DAY_FORCE_SELL_LOSS_PCT = -0.05
SAME_DAY_FORCE_SELL_WIN_PCT = 0.05

# 注意：这里用你代码“实际数值”，不是注释文字
STAGE_RULES = [
    # stage, profit_pct, sell_ratio
    (1, 0.20, 0.20),
    (2, 0.35, 0.20),
    (3, 0.60, 0.15),
    (4, 0.85, 0.10),
    (5, 1.20, 0.10),
]

# 模拟模式：
# stop_first  = 保守：先检查 low 是否打旧 SL，再看 high 是否抬 SL / 触发 stage
# profit_first = 乐观：先按 high 抬 SL / 触发 stage，再检查 low 是否打新 SL
SIM_MODE = "stop_first"


# ============================================================
# 数据结构
# ============================================================
@dataclass
class EntryDecision:
    passed: bool
    reason: str
    signal_date: str
    prev_close: float
    open_price: float
    high: float
    low: float
    close: float
    trigger_price: float
    buy_price: Optional[float]
    initial_sl: Optional[float]
    day_up_pct: float


@dataclass
class DailyRecord:
    date: str
    open: float
    high: float
    low: float
    close: float
    qty_before: int
    qty_after: int
    sl_before: float
    sl_after: float
    up_pct_close: float
    up_pct_high: float
    action: str
    action_price: Optional[float]
    action_qty: int
    realized_pnl: float
    note: str


# ============================================================
# 工具函数
# ============================================================
def _safe_float(v, default=0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _safe_int(v, default=0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _connect():
    return pymysql.connect(**DB)


def load_price_data(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    sql = f"""
    SELECT `symbol`, `date`, `open`, `high`, `low`, `close`, `volume`
    FROM `{PRICES_TABLE}`
    WHERE `symbol`=%s
      AND `date` BETWEEN %s AND %s
    ORDER BY `date` ASC
    """
    conn = _connect()
    try:
        df = pd.read_sql(sql, conn, params=[symbol.upper(), start_date, end_date])
    finally:
        conn.close()

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"]).dt.date
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)
    return df


def load_trigger_from_ops(symbol: str) -> Optional[float]:
    """
    如果 stock_operations 里有这只 B 票，就优先拿 trigger_price。
    没有的话返回 None。
    """
    sql = f"""
    SELECT trigger_price
    FROM `{OPS_TABLE}`
    WHERE stock_code=%s AND stock_type='B'
    LIMIT 1
    """
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (symbol.upper(),))
            row = cur.fetchone()
            if not row:
                return None
            v = _safe_float(row.get("trigger_price"), 0.0)
            return v if v > 0 else None
    finally:
        conn.close()


def calc_dynamic_trail_sl(cost: float, price_ref: float, sl_old: float) -> float:
    """
    完全按你当前无加仓版 strategy_B_sell 的动态止损节奏：
      < +8%  : 不动
      >=+8% : SL = cost
      >=+15%: SL = cost*1.05
      >=+25%: SL = max(old, price_ref*0.93)
    """
    cost = _safe_float(cost, 0.0)
    price_ref = _safe_float(price_ref, 0.0)
    sl_old = _safe_float(sl_old, 0.0)

    if cost <= 0 or price_ref <= 0:
        return round(sl_old, 2)

    up_pct = (price_ref - cost) / cost
    new_sl = sl_old

    if up_pct < DYNAMIC_TRAIL_START_PCT:
        return round(new_sl, 2)

    if up_pct >= TRAIL_BREAKEVEN_PCT:
        new_sl = max(new_sl, cost * 1.00)

    if up_pct >= TRAIL_LOCK_LIGHT_PCT:
        new_sl = max(new_sl, cost * 1.05)

    if up_pct >= TRAIL_PRICE_TRACK_PCT:
        new_sl = max(new_sl, price_ref * (1.0 - TRAIL_BACKOFF_PCT))

    return round(new_sl, 2)


def allow_sell_same_day(is_same_day: bool, ref_up_pct: float) -> bool:
    if not is_same_day:
        return True
    if ref_up_pct <= SAME_DAY_FORCE_SELL_LOSS_PCT:
        return True
    if ref_up_pct >= SAME_DAY_FORCE_SELL_WIN_PCT:
        return True
    return False


def find_highest_hit_stage(up_pct: float, last_stage: int) -> Optional[int]:
    hit = None
    for stage, pct, _sell_ratio in STAGE_RULES:
        if up_pct >= pct and stage > last_stage:
            hit = stage
    return hit


def get_stage_sell_ratio(stage: int) -> Optional[float]:
    for s, _pct, sell_ratio in STAGE_RULES:
        if s == stage:
            return sell_ratio
    return None


def get_stage_pct(stage: int) -> Optional[float]:
    for s, pct, _sell_ratio in STAGE_RULES:
        if s == stage:
            return pct
    return None


def get_recent_closes_for_structure(df: pd.DataFrame, idx: int) -> Optional[Tuple[float, float, float, float]]:
    if idx < 3:
        return None
    c0 = _safe_float(df.iloc[idx]["close"])
    c1 = _safe_float(df.iloc[idx - 1]["close"])
    c2 = _safe_float(df.iloc[idx - 2]["close"])
    c3 = _safe_float(df.iloc[idx - 3]["close"])
    return c0, c1, c2, c3


# ============================================================
# 买入检查
# ============================================================
def check_entry(df: pd.DataFrame, signal_date: str, trigger_price: Optional[float]) -> EntryDecision:
    sig_date = pd.to_datetime(signal_date).date()

    idx_list = df.index[df["date"] == sig_date].tolist()
    if not idx_list:
        return EntryDecision(
            passed=False,
            reason=f"找不到交易日 {signal_date}",
            signal_date=signal_date,
            prev_close=0,
            open_price=0,
            high=0,
            low=0,
            close=0,
            trigger_price=0,
            buy_price=None,
            initial_sl=None,
            day_up_pct=0,
        )

    idx = idx_list[0]
    row = df.iloc[idx]

    if idx == 0:
        return EntryDecision(
            passed=False,
            reason="没有前一交易日，无法计算 prev_close",
            signal_date=signal_date,
            prev_close=0,
            open_price=_safe_float(row["open"]),
            high=_safe_float(row["high"]),
            low=_safe_float(row["low"]),
            close=_safe_float(row["close"]),
            trigger_price=0,
            buy_price=None,
            initial_sl=None,
            day_up_pct=0,
        )

    prev_close = _safe_float(df.iloc[idx - 1]["close"], 0.0)
    open_price = _safe_float(row["open"], 0.0)
    high = _safe_float(row["high"], 0.0)
    low = _safe_float(row["low"], 0.0)
    close = _safe_float(row["close"], 0.0)

    if prev_close <= 0:
        return EntryDecision(
            passed=False,
            reason="prev_close <= 0",
            signal_date=signal_date,
            prev_close=prev_close,
            open_price=open_price,
            high=high,
            low=low,
            close=close,
            trigger_price=0,
            buy_price=None,
            initial_sl=None,
            day_up_pct=0,
        )

    day_up_pct = (close - prev_close) / prev_close

    if trigger_price is None or trigger_price <= 0:
        # 如果没传 trigger，就做一个“近似默认 trigger”
        # 用前收 + 3% 作为触发价，便于快速测试
        trigger_price = round(prev_close * (1.0 + B_MIN_UP_PCT), 2)
    else:
        trigger_price = round(float(trigger_price), 2)

    if day_up_pct <= B_MIN_UP_PCT:
        return EntryDecision(
            passed=False,
            reason=f"day_up_pct={day_up_pct:.2%} <= min_up={B_MIN_UP_PCT:.2%}",
            signal_date=signal_date,
            prev_close=prev_close,
            open_price=open_price,
            high=high,
            low=low,
            close=close,
            trigger_price=trigger_price,
            buy_price=None,
            initial_sl=None,
            day_up_pct=day_up_pct,
        )

    if day_up_pct >= B_MAX_BUY_UP_PCT:
        return EntryDecision(
            passed=False,
            reason=f"day_up_pct={day_up_pct:.2%} >= max_buy_up={B_MAX_BUY_UP_PCT:.2%}",
            signal_date=signal_date,
            prev_close=prev_close,
            open_price=open_price,
            high=high,
            low=low,
            close=close,
            trigger_price=trigger_price,
            buy_price=None,
            initial_sl=None,
            day_up_pct=day_up_pct,
        )

    if high <= trigger_price:
        return EntryDecision(
            passed=False,
            reason=f"当天 high={high:.2f} 没有突破 trigger={trigger_price:.2f}",
            signal_date=signal_date,
            prev_close=prev_close,
            open_price=open_price,
            high=high,
            low=low,
            close=close,
            trigger_price=trigger_price,
            buy_price=None,
            initial_sl=None,
            day_up_pct=day_up_pct,
        )

    # 日线近似买入价：
    # - 如果开盘已高于 trigger，视为开盘直接突破，按 open 买
    # - 否则按 trigger 买
    buy_price = max(open_price, trigger_price)

    # 完全对齐你 strategy_B_buy 的 init_sl：
    # init_sl = max(entry_close, cost*0.97)
    # 日线工具这里没有 entry_close 字段，就近似用 prev_close
    initial_sl = max(prev_close, buy_price * 0.97)

    return EntryDecision(
        passed=True,
        reason="符合买入条件",
        signal_date=signal_date,
        prev_close=round(prev_close, 2),
        open_price=round(open_price, 2),
        high=round(high, 2),
        low=round(low, 2),
        close=round(close, 2),
        trigger_price=round(trigger_price, 2),
        buy_price=round(buy_price, 2),
        initial_sl=round(initial_sl, 2),
        day_up_pct=day_up_pct,
    )


# ============================================================
# 卖出模拟
# ============================================================
def simulate(df: pd.DataFrame, signal_date: str, entry: EntryDecision, initial_capital: float):
    sig_date = pd.to_datetime(signal_date).date()
    idx_list = df.index[df["date"] == sig_date].tolist()
    if not idx_list:
        raise RuntimeError("signal_date 不在数据里")

    start_idx = idx_list[0]

    buy_price = _safe_float(entry.buy_price, 0.0)
    sl = _safe_float(entry.initial_sl, 0.0)
    cost = buy_price

    qty = int(initial_capital // buy_price) if buy_price > 0 else 0
    if qty <= 0:
        raise RuntimeError(f"初始资金 {initial_capital:.2f} 不够买 1 股，buy_price={buy_price:.2f}")

    cash_spent = qty * buy_price
    realized_pnl = 0.0
    last_stage = 0
    final_exit_date = None
    final_exit_price = None
    was_stopped = False

    daily_records: List[DailyRecord] = []

    for i in range(start_idx, len(df)):
        row = df.iloc[i]
        dt = str(row["date"])
        o = _safe_float(row["open"])
        h = _safe_float(row["high"])
        l = _safe_float(row["low"])
        c = _safe_float(row["close"])

        is_same_day = (i == start_idx)

        qty_before = qty
        sl_before = sl
        up_pct_close = (c - cost) / cost if cost > 0 else 0.0
        up_pct_high = (h - cost) / cost if cost > 0 else 0.0

        action = "HOLD"
        action_price = None
        action_qty = 0
        pnl_today = 0.0
        note = ""

        if SIM_MODE == "stop_first":
            # ----------------------------------------------------
            # 先看当日 low 是否先打旧 SL（保守）
            # ----------------------------------------------------
            if qty > 0 and sl > 0 and l <= sl:
                ref_up_pct = (sl - cost) / cost if cost > 0 else 0.0
                if allow_sell_same_day(is_same_day, ref_up_pct):
                    pnl_today += qty * (sl - cost)
                    realized_pnl += pnl_today
                    action = "STOP_ALL"
                    action_price = round(sl, 2)
                    action_qty = qty
                    qty = 0
                    final_exit_date = dt
                    final_exit_price = round(sl, 2)
                    was_stopped = True

                    daily_records.append(DailyRecord(
                        date=dt,
                        open=round(o, 2),
                        high=round(h, 2),
                        low=round(l, 2),
                        close=round(c, 2),
                        qty_before=qty_before,
                        qty_after=qty,
                        sl_before=round(sl_before, 2),
                        sl_after=round(sl, 2),
                        up_pct_close=up_pct_close,
                        up_pct_high=up_pct_high,
                        action=action,
                        action_price=action_price,
                        action_qty=action_qty,
                        realized_pnl=round(pnl_today, 2),
                        note="当日 low 先打旧 SL（保守路径）",
                    ))
                    break
                else:
                    note += "same-day lock 阻止止损; "

            # ----------------------------------------------------
            # 再用 high 去更新 dynamic SL
            # ----------------------------------------------------
            if qty > 0:
                new_sl = calc_dynamic_trail_sl(cost, h, sl)
                if new_sl > sl:
                    sl = new_sl
                    note += f"dyn_sl->{sl:.2f}; "

            # ----------------------------------------------------
            # 再看 stage（按 high 判定）
            # 跳级时：对齐你当前实盘代码，只执行最高档一档卖出
            # ----------------------------------------------------
            if qty > 0:
                highest_stage = find_highest_hit_stage(up_pct_high, last_stage)
                if highest_stage is not None and highest_stage > last_stage:
                    sell_ratio = get_stage_sell_ratio(highest_stage)
                    if sell_ratio is not None and sell_ratio > 0:
                        if allow_sell_same_day(is_same_day, up_pct_high):
                            sell_qty = min(qty, max(1, int(math.floor(qty * sell_ratio))))
                            stage_pct = get_stage_pct(highest_stage)
                            stage_price = round(cost * (1.0 + stage_pct), 2)
                            pnl_stage = sell_qty * (stage_price - cost)
                            pnl_today += pnl_stage
                            realized_pnl += pnl_stage
                            qty -= sell_qty
                            action = f"STAGE{highest_stage}_SELL"
                            action_price = stage_price
                            action_qty = sell_qty
                            last_stage = highest_stage
                            note += f"stage={highest_stage}; "
                        else:
                            note += f"same-day lock 阻止 stage{highest_stage} 卖出; "

        else:
            # 乐观路径：先盈利再回撤
            if qty > 0:
                new_sl = calc_dynamic_trail_sl(cost, h, sl)
                if new_sl > sl:
                    sl = new_sl
                    note += f"dyn_sl->{sl:.2f}; "

            if qty > 0:
                highest_stage = find_highest_hit_stage(up_pct_high, last_stage)
                if highest_stage is not None and highest_stage > last_stage:
                    sell_ratio = get_stage_sell_ratio(highest_stage)
                    if sell_ratio is not None and sell_ratio > 0:
                        if allow_sell_same_day(is_same_day, up_pct_high):
                            sell_qty = min(qty, max(1, int(math.floor(qty * sell_ratio))))
                            stage_pct = get_stage_pct(highest_stage)
                            stage_price = round(cost * (1.0 + stage_pct), 2)
                            pnl_stage = sell_qty * (stage_price - cost)
                            pnl_today += pnl_stage
                            realized_pnl += pnl_stage
                            qty -= sell_qty
                            action = f"STAGE{highest_stage}_SELL"
                            action_price = stage_price
                            action_qty = sell_qty
                            last_stage = highest_stage
                            note += f"stage={highest_stage}; "

            if qty > 0 and sl > 0 and l <= sl:
                ref_up_pct = (sl - cost) / cost if cost > 0 else 0.0
                if allow_sell_same_day(is_same_day, ref_up_pct):
                    pnl_stop = qty * (sl - cost)
                    pnl_today += pnl_stop
                    realized_pnl += pnl_stop
                    action = "STOP_ALL" if action == "HOLD" else action + "+STOP_ALL"
                    action_price = round(sl, 2)
                    action_qty = qty if action_qty == 0 else action_qty
                    qty = 0
                    final_exit_date = dt
                    final_exit_price = round(sl, 2)
                    was_stopped = True
                    note += "profit_first 后 low 打到 SL; "

                    daily_records.append(DailyRecord(
                        date=dt,
                        open=round(o, 2),
                        high=round(h, 2),
                        low=round(l, 2),
                        close=round(c, 2),
                        qty_before=qty_before,
                        qty_after=qty,
                        sl_before=round(sl_before, 2),
                        sl_after=round(sl, 2),
                        up_pct_close=up_pct_close,
                        up_pct_high=up_pct_high,
                        action=action,
                        action_price=action_price,
                        action_qty=action_qty,
                        realized_pnl=round(pnl_today, 2),
                        note=note.strip(),
                    ))
                    break

        # --------------------------------------------------------
        # 结构退出（stage>=3 后）
        # --------------------------------------------------------
        if qty > 0 and last_stage >= ENABLE_STRUCTURE_EXIT_STAGE:
            closes4 = get_recent_closes_for_structure(df, i)
            if closes4 is not None:
                c0, c1, c2, c3 = closes4
                min3 = min(c1, c2, c3)
                if c0 > 0 and min3 > 0 and c0 < min3:
                    ref_up_pct = (c0 - cost) / cost if cost > 0 else 0.0
                    if allow_sell_same_day(is_same_day, ref_up_pct):
                        pnl_struct = qty * (c0 - cost)
                        pnl_today += pnl_struct
                        realized_pnl += pnl_struct
                        action = "STRUCT_EXIT" if action == "HOLD" else action + "+STRUCT_EXIT"
                        action_price = round(c0, 2)
                        action_qty = qty if action_qty == 0 else action_qty
                        qty = 0
                        final_exit_date = dt
                        final_exit_price = round(c0, 2)
                        note += f"struct_exit close={c0:.2f}<min3={min3:.2f}; "

        daily_records.append(DailyRecord(
            date=dt,
            open=round(o, 2),
            high=round(h, 2),
            low=round(l, 2),
            close=round(c, 2),
            qty_before=qty_before,
            qty_after=qty,
            sl_before=round(sl_before, 2),
            sl_after=round(sl, 2),
            up_pct_close=up_pct_close,
            up_pct_high=up_pct_high,
            action=action,
            action_price=action_price,
            action_qty=action_qty,
            realized_pnl=round(pnl_today, 2),
            note=note.strip(),
        ))

        if qty <= 0:
            break

    # 最终未平仓浮盈
    if qty > 0:
        last_close = _safe_float(df.iloc[-1]["close"], 0.0)
        unrealized_pnl = qty * (last_close - cost)
    else:
        unrealized_pnl = 0.0

    total_pnl = realized_pnl + unrealized_pnl
    total_return_pct = (total_pnl / cash_spent) if cash_spent > 0 else 0.0
    washed_out = was_stopped and total_return_pct <= 0.02

    return {
        "symbol": str(df.iloc[0]["symbol"]) if not df.empty else "",
        "signal_date": signal_date,
        "sim_mode": SIM_MODE,
        "passed_entry": entry.passed,
        "entry_reason": entry.reason,
        "trigger_price": entry.trigger_price,
        "buy_price": entry.buy_price,
        "initial_sl": entry.initial_sl,
        "initial_qty": int(cash_spent // buy_price) if buy_price > 0 else 0,
        "cash_spent": round(cash_spent, 2),
        "final_qty": qty,
        "final_stage": last_stage,
        "final_sl": round(sl, 2) if sl else None,
        "exit_date": final_exit_date,
        "exit_price": final_exit_price,
        "realized_pnl": round(realized_pnl, 2),
        "unrealized_pnl": round(unrealized_pnl, 2),
        "total_pnl": round(total_pnl, 2),
        "total_return_pct": round(total_return_pct * 100, 2),
        "was_stopped": was_stopped,
        "washed_out": washed_out,
        "records": daily_records,
    }


# ============================================================
# 打印输出
# ============================================================
def print_summary(entry: EntryDecision, result: dict, max_rows: int = 200):
    print("\n" + "=" * 100)
    print("买入检查")
    print("=" * 100)
    print(f"日期           : {entry.signal_date}")
    print(f"是否通过       : {entry.passed}")
    print(f"原因           : {entry.reason}")
    print(f"前收           : {entry.prev_close:.2f}")
    print(f"当日 O/H/L/C   : {entry.open_price:.2f} / {entry.high:.2f} / {entry.low:.2f} / {entry.close:.2f}")
    print(f"day_up_pct     : {entry.day_up_pct:.2%}")
    print(f"trigger_price  : {entry.trigger_price:.2f}")
    print(f"buy_price      : {entry.buy_price}")
    print(f"initial_sl     : {entry.initial_sl}")

    if not entry.passed:
        return

    print("\n" + "=" * 100)
    print("回放结果")
    print("=" * 100)
    print(f"股票           : {result['symbol']}")
    print(f"模式           : {result['sim_mode']}")
    print(f"买入日期       : {result['signal_date']}")
    print(f"买入价         : {result['buy_price']}")
    print(f"初始止损       : {result['initial_sl']}")
    print(f"初始股数       : {result['initial_qty']}")
    print(f"投入资金       : {result['cash_spent']}")
    print(f"最终剩余股数   : {result['final_qty']}")
    print(f"最终阶段       : {result['final_stage']}")
    print(f"最终止损       : {result['final_sl']}")
    print(f"最终退出日期   : {result['exit_date']}")
    print(f"最终退出价     : {result['exit_price']}")
    print(f"已实现盈亏     : {result['realized_pnl']}")
    print(f"未实现盈亏     : {result['unrealized_pnl']}")
    print(f"总盈亏         : {result['total_pnl']}")
    print(f"总收益率       : {result['total_return_pct']}%")
    print(f"是否触发止损   : {result['was_stopped']}")
    print(f"是否算被洗掉   : {result['washed_out']}")

    print("\n" + "=" * 140)
    print("每日过程")
    print("=" * 140)
    header = (
        f"{'date':<12}"
        f"{'O':>8}{'H':>8}{'L':>8}{'C':>8}"
        f"{'qty_b':>8}{'qty_a':>8}"
        f"{'sl_b':>9}{'sl_a':>9}"
        f"{'up_close%':>11}{'up_high%':>10}"
        f"{'action':>18}{'px':>9}{'aqty':>8}  note"
    )
    print(header)
    print("-" * 140)

    for rec in result["records"][:max_rows]:
        print(
            f"{rec.date:<12}"
            f"{rec.open:>8.2f}{rec.high:>8.2f}{rec.low:>8.2f}{rec.close:>8.2f}"
            f"{rec.qty_before:>8}{rec.qty_after:>8}"
            f"{rec.sl_before:>9.2f}{rec.sl_after:>9.2f}"
            f"{rec.up_pct_close*100:>10.2f}%{rec.up_pct_high*100:>9.2f}%"
            f"{rec.action:>18}{(rec.action_price if rec.action_price is not None else 0):>9.2f}{rec.action_qty:>8}  {rec.note}"
        )


# ============================================================
# CLI
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="验证 strategy_B 从指定日期到今天的历史表现（日线近似回放）")
    parser.add_argument("--symbol", required=True, help="股票代码，例如 BTM")
    parser.add_argument("--date", required=True, help="信号日期，例如 2026-04-10")
    parser.add_argument("--trigger", type=float, default=None, help="可选，手动传 trigger_price；不传则优先从 stock_operations 取，没有则用 prev_close*(1+3%)")
    parser.add_argument("--capital", type=float, default=B_TARGET_NOTIONAL_USD, help="初始投入资金，默认用当前 B_TARGET_NOTIONAL_USD")
    parser.add_argument("--start-buffer-days", type=int, default=30, help="向前多读多少天数据，默认 30")
    parser.add_argument("--mode", choices=["stop_first", "profit_first"], default=SIM_MODE, help="日线模拟路径")
    parser.add_argument("--max-rows", type=int, default=200, help="最多打印多少行 daily records")
    args = parser.parse_args()

    global SIM_MODE
    SIM_MODE = args.mode

    symbol = args.symbol.strip().upper()
    signal_date = pd.to_datetime(args.date).date()

    start_date = signal_date - pd.Timedelta(days=args.start_buffer_days)
    end_date = date.today()

    df = load_price_data(symbol, str(start_date), str(end_date))
    if df.empty:
        print(f"没有读取到 {symbol} 的历史数据。")
        sys.exit(1)

    trigger = args.trigger
    if trigger is None:
        trigger = load_trigger_from_ops(symbol)

    entry = check_entry(df, str(signal_date), trigger)
    result = simulate(df, str(signal_date), entry, args.capital) if entry.passed else {
        "records": []
    }

    print_summary(entry, result, max_rows=args.max_rows)


if __name__ == "__main__":
    main()
