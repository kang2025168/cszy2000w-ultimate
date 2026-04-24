# -*- coding: utf-8 -*-
"""
backtest/engine.py
策略B 回测引擎
- 单股回测：输入 symbol，输出 K线+买卖点+收益
- 全市场回测：按筛选条件扫全市场，模拟资金池
"""

import os
import math
from datetime import datetime, timedelta
from typing import Optional

import pymysql
import pymysql.cursors

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

def _connect():
    return pymysql.connect(**DB)

def _sf(v, d=0.0):
    try:
        return float(v) if v is not None else d
    except Exception:
        return d

# =========================
# 筛选条件参数（与 strategy_b_pick 保持一致）
# =========================
LOW_PCT           = float(os.getenv("B_LOW_PCT", "0.95"))
HIGH_PCT          = float(os.getenv("B_HIGH_PCT", "1.15"))
VOL_MULT          = float(os.getenv("B_VOL_MULT", "1.5"))
UP_PCT_MIN        = float(os.getenv("B_UP_PCT_MIN", "0.02"))
MIN_PRICE         = float(os.getenv("B_MIN_PRICE", "2.0"))
MIN_VOL_TODAY     = float(os.getenv("B_MIN_VOL_TODAY", "1000000"))
MAX_RISE_FROM_LOW = float(os.getenv("B_MAX_RISE_FROM_LOW_PCT", "0.20"))

# =========================
# 交易参数（与 strategy_b 保持一致）
# =========================
NOTIONAL          = float(os.getenv("B_TARGET_NOTIONAL_USD", "2100"))
MIN_UP_PCT        = float(os.getenv("B_MIN_UP_PCT", "0.03"))
MAX_BUY_UP_PCT    = 0.10
TRAIL_BACKOFF_PCT = 0.03
DYNAMIC_TRAIL_START_PCT = 0.08
FLASH_CRASH_WAIT_DAYS   = 0   # 日线回测简化：不做分钟级闪崩保护

STAGE_RULES = [
    (1, 0.03, 1.01, None, None),
    (2, 0.08, 1.06, None, None),
    (3, 0.15, 1.10, None, None),
    (4, 0.20, 1.15, None, None),
    (5, 0.30, 1.25, None, 0.20),
    (6, 0.40, 1.35, None, 0.20),
    (7, 0.50, 1.45, None, 0.15),
    (8, 0.65, 1.55, None, 0.10),
    (9, 0.80, 1.70, None, 0.10),
    (10, 1.00, 1.90, None, 0.05),
]

# =========================
# 数据加载
# =========================
def load_bars(conn, symbol: str, start_date=None, end_date=None):
    """加载日线数据，返回按日期升序列表"""
    where = "WHERE symbol=%s"
    params = [symbol]
    if start_date:
        where += " AND DATE(`date`) >= %s"
        params.append(str(start_date))
    if end_date:
        where += " AND DATE(`date`) <= %s"
        params.append(str(end_date))
    sql = f"""
    SELECT DATE(`date`) AS d, `open`, `high`, `low`, `close`, `volume`
    FROM `{SRC_TABLE}` {where}
    ORDER BY `date` ASC
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []
    return [{
        "d": str(r["d"]),
        "open":   _sf(r["open"]),
        "high":   _sf(r["high"]),
        "low":    _sf(r["low"]),
        "close":  _sf(r["close"]),
        "volume": _sf(r["volume"]),
    } for r in rows]


def load_pressure_levels(conn, symbol: str):
    """加载压力位，返回列表"""
    sql = f"""
    SELECT pressure_price, pressure_date
    FROM `{LEVELS_TABLE}`
    WHERE symbol=%s
    ORDER BY pressure_date ASC
    """
    with conn.cursor() as cur:
        cur.execute(sql, (symbol,))
        rows = cur.fetchall() or []
    return [{"price": _sf(r["pressure_price"]), "date": str(r["pressure_date"])} for r in rows]


def load_all_symbols_with_levels(conn):
    """拉取所有有压力位数据的股票"""
    sql = f"SELECT DISTINCT symbol FROM `{LEVELS_TABLE}` ORDER BY symbol"
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall() or []
    return [r["symbol"] for r in rows]


# =========================
# 技术指标计算
# =========================
def _check_entry_signal(bars, idx):
    """
    在 idx 这天检查是否满足入选条件（与 strategy_b_pick 一致）
    需要至少 idx >= 20
    返回 True/False
    """
    if idx < 20:
        return False

    closes  = [bars[i]["close"]  for i in range(idx - 20, idx + 1)]
    volumes = [bars[i]["volume"] for i in range(idx - 20, idx + 1)]

    c_today = closes[-1]
    c_prev  = closes[-2]
    v_today = volumes[-1]

    if c_today <= MIN_PRICE:
        return False
    if v_today <= MIN_VOL_TODAY:
        return False

    up_pct = (c_today - c_prev) / c_prev if c_prev > 0 else 0
    if up_pct <= UP_PCT_MIN:
        return False

    # MA
    ma3  = sum(closes[-3:]) / 3
    ma8  = sum(closes[-8:]) / 8
    ma10 = sum(closes[-10:]) / 10
    if not (ma3 > ma8 > 0 and ma3 > ma10):
        return False

    # 三连增
    if idx < 22:
        return False
    closes_ext = [bars[i]["close"] for i in range(idx - 22, idx + 1)]
    ma3_t = sum(closes_ext[-3:]) / 3
    ma8_t = sum(closes_ext[-8:]) / 8
    ma3_y = sum(closes_ext[-4:-1]) / 3
    ma8_y = sum(closes_ext[-9:-1]) / 8
    ma3_2 = sum(closes_ext[-5:-2]) / 3
    ma8_2 = sum(closes_ext[-10:-2]) / 8
    diff_t = ma3_t - ma8_t
    diff_y = ma3_y - ma8_y
    diff_2 = ma3_2 - ma8_2
    if not (diff_t > diff_y > diff_2):
        return False

    # 量能
    vol_avg20 = sum(volumes[-21:-1]) / 20
    if v_today <= vol_avg20 * VOL_MULT:
        return False

    # 近20日低点涨幅
    min20 = min(closes[-21:-1])
    rise  = (c_today - min20) / min20 if min20 > 0 else 0
    if rise >= MAX_RISE_FROM_LOW:
        return False

    return True


def _calc_dynamic_sl(cost, price, sl_old):
    if cost <= 0 or price <= cost:
        return sl_old
    up_pct = (price - cost) / cost
    if up_pct < DYNAMIC_TRAIL_START_PCT:
        return sl_old
    trail_sl = price - cost * TRAIL_BACKOFF_PCT
    return round(max(sl_old, trail_sl), 4)


# =========================
# 单股回测核心
# =========================
def backtest_single(symbol: str, start_date=None, end_date=None,
                    notional=NOTIONAL, conn=None):
    """
    单股回测
    返回：{
        bars: [...],          # K线数据
        trades: [...],        # 每笔交易
        equity_curve: [...],  # 每日资金曲线（相对于初始 notional）
        stats: {...},         # 统计指标
    }
    """
    close_conn = False
    if conn is None:
        conn = _connect()
        close_conn = True

    try:
        bars   = load_bars(conn, symbol, start_date, end_date)
        levels = load_pressure_levels(conn, symbol)

        if len(bars) < 25:
            return {"error": f"{symbol} 数据不足（{len(bars)}天）"}

        # 把压力位按日期整理成 dict: date -> [price]
        level_map = {}
        for lv in levels:
            d = lv["date"]
            level_map.setdefault(d, []).append(lv["price"])

        trades = []
        equity_curve = []
        cash = notional

        # 持仓状态
        pos = None  # None 或 dict

        for i, bar in enumerate(bars):
            d     = bar["d"]
            open_ = bar["open"]
            high  = bar["high"]
            low   = bar["low"]
            close = bar["close"]

            day_equity = cash + (pos["qty"] * close if pos else 0)
            equity_curve.append({"d": d, "equity": round(day_equity, 2)})

            # ── 持仓中：检查止损 / 阶段退出 ──────────────────────────
            if pos:
                cost  = pos["cost"]
                qty   = pos["qty"]
                sl    = pos["sl"]
                stage = pos["stage"]
                base_qty = pos["base_qty"]
                up_pct = (close - cost) / cost if cost > 0 else 0

                # 动态止损上移
                new_sl = _calc_dynamic_sl(cost, close, sl)
                if new_sl > sl:
                    pos["sl"] = new_sl
                    sl = new_sl

                # 止损触发（用 low 判断当天是否触及止损）
                if sl > 0 and low <= sl:
                    # 用止损价卖出（近似）
                    sell_price = min(sl, open_)  # 跳空则用 open
                    pnl = (sell_price - cost) * qty
                    cash += qty * sell_price
                    trades.append({
                        "type": "sell",
                        "reason": "STOP",
                        "date": d,
                        "price": round(sell_price, 4),
                        "qty": qty,
                        "pnl": round(pnl, 2),
                        "pnl_pct": round((sell_price - cost) / cost * 100, 2),
                        "stage": stage,
                    })
                    pos = None
                    continue

                # 阶段退出
                next_stage = stage + 1
                for s, pct, sl_mult, add_ratio, sell_ratio in STAGE_RULES:
                    if s == next_stage and up_pct >= pct:
                        stage_sl = round(cost * sl_mult, 4)
                        pos["sl"] = max(sl, stage_sl)
                        pos["stage"] = s

                        # 减仓
                        if sell_ratio and sell_ratio > 0:
                            sell_qty = max(1, int(math.floor(qty * sell_ratio)))
                            sell_qty = min(sell_qty, qty)
                            pnl = (close - cost) * sell_qty
                            cash += sell_qty * close
                            trades.append({
                                "type": "sell",
                                "reason": f"STAGE{s}_SELL",
                                "date": d,
                                "price": round(close, 4),
                                "qty": sell_qty,
                                "pnl": round(pnl, 2),
                                "pnl_pct": round((close - cost) / cost * 100, 2),
                                "stage": s,
                            })
                            pos["qty"] -= sell_qty
                            if pos["qty"] <= 0:
                                pos = None
                        break

                # 结构退出（stage >= 6，连续3天收盘低于前3天最低）
                if pos and pos["stage"] >= 6 and i >= 3:
                    c0 = bars[i]["close"]
                    c1 = bars[i-1]["close"]
                    c2 = bars[i-2]["close"]
                    c3 = bars[i-3]["close"]
                    if c0 < min(c1, c2, c3):
                        qty = pos["qty"]
                        cost = pos["cost"]
                        pnl = (close - cost) * qty
                        cash += qty * close
                        trades.append({
                            "type": "sell",
                            "reason": "STRUCT_EXIT",
                            "date": d,
                            "price": round(close, 4),
                            "qty": qty,
                            "pnl": round(pnl, 2),
                            "pnl_pct": round((close - cost) / cost * 100, 2),
                            "stage": pos["stage"],
                        })
                        pos = None

            # ── 无持仓：检查买入信号 ──────────────────────────────────
            if pos is None and cash >= notional:
                # 检查今天是否有压力位命中（价格在区间内）
                matched_pressure = None
                for lv in levels:
                    lv_price = lv["price"]
                    if lv_price * LOW_PCT <= close <= lv_price * HIGH_PCT:
                        matched_pressure = lv_price
                        break

                if matched_pressure and _check_entry_signal(bars, i):
                    # 次日买入（用次日 open 近似）
                    if i + 1 < len(bars):
                        next_bar = bars[i + 1]
                        buy_price = next_bar["open"]
                        # 验证次日涨幅条件（用昨收 vs 次日 open 近似）
                        up_today = (buy_price - close) / close if close > 0 else 0
                        if MIN_UP_PCT < up_today < MAX_BUY_UP_PCT and buy_price > matched_pressure:
                            qty = int(math.floor(min(notional, cash) / buy_price))
                            if qty > 0:
                                cost = buy_price
                                init_sl = max(matched_pressure, cost * 0.97)
                                pos = {
                                    "cost": cost,
                                    "qty": qty,
                                    "base_qty": qty,
                                    "sl": round(init_sl, 4),
                                    "stage": 0,
                                }
                                cash -= qty * buy_price
                                trades.append({
                                    "type": "buy",
                                    "reason": "SIGNAL",
                                    "date": next_bar["d"],
                                    "price": round(buy_price, 4),
                                    "qty": qty,
                                    "sl": round(init_sl, 4),
                                    "trigger": matched_pressure,
                                    "pnl": 0,
                                    "pnl_pct": 0,
                                    "stage": 0,
                                })

        # 强制平仓（回测结束）
        if pos:
            last_bar = bars[-1]
            close = last_bar["close"]
            qty   = pos["qty"]
            cost  = pos["cost"]
            pnl   = (close - cost) * qty
            cash += qty * close
            trades.append({
                "type": "sell",
                "reason": "END_OF_BACKTEST",
                "date": last_bar["d"],
                "price": round(close, 4),
                "qty": qty,
                "pnl": round(pnl, 2),
                "pnl_pct": round((close - cost) / cost * 100, 2),
                "stage": pos["stage"],
            })
            pos = None

        # ── 统计 ─────────────────────────────────────────────────────
        sell_trades = [t for t in trades if t["type"] == "sell" and t["reason"] != "END_OF_BACKTEST"]
        total_trades = len(sell_trades)
        win_trades   = [t for t in sell_trades if t["pnl"] > 0]
        loss_trades  = [t for t in sell_trades if t["pnl"] <= 0]
        win_rate     = len(win_trades) / total_trades * 100 if total_trades > 0 else 0
        avg_win      = sum(t["pnl_pct"] for t in win_trades)  / len(win_trades)  if win_trades  else 0
        avg_loss     = sum(t["pnl_pct"] for t in loss_trades) / len(loss_trades) if loss_trades else 0

        # 最大回撤
        peak = notional
        max_dd = 0
        for e in equity_curve:
            if e["equity"] > peak:
                peak = e["equity"]
            dd = (peak - e["equity"]) / peak * 100
            if dd > max_dd:
                max_dd = dd

        final_equity = equity_curve[-1]["equity"] if equity_curve else notional
        total_return = (final_equity - notional) / notional * 100

        stats = {
            "symbol":        symbol,
            "total_return":  round(total_return, 2),
            "final_equity":  round(final_equity, 2),
            "total_trades":  total_trades,
            "win_rate":      round(win_rate, 1),
            "avg_win_pct":   round(avg_win, 2),
            "avg_loss_pct":  round(avg_loss, 2),
            "profit_factor": round(abs(avg_win / avg_loss), 2) if avg_loss != 0 else 0,
            "max_drawdown":  round(max_dd, 2),
            "days":          len(bars),
        }

        return {
            "bars":         bars,
            "trades":       trades,
            "equity_curve": equity_curve,
            "stats":        stats,
        }

    finally:
        if close_conn:
            conn.close()


# =========================
# 全市场回测
# =========================
def backtest_market(start_date=None, end_date=None,
                    initial_cash=10000.0, max_positions=5,
                    notional=NOTIONAL):
    """
    全市场回测：模拟资金池，最多同时持仓 max_positions 只
    返回：{
        equity_curve: [...],
        trades: [...],
        stats: {...},
        top_stocks: [...],  # 贡献最大的股票
    }
    """
    conn = _connect()
    try:
        symbols = load_all_symbols_with_levels(conn)
        print(f"[BACKTEST] 全市场回测 symbols={len(symbols)} start={start_date} end={end_date}", flush=True)

        # 预加载所有股票数据（只加载有压力位的）
        all_bars = {}
        all_levels = {}
        for sym in symbols:
            bars = load_bars(conn, sym, start_date, end_date)
            if len(bars) >= 25:
                all_bars[sym] = bars
                all_levels[sym] = load_pressure_levels(conn, sym)

        print(f"[BACKTEST] 有效股票数={len(all_bars)}", flush=True)

        # 构建全局日期列表
        all_dates = sorted(set(
            bar["d"] for bars in all_bars.values() for bar in bars
        ))

        cash = initial_cash
        positions = {}  # symbol -> pos dict
        all_trades = []
        equity_curve = []

        for date in all_dates:
            day_value = cash
            for sym, pos in positions.items():
                # 找当天 bar
                bars = all_bars[sym]
                bar = next((b for b in bars if b["d"] == date), None)
                if bar:
                    day_value += pos["qty"] * bar["close"]

            equity_curve.append({"d": date, "equity": round(day_value, 2)})

            # ── 检查持仓止损/退出 ──────────────────────────────────
            to_close = []
            for sym, pos in positions.items():
                bars = all_bars[sym]
                bar  = next((b for b in bars if b["d"] == date), None)
                if not bar:
                    continue

                close = bar["close"]
                low   = bar["low"]
                cost  = pos["cost"]
                qty   = pos["qty"]
                sl    = pos["sl"]
                stage = pos["stage"]
                up_pct = (close - cost) / cost if cost > 0 else 0

                # 动态止损
                new_sl = _calc_dynamic_sl(cost, close, sl)
                if new_sl > sl:
                    pos["sl"] = new_sl
                    sl = new_sl

                # 止损
                if sl > 0 and low <= sl:
                    sell_price = min(sl, bar["open"])
                    pnl = (sell_price - cost) * qty
                    cash += qty * sell_price
                    all_trades.append({
                        "symbol": sym, "type": "sell", "reason": "STOP",
                        "date": date, "price": round(sell_price, 4),
                        "qty": qty, "pnl": round(pnl, 2),
                        "pnl_pct": round((sell_price - cost) / cost * 100, 2),
                    })
                    to_close.append(sym)
                    continue

                # 阶段退出
                next_stage = stage + 1
                for s, pct, sl_mult, add_ratio, sell_ratio in STAGE_RULES:
                    if s == next_stage and up_pct >= pct:
                        pos["sl"] = max(sl, round(cost * sl_mult, 4))
                        pos["stage"] = s
                        if sell_ratio and sell_ratio > 0:
                            sell_qty = max(1, int(math.floor(qty * sell_ratio)))
                            sell_qty = min(sell_qty, qty)
                            pnl = (close - cost) * sell_qty
                            cash += sell_qty * close
                            all_trades.append({
                                "symbol": sym, "type": "sell",
                                "reason": f"STAGE{s}_SELL",
                                "date": date, "price": round(close, 4),
                                "qty": sell_qty, "pnl": round(pnl, 2),
                                "pnl_pct": round((close - cost) / cost * 100, 2),
                            })
                            pos["qty"] -= sell_qty
                            if pos["qty"] <= 0:
                                to_close.append(sym)
                        break

            for sym in to_close:
                positions.pop(sym, None)

            # ── 检查买入信号 ──────────────────────────────────────
            if len(positions) < max_positions and cash >= notional:
                for sym, bars in all_bars.items():
                    if sym in positions:
                        continue
                    if len(positions) >= max_positions:
                        break

                    idx = next((i for i, b in enumerate(bars) if b["d"] == date), None)
                    if idx is None or idx < 22:
                        continue

                    bar   = bars[idx]
                    close = bar["close"]
                    levels = all_levels.get(sym, [])

                    matched_pressure = None
                    for lv in levels:
                        lv_price = lv["price"]
                        if lv_price * LOW_PCT <= close <= lv_price * HIGH_PCT:
                            matched_pressure = lv_price
                            break

                    if matched_pressure and _check_entry_signal(bars, idx):
                        if idx + 1 < len(bars):
                            next_bar = bars[idx + 1]
                            buy_price = next_bar["open"]
                            up_today = (buy_price - close) / close if close > 0 else 0
                            if MIN_UP_PCT < up_today < MAX_BUY_UP_PCT and buy_price > matched_pressure:
                                qty = int(math.floor(min(notional, cash) / buy_price))
                                if qty > 0:
                                    cost = buy_price
                                    init_sl = max(matched_pressure, cost * 0.97)
                                    positions[sym] = {
                                        "cost": cost, "qty": qty,
                                        "base_qty": qty,
                                        "sl": round(init_sl, 4),
                                        "stage": 0,
                                    }
                                    cash -= qty * buy_price
                                    all_trades.append({
                                        "symbol": sym, "type": "buy",
                                        "reason": "SIGNAL",
                                        "date": next_bar["d"],
                                        "price": round(buy_price, 4),
                                        "qty": qty,
                                        "sl": round(init_sl, 4),
                                        "pnl": 0, "pnl_pct": 0,
                                    })

        # 强制平仓
        for sym, pos in positions.items():
            bars = all_bars.get(sym, [])
            if not bars:
                continue
            last = bars[-1]
            close = last["close"]
            qty   = pos["qty"]
            cost  = pos["cost"]
            pnl   = (close - cost) * qty
            cash += qty * close
            all_trades.append({
                "symbol": sym, "type": "sell",
                "reason": "END_OF_BACKTEST",
                "date": last["d"], "price": round(close, 4),
                "qty": qty, "pnl": round(pnl, 2),
                "pnl_pct": round((close - cost) / cost * 100, 2),
            })

        # 统计
        sell_trades  = [t for t in all_trades if t["type"] == "sell" and t["reason"] != "END_OF_BACKTEST"]
        total_trades = len(sell_trades)
        win_trades   = [t for t in sell_trades if t["pnl"] > 0]
        win_rate     = len(win_trades) / total_trades * 100 if total_trades > 0 else 0

        peak = initial_cash
        max_dd = 0
        for e in equity_curve:
            if e["equity"] > peak:
                peak = e["equity"]
            dd = (peak - e["equity"]) / peak * 100
            if dd > max_dd:
                max_dd = dd

        final_equity = equity_curve[-1]["equity"] if equity_curve else initial_cash
        total_return = (final_equity - initial_cash) / initial_cash * 100

        # 按股票统计盈亏
        sym_pnl = {}
        for t in all_trades:
            sym = t["symbol"]
            sym_pnl[sym] = sym_pnl.get(sym, 0) + t.get("pnl", 0)
        top_stocks = sorted(
            [{"symbol": k, "pnl": round(v, 2)} for k, v in sym_pnl.items()],
            key=lambda x: x["pnl"], reverse=True
        )[:10]

        stats = {
            "total_return":  round(total_return, 2),
            "final_equity":  round(final_equity, 2),
            "initial_cash":  initial_cash,
            "total_trades":  total_trades,
            "win_rate":      round(win_rate, 1),
            "max_drawdown":  round(max_dd, 2),
            "days":          len(all_dates),
            "symbols_tested": len(all_bars),
        }

        return {
            "equity_curve": equity_curve,
            "trades":       all_trades,
            "stats":        stats,
            "top_stocks":   top_stocks,
        }

    finally:
        conn.close()
