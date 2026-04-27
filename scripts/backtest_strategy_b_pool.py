# -*- coding: utf-8 -*-
"""
scripts/backtest_strategy_b_pool.py

用途：
1) 给定开始日期、初始本金、股票池
2) 从 stock_prices_pool 读取历史日线
3) 按当前 strategy_b.py 的“无加仓版”逻辑，做组合级日线近似回测
4) 每天：
   - 先处理已有持仓的卖出
   - 再用释放后的现金扫描股票池做买入
5) 输出：
   - 最终资金 / 收益率
   - 已实现 / 未实现盈亏
   - 交易明细
   - 每日权益曲线（可选保存 CSV）

重要说明：
- 这是“日线近似回测”，不是分钟级实盘回放
- 默认 sim_mode=stop_first（保守）：先看 low 是否打旧 SL，再按 high 抬 SL / 触发 stage
- 同一天内，卖出后可以再买别的股票
- 同一只股票如果当天刚卖掉，不会当天再买回
- 买入顺序按你传入的股票池顺序执行
"""

import os
import sys
import math
import argparse
from pathlib import Path
from dataclasses import dataclass, asdict
from datetime import date
from typing import Dict, List, Optional, Tuple

import pymysql
import pandas as pd

# ============================================================
# 把项目根目录加入 sys.path
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ============================================================
# 导入主策略文件，复用参数
# ============================================================
try:
    from app import strategy_b as sb
except Exception as e:
    raise RuntimeError(f"无法导入 app.strategy_b，PROJECT_ROOT={PROJECT_ROOT}，错误: {e}")

# ============================================================
# DB
# 默认本地；连云库示例：
# DB_HOST=138.197.75.51 DB_PORT=3307 python scripts/backtest_strategy_b_pool.py ...
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
# 复用当前策略参数
# ============================================================
B_MIN_UP_PCT = float(getattr(sb, "B_MIN_UP_PCT", 0.03))
B_MAX_BUY_UP_PCT = float(getattr(sb, "B_MAX_BUY_UP_PCT", 0.10))
B_TARGET_NOTIONAL_USD = float(getattr(sb, "B_TARGET_NOTIONAL_USD", 2100.0))

ENABLE_STRUCTURE_EXIT_STAGE = 3
DYNAMIC_TRAIL_START_PCT = 0.08
TRAIL_BREAKEVEN_PCT = 0.08
TRAIL_LOCK_LIGHT_PCT = 0.15
TRAIL_PRICE_TRACK_PCT = 0.25
TRAIL_BACKOFF_PCT = 0.07

SAME_DAY_FORCE_SELL_LOSS_PCT = -0.05
SAME_DAY_FORCE_SELL_WIN_PCT = 0.05

STAGE_RULES = [
    # stage, profit_pct, sell_ratio
    (1, 0.20, 0.20),
    (2, 0.35, 0.20),
    (3, 0.60, 0.15),
    (4, 0.85, 0.10),
    (5, 1.20, 0.10),
]

DEFAULT_SIM_MODE = "stop_first"


# ============================================================
# 数据结构
# ============================================================
@dataclass
class Position:
    symbol: str
    entry_date: str
    buy_price: float
    cost: float
    qty: int
    sl: float
    last_stage: int = 0


@dataclass
class TradeRecord:
    date: str
    symbol: str
    side: str              # BUY / SELL / PARTIAL_SELL
    price: float
    qty: int
    amount: float
    realized_pnl: float
    note: str


@dataclass
class EquityRecord:
    date: str
    cash: float
    market_value: float
    total_equity: float
    holding_count: int


# ============================================================
# 工具函数
# ============================================================
def _safe_float(v, default=0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _connect():
    return pymysql.connect(**DB)


def load_symbols_from_file(pool_file: str) -> List[str]:
    p = Path(pool_file)
    if not p.exists():
        raise FileNotFoundError(f"股票池文件不存在: {pool_file}")

    symbols = []
    for line in p.read_text(encoding="utf-8").splitlines():
        s = line.strip().upper()
        if not s:
            continue
        if "," in s:
            parts = [x.strip().upper() for x in s.split(",") if x.strip()]
            symbols.extend(parts)
        else:
            symbols.append(s)

    # 去重但保序
    seen = set()
    out = []
    for s in symbols:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def load_symbols_from_ops(stock_type: str = "B") -> List[str]:
    sql = f"""
    SELECT DISTINCT stock_code
    FROM `{OPS_TABLE}`
    WHERE stock_type=%s
    ORDER BY stock_code
    """
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (stock_type,))
            rows = cur.fetchall() or []
    finally:
        conn.close()

    return [str(r["stock_code"]).strip().upper() for r in rows if r.get("stock_code")]


def load_trigger_map_from_ops(symbols: List[str]) -> Dict[str, float]:
    if not symbols:
        return {}

    placeholders = ",".join(["%s"] * len(symbols))
    sql = f"""
    SELECT stock_code, trigger_price
    FROM `{OPS_TABLE}`
    WHERE stock_type='B'
      AND stock_code IN ({placeholders})
    """
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(symbols))
            rows = cur.fetchall() or []
    finally:
        conn.close()

    mp = {}
    for r in rows:
        code = str(r.get("stock_code") or "").strip().upper()
        tp = _safe_float(r.get("trigger_price"), 0.0)
        if code and tp > 0:
            mp[code] = tp
    return mp


def load_all_price_data(symbols: List[str], start_date: str, end_date: str) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"])

    placeholders = ",".join(["%s"] * len(symbols))
    sql = f"""
    SELECT `symbol`, `date`, `open`, `high`, `low`, `close`, `volume`
    FROM `{PRICES_TABLE}`
    WHERE `symbol` IN ({placeholders})
      AND `date` BETWEEN %s AND %s
    ORDER BY `symbol`, `date` ASC
    """

    params = list(symbols) + [start_date, end_date]

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    finally:
        conn.close()

    if not rows:
        return pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(list(rows))
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).copy()
    df["date"] = df["date"].dt.date

    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)
    return df


def build_symbol_frames(df_all: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    mp: Dict[str, pd.DataFrame] = {}
    if df_all.empty:
        return mp

    for sym, g in df_all.groupby("symbol", sort=False):
        gg = g.sort_values("date").reset_index(drop=True).copy()
        mp[str(sym).strip().upper()] = gg
    return mp


def calc_dynamic_trail_sl(cost: float, price_ref: float, sl_old: float) -> float:
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
# 买入检查：对单只股票、某一天
# ============================================================
def check_entry_on_day(
    df_sym: pd.DataFrame,
    idx: int,
    trigger_price: Optional[float],
) -> Tuple[bool, str, Optional[float], Optional[float], Optional[float]]:
    """
    返回:
      passed, reason, buy_price, initial_sl, used_trigger
    """
    if idx <= 0:
        return False, "没有前一交易日", None, None, None

    row = df_sym.iloc[idx]
    prev_row = df_sym.iloc[idx - 1]

    prev_close = _safe_float(prev_row["close"], 0.0)
    open_price = _safe_float(row["open"], 0.0)
    high = _safe_float(row["high"], 0.0)
    close = _safe_float(row["close"], 0.0)

    if prev_close <= 0:
        return False, "prev_close<=0", None, None, None

    day_up_pct = (close - prev_close) / prev_close

    if trigger_price is None or trigger_price <= 0:
        used_trigger = round(prev_close * (1.0 + B_MIN_UP_PCT), 2)
    else:
        used_trigger = round(float(trigger_price), 2)

    if day_up_pct <= B_MIN_UP_PCT:
        return False, f"day_up_pct={day_up_pct:.2%}<=min_up", None, None, used_trigger

    if day_up_pct >= B_MAX_BUY_UP_PCT:
        return False, f"day_up_pct={day_up_pct:.2%}>=max_buy_up", None, None, used_trigger

    if high <= used_trigger:
        return False, f"high={high:.2f}<=trigger={used_trigger:.2f}", None, None, used_trigger

    buy_price = max(open_price, used_trigger)
    initial_sl = max(prev_close, buy_price * 0.97)

    return True, "PASS", round(buy_price, 2), round(initial_sl, 2), used_trigger


# ============================================================
# 单只持仓的一天卖出处理
# ============================================================
def process_position_one_day(
    pos: Position,
    df_sym: pd.DataFrame,
    idx: int,
    sim_mode: str,
    trade_log: List[TradeRecord],
) -> Tuple[Position, float, bool]:
    """
    返回:
      new_position, cash_delta, closed_today
    cash_delta > 0 表示卖出回笼现金
    """
    row = df_sym.iloc[idx]
    dt = str(row["date"])
    o = _safe_float(row["open"])
    h = _safe_float(row["high"])
    l = _safe_float(row["low"])
    c = _safe_float(row["close"])

    qty = pos.qty
    sl = pos.sl
    cost = pos.cost
    last_stage = pos.last_stage
    is_same_day = (dt == pos.entry_date)

    cash_delta = 0.0
    closed_today = False

    if qty <= 0:
        return pos, 0.0, True

    if sim_mode == "stop_first":
        # 1) 先看 low 是否打旧 SL
        if sl > 0 and l <= sl:
            ref_up_pct = (sl - cost) / cost if cost > 0 else 0.0
            if allow_sell_same_day(is_same_day, ref_up_pct):
                sell_qty = qty
                sell_price = round(sl, 2)
                realized = sell_qty * (sell_price - cost)
                cash_delta += sell_qty * sell_price
                trade_log.append(TradeRecord(
                    date=dt,
                    symbol=pos.symbol,
                    side="SELL",
                    price=sell_price,
                    qty=sell_qty,
                    amount=round(sell_qty * sell_price, 2),
                    realized_pnl=round(realized, 2),
                    note="STOP_ALL stop_first",
                ))
                pos.qty = 0
                closed_today = True
                return pos, cash_delta, closed_today

        # 2) 再按 high 抬 dynamic SL
        new_sl = calc_dynamic_trail_sl(cost, h, sl)
        if new_sl > sl:
            pos.sl = new_sl
            sl = new_sl

        # 3) 再触发 stage（只执行最高一档）
        up_pct_high = (h - cost) / cost if cost > 0 else 0.0
        highest_stage = find_highest_hit_stage(up_pct_high, last_stage)
        if highest_stage is not None and highest_stage > last_stage and pos.qty > 0:
            sell_ratio = get_stage_sell_ratio(highest_stage)
            if sell_ratio is not None and sell_ratio > 0:
                if allow_sell_same_day(is_same_day, up_pct_high):
                    sell_qty = min(pos.qty, max(1, int(math.floor(pos.qty * sell_ratio))))
                    stage_pct = get_stage_pct(highest_stage)
                    sell_price = round(cost * (1.0 + stage_pct), 2)
                    realized = sell_qty * (sell_price - cost)
                    cash_delta += sell_qty * sell_price
                    pos.qty -= sell_qty
                    pos.last_stage = highest_stage
                    trade_log.append(TradeRecord(
                        date=dt,
                        symbol=pos.symbol,
                        side="PARTIAL_SELL",
                        price=sell_price,
                        qty=sell_qty,
                        amount=round(sell_qty * sell_price, 2),
                        realized_pnl=round(realized, 2),
                        note=f"STAGE{highest_stage}_SELL",
                    ))
    else:
        # 乐观：先抬 SL，再 stage，再看 low
        new_sl = calc_dynamic_trail_sl(cost, h, sl)
        if new_sl > sl:
            pos.sl = new_sl
            sl = new_sl

        up_pct_high = (h - cost) / cost if cost > 0 else 0.0
        highest_stage = find_highest_hit_stage(up_pct_high, last_stage)
        if highest_stage is not None and highest_stage > last_stage and pos.qty > 0:
            sell_ratio = get_stage_sell_ratio(highest_stage)
            if sell_ratio is not None and sell_ratio > 0:
                if allow_sell_same_day(is_same_day, up_pct_high):
                    sell_qty = min(pos.qty, max(1, int(math.floor(pos.qty * sell_ratio))))
                    stage_pct = get_stage_pct(highest_stage)
                    sell_price = round(cost * (1.0 + stage_pct), 2)
                    realized = sell_qty * (sell_price - cost)
                    cash_delta += sell_qty * sell_price
                    pos.qty -= sell_qty
                    pos.last_stage = highest_stage
                    trade_log.append(TradeRecord(
                        date=dt,
                        symbol=pos.symbol,
                        side="PARTIAL_SELL",
                        price=sell_price,
                        qty=sell_qty,
                        amount=round(sell_qty * sell_price, 2),
                        realized_pnl=round(realized, 2),
                        note=f"STAGE{highest_stage}_SELL",
                    ))

        if pos.qty > 0 and sl > 0 and l <= sl:
            ref_up_pct = (sl - cost) / cost if cost > 0 else 0.0
            if allow_sell_same_day(is_same_day, ref_up_pct):
                sell_qty = pos.qty
                sell_price = round(sl, 2)
                realized = sell_qty * (sell_price - cost)
                cash_delta += sell_qty * sell_price
                trade_log.append(TradeRecord(
                    date=dt,
                    symbol=pos.symbol,
                    side="SELL",
                    price=sell_price,
                    qty=sell_qty,
                    amount=round(sell_qty * sell_price, 2),
                    realized_pnl=round(realized, 2),
                    note="STOP_ALL profit_first",
                ))
                pos.qty = 0
                closed_today = True
                return pos, cash_delta, closed_today

    # 4) 结构退出
    if pos.qty > 0 and pos.last_stage >= ENABLE_STRUCTURE_EXIT_STAGE:
        closes4 = get_recent_closes_for_structure(df_sym, idx)
        if closes4 is not None:
            c0, c1, c2, c3 = closes4
            min3 = min(c1, c2, c3)
            if c0 > 0 and min3 > 0 and c0 < min3:
                ref_up_pct = (c0 - cost) / cost if cost > 0 else 0.0
                if allow_sell_same_day(is_same_day, ref_up_pct):
                    sell_qty = pos.qty
                    sell_price = round(c0, 2)
                    realized = sell_qty * (sell_price - cost)
                    cash_delta += sell_qty * sell_price
                    trade_log.append(TradeRecord(
                        date=dt,
                        symbol=pos.symbol,
                        side="SELL",
                        price=sell_price,
                        qty=sell_qty,
                        amount=round(sell_qty * sell_price, 2),
                        realized_pnl=round(realized, 2),
                        note=f"STRUCT_EXIT close={c0:.2f}<min3={min3:.2f}",
                    ))
                    pos.qty = 0
                    closed_today = True
                    return pos, cash_delta, closed_today

    return pos, cash_delta, closed_today


# ============================================================
# 主回测
# ============================================================
def backtest_pool(
    symbols: List[str],
    start_date: str,
    initial_capital: float,
    trade_notional: float,
    sim_mode: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, dict]:
    end_date = date.today()

    # 向前多读一些，为了首日能拿到 prev_close / 结构退出
    load_start = pd.to_datetime(start_date).date() - pd.Timedelta(days=40)

    df_all = load_all_price_data(symbols, str(load_start), str(end_date))
    if df_all.empty:
        raise RuntimeError("没有读取到任何历史数据。")

    frames = build_symbol_frames(df_all)
    trigger_map = load_trigger_map_from_ops(symbols)

    # 建立全局交易日历
    calendar = sorted({d for d in df_all["date"].tolist() if d >= pd.to_datetime(start_date).date()})

    cash = float(initial_capital)
    positions: Dict[str, Position] = {}
    trade_log: List[TradeRecord] = []
    equity_log: List[EquityRecord] = []

    for cur_date in calendar:
        sold_today = set()

        # --------------------------------------------------------
        # 1) 先卖出
        # --------------------------------------------------------
        holding_symbols = list(positions.keys())
        for sym in holding_symbols:
            pos = positions.get(sym)
            if pos is None:
                continue

            df_sym = frames.get(sym)
            if df_sym is None or df_sym.empty:
                continue

            idx_list = df_sym.index[df_sym["date"] == cur_date].tolist()
            if not idx_list:
                continue
            idx = idx_list[0]

            new_pos, cash_delta, closed_today = process_position_one_day(
                pos=pos,
                df_sym=df_sym,
                idx=idx,
                sim_mode=sim_mode,
                trade_log=trade_log,
            )
            cash += cash_delta

            if new_pos.qty <= 0:
                positions.pop(sym, None)
            else:
                positions[sym] = new_pos

            if closed_today:
                sold_today.add(sym)

        # --------------------------------------------------------
        # 2) 再扫描买入
        # 顺序：按 symbols 传入顺序
        # --------------------------------------------------------
        for sym in symbols:
            if sym in positions:
                continue
            if sym in sold_today:
                continue

            df_sym = frames.get(sym)
            if df_sym is None or df_sym.empty:
                continue

            idx_list = df_sym.index[df_sym["date"] == cur_date].tolist()
            if not idx_list:
                continue
            idx = idx_list[0]

            # 资金不够目标仓位就不买
            if cash < trade_notional:
                continue

            trigger = trigger_map.get(sym)
            passed, reason, buy_price, initial_sl, used_trigger = check_entry_on_day(df_sym, idx, trigger)

            if not passed:
                continue

            qty = int(trade_notional // buy_price) if buy_price and buy_price > 0 else 0
            if qty <= 0:
                continue

            amount = round(qty * buy_price, 2)
            if cash < amount:
                continue

            cash -= amount
            positions[sym] = Position(
                symbol=sym,
                entry_date=str(cur_date),
                buy_price=round(buy_price, 2),
                cost=round(buy_price, 2),
                qty=int(qty),
                sl=round(initial_sl, 2),
                last_stage=0,
            )
            trade_log.append(TradeRecord(
                date=str(cur_date),
                symbol=sym,
                side="BUY",
                price=round(buy_price, 2),
                qty=int(qty),
                amount=amount,
                realized_pnl=0.0,
                note=f"trigger={used_trigger:.2f}",
            ))

        # --------------------------------------------------------
        # 3) 记录日终权益
        # --------------------------------------------------------
        market_value = 0.0
        for sym, pos in positions.items():
            df_sym = frames.get(sym)
            if df_sym is None or df_sym.empty:
                continue
            idx_list = df_sym.index[df_sym["date"] == cur_date].tolist()
            if not idx_list:
                continue
            idx = idx_list[0]
            close_px = _safe_float(df_sym.iloc[idx]["close"], 0.0)
            market_value += pos.qty * close_px

        total_equity = cash + market_value
        equity_log.append(EquityRecord(
            date=str(cur_date),
            cash=round(cash, 2),
            market_value=round(market_value, 2),
            total_equity=round(total_equity, 2),
            holding_count=len(positions),
        ))

    # ------------------------------------------------------------
    # 统计
    # ------------------------------------------------------------
    trades_df = pd.DataFrame([asdict(x) for x in trade_log]) if trade_log else pd.DataFrame(
        columns=["date", "symbol", "side", "price", "qty", "amount", "realized_pnl", "note"]
    )
    equity_df = pd.DataFrame([asdict(x) for x in equity_log]) if equity_log else pd.DataFrame(
        columns=["date", "cash", "market_value", "total_equity", "holding_count"]
    )

    realized_pnl = float(trades_df["realized_pnl"].sum()) if not trades_df.empty else 0.0

    unrealized_pnl = 0.0
    open_positions = []
    if positions:
        last_date = calendar[-1]
        for sym, pos in positions.items():
            df_sym = frames.get(sym)
            if df_sym is None or df_sym.empty:
                continue
            idx_list = df_sym.index[df_sym["date"] == last_date].tolist()
            if not idx_list:
                continue
            idx = idx_list[0]
            last_close = _safe_float(df_sym.iloc[idx]["close"], 0.0)
            upnl = pos.qty * (last_close - pos.cost)
            unrealized_pnl += upnl
            open_positions.append({
                "symbol": sym,
                "qty": pos.qty,
                "cost": round(pos.cost, 2),
                "last_close": round(last_close, 2),
                "sl": round(pos.sl, 2),
                "last_stage": pos.last_stage,
                "unrealized_pnl": round(upnl, 2),
            })

    final_equity = float(equity_df.iloc[-1]["total_equity"]) if not equity_df.empty else initial_capital
    total_pnl = final_equity - float(initial_capital)
    total_return_pct = (total_pnl / float(initial_capital) * 100.0) if initial_capital > 0 else 0.0

    sell_like = trades_df[trades_df["side"].isin(["SELL", "PARTIAL_SELL"])].copy() if not trades_df.empty else pd.DataFrame()
    win_trades = sell_like[sell_like["realized_pnl"] > 0] if not sell_like.empty else pd.DataFrame()
    loss_trades = sell_like[sell_like["realized_pnl"] < 0] if not sell_like.empty else pd.DataFrame()
    win_rate = (len(win_trades) / len(sell_like) * 100.0) if len(sell_like) > 0 else 0.0

    summary = {
        "start_date": str(start_date),
        "end_date": str(calendar[-1]) if calendar else str(start_date),
        "initial_capital": round(float(initial_capital), 2),
        "trade_notional": round(float(trade_notional), 2),
        "sim_mode": sim_mode,
        "symbols_count": len(symbols),
        "buy_count": int((trades_df["side"] == "BUY").sum()) if not trades_df.empty else 0,
        "sell_count": int((trades_df["side"] == "SELL").sum()) if not trades_df.empty else 0,
        "partial_sell_count": int((trades_df["side"] == "PARTIAL_SELL").sum()) if not trades_df.empty else 0,
        "realized_pnl": round(realized_pnl, 2),
        "unrealized_pnl": round(unrealized_pnl, 2),
        "total_pnl": round(total_pnl, 2),
        "final_equity": round(final_equity, 2),
        "total_return_pct": round(total_return_pct, 2),
        "win_rate": round(win_rate, 2),
        "open_positions": open_positions,
    }

    return trades_df, equity_df, summary


# ============================================================
# 打印
# ============================================================
def print_summary(summary: dict):
    print("\n" + "=" * 100)
    print("组合回测结果")
    print("=" * 100)
    print(f"开始日期         : {summary['start_date']}")
    print(f"结束日期         : {summary['end_date']}")
    print(f"模拟模式         : {summary['sim_mode']}")
    print(f"股票池数量       : {summary['symbols_count']}")
    print(f"初始本金         : {summary['initial_capital']}")
    print(f"单笔目标资金     : {summary['trade_notional']}")
    print(f"BUY 笔数         : {summary['buy_count']}")
    print(f"SELL 笔数        : {summary['sell_count']}")
    print(f"PARTIAL_SELL 笔数: {summary['partial_sell_count']}")
    print(f"已实现盈亏       : {summary['realized_pnl']}")
    print(f"未实现盈亏       : {summary['unrealized_pnl']}")
    print(f"总盈亏           : {summary['total_pnl']}")
    print(f"最终权益         : {summary['final_equity']}")
    print(f"总收益率         : {summary['total_return_pct']}%")
    print(f"卖出胜率         : {summary['win_rate']}%")

    if summary["open_positions"]:
        print("\n当前未平仓：")
        for x in summary["open_positions"]:
            print(
                f"  {x['symbol']} qty={x['qty']} cost={x['cost']} last_close={x['last_close']} "
                f"sl={x['sl']} stage={x['last_stage']} upnl={x['unrealized_pnl']}"
            )


def print_trades(trades_df: pd.DataFrame, max_rows: int = 300):
    if trades_df.empty:
        print("\n没有交易记录。")
        return

    print("\n" + "=" * 140)
    print("交易明细")
    print("=" * 140)
    print(f"{'date':<12}{'symbol':<8}{'side':<14}{'price':>10}{'qty':>8}{'amount':>12}{'real_pnl':>12}  note")
    print("-" * 140)

    for _, r in trades_df.head(max_rows).iterrows():
        print(
            f"{str(r['date']):<12}"
            f"{str(r['symbol']):<8}"
            f"{str(r['side']):<14}"
            f"{float(r['price']):>10.2f}"
            f"{int(r['qty']):>8}"
            f"{float(r['amount']):>12.2f}"
            f"{float(r['realized_pnl']):>12.2f}  "
            f"{str(r['note'])}"
        )


# ============================================================
# CLI
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="策略B股票池组合级日线近似回测")
    parser.add_argument("--start-date", required=True, help="开始日期，例如 2026-04-01")
    parser.add_argument("--capital", type=float, default=20000, help="初始本金，默认 20000")
    parser.add_argument("--trade-notional", type=float, default=B_TARGET_NOTIONAL_USD, help="单笔目标资金，默认当前 B_TARGET_NOTIONAL_USD")
    parser.add_argument("--symbols", default="", help="逗号分隔股票池，例如 MXL,BTM,FCEL")
    parser.add_argument("--pool-file", default="", help="股票池文件路径，每行一个代码，或逗号分隔")
    parser.add_argument("--pool-from-ops", action="store_true", help="直接从 stock_operations 读取 B 股票池")
    parser.add_argument("--mode", choices=["stop_first", "profit_first"], default=DEFAULT_SIM_MODE, help="日线模拟路径")
    parser.add_argument("--max-trades-print", type=int, default=300, help="最多打印多少行交易明细")
    parser.add_argument("--save-trades", default="", help="可选，保存交易明细 CSV 路径")
    parser.add_argument("--save-equity", default="", help="可选，保存权益曲线 CSV 路径")
    args = parser.parse_args()

    symbols: List[str] = []

    if args.symbols.strip():
        symbols.extend([x.strip().upper() for x in args.symbols.split(",") if x.strip()])

    if args.pool_file.strip():
        symbols.extend(load_symbols_from_file(args.pool_file.strip()))

    if args.pool_from_ops:
        symbols.extend(load_symbols_from_ops("B"))

    # 去重保序
    seen = set()
    pool = []
    for s in symbols:
        if s and s not in seen:
            seen.add(s)
            pool.append(s)

    if not pool:
        raise RuntimeError("没有股票池。请传 --symbols 或 --pool-file 或 --pool-from-ops")

    trades_df, equity_df, summary = backtest_pool(
        symbols=pool,
        start_date=args.start_date,
        initial_capital=float(args.capital),
        trade_notional=float(args.trade_notional),
        sim_mode=args.mode,
    )

    print_summary(summary)
    print_trades(trades_df, max_rows=args.max_trades_print)

    if args.save_trades.strip():
        Path(args.save_trades).parent.mkdir(parents=True, exist_ok=True)
        trades_df.to_csv(args.save_trades, index=False, encoding="utf-8-sig")
        print(f"\n交易明细已保存: {args.save_trades}")

    if args.save_equity.strip():
        Path(args.save_equity).parent.mkdir(parents=True, exist_ok=True)
        equity_df.to_csv(args.save_equity, index=False, encoding="utf-8-sig")
        print(f"权益曲线已保存: {args.save_equity}")


if __name__ == "__main__":
    main()

# DB_HOST=138.197.75.51 DB_PORT=3307 python scripts/backtest_strategy_b_pool.py \
#   --start-date 2026-04-01 \
#   --capital 20000 \
#   --pool-from-ops

# DB_HOST=138.197.75.51 DB_PORT=3307 python scripts/backtest_strategy_b_pool.py \
#   --start-date 2026-04-01 \
#   --capital 20000 \
#   --symbols MXL,BTM,FCEL,CMPS,POET,BB,CAR,HPP,DAVE

# DB_HOST=138.197.75.51 DB_PORT=3307 python scripts/backtest_strategy_b_pool.py \
#   --start-date 2026-04-01 \
#   --capital 20000 \
#   --symbols MXL,BTM,FCEL,CMPS \
#   --save-trades outputs/trades.csv \
#   --save-equity outputs/equity.csv