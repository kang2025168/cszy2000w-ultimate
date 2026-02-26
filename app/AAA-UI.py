# ======================  仓位管理助手 v0.7.3 + Alpaca真实仓位同步 ==========================

"""
仓位管理助手（PyQt6）v0.7.3（优选股区域扩容 + 重影修复 + 分仓模式切换）
+ 新增：从 Alpaca 获取真实仓位，并按 stock_operations 的 A/B/C/D 分类自动填充四个桶

分类映射：
A -> 优选股
B -> 策略B
C -> 成长型
D -> 对冲
期权(Options) -> 默认放 对冲

依赖：
- PyQt6
- yfinance
- mysql-connector-python
- alpaca-py

环境变量（建议）：
- ALPACA_API_KEY
- ALPACA_SECRET_KEY
- ALPACA_PAPER   (1/0, true/false)
- ALPACA_BASE_URL (可选；alpaca-py 默认会按 paper/live 处理，你也可强制)

MySQL（按你项目记忆默认）：
host=localhost user=root password=mlp009988 database=cszy2000
表：stock_operations
你只需要保证 stock_operations 里能查到 “股票代码 -> 类型(A/B/C/D)” 的最新一条记录即可。
"""

# ====================== 仓位管理助手 ==========================

from __future__ import annotations   # ← 必须第一行（除注释）

from dataclasses import dataclass
from typing import Dict, Tuple, Optional
import os
import re

# ===== 加载 .env（必须放在普通 import 之后）=====
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(env_path)

import yfinance as yf

from PyQt6.QtCore import Qt, pyqtSignal, QTimer
from PyQt6.QtGui import QFont, QColor, QBrush
from PyQt6.QtWidgets import (
    QApplication, QWidget, QLabel, QComboBox, QDoubleSpinBox, QVBoxLayout, QHBoxLayout, QGridLayout,
    QGroupBox, QTableWidget, QTableWidgetItem, QHeaderView, QPushButton, QStackedWidget, QMessageBox
)


# ===================== MySQL 固定连接（本机连 Docker） =====================
DB_HOST = "127.0.0.1"
DB_PORT = 13307
DB_USER = "tradebot"
DB_PASS = "TradeBot#2026!"
DB_NAME = "cszy2000"

# stock_operations 字段名（如果你表里字段名不一样，改这里就行）
# 常见情况：stock_code / code；stock_type / type / category
COL_CODE_CANDIDATES = ["stock_code", "code", "symbol", "ticker"]
COL_TYPE_CANDIDATES = ["stock_type", "type", "category", "bucket_type"]
COL_ID_CANDIDATES = ["id", "op_id", "operation_id"]
COL_TIME_CANDIDATES = ["created_at", "time", "op_time", "trade_time", "ts"]


# ===================== Alpaca 环境变量 =====================
def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")


# ALPACA_MODE = (os.getenv("ALPACA_MODE") or "paper").lower()
#
# if ALPACA_MODE == "live":
#     ALPACA_API_KEY = os.getenv("LIVE_APCA_API_KEY_ID", "")
#     ALPACA_SECRET_KEY = os.getenv("LIVE_APCA_API_SECRET_KEY", "")
#     ALPACA_BASE_URL = os.getenv("LIVE_ALPACA_BASE_URL")
#     ALPACA_PAPER = False
# else:
#     ALPACA_API_KEY = os.getenv("PAPER_APCA_API_KEY_ID", "")
#     ALPACA_SECRET_KEY = os.getenv("PAPER_APCA_API_SECRET_KEY", "")
#     ALPACA_BASE_URL = os.getenv("PAPER_ALPACA_BASE_URL")
#     ALPACA_PAPER = True

# ===================== 强制使用 LIVE 账户 =====================
ALPACA_API_KEY = os.getenv("LIVE_APCA_API_KEY_ID", "")
ALPACA_SECRET_KEY = os.getenv("LIVE_APCA_API_SECRET_KEY", "")
ALPACA_BASE_URL = "https://api.alpaca.markets"
ALPACA_PAPER = False
ALPACA_MODE = "live"

# ===================== Alpaca client（alpaca-py） =====================
def get_alpaca_trading_client():
    """
    使用 alpaca-py 的 TradingClient.
    """
    try:
        from alpaca.trading.client import TradingClient
    except Exception as e:
        raise RuntimeError("未安装 alpaca-py：pip install alpaca-py") from e

    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        raise RuntimeError("缺少环境变量 ALPACA_API_KEY / ALPACA_SECRET_KEY")

    # alpaca-py：paper=True 会用 paper 环境
    # client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=ALPACA_PAPER)
    from alpaca.trading.client import TradingClient

    client = TradingClient(
        api_key=ALPACA_API_KEY,
        secret_key=ALPACA_SECRET_KEY,
        paper=ALPACA_PAPER
    )




    return client


# ===================== MySQL：读取 股票->类型(A/B/C/D) 映射 =====================
def mysql_connect():
    try:
        import mysql.connector
    except Exception as e:
        raise RuntimeError("未安装 mysql-connector-python") from e

    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        port=DB_PORT,
        autocommit=True,
    )


def _pick_existing_column(cursor, table: str, candidates: list[str]) -> Optional[str]:
    cursor.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
        """,
        (DB_NAME, table),
    )
    cols = {r[0] for r in cursor.fetchall()}
    for c in candidates:
        if c in cols:
            return c
    return None


def load_code_type_map_from_stock_operations() -> Dict[str, str]:
    """
    返回：{ 'AAPL': 'A', 'TSLA': 'B', ... }
    从 stock_operations 取每个 code 最新的一条 type 记录。
    """
    table = "stock_operations"
    conn = mysql_connect()
    try:
        cur = conn.cursor()

        code_col = _pick_existing_column(cur, table, COL_CODE_CANDIDATES)
        type_col = _pick_existing_column(cur, table, COL_TYPE_CANDIDATES)
        id_col = _pick_existing_column(cur, table, COL_ID_CANDIDATES)
        time_col = _pick_existing_column(cur, table, COL_TIME_CANDIDATES)

        if not code_col or not type_col:
            raise RuntimeError(
                f"stock_operations 找不到代码列/类型列。请确认字段名，并修改 COL_CODE_CANDIDATES / COL_TYPE_CANDIDATES。\n"
                f"当前识别到：code_col={code_col}, type_col={type_col}"
            )

        # 优先按时间列，其次按 id
        order_col = time_col or id_col or code_col

        # MySQL 8：窗口函数取最新
        sql = f"""
        WITH ranked AS (
            SELECT
                {code_col} AS code,
                {type_col} AS tp,
                ROW_NUMBER() OVER (PARTITION BY {code_col} ORDER BY {order_col} DESC) AS rn
            FROM {table}
            WHERE {code_col} IS NOT NULL AND {code_col} <> ''
              AND {type_col} IS NOT NULL AND {type_col} <> ''
        )
        SELECT code, tp
        FROM ranked
        WHERE rn = 1
        """
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        except Exception:
            # 兼容：如果窗口函数失败（极少数），用子查询 max(order_col)
            sql2 = f"""
            SELECT t1.{code_col} AS code, t1.{type_col} AS tp
            FROM {table} t1
            JOIN (
                SELECT {code_col} AS code, MAX({order_col}) AS mx
                FROM {table}
                WHERE {code_col} IS NOT NULL AND {code_col} <> ''
                  AND {type_col} IS NOT NULL AND {type_col} <> ''
                GROUP BY {code_col}
            ) t2
            ON t1.{code_col} = t2.code AND t1.{order_col} = t2.mx
            """
            cur.execute(sql2)
            rows = cur.fetchall()

        mp: Dict[str, str] = {}
        for code, tp in rows:
            if not code:
                continue
            c = str(code).strip().upper()
            t = str(tp).strip().upper()
            if t in ("A", "B", "C", "D"):
                mp[c] = t
        return mp
    finally:
        try:
            conn.close()
        except Exception:
            pass


def map_type_to_bucket(tp: str) -> str:
    """
    A -> 优选股
    B -> 策略B
    C -> 成长型
    D -> 对冲
    """
    tp = (tp or "").strip().upper()
    return {
        "A": "优选股",
        "B": "策略B",
        "C": "成长型",
        "D": "对冲",
    }.get(tp, "优选股")  # 默认兜底：优选股


# ===================== Option 符号解析（OCC格式） =====================
def parse_occ_option_symbol(sym: str) -> Optional[dict]:
    """
    解析 OCC: e.g. AAPL240621C00150000
    返回：{underlying, expiry(YYYY-MM-DD), cp, strike(float)}
    """
    s = (sym or "").strip().upper()
    m = re.fullmatch(r"([A-Z]{1,6})(\d{2})(\d{2})(\d{2})([CP])(\d{8})", s)
    if not m:
        return None
    underlying = m.group(1)
    yy, mm, dd = m.group(2), m.group(3), m.group(4)
    cp = m.group(5)
    strike_raw = int(m.group(6))
    strike = strike_raw / 1000.0
    expiry = f"20{yy}-{mm}-{dd}"
    return {"underlying": underlying, "expiry": expiry, "cp": cp, "strike": strike}


def format_hedge_code(underlying: str, strike: float, cp: str) -> str:
    """
    你原来对冲代码像：QQQ-603C
    这里做一个稳定格式：
    - strike 60.3 -> 603
    - strike 105 -> 1050 (不含小数会变 1050) —— 这可能和你习惯不同，但至少可逆/一致
    你如果想保持“整数不乘10”，我也能按你的规则改。
    """
    # 用 1 位小数精度转成“去点”
    s1 = f"{strike:.1f}"
    num = s1.replace(".", "")
    return f"{underlying}-{num}{cp}"


# ===================== 你原来的仓位推荐逻辑（不改） =====================
@dataclass
class PositionPlan:
    total_exposure: float
    bucket_weights: Dict[str, float]
    warnings: list[str]


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def base_exposure_by_trend(trend: str) -> float:
    if trend == "向上":
        return 0.8
    elif trend == "横盘":
        return 0.5
    else:
        return 0.2


def split_by_trend(trend: str) -> Dict[str, float]:
    if trend == "向上":
        stable, growth, aggressive = 0.35, 0.4, 0.25
    elif trend == "横盘":
        stable, growth, aggressive = 0.5, 0.35, 0.15
    else:
        stable, growth, aggressive = 0.7, 0.25, 0.05

    hedge = stable * 0.4
    strategy_b = stable * 0.6
    growth_weight = growth * 0.7
    selected = growth * 0.3 + aggressive
    return {
        "对冲": hedge,
        "策略B": strategy_b,
        "成长型": growth_weight,
        "优选股": selected,
    }


def tilt_by_risk(weights: Dict[str, float], risk: str) -> Dict[str, float]:
    """
    风险偏好微调：
    - 对冲当现金池：不参与风险挪动（保持你设定的 10% 或动态结果里的对冲比例）
    - 保守：优选股 -> 成长型（底仓更稳）
    - 激进：成长型 -> 优选股（更激进）
    """
    w = weights.copy()
    shift = 0.05  # 建议 0.03~0.08；0.1 太猛，会把 20% 直接砍成 10%

    if risk == "保守":
        delta = min(shift, w.get("优选股", 0.0))
        w["优选股"] -= delta
        w["成长型"] += delta

    elif risk == "激进":
        delta = min(shift, w.get("成长型", 0.0))
        w["成长型"] -= delta
        w["优选股"] += delta

    # 归一化，防止小数误差
    s = sum(w.values())
    if s > 0:
        for k in w:
            w[k] /= s

    return w
BASE_BUCKET_WEIGHTS = {
    "对冲": 0.10,
    "优选股": 0.20,
    "策略B": 0.30,
    "成长型": 0.40,
}

def normalize_weights(w: Dict[str, float]) -> Dict[str, float]:
    s = sum(w.values())
    if s <= 0:
        return BASE_BUCKET_WEIGHTS.copy()
    return {k: v / s for k, v in w.items()}


def dynamic_weights_from_base(trend: str, vix: float) -> Dict[str, float]:
    """
    动态分仓：以 BASE_BUCKET_WEIGHTS 为基准，然后按规则微调（不改变你“基准比例”的思想）
    三档：
    - 强势（trend=向上 且 vix<18）：成长 +5%，对冲 -5%
    - 震荡（trend=横盘 或 vix 18~25）：优选 -5%，对冲 +5%
    - 高风险（trend=向下 或 vix>28）：成长 -10%，策略B -5%，优选 -10%，对冲 +25%
    """
    w = BASE_BUCKET_WEIGHTS.copy()

    # 高风险（优先级最高）
    if vix > 28 or trend == "向下":
        w["成长型"] -= 0.10
        w["策略B"] -= 0.05
        w["优选股"] -= 0.10
        w["对冲"]   += 0.25
        return normalize_weights(w)

    # 震荡
    if trend == "横盘" or (18 <= vix <= 25):
        w["优选股"] -= 0.05
        w["对冲"]   += 0.05
        return normalize_weights(w)

    # 强势
    if trend == "向上" and vix < 18:
        w["成长型"] += 0.05
        w["对冲"]   -= 0.05
        return normalize_weights(w)

    return normalize_weights(w)

def recommend_position(trend: str, idx_chg_pct: float, vix: float, risk: str, mode: str) -> PositionPlan:
    total = base_exposure_by_trend(trend)
    total += clamp(idx_chg_pct / 20.0, -0.1, 0.1)

    if vix <= 14:
        total += 0.1
    elif vix <= 20:
        total += 0
    elif vix <= 28:
        total -= 0.1
    else:
        total -= 0.2

    if risk == "保守":
        total -= 0.1
    elif risk == "激进":
        total += 0.1

    total = clamp(total, 0, 1)

    if mode == "平均分仓":
        base_weights = BASE_BUCKET_WEIGHTS.copy()
    else:
        base_weights = dynamic_weights_from_base(trend, vix)

    weights = tilt_by_risk(base_weights, risk)

    warns = []
    if vix > 28:
        warns.append("VIX 高于28，系统风险较高。")

    return PositionPlan(total, weights, warns)


# ===================== 表格控件（加了 set_rows_data / clear_rows） =====================
class EditableStockTable(QTableWidget):
    table_changed = pyqtSignal()

    def __init__(self, title: str):
        row_count = 10
        super().__init__(row_count, 8)

        self.title = title
        if self.title == "对冲":
            self.setHorizontalHeaderLabels(
                ["期权代码", "成本价", "现价", "止损价", "到期日期", "数量", "盈亏%", "总市值"])
        else:
            self.setHorizontalHeaderLabels(
                ["股票代码", "成本价", "现价", "止损价", "买入日期", "数量", "盈亏%", "总市值"])

        self.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.verticalHeader().setVisible(False)
        self.cellChanged.connect(self.on_cell_changed)

        # 从文件加载数据
        self.load_from_file()

    def clear_rows(self):
        # 临时断开信号，避免 setItem 触发反复保存/计算
        self.blockSignals(True)
        try:
            for r in range(self.rowCount()):
                for c in range(self.columnCount()):
                    self.setItem(r, c, QTableWidgetItem(""))
        finally:
            self.blockSignals(False)

    def set_rows_data(self, rows: list[dict]):
        """
        rows: [{'code','cost','current','date','qty'}]
        """
        self.blockSignals(True)
        try:
            self.clear_rows()
            for r, row in enumerate(rows[: self.rowCount()]):
                self.setItem(r, 0, QTableWidgetItem(str(row.get("code", "")).strip()))
                self.setItem(r, 1, QTableWidgetItem(str(row.get("cost", "")).strip()))
                self.setItem(r, 2, QTableWidgetItem(str(row.get("current", "")).strip()))
                self.setItem(r, 3, QTableWidgetItem(""))  # 止损由 on_cell_changed 计算
                self.setItem(r, 4, QTableWidgetItem(str(row.get("date", "")).strip()))
                self.setItem(r, 5, QTableWidgetItem(str(row.get("qty", "")).strip()))
        finally:
            self.blockSignals(False)

        # 逐行触发计算（不然盈亏/止损/市值不会刷新）
        for r in range(self.rowCount()):
            self.on_cell_changed(r, 2)

        # 保存
        try:
            self.save_to_file()
        except Exception:
            pass

        self.table_changed.emit()

    def load_from_file(self):
        import csv
        filename = "positions.csv"
        try:
            with open(filename, newline='', encoding='utf-8') as f:
                rdr = csv.DictReader(f)
                rows = list(rdr)

            filtered = []
            for r in rows:
                cat = (r.get('category') or "").strip()
                if self.title == "策略B":
                    if cat in ("策略B", "市场ETF"):
                        filtered.append(r)
                else:
                    if cat == self.title:
                        filtered.append(r)

            for r, row in enumerate(filtered):
                self.setItem(r, 0, QTableWidgetItem(row.get('code', '').strip()))
                self.setItem(r, 1, QTableWidgetItem(row.get('cost', '').strip()))
                self.setItem(r, 2, QTableWidgetItem(row.get('current', '').strip()))
                self.setItem(r, 3, QTableWidgetItem(""))
                self.setItem(r, 4, QTableWidgetItem(row.get('date', '').strip()))
                self.setItem(r, 5, QTableWidgetItem(row.get('qty', '').strip()))
                self.on_cell_changed(r, 2)
        except Exception:
            pass

    def update_prices(self):
        import re
        for r in range(self.rowCount()):
            code_item = self.item(r, 0)
            qty_item = self.item(r, 5)
            date_item = self.item(r, 4)
            if not code_item or not code_item.text():
                continue

            code_text = code_item.text().strip()
            qty = int(float(qty_item.text())) if qty_item and qty_item.text() else 0
            _ = qty

            expiry = date_item.text().strip() if date_item else ""

            if self.title == "对冲":
                m = re.fullmatch(r"([A-Z]+)-(\d+)([CP])", code_text)
                if not m:
                    continue
                underlying = m.group(1)
                strike_raw = m.group(2)  # 例如 603
                cp = m.group(3)
                try:
                    # strike: 603 -> 60.3（按 format_hedge_code 的规则）
                    strike = float(strike_raw) / 10.0
                    chain = yf.Ticker(underlying).option_chain(expiry)
                    table = chain.calls if cp == "C" else chain.puts
                    row = table[abs(table["strike"] - strike) < 1e-9]
                    if not row.empty:
                        price = float(row["lastPrice"].iloc[0])
                        self.setItem(r, 2, QTableWidgetItem(f"{price:.2f}"))
                        self.on_cell_changed(r, 2)
                    continue
                except Exception:
                    continue

            try:
                price = yf.Ticker(code_text).history(period="1d")["Close"].iloc[-1]
                self.setItem(r, 2, QTableWidgetItem(f"{price:.2f}"))
                self.on_cell_changed(r, 2)
            except Exception:
                continue

    def on_cell_changed(self, row, col):
        if col not in [1, 2, 4]:
            return

        try:
            cost = float(self.item(row, 1).text()) if self.item(row, 1) and self.item(row, 1).text() else 0.0
            cur = float(self.item(row, 2).text()) if self.item(row, 2) and self.item(row, 2).text() else 0.0
            qty = int(float(self.item(row, 5).text())) if self.item(row, 5) and self.item(row, 5).text() else 0

            if cost > 0:
                change = (cur - cost) / cost * 100
                itm = QTableWidgetItem(f"{change:.2f}%")
                itm.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                itm.setForeground(QBrush(QColor("green" if change >= 0 else "red")))
                self.setItem(row, 6, itm)

            stop_price = cost * (0.80 if self.title == "对冲" else 0.98)
            stop_item = QTableWidgetItem(f"{stop_price:.2f}")
            stop_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            stop_item.setForeground(QBrush(QColor("orange")))
            self.setItem(row, 3, stop_item)

            total_val = cur * qty
            val_item = QTableWidgetItem(f"$ {total_val:,.2f}")
            val_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.setItem(row, 7, val_item)

            self.table_changed.emit()

            try:
                self.save_to_file()
            except Exception:
                pass
        except Exception:
            pass

    def save_to_file(self):
        import csv
        filename = "positions.csv"
        try:
            with open(filename, newline='', encoding='utf-8') as f:
                rows = list(csv.DictReader(f))
        except Exception:
            rows = []

        if self.title == "策略B":
            rows = [r for r in rows if (r.get('category') or "").strip() not in ("策略B", "市场ETF")]
        else:
            rows = [r for r in rows if (r.get('category') or "").strip() != self.title]

        for r in range(self.rowCount()):
            code = self.item(r, 0).text().strip() if self.item(r, 0) and self.item(r, 0).text() else ""
            cost = self.item(r, 1).text().strip() if self.item(r, 1) and self.item(r, 1).text() else ""
            cur = self.item(r, 2).text().strip() if self.item(r, 2) and self.item(r, 2).text() else ""
            date = self.item(r, 4).text().strip() if self.item(r, 4) and self.item(r, 4).text() else ""
            qty = self.item(r, 5).text().strip() if self.item(r, 5) and self.item(r, 5).text() else ""
            if code:
                rows.append({
                    'category': self.title,
                    'code': code,
                    'cost': cost,
                    'current': cur,
                    'date': date,
                    'qty': qty
                })

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['category', 'code', 'cost', 'current', 'date', 'qty'])
            writer.writeheader()
            writer.writerows(rows)

    def calc_summary(self) -> Tuple[float, float, float, str, float]:
        total_val = 0.0
        profit = []
        max_price = 0.0
        top_stock = ""
        qty_sum = 0

        for r in range(self.rowCount()):
            try:
                code = self.item(r, 0).text().strip() if self.item(r, 0) and self.item(r, 0).text() else ""
                cur = float(self.item(r, 2).text()) if self.item(r, 2) and self.item(r, 2).text() else 0.0
                qty = int(float(self.item(r, 5).text())) if self.item(r, 5) and self.item(r, 5).text() else 0

                if qty > 0:
                    qty_sum += qty
                    total_val += cur * qty

                if cur > max_price and code:
                    max_price = cur
                    top_stock = code

                pct_item = self.item(r, 6)
                if pct_item and pct_item.text():
                    pct = float(pct_item.text().replace("%", ""))
                    profit.append(pct)
            except Exception:
                continue

        avg = sum(profit) / len(profit) if profit else 0.0
        return total_val, avg, max_price, top_stock, float(qty_sum)


# ===================== 主程序 UI（加了同步按钮和同步逻辑） =====================
class PositionSizerApp(QWidget):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("仓位管理助手 v0.7.3")
        self.setMinimumWidth(1200)

        self.buying_power = 0.0
        self.goal_amount = 100000

        self._init_ui()

        self.timer = QTimer()
        self.timer.timeout.connect(self.refresh_prices)
        self.timer.start(10000)

    def set_active_cat(self, cat: str):
        if cat not in self.cat_index:
            return
        idx = self.cat_index[cat]
        self.stack.setCurrentIndex(idx)

        for k, btn in self.cat_buttons.items():
            if k == cat:
                btn.setStyleSheet("font-weight:bold; padding:6px;")
            else:
                btn.setStyleSheet("font-weight:normal; padding:6px;")

    def refresh_prices(self):
        try:
            qqq_hist = yf.Ticker("QQQ").history(period="2d")["Close"]
            if len(qqq_hist) >= 2:
                qqq_prev = float(qqq_hist.iloc[0])
                qqq_last = float(qqq_hist.iloc[-1])
                idx_chg = (qqq_last - qqq_prev) / qqq_prev * 100
                self.idx_chg.setValue(idx_chg)
        except Exception:
            pass

        try:
            vix_val = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
            self.vix.setValue(float(vix_val))
        except Exception:
            pass

        self.on_calculate()
        for tbl in self.tables.values():
            tbl.update_prices()
        self.on_calculate()

    # ----------------- 新增：同步 Alpaca 真实仓位 -----------------
    def sync_from_alpaca(self):
        try:
            client = get_alpaca_trading_client()
        except Exception as e:
            QMessageBox.critical(self, "Alpaca 初始化失败", str(e))
            return

        try:
            # 1) cash / buying power
            acct = client.get_account()
            # alpaca-py Account 里字段很多，这里优先拿 cash，其次 buying_power
            cash = None
            for k in ("cash", "buying_power"):
                if hasattr(acct, k):
                    cash = float(getattr(acct, k))
                    break
            if cash is None:
                cash = 0.0
            self.buying_power = cash

            # 2) positions
            positions = client.get_all_positions()

            # 3) 从 MySQL 拉最新 A/B/C/D 映射
            code_type_map = load_code_type_map_from_stock_operations()

            # 分桶容器
            buckets_rows: Dict[str, list[dict]] = {"对冲": [], "优选股": [], "策略B": [], "成长型": []}

            for p in positions:
                sym = str(getattr(p, "symbol", "")).strip().upper()
                if not sym:
                    continue

                qty = int(float(getattr(p, "qty", 0) or 0))
                avg_entry = float(getattr(p, "avg_entry_price", 0) or 0)
                cur_price = float(getattr(p, "current_price", 0) or 0)

                # 期权：asset_class 可能为 'us_option' / 'option'，也可能没有
                asset_class = ""
                if hasattr(p, "asset_class") and getattr(p, "asset_class") is not None:
                    asset_class = str(getattr(p, "asset_class")).lower()

                is_option = ("option" in asset_class) or (parse_occ_option_symbol(sym) is not None)

                if is_option:
                    info = parse_occ_option_symbol(sym)
                    if info:
                        code_show = format_hedge_code(info["underlying"], info["strike"], info["cp"])
                        date_show = info["expiry"]
                    else:
                        code_show = sym
                        date_show = ""
                    buckets_rows["对冲"].append({
                        "code": code_show,
                        "cost": f"{avg_entry:.2f}",
                        "current": f"{cur_price:.2f}",
                        "date": date_show,
                        "qty": str(qty),
                    })
                else:
                    tp = code_type_map.get(sym, "A")
                    bucket = map_type_to_bucket(tp)
                    buckets_rows[bucket].append({
                        "code": sym,
                        "cost": f"{avg_entry:.2f}",
                        "current": f"{cur_price:.2f}",
                        "date": "",
                        "qty": str(qty),
                    })

            # 4) 写入四张表（先清空）
            # 对冲/策略B/成长型 只有 3 行：如果超过 3，会截断；优选股 6 行
            for cat in ("对冲", "优选股", "策略B", "成长型"):
                self.tables[cat].set_rows_data(buckets_rows[cat])

            # 5) 更新显示/重新计算
            self.update_buying_power_display_only()
            self.on_calculate()

            QMessageBox.information(self, "同步完成", "已从 Alpaca 同步真实仓位，并按 A/B/C/D 分类填入四个桶。")
        except Exception as e:
            QMessageBox.critical(self, "同步失败", f"{e}")

    def update_buying_power_display_only(self):
        total_val = sum(tbl.calc_summary()[0] for tbl in self.tables.values())
        account_total = total_val + self.buying_power
        self.stock_value_display.setText(f"$ {total_val:,.2f}")
        self.capital_display.setText(f"$ {account_total:,.2f}")
        self.buying_power_label.setText(f"$ {self.buying_power:,.2f}")

    # ------------------------------------------------------

    def _init_ui(self):
        title = QLabel("仓位管理助手 v0.7.3")
        title.setFont(QFont("Arial", 18, QFont.Weight.Bold))

        motto = QLabel(
            "1.亏损会让人痛苦，把握确定性，不要赌。活得久，才是赢家。\n"
            "2. 风险不是消失，而是转嫁，财富不是增加，而是重新分配。\n"
            "3. 顺势做多，逆势做空。\n"
            "4.把钱分为4份，一份做期权，一份执行方法1，\n一份执行方法2，一份执行长期看好的板块。\n"
            "5.机会永远是千载难逢的，要学会识别机会，要把握机会。\n"
            "6.对冲只用来在危险来临之时，锁住收益（保险的作用）。"
        )
        motto.setFont(QFont("Arial", 14))

        header = QWidget()
        header_layout = QVBoxLayout(header)
        header_layout.addWidget(title)
        header_layout.addWidget(motto)

        form = QGridLayout()
        row = 0

        self.trend = QComboBox()
        self.trend.addItems(["向上", "横盘", "向下"])

        self.idx_chg = QDoubleSpinBox()
        self.idx_chg.setRange(-10, 10)
        self.idx_chg.setDecimals(2)
        self.idx_chg.setSuffix(" %")

        self.vix = QDoubleSpinBox()
        self.vix.setRange(5, 80)
        self.vix.setValue(18)

        self.risk = QComboBox()
        self.risk.addItems(["保守", "中性", "激进"])

        self.alloc_mode = QComboBox()
        self.alloc_mode.addItems(["动态分仓", "平均分仓"])

        self.buying_power_label = QLabel("$0.00")
        self.capital_adjust = QDoubleSpinBox()
        self.capital_adjust.setRange(-1000000, 1000000)
        self.capital_adjust.setPrefix("$ ")
        self.capital_adjust.setDecimals(2)

        self.calc_button = QPushButton("计算")
        self.calc_button.clicked.connect(self.update_buying_power)

        # 新增：同步按钮
        self.sync_button = QPushButton("同步Alpaca真实仓位")
        self.sync_button.clicked.connect(self.sync_from_alpaca)

        self.capital_display = QLabel("$0.00")

        form.addWidget(QLabel("市场趋势"), row, 0)
        form.addWidget(self.trend, row, 1)
        row += 1

        form.addWidget(QLabel("QQQ涨跌幅"), row, 0)
        form.addWidget(self.idx_chg, row, 1)
        row += 1

        form.addWidget(QLabel("VIX"), row, 0)
        form.addWidget(self.vix, row, 1)
        row += 1

        form.addWidget(QLabel("风险承受度"), row, 0)
        form.addWidget(self.risk, row, 1)
        row += 1

        form.addWidget(QLabel("分仓模式"), row, 0)
        form.addWidget(self.alloc_mode, row, 1)
        row += 1

        form.addWidget(QLabel("剩余购买力"), row, 0)
        form.addWidget(self.buying_power_label, row, 1)
        row += 1

        form.addWidget(QLabel("资金调整"), row, 0)
        form.addWidget(self.capital_adjust, row, 1)
        form.addWidget(self.calc_button, row, 2)
        row += 1

        # 同步按钮放在参数区下面
        form.addWidget(QLabel(""), row, 0)
        form.addWidget(self.sync_button, row, 1)
        row += 1

        form.addWidget(QLabel("股票市值"), row, 0)
        self.stock_value_display = QLabel("$0.00")
        form.addWidget(self.stock_value_display, row, 1)
        row += 1

        form.addWidget(QLabel("账户资金"), row, 0)
        form.addWidget(self.capital_display, row, 1)

        param_box = QGroupBox("输入参数")
        param_box.setLayout(form)

        self.result_total = QLabel("建议总仓位：--")
        self.result_total.setFont(QFont("Arial", 14, QFont.Weight.Bold))

        self.table = QTableWidget(4, 3)
        self.table.setHorizontalHeaderLabels(["类型", "权重", "建议金额"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)

        suggest = QWidget()
        suggest_layout = QVBoxLayout(suggest)
        suggest_layout.addWidget(self.result_total)
        suggest_layout.addWidget(self.table)

        self.tables = {}
        self.summaries = {}
        self.tips = {}

        buckets = ["对冲", "优选股", "策略B", "成长型"]

        btn_bar = QWidget()
        btn_layout = QHBoxLayout(btn_bar)
        btn_layout.setContentsMargins(0, 0, 0, 0)
        btn_layout.setSpacing(6)

        self.cat_buttons = {}
        for cat in buckets:
            b = QPushButton(cat)
            b.setCursor(Qt.CursorShape.PointingHandCursor)
            b.clicked.connect(lambda _, c=cat: self.set_active_cat(c))
            self.cat_buttons[cat] = b
            btn_layout.addWidget(b)
        btn_layout.addStretch()

        self.stack = QStackedWidget()
        self.cat_index = {}

        for idx, cat in enumerate(buckets):
            tbl = EditableStockTable(cat)

            row_h = tbl.verticalHeader().defaultSectionSize()
            header_h = tbl.horizontalHeader().height()
            margin = 12
            tbl.setFixedHeight(row_h * 10 + header_h + margin)

            tbl.table_changed.connect(self.on_calculate)
            self.tables[cat] = tbl

            label = QLabel(f"{cat}持仓表")
            label.setFont(QFont("Arial", 12, QFont.Weight.Bold))

            summary = QLabel("合计金额：$0.00｜平均盈亏：0.00%")
            tip = QLabel("")
            tip.setWordWrap(True)

            self.summaries[cat] = summary
            self.tips[cat] = tip

            page = QWidget()
            page_layout = QVBoxLayout(page)
            page_layout.setContentsMargins(0, 4, 0, 4)
            page_layout.setSpacing(2)
            page_layout.addWidget(label)
            page_layout.addWidget(tbl)
            page_layout.addWidget(summary)
            page_layout.addWidget(tip)
            page_layout.addStretch()

            self.stack.addWidget(page)
            self.cat_index[cat] = idx

        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(8)
        right_layout.addWidget(btn_bar)
        right_layout.addWidget(self.stack)

        self.set_active_cat("对冲")

        self.total_info = QLabel("实时账户总市值：$0.00｜与建议差距：0.00%")
        self.total_info.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.total_info.setStyleSheet("background:#f2f2f2; padding:6px; font-weight:bold;")

        grid = QGridLayout()
        grid.addWidget(header, 0, 0)
        grid.addWidget(right_widget, 0, 1, 3, 1)
        grid.addWidget(param_box, 1, 0)
        grid.addWidget(suggest, 2, 0)
        grid.addWidget(self.total_info, 3, 0, 1, 2)

        grid.setColumnStretch(0, 3)
        grid.setColumnStretch(1, 7)

        self.setLayout(grid)
        self.on_calculate()

    def update_buying_power(self):
        adjust = self.capital_adjust.value()
        self.buying_power += adjust
        self.capital_adjust.setValue(0)

        total_val = sum(tbl.calc_summary()[0] for tbl in self.tables.values())
        account_total = total_val + self.buying_power

        self.stock_value_display.setText(f"$ {total_val:,.2f}")
        self.capital_display.setText(f"$ {account_total:,.2f}")
        self.buying_power_label.setText(f"$ {self.buying_power:,.2f}")

        self.on_calculate()

    def on_calculate(self):
        total_val = sum(tbl.calc_summary()[0] for tbl in self.tables.values())
        account_total = total_val + self.buying_power

        self.stock_value_display.setText(f"$ {total_val:,.2f}")
        self.capital_display.setText(f"$ {account_total:,.2f}")
        self.buying_power_label.setText(f"$ {self.buying_power:,.2f}")

        plan = recommend_position(
            self.trend.currentText(),
            self.idx_chg.value(),
            self.vix.value(),
            self.risk.currentText(),
            self.alloc_mode.currentText()
        )

        total = plan.total_exposure
        cap = account_total

        self.result_total.setText(f"建议总仓位：{total * 100:.1f}%（≈ $ {cap * total:,.2f}）")

        buckets = ["对冲", "优选股", "策略B", "成长型"]
        real_total = 0.0
        actual_vals = {}

        # 左侧建议表 + 统计各桶实际值
        for i, b in enumerate(buckets):
            w = plan.bucket_weights[b]
            sugg_amt = cap * total * w

            self.table.setItem(i, 0, QTableWidgetItem(b))
            self.table.setItem(i, 1, QTableWidgetItem(f"{w * 100:.1f}%"))
            self.table.setItem(i, 2, QTableWidgetItem(f"$ {sugg_amt:,.2f}"))

            t_val, avg, max_p, max_code, qty = self.tables[b].calc_summary()
            actual_vals[b] = (t_val, avg, max_p, max_code, qty, sugg_amt)
            real_total += t_val

        # 更新每个类别 summary + tip
        for cat, (val, avg, maxp, top, qty, sugg) in actual_vals.items():
            diff = val - sugg
            diff_pct = diff / sugg * 100 if sugg > 0 else 0.0

            summary = self.summaries[cat]
            tip = self.tips[cat]

            color = "green" if avg >= 0 else "red"
            summary.setText(f"合计金额：$ {val:,.2f}｜平均盈亏：<font color='{color}'>{avg:.2f}%</font>")

            # 1) 止损判断（现价 < 止损价）
            stop_hit = False
            for r in range(self.tables[cat].rowCount()):
                cur_item = self.tables[cat].item(r, 2)
                stop_item = self.tables[cat].item(r, 3)
                code_item = self.tables[cat].item(r, 0)

                if cur_item and stop_item and code_item:
                    try:
                        cur_p = float(cur_item.text())
                        stop_p = float(stop_item.text())
                        code_name = code_item.text().strip()

                        if code_name and cur_p < stop_p:
                            stop_hit = True
                            tip.setText(
                                f"<font color='red'>⚠️ 触发止损：{code_name} 当前价格 {cur_p:.2f} 跌破止损价 {stop_p:.2f}，建议立即减仓。</font>"
                            )
                            break
                    except Exception:
                        pass

            # 2) 若未触发止损，给超配/低配建议
            if not stop_hit:
                # ===== 对冲桶按“现金预算”处理：不提示买卖股数 =====
                if cat == "对冲":
                    # sugg 是你建议预留的现金预算；val 是对冲表实际持仓市值（一般为0）
                    gap = sugg - val
                    if gap > 1:
                        tip.setText(
                            f"<font color='green'>💡 现金预算建议：$ {sugg:,.2f}（当前对冲持仓 $ {val:,.2f}），建议保留现金 $ {gap:,.2f}</font>"
                        )
                    else:
                        tip.setText("")
                    continue
                if abs(diff) > 1:
                    # 用该桶内“价格最高的标的”来估算股数（你原逻辑）
                    denom = maxp if maxp and maxp > 0 else 1.0
                    shares = int(abs(diff) / denom)

                    if diff > 0:
                        tip.setText(
                            f"<font color='red'>⚠️ 超出建议 {diff_pct:.1f}%，建议减仓 {top} $ {abs(diff):,.2f}（约 {shares} 股）</font>"
                        )
                    else:
                        tip.setText(
                            f"<font color='green'>💡 低于建议 {abs(diff_pct):.1f}%，建议加仓 {top} $ {abs(diff):,.2f}（约 {shares} 股）</font>"
                        )
                else:
                    tip.setText("")

        # 3) 底部总览：实际持仓 vs 建议总仓位金额
        target_total_amt = cap * total
        diff_all = real_total - target_total_amt
        diff_pct_all = diff_all / target_total_amt * 100 if target_total_amt > 0 else 0.0
        self.total_info.setText(f"实时账户总市值：$ {real_total:,.2f}｜与建议差距：{diff_pct_all:+.2f}%")

if __name__ == "__main__":
    import sys

    app = QApplication(sys.argv)
    w = PositionSizerApp()
    w.show()
    sys.exit(app.exec())
