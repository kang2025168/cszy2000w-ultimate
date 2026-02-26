# -*- coding: utf-8 -*-
"""
极简版：
把 Alpaca 当前持仓同步到 stock_operations 表

同步规则：
- cost_price = avg_entry_price
- stop_loss_price = cost_price * 0.95
- is_bought = 1
- can_sell = 1
- qty = int(qty) 取整数部分
"""

import os
import math
import pymysql

# ====== 读取环境 ======
TRADE_ENV = (os.getenv("TRADE_ENV") or os.getenv("ALPACA_MODE") or "paper").strip().lower()
paper = (TRADE_ENV != "live")

APCA_API_KEY_ID = os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_KEY")
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_SECRET")

OPS_TABLE = os.getenv("OPS_TABLE", "stock_operations")

DB = dict(
    host=os.getenv("DB_HOST", "mysql"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "tradebot"),
    password=os.getenv("DB_PASS", "TradeBot#2026!"),
    database=os.getenv("DB_NAME", "cszy2000"),
    autocommit=True,
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

# ====== Alpaca 客户端 ======
from alpaca.trading.client import TradingClient

client = TradingClient(APCA_API_KEY_ID, APCA_API_SECRET_KEY, paper=paper)

positions = client.get_all_positions()

if not positions:
    print("没有持仓")
    exit()

conn = pymysql.connect(**DB)

for p in positions:
    symbol = p.symbol.strip().upper()
    qty = float(p.qty)
    avg_price = float(p.avg_entry_price)

    qty_int = int(math.floor(qty))
    if qty_int <= 0:
        continue

    cost_price = round(avg_price, 2)
    stop_loss = round(cost_price * 0.95, 2)

    sql = f"""
    UPDATE `{OPS_TABLE}`
    SET cost_price=%s,
        stop_loss_price=%s,
        qty=%s,
        is_bought=1,
        can_sell=1,
        updated_at=CURRENT_TIMESTAMP
    WHERE stock_code=%s;
    """

    with conn.cursor() as cur:
        cur.execute(sql, (cost_price, stop_loss, qty_int, symbol))

    print(f"已同步 {symbol}  qty={qty_int}  cost={cost_price}  sl={stop_loss}")

conn.close()
print("同步完成")
