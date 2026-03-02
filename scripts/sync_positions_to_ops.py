# -*- coding: utf-8 -*-
"""
sync_positions_to_ops.py

用 ALPACA_MODE=paper/live 拉 Alpaca 持仓，同步到 MySQL stock_operations
"""

import os
import pymysql
from alpaca.trading.client import TradingClient


# =========================
# Config
# =========================
OPS_TABLE = os.getenv("OPS_TABLE", "stock_operations")

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

ALPACA_MODE = (os.getenv("ALPACA_MODE") or "paper").strip().lower()
if ALPACA_MODE not in ("paper", "live"):
    raise RuntimeError(f"ALPACA_MODE must be paper/live, got {ALPACA_MODE}")

# 如果你已经在外部注入了 APCA_API_KEY_ID/APCA_API_SECRET_KEY，这里也兼容
def _alpaca_keys(mode: str):
    if mode == "paper":
        key = os.getenv("PAPER_APCA_API_KEY_ID") or os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_KEY") or ""
        sec = os.getenv("PAPER_APCA_API_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_SECRET") or ""
    else:
        key = os.getenv("LIVE_APCA_API_KEY_ID") or os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_KEY") or ""
        sec = os.getenv("LIVE_APCA_API_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_SECRET") or ""
    if not key or not sec:
        raise RuntimeError("Alpaca key/secret missing (paper/live). Check env vars.")
    return key, sec

# DB 里不存在时，插入用什么 stock_type（你可以改成 A/B/C/D/E 任意一个）
DEFAULT_STOCK_TYPE = (os.getenv("SYNC_DEFAULT_STOCK_TYPE") or "B").strip().upper()
if DEFAULT_STOCK_TYPE not in ("A", "B", "C", "D", "E"):
    DEFAULT_STOCK_TYPE = "B"


# =========================
# Helpers
# =========================
def get_conn():
    return pymysql.connect(**DB)

def get_trading_client():
    key, sec = _alpaca_keys(ALPACA_MODE)
    return TradingClient(key, sec, paper=(ALPACA_MODE == "paper"))

def f2(x, default=None):
    try:
        if x is None or str(x).strip() == "":
            return default
        return float(x)
    except Exception:
        return default


# =========================
# Main sync
# =========================
def sync_positions():
    tc = get_trading_client()
    positions = tc.get_all_positions() or []

    # Alpaca 当前持仓 symbol 集合
    alpaca_syms = set()

    # 1) UPSERT 每个 position
    upsert_sql = f"""
    INSERT INTO `{OPS_TABLE}` (
        stock_code, stock_type,
        qty, cost_price, close_price,
        is_bought, can_sell, can_buy,
        last_order_side, last_order_intent, last_order_time
    )
    VALUES (
        %s, %s,
        %s, %s, %s,
        1, 1, 0,
        'sync', %s, NOW()
    )
    ON DUPLICATE KEY UPDATE
        qty=VALUES(qty),
        cost_price=VALUES(cost_price),
        close_price=VALUES(close_price),
        is_bought=1,
        can_sell=1,
        can_buy=0,
        last_order_side='sync',
        last_order_intent=VALUES(last_order_intent),
        last_order_time=NOW(),
        updated_at=CURRENT_TIMESTAMP
    ;
    """

    # 2) 清仓：DB 里 is_bought=1 但 Alpaca 已经没有的
    #    注意：这里不动 trigger_price/weight/stock_type，只把持仓状态清掉
    def mark_flat(conn, keep_syms: set):
        if not keep_syms:
            # 如果 Alpaca 没有任何持仓：把 DB 所有 is_bought=1 都清掉
            sql = f"""
            UPDATE `{OPS_TABLE}`
            SET
                is_bought=0,
                qty=0,
                can_sell=0,
                can_buy=1,
                cost_price=NULL,
                close_price=NULL,
                stop_loss_price=NULL,
                take_profit_price=NULL,
                last_order_side='sync_flat',
                last_order_intent=%s,
                last_order_time=NOW(),
                updated_at=CURRENT_TIMESTAMP
            WHERE is_bought=1;
            """
            with conn.cursor() as cur:
                cur.execute(sql, (f"SYNC_FLAT:{ALPACA_MODE}",))
            return

        placeholders = ",".join(["%s"] * len(keep_syms))
        sql = f"""
        UPDATE `{OPS_TABLE}`
        SET
            is_bought=0,
            qty=0,
            can_sell=0,
            can_buy=1,
            cost_price=NULL,
            close_price=NULL,
            stop_loss_price=NULL,
            take_profit_price=NULL,
            last_order_side='sync_flat',
            last_order_intent=%s,
            last_order_time=NOW(),
            updated_at=CURRENT_TIMESTAMP
        WHERE is_bought=1
          AND stock_code NOT IN ({placeholders});
        """
        with conn.cursor() as cur:
            cur.execute(sql, tuple([f"SYNC_FLAT:{ALPACA_MODE}"] + list(keep_syms)))

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for p in positions:
                sym = (getattr(p, "symbol", None) or "").strip().upper()
                if not sym:
                    continue

                # Alpaca qty 可能是字符串/小数（fractional），你表是 int，所以取 floor(abs)
                q = f2(getattr(p, "qty", None), 0.0) or 0.0
                qty = int(abs(q))

                # avg_entry_price 成本价
                cost = f2(getattr(p, "avg_entry_price", None), None)

                # current_price 作为 close_price（你这里想放“当前价”更实用）
                cur_price = f2(getattr(p, "current_price", None), None)

                # 没有整数股就跳过（避免 fractional 导致 qty=0）
                if qty <= 0:
                    continue

                alpaca_syms.add(sym)

                intent = f"SYNC:{ALPACA_MODE} qty={qty}"
                cur.execute(
                    upsert_sql,
                    (
                        sym,
                        DEFAULT_STOCK_TYPE,
                        qty,
                        cost,
                        cur_price,
                        intent,
                    ),
                )

        # 清理 DB 中“已买入但 Alpaca 没有的”
        mark_flat(conn, alpaca_syms)

        print(f"[SYNC] mode={ALPACA_MODE} positions={len(alpaca_syms)} done.", flush=True)

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    sync_positions()