#封控开关
# -*- coding: utf-8 -*-
"""
QQQ 总风控开关（简版）

规则：
- 今天 M3-M20 > 0
- 昨天 M3-M20 > 0
- 前天 M3-M20 > 0

满足 => 将 stock_operations 中
    stock_code='QQQ' AND stock_type='N'
的 entry_open 写为 1

否则写为 0
"""

import os
import pymysql

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
OPS_TABLE = os.getenv("OPS_TABLE", "stock_operations")

GATE_SYMBOL = "QQQ"
GATE_TYPE = "N"


def _connect():
    return pymysql.connect(**DB)


def _safe_float(x, default=0.0):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _avg(xs):
    return sum(xs) / float(len(xs)) if xs else 0.0


def _load_recent_closes(cur, symbol, limit=30):
    sql = f"""
    SELECT DATE(`date`) AS d, `close`
    FROM `{SRC_TABLE}`
    WHERE symbol=%s
    ORDER BY `date` DESC
    LIMIT %s;
    """
    cur.execute(sql, (symbol, int(limit)))
    return cur.fetchall() or []


def _calc_gate(rows):
    """
    rows: 最近日期在前（DESC）
    至少要 22 天，才能算：
    今天   M3/M20
    昨天   M3/M20
    前天   M3/M20
    """
    if len(rows) < 22:
        return 0, {}

    closes = [_safe_float(r.get("close")) for r in rows]

    m3_0 = _avg(closes[0:3])
    m20_0 = _avg(closes[0:20])
    diff0 = m3_0 - m20_0

    m3_1 = _avg(closes[1:4])
    m20_1 = _avg(closes[1:21])
    diff1 = m3_1 - m20_1

    m3_2 = _avg(closes[2:5])
    m20_2 = _avg(closes[2:22])
    diff2 = m3_2 - m20_2

    gate = 1 if (diff0 > 0 and diff1 > 0 and diff2 > 0) else 0

    info = {
        "last_date": rows[0].get("d"),
        "m3_0": m3_0, "m20_0": m20_0, "diff0": diff0,
        "m3_1": m3_1, "m20_1": m20_1, "diff1": diff1,
        "m3_2": m3_2, "m20_2": m20_2, "diff2": diff2,
    }
    return gate, info


def _ensure_gate_row(cur):
    """
    确保 stock_operations 里存在 QQQ + N 这条记录
    不存在就插入一条。

    注意：
    - QQQ/N 是系统风控开关，不允许参与任何买卖队列。
    - stock_operations 迁移到 UNIQUE(stock_code, stock_type) 后，QQQ/N 可以和 QQQ/C 共存。
    - 如果数据库还没迁移，ON DUPLICATE KEY 会把误写的 QQQ 行修回 N。
    """
    sql = f"""
    INSERT INTO `{OPS_TABLE}` (stock_code, stock_type, is_bought, can_buy, can_sell, entry_open)
    VALUES (%s, %s, 0, 0, 0, 0)
    ON DUPLICATE KEY UPDATE
        stock_type=%s,
        is_bought=0,
        can_buy=0,
        can_sell=0,
        last_order_side=NULL,
        last_order_id=NULL,
        last_order_time=NULL,
        last_order_intent='N:GATE control row';
    """
    cur.execute(sql, (GATE_SYMBOL, GATE_TYPE, GATE_TYPE))


def _write_gate(cur, gate):
    sql = f"""
    UPDATE `{OPS_TABLE}`
    SET entry_open=%s
    WHERE stock_code=%s AND stock_type=%s;
    """
    return cur.execute(sql, (int(gate), GATE_SYMBOL, GATE_TYPE))


def main():
    conn = _connect()
    try:
        with conn.cursor() as cur:
            _ensure_gate_row(cur)

            rows = _load_recent_closes(cur, GATE_SYMBOL, limit=30)
            gate, info = _calc_gate(rows)

            if not info:
                print(f"[WARN] {GATE_SYMBOL} 数据不足，entry_open=0", flush=True)
            else:
                print(
                    f"[INFO] {GATE_SYMBOL} last_date={info['last_date']} "
                    f"diff0={info['diff0']:.4f} diff1={info['diff1']:.4f} diff2={info['diff2']:.4f} "
                    f"-> gate={gate}",
                    flush=True,
                )

            affected = _write_gate(cur, gate)
            print(
                f"[OK] stock_operations: stock_code='{GATE_SYMBOL}', stock_type='{GATE_TYPE}', entry_open={gate}, rows={affected}",
                flush=True,
            )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
