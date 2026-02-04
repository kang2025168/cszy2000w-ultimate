# -*- coding: utf-8 -*-
import os
import pymysql
import pandas as pd
import numpy as np

import warnings
warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable",
    category=UserWarning,
)

# -------------------- DB config (env first, fallback to local) --------------------
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return (v if v is not None else default).strip()

DB_CONFIG = {
    "host": _env("DB_HOST", "127.0.0.1"),
    "port": int(_env("DB_PORT", "3306") or "3306"),
    "user": _env("DB_USER", "root"),
    "password": _env("DB_PASS", "mlp009988"),
    "database": _env("DB_NAME", "cszy2000"),
}

def get_conn():
    return pymysql.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        charset="utf8mb4",
        autocommit=True,
        # ✅ 关键：不要 DictCursor，pandas 更稳定
        cursorclass=pymysql.cursors.Cursor,
    )
# -------------------- weights --------------------
def _safe_norm(w):
    w = np.array(w, dtype=float)
    w[w < 0] = 0
    s = w.sum()
    if s <= 0:
        return np.ones_like(w) / len(w)
    return w / s

def _zscore(x):
    x = np.array(x, dtype=float)
    mu = np.nanmean(x)
    sd = np.nanstd(x)
    if not np.isfinite(sd) or sd == 0:
        return np.zeros_like(x)
    return (x - mu) / sd

def compute_weights(
    top_df: pd.DataFrame,
    mode: str = "equal",
    min_w: float = 0.10,
    max_w: float = 0.60,
):
    """
    mode:
      - equal: equal weight
      - hybrid: higher score & volume, lower volatility => higher weight
    """
    k = len(top_df)
    if k == 0:
        return np.array([])

    if mode == "equal":
        return np.ones(k) / k

    c = top_df["close"].astype(float).values
    h = top_df["high"].astype(float).values
    l = top_df["low"].astype(float).values
    v = top_df["volume"].astype(float).values
    score = top_df["score"].astype(float).values

    tr1 = np.abs(h - l)
    prev_c = np.r_[c[0], c[:-1]]
    tr2 = np.abs(h - prev_c)
    tr3 = np.abs(l - prev_c)
    tr = np.nanmax(np.vstack([tr1, tr2, tr3]), axis=0)
    atr14 = pd.Series(tr).rolling(14, min_periods=1).mean().values
    atr_pct = np.clip(atr14 / np.maximum(c, 1e-9), 1e-6, None)

    vol5 = pd.Series(v).rolling(5, min_periods=1).mean().values
    vol_ratio = np.clip(v / np.maximum(vol5, 1e-9), 1e-3, 5.0)

    score_z = _zscore(score) + 1.0
    w_raw = score_z * vol_ratio / atr_pct
    w_raw = np.clip(w_raw, 0, None)

    w = _safe_norm(w_raw)
    w = np.clip(w, min_w, max_w)
    w = w / w.sum()
    return w

# -------------------- pick logic --------------------
def pick_topk_strategy_a(
    pick_date: str | None = None,     # None = latest trading day in DB
    price_low: float = 1.0,
    price_high: float = 10.0,
    min_week_vol: int = 500_000,
    require_gain_today: float | None = 0.10,
    topk: int = 2,
    lookback_days: int = 60,
) -> tuple[pd.Timestamp, pd.DataFrame]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(DATE(`date`)) FROM stock_prices_pool")
            latest_day = cur.fetchone()[0]

        if latest_day is None:
            raise ValueError("stock_prices_pool has no data")

        latest_day = pd.to_datetime(latest_day)

        d0 = latest_day if pick_date is None else pd.to_datetime(pick_date)
        if d0 > latest_day:
            d0 = latest_day

        start_buffer = (d0 - pd.Timedelta(days=lookback_days)).date().isoformat()
        end_day = d0.date().isoformat()

        sql = """
        SELECT symbol,
               DATE(`date`) AS d,
               open, high, low, close, volume
        FROM stock_prices_pool
        WHERE DATE(`date`) >= %s
          AND DATE(`date`) <= %s
        ORDER BY symbol, d
        """
        df = pd.read_sql(sql, conn, params=[start_buffer, end_day])
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if df.empty:
        return pd.Timestamp(d0.date()), pd.DataFrame()

    df["d"] = pd.to_datetime(df["d"])

    def enrich(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("d").reset_index(drop=True)
        c, o, h, l, v = g["close"], g["open"], g["high"], g["low"], g["volume"]

        inc3 = (c.shift(2) < c.shift(1)) & (c.shift(1) < c)
        wk_min_ok = v.rolling(5, min_periods=5).min() > min_week_vol
        in_price = c.between(price_low, price_high)

        if require_gain_today is not None:
            gain_today_ok = (c - o) / o >= float(require_gain_today)
        else:
            gain_today_ok = pd.Series(True, index=g.index)

        day_range = (h - l).replace(0, np.nan)
        pos_in_range = ((c - l) / day_range).fillna(0.5)
        vol_5ma = v.rolling(5, min_periods=1).mean()
        vol_ratio = (v / vol_5ma).clip(upper=5.0)
        gain_today = (c - o) / o

        tr1 = (h - l).abs()
        tr2 = (h - c.shift(1)).abs()
        tr3 = (l - c.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr14 = tr.rolling(14, min_periods=1).mean()
        vol_penalty = (atr14 / c).fillna(0.0)

        score = 2.0 * pos_in_range + 1.0 * vol_ratio + 1.0 * gain_today - 1.0 * vol_penalty

        g["pick"] = inc3 & wk_min_ok & in_price & gain_today_ok
        g["score"] = score
        return g

    df = df.groupby("symbol", group_keys=False)[["symbol", "d", "open", "high", "low", "close", "volume"]].apply(enrich)
    d0_ts = pd.Timestamp(d0.date())
    today = df[df["d"] == d0_ts].copy()
    if today.empty:
        return d0_ts, pd.DataFrame()

    picks = today[today["pick"]].copy()
    if picks.empty:
        return d0_ts, pd.DataFrame()

    picks = picks.sort_values("score", ascending=False).head(topk).copy()
    return d0_ts, picks

# -------------------- ensure table exists (optional, minimal, safe) --------------------
def ensure_stock_operations_table():
    """
    Minimal safety: ensure stock_operations exists and stock_code is PRIMARY KEY.
    If you already create it in mysql-init, this will do nothing.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS stock_operations (
      stock_code VARCHAR(16) NOT NULL,
      close_price DOUBLE NULL,
      trigger_price DOUBLE NULL,
      cost_price DOUBLE NULL,
      stop_loss_price DOUBLE NULL,
      take_profit_price DOUBLE NULL,
      weight DOUBLE NULL,
      stock_type VARCHAR(2) NOT NULL DEFAULT 'A',
      is_bought TINYINT NOT NULL DEFAULT 0,
      qty INT NOT NULL DEFAULT 0,
      can_buy TINYINT NOT NULL DEFAULT 0,
      can_sell TINYINT NOT NULL DEFAULT 0,
      last_order_intent VARCHAR(80) NULL,
      last_order_side VARCHAR(8) NULL,
      last_order_id VARCHAR(64) NULL,
      last_order_time DATETIME NULL,
      created_at DATETIME NULL,
      updated_at DATETIME NULL,
      PRIMARY KEY (stock_code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
    finally:
        try:
            conn.close()
        except Exception:
            pass

# -------------------- upsert --------------------
def upsert_stock_operations(rows: list[dict]):
    """
    rows each contains:
      stock_code, buy_price, tp_price, sl_price, weight, qty
    """
    sql = """
    INSERT INTO stock_operations
      (stock_code, close_price, trigger_price, cost_price, stop_loss_price, take_profit_price, weight,
       stock_type, is_bought, qty, created_at, updated_at, can_buy, can_sell)
    VALUES
      (%s, %s, %s, NULL, %s, %s, %s,
       'A', 0, %s, NOW(), NOW(), 1, 0)
    ON DUPLICATE KEY UPDATE
      close_price = VALUES(close_price),
      trigger_price = VALUES(trigger_price),
      stop_loss_price = VALUES(stop_loss_price),
      take_profit_price = VALUES(take_profit_price),
      weight = VALUES(weight),
      qty = VALUES(qty),
      stock_type = 'A',
      can_buy = 1,
      updated_at = NOW();
    """

    data = []
    for r in rows:
        data.append((
            r["stock_code"],
            float(r["buy_price"]),
            float(r["buy_price"]),
            float(r["sl_price"]),
            float(r["tp_price"]),
            float(r["weight"]),
            int(r["qty"]),
        ))

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, data)
    finally:
        try:
            conn.close()
        except Exception:
            pass

# -------------------- entry --------------------
def recommend_and_save(
    pick_date: str | None = None,
    topk: int = 2,
    weight_mode: str = "equal",
    tp_pct: float = 0.11,
    sl_pct: float = 0.005,
    qty_default: int = 1,
):
    ensure_stock_operations_table()

    d0, top = pick_topk_strategy_a(pick_date=pick_date, topk=topk)

    if top.empty:
        print(f"[A] {d0.date()} no picks")
        return []

    w = compute_weights(top, mode=weight_mode)
    rows = []

    for i, r in enumerate(top.itertuples(index=False)):
        sym = str(r.symbol).upper()
        buy = float(r.close)
        tp = round(buy * (1.0 + tp_pct), 2)
        sl = round(buy * (1.0 - sl_pct), 2)

        rows.append({
            "stock_code": sym,
            "buy_price": buy,
            "tp_price": tp,
            "sl_price": sl,
            "weight": float(w[i]),
            "qty": int(qty_default),
        })

    upsert_stock_operations(rows)

    codes = [x["stock_code"] for x in rows]
    print(f"[A] {d0.date()} upsert into stock_operations: {codes}")
    return codes

if __name__ == "__main__":
    pick_date = os.getenv("PICK_DATE", "").strip() or None
    topk = int(os.getenv("TOPK", "2"))
    weight_mode = os.getenv("WEIGHT_MODE", "hybrid").strip() or "hybrid"
    tp_pct = float(os.getenv("TP_PCT", "0.11"))
    sl_pct = float(os.getenv("SL_PCT", "0.005"))
    qty_default = int(os.getenv("QTY_DEFAULT", "1"))

    print(f"[A] env PICK_DATE={pick_date} TOPK={topk} WEIGHT_MODE={weight_mode}", flush=True)

    recommend_and_save(
        pick_date=pick_date,
        topk=topk,
        weight_mode=weight_mode,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        qty_default=qty_default,
    )