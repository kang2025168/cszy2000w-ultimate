# -*- coding: utf-8 -*-
"""
monitor/api.py
交易机器人监控 API — Flask 后端
对接 stock_operations 数据库 + Alpaca 账户
"""

import os
import time
import traceback
from datetime import datetime, time as dt_time
from functools import wraps

import pymysql
import pymysql.cursors
from flask import Flask, jsonify
from flask_cors import CORS

try:
    from zoneinfo import ZoneInfo
    LA_TZ = ZoneInfo("America/Los_Angeles")
except Exception:
    LA_TZ = None

# ─── 配置（从环境变量读，与 tradebot 保持一致）─────────────────────────
TRADE_ENV = (os.getenv("TRADE_ENV") or os.getenv("ALPACA_MODE") or "paper").strip().lower()

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

OPS_TABLE    = os.getenv("OPS_TABLE", "stock_operations")
PRICES_TABLE = os.getenv("B_PRICES_TABLE", "stock_prices_pool")
MIN_BUYING_POWER = float(os.getenv("MIN_BUYING_POWER", "2100"))

# Alpaca keys（由主程序逻辑注入，这里直接读通用变量）
APCA_KEY    = os.getenv("APCA_API_KEY_ID", "") or os.getenv("ALPACA_KEY", "")
APCA_SECRET = os.getenv("APCA_API_SECRET_KEY", "") or os.getenv("ALPACA_SECRET", "")

# ─── 市场时间判断 ──────────────────────────────────────────────────────
MARKET_OPEN  = dt_time(6, 40)
MARKET_CLOSE = dt_time(13, 0)

def now_la():
    if LA_TZ:
        return datetime.now(LA_TZ)
    return datetime.now()

def is_trading_time():
    now = now_la()
    if now.weekday() >= 5:
        return False
    t = now.time().replace(tzinfo=None)
    return MARKET_OPEN <= t <= MARKET_CLOSE

# ─── Flask App ─────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app)  # 允许前端跨域访问

# ─── 简单缓存（避免频繁打 Alpaca API）────────────────────────────────
_cache = {}
CACHE_TTL = int(os.getenv("API_CACHE_TTL", "10"))  # 秒

def cached(key, ttl=CACHE_TTL):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            now = time.time()
            if key in _cache and now - _cache[key]["ts"] < ttl:
                return _cache[key]["val"]
            result = fn(*args, **kwargs)
            _cache[key] = {"ts": now, "val": result}
            return result
        return wrapper
    return decorator

# ─── DB 连接 ───────────────────────────────────────────────────────────
def get_conn():
    return pymysql.connect(**DB)

def safe_float(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except Exception:
        return default

def safe_int(v, default=0):
    try:
        return int(float(v)) if v is not None else default
    except Exception:
        return default

# ─── Alpaca Trading Client（单例）────────────────────────────────────
_tc = None
def get_tc():
    global _tc
    if _tc is None:
        from alpaca.trading.client import TradingClient
        _tc = TradingClient(APCA_KEY, APCA_SECRET, paper=(TRADE_ENV == "paper"))
    return _tc

# ─── Alpaca 账户信息 ───────────────────────────────────────────────────
@cached("alpaca_account", ttl=15)
def get_alpaca_account():
    try:
        tc = get_tc()
        acct = tc.get_account()
        bp = safe_float(getattr(acct, "buying_power", None))
        cash = safe_float(getattr(acct, "cash", None))
        equity = safe_float(getattr(acct, "equity", None))
        portfolio_value = safe_float(getattr(acct, "portfolio_value", None))
        return {
            "buying_power": round(bp, 2),
            "cash": round(cash, 2),
            "equity": round(equity, 2),
            "portfolio_value": round(portfolio_value, 2),
            "bp_ok": bp >= MIN_BUYING_POWER,
        }
    except Exception as e:
        traceback.print_exc()
        return {"error": str(e), "buying_power": 0, "bp_ok": False}

# ─── Alpaca 真实持仓 ───────────────────────────────────────────────────
@cached("alpaca_positions", ttl=10)
def get_alpaca_positions():
    """直接从 Alpaca 拉所有持仓，返回 dict: code -> position info"""
    try:
        tc = get_tc()
        positions = tc.get_all_positions()
        result = {}
        for p in positions:
            code = (getattr(p, "symbol", "") or "").strip().upper()
            if not code:
                continue
            result[code] = {
                "qty": safe_int(getattr(p, "qty", 0)),
                "cost": safe_float(getattr(p, "avg_entry_price", 0)),
                "price": safe_float(getattr(p, "current_price", 0)),
                "market_value": safe_float(getattr(p, "market_value", 0)),
                "unrealized_pl": safe_float(getattr(p, "unrealized_pl", 0)),
                "unrealized_plpc": safe_float(getattr(p, "unrealized_plpc", 0)) * 100,
                "side": str(getattr(p, "side", "") or ""),
            }
        return result
    except Exception as e:
        traceback.print_exc()
        return {"__error__": str(e)}

# ─── API 路由 ──────────────────────────────────────────────────────────

@app.route("/api/status")
def api_status():
    """机器人整体状态：时段 + 购买力 + 大盘开关"""
    try:
        conn = get_conn()
        # 读大盘开关（QQQ entry_open）
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT entry_open FROM `{OPS_TABLE}` WHERE stock_code='QQQ' AND stock_type='N' LIMIT 1"
            )
            row = cur.fetchone() or {}
        market_gate = safe_int(row.get("entry_open"), 0)
        conn.close()

        acct = get_alpaca_account()
        bp_ok = acct.get("bp_ok", False)
        buy_allowed = bp_ok and (market_gate == 1)

        return jsonify({
            "env": TRADE_ENV,
            "trading_time": is_trading_time(),
            "server_time_la": now_la().strftime("%Y-%m-%d %H:%M:%S"),
            "buying_power": acct.get("buying_power", 0),
            "equity": acct.get("equity", 0),
            "portfolio_value": acct.get("portfolio_value", 0),
            "min_buying_power": MIN_BUYING_POWER,
            "bp_ok": bp_ok,
            "market_gate": market_gate,
            "buy_allowed": buy_allowed,
            "alpaca_error": acct.get("error"),
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/holdings")
def api_holdings():
    """
    持仓列表：Alpaca 真实持仓为主（qty/cost/price/pnl），
    数据库为辅（止损价、阶段、last_order_intent 等）
    """
    try:
        # 1) 从 Alpaca 拉真实持仓
        alpaca_pos = get_alpaca_positions()
        if "__error__" in alpaca_pos:
            return jsonify({"error": alpaca_pos["__error__"]}), 500

        # 2) 从数据库拉辅助信息（所有 B/A/C/D/E 类型，不限 is_bought）
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT stock_code, stock_type,
                       stop_loss_price, b_stage, base_qty,
                       last_order_time, last_order_side, last_order_intent,
                       b_stop_pending_since, b_stop_pending_sl,
                       trigger_price
                FROM `{OPS_TABLE}`
                WHERE stock_type IN ('A','B','C','D','E')
            """)
            db_rows = cur.fetchall() or []
        conn.close()

        db_map = {}
        for r in db_rows:
            code = (r.get("stock_code") or "").strip().upper()
            db_map[code] = r

        result = []
        for code, pos in alpaca_pos.items():
            db = db_map.get(code, {})
            qty  = pos["qty"]
            cost = pos["cost"]
            price = pos["price"]
            sl   = safe_float(db.get("stop_loss_price"))
            stage = safe_int(db.get("b_stage"))
            base_qty = safe_int(db.get("base_qty"))
            pending = bool(db.get("b_stop_pending_since"))

            up_pct = pos["unrealized_plpc"]  # Alpaca 直接给，已是百分比
            dist_to_sl = (price - sl) / price * 100 if price > 0 and sl > 0 else 0

            stype = (db.get("stock_type") or "B").strip().upper()

            result.append({
                "code": code,
                "type": stype,
                "qty": qty,
                "base_qty": base_qty,
                "cost": round(cost, 2),
                "price": round(price, 2),
                "market_value": round(pos["market_value"], 2),
                "unrealized_pl": round(pos["unrealized_pl"], 2),
                "sl": round(sl, 2),
                "stage": stage,
                "up_pct": round(up_pct, 2),
                "dist_to_sl_pct": round(dist_to_sl, 2),
                "pending_stop": pending,
                "last_order_time": str(db.get("last_order_time") or ""),
                "last_order_side": db.get("last_order_side") or "",
                "last_order_intent": db.get("last_order_intent") or "",
            })

        # 按浮盈降序排列
        result.sort(key=lambda x: x["up_pct"], reverse=True)
        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/buy_queue")
def api_buy_queue():
    """待买入队列（can_buy=1，未持仓）"""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT stock_code, stock_type, trigger_price,
                       last_order_time, last_order_side
                FROM `{OPS_TABLE}`
                WHERE can_buy=1 AND (is_bought IS NULL OR is_bought<>1)
                  AND stock_type IN ('A','B','C','D','E')
                ORDER BY stock_type, stock_code
            """)
            rows = cur.fetchall() or []
        conn.close()

        result = []
        for r in rows:
            result.append({
                "code": (r.get("stock_code") or "").strip().upper(),
                "type": (r.get("stock_type") or "").strip().upper(),
                "trigger": round(safe_float(r.get("trigger_price")), 2),
                "last_order_time": str(r.get("last_order_time") or ""),
                "last_order_side": r.get("last_order_side") or "",
            })
        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/recent_trades")
def api_recent_trades():
    """最近 50 笔交易记录（按 last_order_time 排序）"""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT stock_code, stock_type,
                       last_order_side, last_order_intent,
                       last_order_id, last_order_time,
                       qty, cost_price, stop_loss_price, b_stage
                FROM `{OPS_TABLE}`
                WHERE last_order_time IS NOT NULL
                  AND stock_type IN ('A','B','C','D','E')
                ORDER BY last_order_time DESC
                LIMIT 50
            """)
            rows = cur.fetchall() or []
        conn.close()

        result = []
        for r in rows:
            result.append({
                "code": (r.get("stock_code") or "").strip().upper(),
                "type": (r.get("stock_type") or "").strip().upper(),
                "side": r.get("last_order_side") or "",
                "intent": r.get("last_order_intent") or "",
                "order_id": r.get("last_order_id") or "",
                "time": str(r.get("last_order_time") or ""),
                "qty": safe_int(r.get("qty")),
                "cost": round(safe_float(r.get("cost_price")), 2),
                "sl": round(safe_float(r.get("stop_loss_price")), 2),
                "stage": safe_int(r.get("b_stage")),
            })
        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/summary")
def api_summary():
    """一次性拉取所有数据（减少前端请求次数）"""
    try:
        status_resp = app.test_client().get("/api/status")
        holdings_resp = app.test_client().get("/api/holdings")
        queue_resp = app.test_client().get("/api/buy_queue")
        trades_resp = app.test_client().get("/api/recent_trades")

        return jsonify({
            "status": status_resp.get_json(),
            "holdings": holdings_resp.get_json(),
            "buy_queue": queue_resp.get_json(),
            "recent_trades": trades_resp.get_json(),
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ─── 实时价格（复用 strategy_b 的 snapshot 逻辑）─────────────────────
_price_cache = {}
PRICE_CACHE_TTL = 5  # 秒

def _get_snapshot_price(code: str) -> float:
    now = time.time()
    cached = _price_cache.get(code)
    if cached and (now - cached[0]) < PRICE_CACHE_TTL:
        return cached[1]

    feed = os.getenv("B_DATA_FEED", "iex")
    data_url = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets")
    url = f"{data_url}/v2/stocks/{code}/snapshot"

    import requests
    r = requests.get(url, headers={
        "APCA-API-KEY-ID": APCA_KEY,
        "APCA-API-SECRET-KEY": APCA_SECRET,
    }, params={"feed": feed}, timeout=5)

    if r.status_code != 200:
        raise RuntimeError(f"snapshot {r.status_code}")

    js = r.json()
    lt = js.get("latestTrade") or {}
    price = float(lt["p"]) if lt.get("p") is not None else None
    if price is None:
        lq = js.get("latestQuote") or {}
        bid = float(lq.get("bp") or 0)
        ask = float(lq.get("ap") or 0)
        if bid > 0 and ask > 0:
            price = (bid + ask) / 2.0

    if price:
        _price_cache[code] = (now, price)
        return price
    raise RuntimeError("no price in snapshot")


# ─── 健康检查 ──────────────────────────────────────────────────────────
@app.route("/health")
def health():
    return jsonify({"ok": True, "env": TRADE_ENV, "time": now_la().strftime("%H:%M:%S")})


if __name__ == "__main__":
    port = int(os.getenv("MONITOR_PORT", "5050"))
    print(f"[Monitor API] starting on :{port} env={TRADE_ENV}", flush=True)
    app.run(host="0.0.0.0", port=port, debug=False)