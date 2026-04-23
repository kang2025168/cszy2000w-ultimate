# -*- coding: utf-8 -*-
"""
monitor/api.py
交易机器人监控 API — Flask 后端
持仓：完全以 Alpaca 真实持仓为准，数据库补充止损/阶段
"""

import os
import time
import traceback
import requests as req
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
MIN_BUYING_POWER = float(os.getenv("MIN_BUYING_POWER", "2100"))

APCA_KEY    = os.getenv("APCA_API_KEY_ID", "") or os.getenv("ALPACA_KEY", "")
APCA_SECRET = os.getenv("APCA_API_SECRET_KEY", "") or os.getenv("ALPACA_SECRET", "")

ALPACA_TRADE_URL = "https://api.alpaca.markets" if TRADE_ENV == "live" else "https://paper-api.alpaca.markets"

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

app = Flask(__name__)
CORS(app)

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

def alpaca_headers():
    return {
        "APCA-API-KEY-ID": APCA_KEY,
        "APCA-API-SECRET-KEY": APCA_SECRET,
    }

def get_conn():
    return pymysql.connect(**DB)

# ─── Alpaca 账户信息 ───────────────────────────────────────────────────
_acct_cache = {"ts": 0, "val": None}

def get_alpaca_account():
    now = time.time()
    if now - _acct_cache["ts"] < 15 and _acct_cache["val"]:
        return _acct_cache["val"]
    try:
        r = req.get(f"{ALPACA_TRADE_URL}/v2/account", headers=alpaca_headers(), timeout=8)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
        d = r.json()
        result = {
            "buying_power": round(safe_float(d.get("buying_power")), 2),
            "cash": round(safe_float(d.get("cash")), 2),
            "equity": round(safe_float(d.get("equity")), 2),
            "portfolio_value": round(safe_float(d.get("portfolio_value")), 2),
            "bp_ok": safe_float(d.get("buying_power")) >= MIN_BUYING_POWER,
        }
        _acct_cache["ts"] = now
        _acct_cache["val"] = result
        return result
    except Exception as e:
        traceback.print_exc()
        return {"error": str(e), "buying_power": 0, "bp_ok": False}

# ─── Alpaca 真实持仓（直接 REST，不用 SDK）────────────────────────────
_pos_cache = {"ts": 0, "val": None}

def get_alpaca_positions():
    now = time.time()
    if now - _pos_cache["ts"] < 10 and _pos_cache["val"] is not None:
        return _pos_cache["val"]
    try:
        r = req.get(f"{ALPACA_TRADE_URL}/v2/positions", headers=alpaca_headers(), timeout=8)
        if r.status_code != 200:
            return {"__error__": f"HTTP {r.status_code}: {r.text[:200]}"}
        positions = r.json()
        result = {}
        for p in positions:
            code = (p.get("symbol") or "").strip().upper()
            if not code:
                continue
            result[code] = {
                "qty": safe_int(p.get("qty", 0)),
                "cost": safe_float(p.get("avg_entry_price", 0)),
                "price": safe_float(p.get("current_price", 0)),
                "market_value": safe_float(p.get("market_value", 0)),
                "unrealized_pl": safe_float(p.get("unrealized_pl", 0)),
                "unrealized_plpc": safe_float(p.get("unrealized_plpc", 0)) * 100,
                "side": p.get("side", ""),
            }
        _pos_cache["ts"] = now
        _pos_cache["val"] = result
        return result
    except Exception as e:
        traceback.print_exc()
        return {"__error__": str(e)}

# ─── API 路由 ──────────────────────────────────────────────────────────

@app.route("/health")
def health():
    return jsonify({"ok": True, "env": TRADE_ENV, "time": now_la().strftime("%H:%M:%S")})

@app.route("/api/status")
def api_status():
    try:
        conn = get_conn()
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
    """Alpaca 真实持仓为主，数据库补充止损/阶段"""
    try:
        # 1) Alpaca 真实持仓
        alpaca_pos = get_alpaca_positions()
        if "__error__" in alpaca_pos:
            return jsonify({"error": alpaca_pos["__error__"]}), 500

        # 2) 数据库辅助信息（全部股票，不过滤）
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT stock_code, stock_type,
                       stop_loss_price, b_stage, base_qty,
                       last_order_time, last_order_side, last_order_intent,
                       b_stop_pending_since
                FROM `{OPS_TABLE}`
                WHERE stock_type IN ('A','B','C','D','E')
            """)
            db_rows = cur.fetchall() or []
        conn.close()

        db_map = {(r.get("stock_code") or "").strip().upper(): r for r in db_rows}

        result = []
        for code, pos in alpaca_pos.items():
            db = db_map.get(code, {})
            price = pos["price"]
            cost  = pos["cost"]
            sl    = safe_float(db.get("stop_loss_price"))
            stage = safe_int(db.get("b_stage"))
            base_qty = safe_int(db.get("base_qty"))
            pending = bool(db.get("b_stop_pending_since"))
            up_pct = pos["unrealized_plpc"]
            dist_to_sl = (price - sl) / price * 100 if price > 0 and sl > 0 else 0
            stype = (db.get("stock_type") or "—").strip().upper()

            result.append({
                "code": code,
                "type": stype,
                "qty": pos["qty"],
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
                "in_db": code in db_map,
            })

        result.sort(key=lambda x: x["up_pct"], reverse=True)
        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/buy_queue")
def api_buy_queue():
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
        return jsonify([{
            "code": (r.get("stock_code") or "").strip().upper(),
            "type": (r.get("stock_type") or "").strip().upper(),
            "trigger": round(safe_float(r.get("trigger_price")), 2),
            "last_order_time": str(r.get("last_order_time") or ""),
            "last_order_side": r.get("last_order_side") or "",
        } for r in rows])
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/recent_trades")
def api_recent_trades():
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
        return jsonify([{
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
        } for r in rows])
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.getenv("MONITOR_PORT", "5050"))
    print(f"[Monitor API] starting on :{port} env={TRADE_ENV} trade_url={ALPACA_TRADE_URL}", flush=True)
    app.run(host="0.0.0.0", port=port, debug=False)