# -*- coding: utf-8 -*-
"""
backtest/api.py  — 回测 API（可并入 monitor 服务）
"""
import os, traceback
from flask import Flask, jsonify, request, Response
from engine import backtest_single, backtest_market

app = Flask(__name__)

@app.route("/")
def index():
    with open(os.path.join(os.path.dirname(__file__), "backtest.html"), encoding="utf-8") as f:
        return Response(f.read(), mimetype="text/html")

@app.route("/api/backtest/single")
def api_single():
    """
    GET /api/backtest/single?symbol=AAPL&start=2024-01-01&end=2024-12-31
    """
    symbol     = (request.args.get("symbol") or "").strip().upper()
    start_date = request.args.get("start") or None
    end_date   = request.args.get("end")   or None
    notional   = float(request.args.get("notional", "2100"))

    if not symbol:
        return jsonify({"error": "symbol required"}), 400
    try:
        result = backtest_single(symbol, start_date, end_date, notional)
        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route("/api/backtest/market")
def api_market():
    """
    GET /api/backtest/market?start=2024-01-01&end=2024-12-31&cash=10000&max_pos=5
    """
    start_date    = request.args.get("start")   or None
    end_date      = request.args.get("end")     or None
    initial_cash  = float(request.args.get("cash",    "10000"))
    max_positions = int(request.args.get("max_pos",   "5"))
    notional      = float(request.args.get("notional","2100"))

    try:
        result = backtest_market(start_date, end_date, initial_cash, max_positions, notional)
        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route("/api/backtest/symbols")
def api_symbols():
    """返回所有可回测的股票列表"""
    try:
        from engine import _connect, load_all_symbols_with_levels
        conn = _connect()
        syms = load_all_symbols_with_levels(conn)
        conn.close()
        return jsonify(syms)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.getenv("BACKTEST_PORT", "5051"))
    print(f"[Backtest API] :{port}", flush=True)
    app.run(host="0.0.0.0", port=port, debug=False)
