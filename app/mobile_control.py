# -*- coding: utf-8 -*-
"""
手机控制台第一版。

功能：
- 登录后查看机器人关键状态。
- 手机切换总买入、只卖不买、紧急停止、B/C/F 策略买入开关。
- 查看 stock_operations 队列、option_spreads 近期记录、QQQ/N 风控开关。

说明：
- 这个服务只改 bot_control 表，不直接下单。
- 主程序 trade_bot_main.py 每轮读取 bot_control，再决定是否允许买入/暂停。
"""

from __future__ import annotations

import html
import json
import os
import time
from datetime import datetime, time as dt_time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, quote, urlparse

import pymysql

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

DB = dict(
    host=os.getenv("DB_HOST", "mysql"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "tradebot"),
    password=os.getenv("DB_PASS", ""),
    database=os.getenv("DB_NAME", "cszy2000"),
    charset="utf8mb4",
    autocommit=True,
    cursorclass=pymysql.cursors.DictCursor,
)

PORT = int(os.getenv("MOBILE_CONTROL_PORT", "5050"))
TOKEN = os.getenv("MOBILE_CONTROL_TOKEN", "change-me-please")
TABLE = os.getenv("OPS_TABLE", "stock_operations")
SPREADS_TABLE = os.getenv("C_SPREADS_TABLE", "option_spreads")
LEGS_TABLE = os.getenv("C_LEGS_TABLE", "option_spread_legs")
POSITION_CLOSE_TABLE = os.getenv("MOBILE_POSITION_CLOSE_TABLE", "broker_position_close_snapshots")

POSITION_CACHE_SEC = int(os.getenv("MOBILE_POSITION_CACHE_SEC", "30"))
SELL_LIMIT_BUFFER_PCT = float(os.getenv("MOBILE_SELL_LIMIT_BUFFER_PCT", "0.005"))
CLOSE_CAPTURE_TZ = os.getenv("MOBILE_CLOSE_CAPTURE_TZ", "America/Los_Angeles")
CLOSE_CAPTURE_TIME = os.getenv("MOBILE_CLOSE_CAPTURE_TIME", "12:59")
CLOSE_CAPTURE_WINDOW_MIN = int(os.getenv("MOBILE_CLOSE_CAPTURE_WINDOW_MIN", "10"))
_position_cache = {
    "ts": 0.0,
    "env": "",
    "rows": [],
    "error": "",
}
_account_cache = {
    "ts": 0.0,
    "env": "",
    "row": {},
    "error": "",
}
_trading_client = None
_trading_client_key = None


def _connect():
    return pymysql.connect(**DB)


def _now_capture_tz():
    if ZoneInfo:
        return datetime.now(ZoneInfo(CLOSE_CAPTURE_TZ))
    return datetime.now()


def _trade_env() -> str:
    env = (os.getenv("TRADE_ENV") or os.getenv("ALPACA_MODE") or "paper").strip().lower()
    return "live" if env == "live" else "paper"


def _alpaca_keys_for_env():
    """
    手机控制台需要自己选择 Alpaca key。

    trade_bot_main.py 会在启动时把 PAPER/LIVE key 注入到 APCA_API_KEY_ID，
    但 monitor 是独立进程，不能依赖主程序已经注入过的环境变量。
    所以这里按 TRADE_ENV/ALPACA_MODE 主动选择：
      - live  -> LIVE_APCA_API_KEY_ID / LIVE_APCA_API_SECRET_KEY
      - paper -> PAPER_APCA_API_KEY_ID / PAPER_APCA_API_SECRET_KEY
    最后再兼容 APCA_API_KEY_ID / ALPACA_KEY 这些通用变量。
    """
    env = _trade_env()
    if env == "live":
        key = os.getenv("LIVE_APCA_API_KEY_ID", "")
        secret = os.getenv("LIVE_APCA_API_SECRET_KEY", "")
    else:
        key = os.getenv("PAPER_APCA_API_KEY_ID", "")
        secret = os.getenv("PAPER_APCA_API_SECRET_KEY", "")

    key = key or os.getenv("APCA_API_KEY_ID", "") or os.getenv("ALPACA_KEY", "")
    secret = secret or os.getenv("APCA_API_SECRET_KEY", "") or os.getenv("ALPACA_SECRET", "")
    return env, key, secret


def _get_trading_client():
    global _trading_client, _trading_client_key
    env, key, secret = _alpaca_keys_for_env()
    if not key or not secret:
        raise RuntimeError("Alpaca key missing for mobile positions")

    cache_key = (env, key[:8])
    if _trading_client is not None and _trading_client_key == cache_key:
        return _trading_client

    from alpaca.trading.client import TradingClient

    _trading_client = TradingClient(key, secret, paper=(env != "live"))
    _trading_client_key = cache_key
    return _trading_client


def _money(v):
    try:
        return f"{float(v):.2f}"
    except Exception:
        return ""


def _float_or_none(v):
    try:
        if v is None or str(v).strip() == "":
            return None
        return float(v)
    except Exception:
        return None


def _limit_price_for_stock(price: float) -> float:
    """
    股票限价精度：
    - >= 1 美元按 2 位小数
    - < 1 美元按 4 位小数
    """
    price = float(price or 0.0)
    if price <= 0:
        return 0.0
    return round(price, 4 if price < 1 else 2)


def _signed_class(v) -> str:
    n = _float_or_none(v)
    if n is None:
        return ""
    if n > 0:
        return "pos"
    if n < 0:
        return "neg"
    return ""


def _signed_td(display, raw=None) -> str:
    cls = _signed_class(raw if raw is not None else display)
    return f'<td class="{cls}">{_esc(display)}</td>'


def _pct(v):
    try:
        return f"{float(v) * 100:.2f}%"
    except Exception:
        return ""


def _is_equity_position(row: dict) -> bool:
    asset_class = str(row.get("asset_class") or "").upper()
    return "EQUITY" in asset_class or asset_class in ("US_EQUITY", "US_EQUITIES")


def _submit_sell_position(symbol: str):
    """
    手机控制台手动卖出持仓。

    盘前/盘后 Alpaca 不接受 market order，只接受：
      - limit order
      - time_in_force=day
      - extended_hours=True

    所以这里用当前价下方一点点的限价卖单，让它尽量成为可成交的
    marketable limit order，同时避免真正 market order 在扩展时段被拒。
    """
    from alpaca.trading.enums import OrderSide, TimeInForce
    from alpaca.trading.requests import LimitOrderRequest

    symbol = (symbol or "").strip().upper()
    if not symbol:
        raise RuntimeError("empty symbol")

    tc = _get_trading_client()
    pos = tc.get_open_position(symbol)
    qty = getattr(pos, "qty", None)
    current_price = getattr(pos, "current_price", None)

    qty_f = float(qty or 0)
    price_f = float(current_price or 0)
    if qty_f <= 0:
        raise RuntimeError(f"{symbol} qty<=0")
    if price_f <= 0:
        raise RuntimeError(f"{symbol} current_price missing")

    limit_price = _limit_price_for_stock(price_f * (1.0 - SELL_LIMIT_BUFFER_PCT))
    if limit_price <= 0:
        raise RuntimeError(f"{symbol} invalid limit_price")

    req = LimitOrderRequest(
        symbol=symbol,
        qty=str(qty),
        side=OrderSide.SELL,
        time_in_force=TimeInForce.DAY,
        limit_price=limit_price,
        extended_hours=True,
    )
    order = tc.submit_order(order_data=req)

    # 卖出后让下一次页面刷新重新拉持仓。
    _position_cache["ts"] = 0.0
    return {
        "symbol": symbol,
        "qty": qty,
        "current_price": price_f,
        "limit_price": limit_price,
        "order_id": getattr(order, "id", ""),
        "status": getattr(order, "status", ""),
    }


def _submit_sell_all_positions():
    """
    一键清仓：只处理股票持仓。

    每只股票都按“当前价下方 buffer”的扩展时段 DAY 限价卖单提交。
    如果某只失败，不影响其它股票继续提交，最后把成功/失败汇总返回。
    """
    tc = _get_trading_client()
    positions = tc.get_all_positions() or []
    symbols = []
    for p in positions:
        asset_class = str(getattr(p, "asset_class", "") or "").upper()
        symbol = str(getattr(p, "symbol", "") or "").strip().upper()
        qty = _float_or_none(getattr(p, "qty", None))
        if symbol and qty and qty > 0 and ("EQUITY" in asset_class or asset_class in ("US_EQUITY", "US_EQUITIES")):
            symbols.append(symbol)

    results = []
    for symbol in symbols:
        try:
            results.append(("ok", _submit_sell_position(symbol)))
        except Exception as e:
            results.append(("err", {"symbol": symbol, "error": str(e)}))

    _position_cache["ts"] = 0.0
    ok_n = sum(1 for status, _ in results if status == "ok")
    err_n = sum(1 for status, _ in results if status != "ok")
    return ok_n, err_n, results


def _get_account_cached():
    now = time.time()
    env = _trade_env()
    if (
        _account_cache["row"]
        and _account_cache["env"] == env
        and now - float(_account_cache["ts"] or 0) < POSITION_CACHE_SEC
    ):
        return _account_cache["row"], _account_cache["error"], int(now - float(_account_cache["ts"] or 0))

    try:
        acct = _get_trading_client().get_account()
        row = {
            "cash": _money(getattr(acct, "cash", "")),
            "buying_power": _money(getattr(acct, "buying_power", "")),
            "options_buying_power": _money(getattr(acct, "options_buying_power", "")),
            "portfolio_value": _money(getattr(acct, "portfolio_value", "")),
            "equity": _money(getattr(acct, "equity", "")),
        }
        _account_cache.update({"ts": now, "env": env, "row": row, "error": ""})
        return row, "", 0
    except Exception as e:
        _account_cache.update({"ts": now, "env": env, "row": {}, "error": str(e)})
        return {}, str(e), 0


def _get_positions_cached():
    """
    Alpaca 持仓接口 30 秒查一次。

    页面刷新很频繁时直接用缓存，避免手机控制台把交易接口打得太密。
    """
    now = time.time()
    env = _trade_env()
    if (
        _position_cache["rows"]
        and _position_cache["env"] == env
        and now - float(_position_cache["ts"] or 0) < POSITION_CACHE_SEC
    ):
        return _position_cache["rows"], _position_cache["error"], int(now - float(_position_cache["ts"] or 0))

    try:
        tc = _get_trading_client()
        positions = tc.get_all_positions() or []
        rows = []
        for p in positions:
            unrealized_pl = _float_or_none(getattr(p, "unrealized_pl", None))
            unrealized_plpc = _float_or_none(getattr(p, "unrealized_plpc", None))
            current_price = _float_or_none(getattr(p, "current_price", None))
            sell_limit = _limit_price_for_stock(float(current_price or 0) * (1.0 - SELL_LIMIT_BUFFER_PCT))
            rows.append({
                "symbol": getattr(p, "symbol", ""),
                "asset_class": getattr(p, "asset_class", ""),
                "qty": getattr(p, "qty", ""),
                "avg_entry_price": _money(getattr(p, "avg_entry_price", "")),
                "current_price": _money(current_price),
                "market_value": _money(getattr(p, "market_value", "")),
                "unrealized_pl": _money(unrealized_pl),
                "unrealized_plpc": _pct(unrealized_plpc),
                "sell_limit": sell_limit,
                "_unrealized_pl": unrealized_pl,
                "_unrealized_plpc": unrealized_plpc,
            })
        rows.sort(key=lambda r: abs(float(r.get("market_value") or 0.0)), reverse=True)
        _position_cache.update({"ts": now, "env": env, "rows": rows, "error": ""})
        return rows, "", 0
    except Exception as e:
        _position_cache.update({"ts": now, "env": env, "rows": [], "error": str(e)})
        return [], str(e), 0


def _esc(v) -> str:
    return html.escape("" if v is None else str(v))


def _ensure_control(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS bot_control (
        id INT NOT NULL PRIMARY KEY DEFAULT 1,
        global_buy_enabled TINYINT NOT NULL DEFAULT 1,
        strategy_b_enabled TINYINT NOT NULL DEFAULT 1,
        strategy_c_enabled TINYINT NOT NULL DEFAULT 1,
        strategy_f_enabled TINYINT NOT NULL DEFAULT 1,
        sell_only_mode TINYINT NOT NULL DEFAULT 0,
        emergency_stop TINYINT NOT NULL DEFAULT 0,
        note VARCHAR(255) NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute("INSERT IGNORE INTO bot_control (id) VALUES (1);")


def _ensure_position_close_table(conn):
    """
    记录每天收盘前的券商持仓盈亏快照。

    不写入 stock_operations，是因为券商持仓可能来自策略 B/C/F，
    也可能来自手动交易。单独建表可以按日期保留历史快照，
    第二天盘前仍然能拿“上一收盘”的值做对比。
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{POSITION_CLOSE_TABLE}` (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        env VARCHAR(16) NOT NULL,
        trade_date DATE NOT NULL,
        symbol VARCHAR(32) NOT NULL,
        qty DECIMAL(20,6) NULL,
        avg_entry_price DECIMAL(20,6) NULL,
        current_price DECIMAL(20,6) NULL,
        market_value DECIMAL(20,2) NULL,
        close_pl DECIMAL(20,2) NULL,
        close_plpc DECIMAL(12,6) NULL,
        captured_at DATETIME NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_env_date_symbol (env, trade_date, symbol),
        KEY idx_env_symbol_date (env, symbol, trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def _capture_time_parts():
    try:
        hh, mm = CLOSE_CAPTURE_TIME.split(":", 1)
        return int(hh), int(mm)
    except Exception:
        return 12, 59


def _in_close_capture_window(now_dt: datetime) -> bool:
    if now_dt.weekday() >= 5:
        return False
    hh, mm = _capture_time_parts()
    start = now_dt.replace(hour=hh, minute=mm, second=0, microsecond=0)
    end_ts = start.timestamp() + CLOSE_CAPTURE_WINDOW_MIN * 60
    return start.timestamp() <= now_dt.timestamp() <= end_ts


def _maybe_capture_close_snapshot(rows, force: bool = False):
    """
    在收盘前 1 分钟附近记录一次持仓盈亏。

    默认美西时间 12:59 到 13:09 之间，页面/接口刷新触发一次 UPSERT。
    如果你希望绝对准时，后面也可以把同样逻辑拆成 cron 脚本跑。
    """
    if not rows:
        return
    now_dt = _now_capture_tz()
    if (not force) and (not _in_close_capture_window(now_dt)):
        return

    env = _trade_env()
    trade_date = now_dt.date().isoformat()
    captured_at = now_dt.strftime("%Y-%m-%d %H:%M:%S")

    args = []
    for r in rows:
        symbol = str(r.get("symbol") or "").strip().upper()
        if not symbol or not _is_equity_position(r):
            continue
        args.append((
            env,
            trade_date,
            symbol,
            _float_or_none(r.get("qty")),
            _float_or_none(r.get("avg_entry_price")),
            _float_or_none(r.get("current_price")),
            _float_or_none(r.get("market_value")),
            _float_or_none(r.get("_unrealized_pl")),
            _float_or_none(r.get("_unrealized_plpc")),
            captured_at,
        ))
    if not args:
        return

    sql = f"""
    INSERT INTO `{POSITION_CLOSE_TABLE}` (
        env, trade_date, symbol, qty, avg_entry_price, current_price,
        market_value, close_pl, close_plpc, captured_at
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        qty=VALUES(qty),
        avg_entry_price=VALUES(avg_entry_price),
        current_price=VALUES(current_price),
        market_value=VALUES(market_value),
        close_pl=VALUES(close_pl),
        close_plpc=VALUES(close_plpc),
        captured_at=VALUES(captured_at);
    """
    with _connect() as conn:
        _ensure_position_close_table(conn)
        with conn.cursor() as cur:
            cur.executemany(sql, args)


def _attach_latest_close_snapshot(rows):
    if not rows:
        return rows

    symbols = [str(r.get("symbol") or "").strip().upper() for r in rows if str(r.get("symbol") or "").strip()]
    symbols = sorted(set(symbols))
    if not symbols:
        return rows

    env = _trade_env()
    placeholders = ",".join(["%s"] * len(symbols))
    sql = f"""
    SELECT s.symbol, s.trade_date, s.close_pl, s.close_plpc
    FROM `{POSITION_CLOSE_TABLE}` s
    JOIN (
        SELECT symbol, MAX(trade_date) AS trade_date
        FROM `{POSITION_CLOSE_TABLE}`
        WHERE env=%s AND symbol IN ({placeholders})
        GROUP BY symbol
    ) t
      ON s.symbol=t.symbol AND s.trade_date=t.trade_date
    WHERE s.env=%s;
    """
    try:
        with _connect() as conn:
            _ensure_position_close_table(conn)
            snap_rows = _fetch_all(conn, sql, tuple([env] + symbols + [env]))
    except Exception:
        snap_rows = []

    by_symbol = {str(r.get("symbol") or "").upper(): r for r in snap_rows}
    for r in rows:
        snap = by_symbol.get(str(r.get("symbol") or "").upper())
        if snap:
            r["close_pl"] = _money(snap.get("close_pl"))
            r["close_plpc"] = _pct(snap.get("close_plpc"))
        else:
            r["close_pl"] = ""
            r["close_plpc"] = ""
    return rows


def _fetch_one(conn, sql, args=None):
    with conn.cursor() as cur:
        cur.execute(sql, args or ())
        return cur.fetchone() or {}


def _fetch_all(conn, sql, args=None):
    with conn.cursor() as cur:
        cur.execute(sql, args or ())
        return cur.fetchall() or []


def _bool_button(name: str, value: int, label: str) -> str:
    checked = "checked" if int(value or 0) == 1 else ""
    return f"""
    <label class="switch-row">
      <span>{_esc(label)}</span>
      <input type="hidden" name="{_esc(name)}" value="0">
      <input type="checkbox" name="{_esc(name)}" value="1" {checked}>
    </label>
    """


def _table(rows, cols) -> str:
    if not rows:
        return '<div class="empty">暂无数据</div>'
    head = "".join(f"<th>{_esc(c)}</th>" for c, _ in cols)
    body = []
    for r in rows:
        tds = "".join(f"<td>{_esc(r.get(k))}</td>" for _, k in cols)
        body.append(f"<tr>{tds}</tr>")
    return f'<div class="table-wrap"><table><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table></div>'


def _positions_table(rows) -> str:
    if not rows:
        return '<div class="empty">暂无数据</div>'

    cols = [
        ("代码", "symbol"),
        ("数量", "qty"),
        ("成本", "avg_entry_price"),
        ("现价", "current_price"),
        ("市值", "market_value"),
        ("浮盈亏", "unrealized_pl"),
        ("收盘盈亏", "close_pl"),
        ("浮盈亏%", "unrealized_plpc"),
        ("收盘盈亏%", "close_plpc"),
    ]
    head = "".join(f"<th>{_esc(c)}</th>" for c, _ in cols) + "<th>操作</th>"
    body = []
    for r in rows:
        tds_parts = []
        for _, k in cols:
            if k in ("unrealized_pl", "unrealized_plpc", "close_pl", "close_plpc"):
                raw_key = {
                    "unrealized_pl": "_unrealized_pl",
                    "unrealized_plpc": "_unrealized_plpc",
                }.get(k, k)
                tds_parts.append(_signed_td(r.get(k), r.get(raw_key)))
            else:
                tds_parts.append(f"<td>{_esc(r.get(k))}</td>")
        tds = "".join(tds_parts)
        symbol = str(r.get("symbol") or "").strip().upper()
        if symbol and _is_equity_position(r):
            limit_price = r.get("sell_limit") or ""
            current_price = r.get("current_price") or ""
            action = f"""
            <form method="post" action="/sell_position" onsubmit="return confirm('确认卖出 {symbol} 全部持仓？\\n当前价: {current_price}\\n卖出限价: {limit_price}\\n订单: DAY + extended_hours=True');">
              <input type="hidden" name="symbol" value="{_esc(symbol)}">
              <button class="danger mini" type="submit">卖出</button>
            </form>
            """
        else:
            action = '<span class="muted">-</span>'
        body.append(f"<tr>{tds}<td>{action}</td></tr>")
    return f'<div class="table-wrap"><table><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table></div>'


def _account_panel(account: dict, error: str = "") -> str:
    if error:
        return f'<div class="status-line"><span class="pill">账户资金: <b class="bad">{_esc(error[:100])}</b></span></div>'
    return f"""
    <div class="status-line">
      <span class="pill">现金: {_esc(account.get('cash'))}</span>
      <span class="pill">股票购买力: {_esc(account.get('buying_power'))}</span>
      <span class="pill">期权购买力: {_esc(account.get('options_buying_power'))}</span>
      <span class="pill">总权益: {_esc(account.get('equity'))}</span>
      <span class="pill">账户市值: {_esc(account.get('portfolio_value'))}</span>
    </div>
    """


def _page(body: str) -> bytes:
    css = """
    <style>
      :root { color-scheme: light dark; --bg:#0f172a; --panel:#111827; --text:#e5e7eb; --muted:#9ca3af; --line:#263244; --green:#22c55e; --red:#ef4444; --blue:#38bdf8; }
      body { margin:0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; background:var(--bg); color:var(--text); }
      header { position:sticky; top:0; z-index:2; padding:14px 16px; background:#020617; border-bottom:1px solid var(--line); display:flex; justify-content:space-between; align-items:center; }
      h1 { font-size:19px; margin:0; }
      h2 { font-size:16px; margin:0 0 10px; color:#f8fafc; }
      main { padding:14px; max-width:980px; margin:0 auto; }
      .grid { display:grid; gap:12px; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); }
      .card { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:14px; }
      .muted { color:var(--muted); font-size:13px; }
      .pill { display:inline-block; padding:3px 8px; border-radius:999px; background:#1f2937; font-size:12px; }
      .ok { color:var(--green); } .bad { color:var(--red); } .blue { color:var(--blue); }
      .pos { color:var(--green); font-weight:700; }
      .neg { color:var(--red); font-weight:700; }
      .switch-row { display:flex; justify-content:space-between; align-items:center; padding:10px 0; border-bottom:1px solid var(--line); gap:12px; }
      .switch-row:last-child { border-bottom:0; }
      input[type=checkbox] { width:26px; height:26px; }
      input[type=password], input[type=text] { width:100%; box-sizing:border-box; padding:12px; border-radius:8px; border:1px solid var(--line); background:#020617; color:var(--text); font-size:16px; }
      button, .btn { display:inline-block; border:0; border-radius:8px; padding:11px 14px; background:#2563eb; color:white; text-decoration:none; font-weight:600; font-size:15px; }
      .danger { background:#dc2626; }
      .secondary { background:#374151; }
      .mini { padding:7px 10px; font-size:13px; white-space:nowrap; }
      .actions { display:flex; gap:8px; flex-wrap:wrap; margin-top:12px; }
      .table-wrap { overflow-x:auto; }
      table { width:100%; border-collapse:collapse; font-size:13px; }
      th, td { padding:8px 7px; border-bottom:1px solid var(--line); text-align:left; white-space:nowrap; }
      th { color:#cbd5e1; font-weight:600; }
      .empty { color:var(--muted); padding:12px 0; }
      .status-line { display:flex; gap:8px; flex-wrap:wrap; margin-top:8px; }
      details.card { display:block; }
      summary { cursor:pointer; list-style:none; display:flex; justify-content:space-between; align-items:center; font-weight:700; font-size:16px; color:#f8fafc; }
      summary::-webkit-details-marker { display:none; }
      summary::after { content:"展开"; color:var(--muted); font-size:13px; font-weight:500; }
      details[open] summary { margin-bottom:10px; }
      details[open] summary::after { content:"收起"; }
      @media (max-width:600px) { main { padding:10px; } .card { padding:12px; } }
    </style>
    """
    html_doc = f"<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>{css}<title>TradeBot</title></head><body>{body}</body></html>"
    return html_doc.encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    def _cookie_token(self) -> str:
        raw = self.headers.get("Cookie", "")
        for part in raw.split(";"):
            k, _, v = part.strip().partition("=")
            if k == "token":
                return v
        return ""

    def _authed(self) -> bool:
        qs = parse_qs(urlparse(self.path).query)
        token = (qs.get("token") or [""])[0] or self._cookie_token()
        return bool(TOKEN) and token == TOKEN

    def _send(self, content: bytes, status=HTTPStatus.OK, headers=None):
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(content)

    def _send_json(self, data: dict, status=HTTPStatus.OK):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def _redirect(self, path="/"):
        self.send_response(302)
        self.send_header("Location", path)
        self.end_headers()

    def _redirect_with_cookie(self, path: str, cookie: str):
        self.send_response(302)
        self.send_header("Location", path)
        self.send_header("Set-Cookie", cookie)
        self.end_headers()

    def _login_page(self, msg=""):
        body = f"""
        <header><h1>TradeBot 登录</h1></header>
        <main>
          <div class="card">
            <h2>手机控制台</h2>
            <p class="muted">请输入 MOBILE_CONTROL_TOKEN。</p>
            <form method="post" action="/login">
              <input type="password" name="token" placeholder="控制台密码">
              <div class="actions"><button type="submit">登录</button></div>
            </form>
            <p class="bad">{_esc(msg)}</p>
          </div>
        </main>
        """
        self._send(_page(body))

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/login":
            self._login_page()
            return
        if path == "/logout":
            self._send(_page("<main>已退出</main>"), headers={"Set-Cookie": "token=; Max-Age=0; Path=/"})
            return
        if not self._authed():
            self._redirect("/login")
            return

        if path == "/" or path == "/index":
            self._dashboard()
            return
        if path == "/api/refresh":
            self._api_refresh()
            return
        self._send(_page("<main>Not found</main>"), HTTPStatus.NOT_FOUND)

    def do_POST(self):
        path = urlparse(self.path).path
        length = int(self.headers.get("Content-Length") or 0)
        data = parse_qs(self.rfile.read(length).decode("utf-8"))

        if path == "/login":
            token = (data.get("token") or [""])[0]
            if token == TOKEN:
                self._redirect_with_cookie("/", f"token={token}; Path=/; HttpOnly; SameSite=Lax")
            else:
                self._login_page("密码不对")
            return

        if not self._authed():
            self._redirect("/login")
            return

        if path == "/control":
            fields = (
                "global_buy_enabled",
                "strategy_b_enabled",
                "strategy_c_enabled",
                "strategy_f_enabled",
                "sell_only_mode",
                "emergency_stop",
            )
            vals = {k: int((data.get(k) or ["0"])[0] or 0) for k in fields}
            note = (data.get("note") or [""])[0][:255]
            with _connect() as conn:
                _ensure_control(conn)
                sql = """
                UPDATE bot_control
                SET global_buy_enabled=%s,
                    strategy_b_enabled=%s,
                    strategy_c_enabled=%s,
                    strategy_f_enabled=%s,
                    sell_only_mode=%s,
                    emergency_stop=%s,
                    note=%s
                WHERE id=1;
                """
                with conn.cursor() as cur:
                    cur.execute(sql, tuple(vals[k] for k in fields) + (note,))
            self._redirect("/")
            return

        if path == "/sell_position":
            symbol = (data.get("symbol") or [""])[0]
            try:
                result = _submit_sell_position(symbol)
                msg = (
                    f"已提交卖出 {result['symbol']} qty={result['qty']} "
                    f"limit={result['limit_price']} status={result['status']} order={result['order_id']}"
                )
            except Exception as e:
                msg = f"卖出失败: {str(e)[:120]}"
            self._redirect(f"/?msg={quote(msg)}")
            return

        if path == "/sell_all_positions":
            try:
                ok_n, err_n, _ = _submit_sell_all_positions()
                msg = f"一键清仓已提交: 成功={ok_n} 失败={err_n}"
            except Exception as e:
                msg = f"一键清仓失败: {str(e)[:120]}"
            self._redirect(f"/?msg={quote(msg)}")
            return

        self._send(_page("<main>Not found</main>"), HTTPStatus.NOT_FOUND)

    def _load_dashboard_parts(self):
        with _connect() as conn:
            _ensure_control(conn)
            control = _fetch_one(conn, "SELECT * FROM bot_control WHERE id=1 LIMIT 1;")
            gate = _fetch_one(conn, f"SELECT stock_code, stock_type, entry_open FROM `{TABLE}` WHERE stock_code='QQQ' AND stock_type='N' LIMIT 1;")
            counts = _fetch_all(conn, f"""
                SELECT stock_type,
                       SUM(CASE WHEN can_buy=1 AND (is_bought IS NULL OR is_bought<>1) THEN 1 ELSE 0 END) AS buy_q,
                       SUM(CASE WHEN is_bought=1 AND can_sell=1 THEN 1 ELSE 0 END) AS sell_q
                FROM `{TABLE}`
                WHERE stock_type IN ('B','C','F')
                GROUP BY stock_type
                ORDER BY stock_type;
            """)
            ops = _fetch_all(conn, f"""
                SELECT stock_code, stock_type, is_bought, can_buy, can_sell, qty,
                       ROUND(cost_price, 2) AS cost_price,
                       last_order_side, last_order_intent, updated_at
                FROM `{TABLE}`
                WHERE stock_type IN ('B','C','F')
                ORDER BY updated_at DESC
                LIMIT 80;
            """)
            spreads = _fetch_all(conn, f"""
                SELECT id, underlying, mode, expiry, status, entry_price, max_loss, updated_at
                FROM `{SPREADS_TABLE}`
                ORDER BY id DESC
                LIMIT 30;
            """)

        gate_val = int(float(gate.get("entry_open") or 0))
        env = _trade_env()
        account, account_error, _ = _get_account_cached()
        positions, positions_error, pos_age = _get_positions_cached()
        if not positions_error:
            _maybe_capture_close_snapshot(positions)
            positions = _attach_latest_close_snapshot(positions)
        pos_status = (
            f'<span class="pill">持仓接口: <b class="bad">{_esc(positions_error[:80])}</b></span>'
            if positions_error
            else f'<span class="pill">持仓缓存: {pos_age}s / {POSITION_CACHE_SEC}s</span>'
        )
        status = f"""
        <div class="status-line">
          <span class="pill">env: {_esc(env)}</span>
          <span class="pill">QQQ gate: <b class="{'ok' if gate_val == 1 else 'bad'}">{gate_val}</b></span>
          <span class="pill">updated: {_esc(control.get('updated_at'))}</span>
          {pos_status}
        </div>
        """

        return {
            "control": control,
            "status": status,
            "account": _account_panel(account, account_error),
            "counts": _table(counts, [('策略','stock_type'),('待买','buy_q'),('待卖','sell_q')]),
            "positions": _positions_table(positions),
            "ops": _table(ops, [('代码','stock_code'),('类','stock_type'),('持仓','is_bought'),('买','can_buy'),('卖','can_sell'),('qty','qty'),('cost','cost_price'),('side','last_order_side'),('intent','last_order_intent'),('更新','updated_at')]),
            "spreads": _table(spreads, [('ID','id'),('标的','underlying'),('模式','mode'),('到期','expiry'),('状态','status'),('入场','entry_price'),('风险','max_loss'),('更新','updated_at')]),
        }

    def _api_refresh(self):
        try:
            parts = self._load_dashboard_parts()
            self._send_json({
                "ok": True,
                "status": parts["status"],
                "account": parts["account"],
                "counts": parts["counts"],
                "positions": parts["positions"],
                "ops": parts["ops"],
                "spreads": parts["spreads"],
            })
        except Exception as e:
            self._send_json({"ok": False, "error": str(e)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _dashboard(self):
        parts = self._load_dashboard_parts()
        control = parts["control"]
        qs = parse_qs(urlparse(self.path).query)
        msg = (qs.get("msg") or [""])[0]

        control_form = f"""
        <form method="post" action="/control">
          {_bool_button("global_buy_enabled", control.get("global_buy_enabled"), "总买入允许")}
          {_bool_button("sell_only_mode", control.get("sell_only_mode"), "只卖不买")}
          {_bool_button("emergency_stop", control.get("emergency_stop"), "紧急停止")}
          {_bool_button("strategy_b_enabled", control.get("strategy_b_enabled"), "策略 B 买入")}
          {_bool_button("strategy_c_enabled", control.get("strategy_c_enabled"), "策略 C 买入")}
          {_bool_button("strategy_f_enabled", control.get("strategy_f_enabled"), "策略 F 买入")}
          <div style="margin-top:10px">
            <input type="text" name="note" value="{_esc(control.get('note'))}" placeholder="备注">
          </div>
          <div class="actions">
            <button type="submit">保存开关</button>
            <a class="btn secondary" href="/">刷新</a>
            <a class="btn secondary" href="/logout">退出</a>
          </div>
        </form>
        """

        body = f"""
        <header><h1>TradeBot 控制台</h1><span class="muted">手机第一版</span></header>
        <main>
          {f'<section class="card"><b>{_esc(msg)}</b></section>' if msg else ''}
          <details class="card" style="margin-top:12px"><summary>状态</summary><div id="status-box">{parts["status"]}</div><div id="counts-box">{parts["counts"]}</div></details>
          <details class="card" style="margin-top:12px"><summary>控制</summary>{control_form}</details>
          <details class="card" style="margin-top:12px" open>
            <summary>券商持仓</summary>
            <div id="account-box">{parts["account"]}</div>
            <p class="muted">卖出按钮会提交 extended_hours=True 的 DAY 限价卖单，限价≈当前价*(1-{SELL_LIMIT_BUFFER_PCT:.2%})。</p>
            <form method="post" action="/sell_all_positions" onsubmit="return confirm('确认一键清仓全部股票持仓？\\n每只股票都会按 当前价*(1-{SELL_LIMIT_BUFFER_PCT:.2%}) 提交 DAY + extended_hours=True 限价卖单。');">
              <button class="danger" type="submit">一键清仓</button>
            </form>
            <div id="positions-box" style="margin-top:10px">{parts["positions"]}</div>
          </details>
          <details class="card" style="margin-top:12px"><summary>策略队列</summary><div id="ops-box">{parts["ops"]}</div></details>
          <details class="card" style="margin-top:12px"><summary>期权组合</summary><div id="spreads-box">{parts["spreads"]}</div></details>
        </main>
        <script>
          async function refreshData() {{
            try {{
              const resp = await fetch('/api/refresh', {{ cache: 'no-store' }});
              const data = await resp.json();
              if (!data.ok) return;
              document.getElementById('status-box').innerHTML = data.status;
              document.getElementById('account-box').innerHTML = data.account;
              document.getElementById('counts-box').innerHTML = data.counts;
              document.getElementById('positions-box').innerHTML = data.positions;
              document.getElementById('ops-box').innerHTML = data.ops;
              document.getElementById('spreads-box').innerHTML = data.spreads;
            }} catch (e) {{
              console.log('refresh failed', e);
            }}
          }}
          setInterval(refreshData, {POSITION_CACHE_SEC * 1000});
        </script>
        """
        self._send(_page(body))


def main():
    print(f"[MOBILE] starting on 0.0.0.0:{PORT}", flush=True)
    if TOKEN == "change-me-please":
        print("[MOBILE] WARNING: MOBILE_CONTROL_TOKEN is using default value. Change it before exposing to internet.", flush=True)
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
