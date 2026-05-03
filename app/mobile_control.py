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
import os
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pymysql


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

POSITION_CACHE_SEC = int(os.getenv("MOBILE_POSITION_CACHE_SEC", "30"))
_position_cache = {
    "ts": 0.0,
    "env": "",
    "rows": [],
    "error": "",
}
_trading_client = None
_trading_client_key = None


def _connect():
    return pymysql.connect(**DB)


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


def _pct(v):
    try:
        return f"{float(v) * 100:.2f}%"
    except Exception:
        return ""


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
            rows.append({
                "symbol": getattr(p, "symbol", ""),
                "asset_class": getattr(p, "asset_class", ""),
                "qty": getattr(p, "qty", ""),
                "avg_entry_price": _money(getattr(p, "avg_entry_price", "")),
                "current_price": _money(getattr(p, "current_price", "")),
                "market_value": _money(getattr(p, "market_value", "")),
                "unrealized_pl": _money(getattr(p, "unrealized_pl", "")),
                "unrealized_plpc": _pct(getattr(p, "unrealized_plpc", "")),
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
      .switch-row { display:flex; justify-content:space-between; align-items:center; padding:10px 0; border-bottom:1px solid var(--line); gap:12px; }
      .switch-row:last-child { border-bottom:0; }
      input[type=checkbox] { width:26px; height:26px; }
      input[type=password], input[type=text] { width:100%; box-sizing:border-box; padding:12px; border-radius:8px; border:1px solid var(--line); background:#020617; color:var(--text); font-size:16px; }
      button, .btn { display:inline-block; border:0; border-radius:8px; padding:11px 14px; background:#2563eb; color:white; text-decoration:none; font-weight:600; font-size:15px; }
      .danger { background:#dc2626; }
      .secondary { background:#374151; }
      .actions { display:flex; gap:8px; flex-wrap:wrap; margin-top:12px; }
      .table-wrap { overflow-x:auto; }
      table { width:100%; border-collapse:collapse; font-size:13px; }
      th, td { padding:8px 7px; border-bottom:1px solid var(--line); text-align:left; white-space:nowrap; }
      th { color:#cbd5e1; font-weight:600; }
      .empty { color:var(--muted); padding:12px 0; }
      .status-line { display:flex; gap:8px; flex-wrap:wrap; margin-top:8px; }
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

        self._send(_page("<main>Not found</main>"), HTTPStatus.NOT_FOUND)

    def _dashboard(self):
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
        positions, positions_error, pos_age = _get_positions_cached()
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
          <div class="grid">
            <section class="card"><h2>状态</h2>{status}{_table(counts, [('策略','stock_type'),('待买','buy_q'),('待卖','sell_q')])}</section>
            <section class="card"><h2>控制</h2>{control_form}</section>
          </div>
          <section class="card" style="margin-top:12px"><h2>券商持仓</h2>{_table(positions, [('代码','symbol'),('类型','asset_class'),('数量','qty'),('成本','avg_entry_price'),('现价','current_price'),('市值','market_value'),('浮盈亏','unrealized_pl'),('浮盈亏%','unrealized_plpc')])}</section>
          <section class="card" style="margin-top:12px"><h2>策略队列</h2>{_table(ops, [('代码','stock_code'),('类','stock_type'),('持仓','is_bought'),('买','can_buy'),('卖','can_sell'),('qty','qty'),('cost','cost_price'),('side','last_order_side'),('intent','last_order_intent'),('更新','updated_at')])}</section>
          <section class="card" style="margin-top:12px"><h2>期权组合</h2>{_table(spreads, [('ID','id'),('标的','underlying'),('模式','mode'),('到期','expiry'),('状态','status'),('入场','entry_price'),('风险','max_loss'),('更新','updated_at')])}</section>
        </main>
        <script>
          setTimeout(function() {{ window.location.reload(); }}, {POSITION_CACHE_SEC * 1000});
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
