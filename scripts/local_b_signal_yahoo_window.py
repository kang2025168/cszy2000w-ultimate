# -*- coding: utf-8 -*-
"""
Local mini window for Strategy B Yahoo signals.

Run:
  .venv/bin/python scripts/local_b_signal_yahoo_window.py

Then open:
  http://127.0.0.1:8877
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import scripts.local_b_signal_yahoo as signal_tool  # noqa: E402


PORT = int(os.getenv("B_SIGNAL_WINDOW_PORT", "8877"))
HOST = os.getenv("B_SIGNAL_WINDOW_HOST", "127.0.0.1")


HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>B Yahoo 信号</title>
  <style>
    :root { color-scheme: light; --ink:#101828; --muted:#667085; --line:#d8e4f0; --green:#08734f; --red:#b42318; --blue:#075985; --bg:#f5f8fc; }
    * { box-sizing:border-box; }
    body { margin:0; background:var(--bg); color:var(--ink); font:13px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }
    main { max-width:1180px; margin:0 auto; padding:14px; display:grid; gap:12px; }
    .top { display:flex; align-items:center; justify-content:space-between; gap:12px; padding:12px; border:1px solid var(--line); border-radius:10px; background:#fff; box-shadow:0 10px 28px rgba(15,23,42,.06); }
    h1 { margin:0; font-size:20px; letter-spacing:0; }
    .sub { color:var(--muted); font-size:12px; font-weight:750; }
    .actions { display:flex; align-items:center; gap:8px; flex-wrap:wrap; justify-content:flex-end; }
    button { height:34px; border:1px solid #cbd8e6; border-radius:8px; background:#fff; color:var(--ink); font-weight:850; padding:0 12px; cursor:pointer; }
    button.primary { border:0; background:#101828; color:#fff; }
    button.danger { border:0; background:var(--red); color:#fff; }
    button:disabled { opacity:.55; cursor:not-allowed; }
    label.check { display:inline-flex; align-items:center; gap:6px; height:34px; padding:0 10px; border:1px solid var(--line); border-radius:8px; background:#fff; color:#344054; font-weight:850; white-space:nowrap; }
    .config { display:grid; grid-template-columns:repeat(8,minmax(70px,1fr)); gap:8px; padding:12px; border:1px solid var(--line); border-radius:10px; background:#fff; }
    .field { display:grid; gap:4px; min-width:0; }
    .field label { color:var(--muted); font-size:11px; font-weight:850; }
    .field.wide { grid-column:span 2; }
    input { width:100%; height:34px; border:1px solid #cbd8e6; border-radius:8px; padding:0 9px; color:var(--ink); font-weight:800; outline:none; }
    input:focus { border-color:#93c5fd; box-shadow:0 0 0 3px rgba(37,99,235,.10); }
    .cards { display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:8px; }
    .card { min-height:68px; border:1px solid var(--line); border-radius:10px; background:#fff; padding:10px; display:grid; align-content:center; gap:4px; box-shadow:0 8px 20px rgba(15,23,42,.035); }
    .card span { color:var(--muted); font-size:11px; font-weight:850; }
    .card b { font-size:22px; line-height:1; }
    .card.buy b { color:var(--green); }
    .card.sell b { color:var(--red); }
    .current { display:grid; grid-template-columns:1.1fr repeat(5,minmax(90px,1fr)); gap:8px; padding:12px; border:1px solid var(--line); border-radius:10px; background:#fff; box-shadow:0 10px 28px rgba(15,23,42,.05); }
    .current .ticker { display:grid; align-content:center; gap:4px; min-height:82px; }
    .current .ticker span, .quote span { color:var(--muted); font-size:11px; font-weight:850; }
    .current .ticker b { font-size:26px; line-height:1; }
    .quote { display:grid; align-content:center; gap:4px; min-height:82px; padding:10px; border:1px solid #edf2f7; border-radius:8px; background:#f8fbff; }
    .quote b { font-size:20px; line-height:1.1; }
    .quote.reason { grid-column:span 2; }
    .quote.reason b { font-size:13px; line-height:1.35; color:#344054; }
    .panel { border:1px solid var(--line); border-radius:10px; background:#fff; overflow:hidden; box-shadow:0 10px 28px rgba(15,23,42,.05); }
    .panel-head { min-height:44px; display:flex; align-items:center; justify-content:space-between; gap:10px; padding:10px 12px; border-bottom:1px solid #e8eef6; background:linear-gradient(180deg,#fff,#f8fbff); }
    .status { color:var(--muted); font-weight:850; }
    .status.good { color:var(--green); }
    .status.bad { color:var(--red); }
    .table-wrap { max-height:520px; overflow:auto; }
    table { width:100%; border-collapse:collapse; min-width:980px; }
    th,td { border-bottom:1px solid #edf2f7; padding:9px 10px; text-align:left; white-space:nowrap; }
    th { position:sticky; top:0; background:#eef4fa; color:#344054; z-index:1; font-size:12px; }
    td.reason { white-space:normal; min-width:360px; max-width:560px; color:#344054; font-weight:760; }
    tr.buy td { background:#f2fbf6; }
    tr.sell td { background:#fff5f5; }
    tr.error td { background:#fff7ed; }
    tr.done td { color:#667085; background:#fafafa; }
    tr.active td { background:#eff6ff; box-shadow:inset 4px 0 0 #2563eb; }
    .pill { display:inline-flex; align-items:center; min-height:24px; border-radius:999px; padding:0 8px; background:#eef2f6; color:#344054; font-weight:900; }
    .pill.buy { background:#dcfce7; color:var(--green); }
    .pill.sell { background:#fee2e2; color:var(--red); }
    .pill.active { background:#dbeafe; color:#1d4ed8; }
    .pill.done { background:#e5e7eb; color:#475467; }
    .pill.error { background:#fee2e2; color:var(--red); }
    .foot { color:var(--muted); font-size:12px; font-weight:750; }
    @media (max-width: 860px) {
      main { padding:10px; }
      .top { align-items:flex-start; flex-direction:column; }
      .config { grid-template-columns:repeat(2,minmax(0,1fr)); }
      .cards { grid-template-columns:repeat(2,minmax(0,1fr)); }
      .current { grid-template-columns:repeat(2,minmax(0,1fr)); }
      .current .ticker, .quote.reason { grid-column:span 2; }
    }
  </style>
</head>
<body>
  <main>
    <section class="top">
      <div>
        <h1>B Yahoo 信号</h1>
        <div class="sub">本地 Yahoo 判断，写入 stock_operations 信号字段；默认只读预览</div>
      </div>
      <div class="actions">
        <button class="primary" id="runOnceBtn">刷新一次</button>
        <button id="loopBtn">开始循环</button>
        <label class="check"><input type="checkbox" id="writeMode"> 写入云库</label>
        <label class="check"><input type="checkbox" id="ignoreWindow"> 忽略时间窗</label>
      </div>
    </section>

    <section class="config">
      <div class="field"><label>Host</label><input id="dbHost" /></div>
      <div class="field"><label>Port</label><input id="dbPort" /></div>
      <div class="field"><label>DB</label><input id="dbName" /></div>
      <div class="field"><label>User</label><input id="dbUser" /></div>
      <div class="field"><label>Password</label><input id="dbPass" type="password" /></div>
      <div class="field"><label>间隔秒</label><input id="interval" /></div>
      <div class="field"><label>上限</label><input id="limit" /></div>
      <div class="field wide"><label>临时观察股票</label><input id="symbols" placeholder="留空=从云库 stock_operations 自动读取 B 候选/持仓" /></div>
    </section>

    <section class="cards">
      <div class="card"><span>扫描</span><b id="mCount">--</b></div>
      <div class="card buy"><span>买入信号</span><b id="mBuy">--</b></div>
      <div class="card sell"><span>卖出信号</span><b id="mSell">--</b></div>
      <div class="card"><span>过期清零</span><b id="mStale">--</b></div>
      <div class="card"><span>耗时秒</span><b id="mElapsed">--</b></div>
    </section>

    <section class="current">
      <div class="ticker"><span>当前循环股票</span><b id="cSymbol">--</b><span id="cRole">等待开始</span></div>
      <div class="quote"><span>动作</span><b id="cAction">--</b></div>
      <div class="quote"><span>价格</span><b id="cPrice">--</b></div>
      <div class="quote"><span>买入信号</span><b id="cBuy">--</b></div>
      <div class="quote"><span>卖出信号</span><b id="cSell">--</b></div>
      <div class="quote"><span>成交量</span><b id="cVolume">--</b></div>
      <div class="quote reason"><span>原因</span><b id="cReason">等待循环</b></div>
    </section>

    <section class="panel">
      <div class="panel-head">
        <strong>待循环股票</strong>
        <span><span class="status" id="sourceText">扫描来源: 云库 stock_operations</span> · <span class="status" id="statusText">未运行</span></span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr><th>序号</th><th>代码</th><th>角色</th><th>状态</th></tr>
          </thead>
          <tbody id="rowsBody"><tr><td colspan="4" class="reason">等待读取队列</td></tr></tbody>
        </table>
      </div>
    </section>
    <div class="foot" id="summaryText">只读模式不会改数据库。勾选“写入云库”后才会补字段并写入信号。</div>
  </main>
  <script>
    const $ = id => document.getElementById(id);
    let timer = null;
    let running = false;
    let queue = [];
    let cursorIndex = 0;
    let totals = { scanned: 0, buy: 0, sell: 0, errors: 0, elapsed: 0, stale: 0 };
    const initialConfig = __CONFIG__;
    function setInitial() {
      $('dbHost').value = initialConfig.host;
      $('dbPort').value = initialConfig.port;
      $('dbName').value = initialConfig.database;
      $('dbUser').value = initialConfig.user;
      $('dbPass').value = '';
      $('dbPass').placeholder = initialConfig.has_password ? '留空使用环境变量' : '';
      $('interval').value = localStorage.getItem('sig.interval') || '15';
      $('limit').value = localStorage.getItem('sig.limit') || initialConfig.limit;
      $('symbols').value = initialConfig.symbols || '';
    }
    function saveLocal() {
      for (const id of ['interval','limit']) {
        localStorage.setItem(`sig.${id}`, $(id).value);
      }
    }
    function payload() {
      saveLocal();
      return {
        host: $('dbHost').value,
        port: $('dbPort').value,
        database: $('dbName').value,
        user: $('dbUser').value,
        password: $('dbPass').value,
        interval: Number($('interval').value || 15),
        limit: Number($('limit').value || 300),
        symbols: $('symbols').value,
        dry_run: !$('writeMode').checked,
        ignore_window: $('ignoreWindow').checked
      };
    }
    function compact(v) {
      const n = Number(v || 0);
      if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
      if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
      return n > 0 ? String(Math.round(n)) : '--';
    }
    function esc(v) {
      return String(v ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
    }
    function renderTotals() {
      $('mCount').textContent = totals.scanned;
      $('mBuy').textContent = totals.buy;
      $('mSell').textContent = totals.sell;
      $('mStale').textContent = totals.stale;
      $('mElapsed').textContent = totals.elapsed ? totals.elapsed.toFixed(3) : '--';
    }
    function renderCurrent(row, pendingLabel) {
      if (!row) {
        $('cSymbol').textContent = '--';
        $('cRole').textContent = pendingLabel || '等待开始';
        $('cAction').textContent = '--';
        $('cPrice').textContent = '--';
        $('cBuy').textContent = '--';
        $('cSell').textContent = '--';
        $('cVolume').textContent = '--';
        $('cReason').textContent = '等待循环';
        return;
      }
      $('cSymbol').textContent = row.symbol || '--';
      $('cRole').textContent = row.role ? `角色: ${row.role}` : '扫描中';
      $('cAction').textContent = row.signal_action || '扫描中';
      $('cPrice').textContent = row.signal_price ? `$${Number(row.signal_price).toFixed(2)}` : '--';
      $('cBuy').textContent = Number(row.buy_signal || 0);
      $('cSell').textContent = Number(row.sell_signal || 0);
      $('cVolume').textContent = compact(row.signal_volume);
      $('cReason').textContent = row.signal_reason || '正在拉取 Yahoo 数据';
    }
    function queueLeft() {
      return queue.filter(r => r.status === 'pending').length;
    }
    function renderQueue(activeIndex = -1) {
      if (!queue.length) {
        $('rowsBody').innerHTML = '<tr><td colspan="4" class="reason">本轮队列为空，下一轮会重新从云库读取</td></tr>';
        return;
      }
      $('rowsBody').innerHTML = queue.map((r, i) => {
        const state = i === activeIndex ? 'active' : r.status || 'pending';
        const label = state === 'active' ? '当前' : state === 'done' ? '已循环' : state === 'error' ? '错误' : '待循环';
        return `<tr class="${state}">
        <td>${i + 1}</td>
        <td><b>${esc(r.symbol)}</b></td>
        <td>${esc(r.role)}</td>
        <td><span class="pill ${state}">${label}</span></td>
      </tr>`;
      }).join('');
    }
    function renderSummary() {
      const mode = $('writeMode').checked ? '写入' : '只读';
      $('summaryText').textContent = `${mode} | ${$('sourceText').textContent.replace('扫描来源: ', '')} | 已扫 ${totals.scanned} | 待循环 ${queueLeft()} | 买 ${totals.buy} | 卖 ${totals.sell} | 错误 ${totals.errors}`;
    }
    async function loadQueue() {
      const resp = await fetch('/api/queue', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload())});
      const data = await resp.json();
      if (!data.ok) throw new Error(data.error || '读取队列失败');
      queue = (data.rows || []).map(r => ({...r, status: 'pending'}));
      cursorIndex = 0;
      totals.stale += Number(data.stale_reset || 0);
      $('sourceText').textContent = `扫描来源: ${data.source_mode || '云库 stock_operations'}`;
      renderTotals();
      renderQueue();
      renderSummary();
      return queue;
    }
    async function scanOne(item, index) {
      renderCurrent({symbol:item.symbol, role:item.role, signal_action:'扫描中'}, '');
      renderQueue(index);
      const body = payload();
      body.symbol = item.symbol;
      const resp = await fetch('/api/scan-symbol', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
      const data = await resp.json();
      totals.scanned += 1;
      totals.elapsed += Number(data.elapsed_sec || 0);
      if (!data.ok) {
        totals.errors += 1;
        item.status = 'error';
        renderCurrent({symbol:item.symbol, role:item.role, signal_action:'ERROR', signal_reason:data.error || '扫描失败'}, '');
      } else {
        const row = data.row || {};
        totals.buy += Number(row.buy_signal || 0);
        totals.sell += Number(row.sell_signal || 0);
        item.status = 'done';
        renderCurrent(row, '');
      }
      renderQueue(index);
      renderTotals();
      renderSummary();
      return data;
    }
    async function runOnce() {
      $('statusText').textContent = '读取队列...';
      $('statusText').className = 'status';
      $('runOnceBtn').disabled = true;
      try {
        totals = { scanned: 0, buy: 0, sell: 0, errors: 0, elapsed: 0, stale: 0 };
        await loadQueue();
        const first = queue[0];
        if (first) {
          $('statusText').textContent = '扫描中...';
          await scanOne(first, 0);
          cursorIndex = 1;
          renderQueue();
        } else {
          renderCurrent(null, '暂无待循环股票');
        }
        $('statusText').textContent = totals.errors ? '有错误' : '运行正常';
        $('statusText').className = `status ${totals.errors ? 'bad' : 'good'}`;
      } catch (err) {
        $('statusText').textContent = '请求失败';
        $('statusText').className = 'status bad';
        $('summaryText').textContent = String(err);
      } finally {
        $('runOnceBtn').disabled = false;
      }
    }
    async function loopStep() {
      try {
        if (!queue.length || cursorIndex >= queue.length) {
          $('statusText').textContent = '读取队列...';
          await loadQueue();
        }
        const next = queue[cursorIndex];
        if (!next) {
          renderCurrent(null, '暂无待循环股票');
          $('statusText').textContent = '队列为空';
          $('statusText').className = 'status';
          return;
        }
        $('statusText').textContent = '扫描中...';
        $('statusText').className = 'status';
        await scanOne(next, cursorIndex);
        cursorIndex += 1;
        renderQueue();
        $('statusText').textContent = totals.errors ? '有错误' : '循环中';
        $('statusText').className = `status ${totals.errors ? 'bad' : 'good'}`;
      } catch (err) {
        totals.errors += 1;
        $('statusText').textContent = '请求失败';
        $('statusText').className = 'status bad';
        $('summaryText').textContent = String(err);
      }
    }
    async function toggleLoop() {
      running = !running;
      $('loopBtn').textContent = running ? '停止循环' : '开始循环';
      if (timer) clearInterval(timer);
      timer = null;
      if (running) {
        totals = { scanned: 0, buy: 0, sell: 0, errors: 0, elapsed: 0, stale: 0 };
        await loopStep();
        timer = setInterval(loopStep, Math.max(Number($('interval').value || 15), 1) * 1000);
      }
    }
    $('runOnceBtn').addEventListener('click', runOnce);
    $('loopBtn').addEventListener('click', toggleLoop);
    setInitial();
    runOnce();
  </script>
</body>
</html>
"""


def _json_response(handler: BaseHTTPRequestHandler, payload: dict, status: int = 200) -> None:
    body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _apply_payload_config(payload: dict) -> SimpleNamespace:
    password = str(payload.get("password") or "")
    signal_tool.DB.update(
        {
            "host": str(payload.get("host") or "localhost").strip(),
            "port": int(float(payload.get("port") or 3307)),
            "database": str(payload.get("database") or "cszy2000").strip(),
            "user": str(payload.get("user") or "tradebot").strip(),
        }
    )
    if password:
        signal_tool.DB["password"] = password
    signal_tool.B_SIGNAL_LIMIT = int(float(payload.get("limit") or 300))
    signal_tool.B_SIGNAL_IGNORE_WINDOW = bool(payload.get("ignore_window"))
    return SimpleNamespace(
        dry_run=bool(payload.get("dry_run", True)),
        once=True,
        loop=False,
        interval=float(payload.get("interval") or 15),
        symbols=str(payload.get("symbols") or ""),
        json=False,
    )


def _source_mode(args: SimpleNamespace) -> str:
    return "临时观察股票" if args.symbols.strip() else "云库 stock_operations"


def _queue_payload(args: SimpleNamespace) -> dict:
    symbols = [s.strip().upper() for s in (args.symbols or "").split(",") if s.strip()]
    with signal_tool._connect() as conn:
        if not args.dry_run:
            signal_tool._ensure_columns(conn)
        stale_reset = 0 if args.dry_run else signal_tool._clear_stale_signals(conn)
        rows = signal_tool._load_b_rows(conn, symbols=symbols)
        seen = {str(row.get("stock_code") or "").strip().upper() for row in rows}
        queue = [
            {
                "symbol": str(row.get("stock_code") or "").strip().upper(),
                "role": "sell" if signal_tool._safe_int(row.get("is_bought")) == 1 else "buy",
            }
            for row in rows
            if str(row.get("stock_code") or "").strip()
        ]
        queue.extend({"symbol": symbol, "role": "watch"} for symbol in symbols if symbol not in seen)
    return {
        "ok": True,
        "dry_run": bool(args.dry_run),
        "rows": queue,
        "count": len(queue),
        "stale_reset": stale_reset,
        "source_mode": _source_mode(args),
    }


def _scan_symbol_payload(args: SimpleNamespace, symbol: str) -> dict:
    started = time.time()
    symbol = (symbol or "").strip().upper()
    if not symbol:
        return {"ok": False, "error": "missing symbol", "elapsed_sec": 0}
    with signal_tool._connect() as conn:
        if not args.dry_run:
            signal_tool._ensure_columns(conn)
        rows = signal_tool._load_b_rows(conn, symbols=[symbol])
        if rows:
            decision = signal_tool._decide_row(conn, rows[0])
            if not args.dry_run:
                signal_tool._write_signal(conn, decision)
        else:
            decision = signal_tool._quote_only_decision(conn, symbol)
        return {
            "ok": True,
            "dry_run": bool(args.dry_run),
            "row": asdict(decision),
            "elapsed_sec": round(time.time() - started, 3),
            "source_mode": _source_mode(args),
        }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path != "/":
            self.send_error(404)
            return
        config = {
            "host": "127.0.0.1" if str(signal_tool.DB["host"]).strip() == "mysql" else signal_tool.DB["host"],
            "port": signal_tool.DB["port"],
            "database": signal_tool.DB["database"],
            "user": signal_tool.DB["user"],
            "has_password": bool(signal_tool.DB["password"]),
            "limit": signal_tool.B_SIGNAL_LIMIT,
            "symbols": os.getenv("B_SIGNAL_WINDOW_SYMBOLS", ""),
        }
        html = HTML.replace("__CONFIG__", json.dumps(config, ensure_ascii=False))
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path not in {"/api/run", "/api/queue", "/api/scan-symbol"}:
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
            payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
            args = _apply_payload_config(payload)
            if parsed.path == "/api/queue":
                summary = _queue_payload(args)
            elif parsed.path == "/api/scan-symbol":
                summary = _scan_symbol_payload(args, str(payload.get("symbol") or ""))
            else:
                summary = signal_tool.run_once(args)
                summary["source_mode"] = _source_mode(args)
            _json_response(self, summary)
        except Exception as exc:
            _json_response(self, {"ok": False, "error": str(exc), "rows": [], "errors": [{"symbol": "", "error": str(exc)}]}, 500)


def main() -> int:
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"[B SIGNAL WINDOW] http://{HOST}:{PORT}", flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
