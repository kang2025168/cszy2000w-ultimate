from __future__ import annotations

"""轻量网页看板：展示资金池、风控状态和 position_holdings 持仓。"""

import json
from datetime import date, datetime
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .bot_supervisor import managed_bot_names, process_status, set_bot_runtime, sync_from_controls
from .capital_manager import get_capital_allocation, get_strategy_used_capital
from .config import env_str, settings
from .db import fetch_all
from .rebalance_monthly import generate_rebalance_report
from .risk_controller import get_risk_state
from .schema import ensure_schema
from .state_store import add_command, bot_controls, bot_heartbeats, capital_state_rows, equity_curve, latest_risk_state
from .sync_positions import last_sync_error, sync_position_holdings


def _json_default(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _allocation_payload() -> dict:
    """组装资金池接口数据。"""
    allocation = get_capital_allocation()
    if allocation is None:
        return {"ok": False, "error": "account_snapshot_failed"}
    used = allocation.used
    available = allocation.available
    usable_total = sum(allocation.target_for(g) for g in ("A", "B", "C", "D"))
    base_total = sum(allocation.base_targets.get(g, 0.0) for g in ("A", "B", "C", "D"))
    used_total = sum(used.get(g, 0.0) for g in ("A", "B", "C", "D"))
    return {
        "ok": True,
        "mode": allocation.mode,
        "allocation_month": allocation.allocation_month,
        "equity": allocation.equity,
        "buying_power": allocation.buying_power,
        "cash": allocation.cash,
        "portfolio_value": allocation.portfolio_value,
        "base_total": base_total,
        "usable_total": usable_total,
        "used_total": used_total,
        "total_risk_percent": allocation.total_risk_percent,
        "targets": {
            "A": allocation.A_target,
            "B": allocation.B_target,
            "C": allocation.C_target,
            "D": allocation.D_target,
        },
        "base_targets": allocation.base_targets,
        "base_percents": allocation.base_percents,
        "pool_risk_percents": allocation.pool_risk_percents,
        "used": used,
        "available": available,
    }


def _risk_payload() -> dict:
    """组装风控接口数据。"""
    state = get_risk_state()
    return {
        "enabled": state.enabled,
        "mode": state.mode,
        "daily_pnl_pct": state.daily_pnl_pct,
        "loss_days": state.loss_days,
        "max_drawdown": state.max_drawdown,
        "risk_multiplier": state.risk_multiplier,
        "block_all_new": state.block_all_new,
        "block_b": state.block_b,
        "block_d": state.block_d,
        "suggest_mode": state.suggest_mode,
        "reason": state.reason,
    }


def _holdings_payload() -> dict:
    """读取持仓展示表，供前端表格渲染。"""
    rows = fetch_all(
        """
        SELECT symbol,
               CASE
                   WHEN strategy_group IN ('A','B','C','D') THEN strategy_group
                   WHEN stock_type IN ('A','B','C','D') THEN stock_type
                   ELSE strategy_group
               END AS strategy_group,
               stock_type, status, qty, avg_entry_price,
               current_price, market_value, cost_basis, unrealized_pnl,
               unrealized_pnl_pct, realized_pnl, entry_time, exit_time,
               holding_days, stop_loss_price, take_profit_price, b_stage,
               capital_pool, margin_used, last_order_side, last_update_time
        FROM position_holdings
        ORDER BY FIELD(status, 'open', 'needs_review', 'closed'), strategy_group, symbol, id DESC
        LIMIT 500
        """
    )
    return {"ok": True, "rows": rows}


def _state_payload() -> dict:
    """读取中央状态：最新风控、资金状态、机器人心跳。"""
    return {
        "ok": True,
        "risk_state": latest_risk_state(),
        "capital_state": capital_state_rows(),
        "bot_heartbeats": bot_heartbeats(),
        "bot_controls": bot_controls(),
        "bot_processes": process_status(),
    }


def _curve_payload(period: str) -> dict:
    """读取账户收益曲线数据。"""
    payload = equity_curve(period)
    payload["ok"] = True
    return payload


INDEX_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>CSZY Ultimate V1</title>
  <style>
    :root { color-scheme: light; --bg:#f4f6f8; --panel:#ffffff; --ink:#17202a; --muted:#667085; --line:#d7dde5; --green:#15936a; --red:#c62828; --amber:#b76e00; --blue:#2563eb; --cyan:#0891b2; --violet:#7c3aed; --gold:#d97706; }
    * { box-sizing: border-box; }
    body { margin:0; font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background:var(--bg); color:var(--ink); }
    header { display:none; }
    h1 { font-size:26px; margin:0; letter-spacing:0; line-height:1; }
    h2 { font-size:15px; margin:0; }
    button { border:1px solid var(--line); background:#fff; color:var(--ink); height:34px; padding:0 12px; border-radius:6px; cursor:pointer; }
    main { padding:18px 24px 34px; max-width:1680px; margin:0 auto; }
    .left-titlebar { height:52px; display:flex; align-items:center; justify-content:space-between; gap:14px; padding:0 8px 0 18px; }
    .brand-lockup { display:flex; align-items:center; gap:12px; min-width:0; }
    .brand-logo { width:42px; height:42px; border-radius:9px; object-fit:contain; background:#fff; box-shadow:0 8px 20px rgba(15,23,42,.08); }
    .refresh-btn { height:38px; padding:0 18px; border:0; border-radius:9px; background:#2563eb; color:#fff; font-weight:850; box-shadow:0 9px 22px rgba(37,99,235,.22); transition:transform .12s ease, background .12s ease, opacity .12s ease; }
    .refresh-btn:hover { background:#1d4ed8; }
    .refresh-btn:active { transform:scale(.96); }
    .refresh-btn.loading { opacity:.72; pointer-events:none; }
    .dash { display:grid; grid-template-columns:minmax(560px, 1.08fr) minmax(520px, .92fr); gap:16px; align-items:stretch; }
    .panel { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:16px; box-shadow:0 12px 30px rgba(15,23,42,.04); }
    .left-stack, .right-stack { display:flex; flex-direction:column; gap:16px; min-width:0; }
    .capital-hero { flex:1; }
    .hero-top { display:grid; grid-template-columns:.92fr 1.08fr; gap:12px; }
    .mode-card { background:#101828; color:#fff; border-radius:8px; padding:16px 14px; min-height:132px; display:flex; flex-direction:column; justify-content:space-between; overflow:hidden; }
    .mode-card .label { color:#b8c1d1; font-size:13px; }
    .mode-card .value { font-size:32px; line-height:1; font-weight:800; margin-top:10px; }
    .metric-grid { display:grid; grid-template-columns:repeat(2, minmax(0,1fr)); gap:12px; }
    .metric { border:1px solid var(--line); border-radius:8px; padding:15px 16px; min-height:76px; }
    .metric-label, .pool-meta, .small-muted { color:var(--muted); font-size:12px; }
    .metric-value { font-size:20px; font-weight:850; margin-top:6px; line-height:1.1; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .risk-strip { margin-top:14px; border:1px solid var(--line); border-radius:8px; padding:12px; display:flex; align-items:center; justify-content:space-between; gap:12px; }
    .risk-line { display:flex; gap:12px; flex-wrap:wrap; color:var(--muted); font-size:12px; }
    .risk-actions { display:flex; align-items:center; gap:10px; flex:0 0 auto; }
    .risk-badge { font-size:13px; font-weight:700; padding:5px 9px; border-radius:999px; background:#e7f6ef; color:var(--green); white-space:nowrap; }
    .clear-btn { height:30px; padding:0 14px; border:0; border-radius:7px; background:#fee2e2; color:#b42318; font-weight:850; }
    .clear-btn:hover { background:#fecaca; }
    .exposure-card { margin-top:14px; border:1px solid var(--line); border-radius:8px; padding:12px 14px; background:#fbfcfe; }
    .exposure-head { display:flex; align-items:center; justify-content:space-between; gap:12px; font-size:13px; font-weight:800; }
    .exposure-value { color:var(--muted); font-size:12px; font-weight:700; }
    .exposure-bar { height:12px; border-radius:999px; overflow:hidden; background:#e9edf3; margin-top:10px; }
    .exposure-fill { height:100%; width:0%; background:linear-gradient(90deg, #15936a, #d97706); }
    .pool-grid { margin-top:26px; display:grid; grid-template-columns:repeat(2, minmax(0,1fr)); gap:12px; }
    .pool-card { border:1px solid var(--line); border-radius:8px; padding:14px; min-height:126px; }
    .pool-head { display:flex; justify-content:space-between; align-items:center; gap:10px; }
    .pool-name { font-size:13px; color:var(--muted); font-weight:700; }
    .pool-value { font-size:25px; font-weight:850; margin-top:8px; line-height:1.1; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .pool-amounts { margin-top:2px; display:flex; justify-content:space-between; gap:10px; color:var(--muted); font-size:12px; }
    .pool-amounts span { min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .bar { height:9px; border-radius:999px; overflow:hidden; background:#e9edf3; margin-top:11px; }
    .fill { height:100%; width:0%; background:var(--blue); }
    .right-top { display:grid; grid-template-columns:1.05fr .95fr; gap:16px; min-height:286px; }
    .donut-panel, .bot-panel { min-height:286px; display:flex; flex-direction:column; }
    .donut-wrap { flex:1; display:flex; align-items:center; justify-content:center; gap:18px; min-height:160px; }
    canvas { max-width:100%; }
    #capitalDonut { width:176px; height:176px; }
    .legend { display:grid; gap:8px; min-width:120px; }
    .legend-row { display:flex; align-items:center; gap:8px; font-size:12px; color:var(--muted); }
    .swatch { width:9px; height:9px; border-radius:2px; }
    .bot-grid { flex:1; display:flex; flex-direction:column; gap:9px; padding:12px 2px 4px; }
    .bot-row { display:grid; grid-template-columns:minmax(90px,1fr) 18px 42px; align-items:center; gap:10px; min-height:24px; font-size:12px; color:var(--ink); }
    .bot-name { overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .bot-dot { width:14px; height:14px; flex:0 0 auto; border-radius:50%; box-shadow:0 0 0 4px rgba(21,147,106,.10), inset 0 0 0 1px rgba(255,255,255,.8); background:var(--green); }
    .bot-dot.bad { background:var(--red); box-shadow:0 0 0 4px rgba(198,40,40,.10), inset 0 0 0 1px rgba(255,255,255,.8); }
    .bot-switch { width:38px; height:20px; border-radius:999px; border:0; padding:2px; background:#d0d5dd; position:relative; }
    .bot-switch::after { content:""; display:block; width:16px; height:16px; border-radius:50%; background:#fff; box-shadow:0 1px 4px rgba(15,23,42,.2); transition:transform .15s ease; }
    .bot-switch.on { background:#15936a; }
    .bot-switch.on::after { transform:translateX(18px); }
    .chart-panel { flex:1; min-height:0; display:flex; flex-direction:column; }
    .chart-head { display:flex; justify-content:space-between; align-items:center; gap:12px; margin-bottom:10px; }
    .chart-title { display:flex; align-items:baseline; gap:14px; }
    .today-pnl { font-size:15px; font-weight:850; color:var(--green); }
    .tabs { display:flex; gap:6px; flex-wrap:wrap; }
    .tab { height:28px; border-radius:6px; padding:0 10px; color:var(--muted); }
    .tab.active { background:#101828; color:#fff; border-color:#101828; }
    #equityChart { width:100%; flex:1; min-height:260px; }
    .section-head { display:flex; align-items:center; justify-content:space-between; margin:18px 0 10px; }
    table { width:100%; border-collapse:collapse; background:#fff; border:1px solid var(--line); border-radius:8px; overflow:hidden; }
    th, td { border-bottom:1px solid var(--line); padding:10px 9px; text-align:left; font-size:13px; white-space:nowrap; }
    th { background:#eef2f6; color:#344054; font-size:12px; }
    tr:last-child td { border-bottom:0; }
    .status { display:inline-block; min-width:64px; text-align:center; padding:3px 8px; border-radius:999px; background:#eef2f6; }
    .open { color:var(--green); background:#e7f6ef; }
    .closed { color:var(--muted); }
    .needs_review { color:var(--amber); background:#fff3d6; }
    .neg { color:var(--red); }
    .pos { color:var(--green); }
    .scroll { overflow:auto; border-radius:8px; }
    .holdings-panel { margin-top:16px; min-height:430px; }
    .holding-head { justify-content:flex-start; gap:16px; }
    .holding-tabs { display:flex; gap:6px; flex-wrap:wrap; background:#eef2f6; padding:5px; border-radius:8px; }
    .holding-tab { height:30px; min-width:58px; border-radius:7px; font-weight:750; color:var(--muted); border:0; background:transparent; }
    .holding-tab.active { background:#101828; color:#fff; border-color:#101828; }
    .sync-positions-btn { height:34px; border:0; border-radius:8px; padding:0 14px; background:#e0f2fe; color:#075985; font-weight:850; transition:transform .12s ease, background .12s ease, opacity .12s ease; }
    .sync-positions-btn:hover { background:#bae6fd; }
    .sync-positions-btn:active { transform:scale(.97); }
    .sync-positions-btn.loading { opacity:.65; pointer-events:none; }
    .modal-backdrop { position:fixed; inset:0; background:rgba(15,23,42,.36); display:none; align-items:center; justify-content:center; z-index:20; }
    .modal-backdrop.show { display:flex; }
    .modal { width:min(420px, calc(100vw - 32px)); background:#fff; border-radius:10px; border:1px solid var(--line); box-shadow:0 24px 70px rgba(15,23,42,.22); padding:18px; }
    .modal p { margin:10px 0 14px; color:var(--muted); font-size:13px; }
    .modal input { width:100%; height:38px; border:1px solid var(--line); border-radius:7px; padding:0 10px; }
    .modal-actions { margin-top:14px; display:flex; justify-content:flex-end; gap:8px; }
    .danger-action { border:0; background:#b42318; color:#fff; font-weight:800; }
    @media (max-width: 1180px) { .dash { grid-template-columns:1fr; } .capital-hero { flex:none; } .chart-panel { min-height:324px; } }
    @media (max-width: 760px) { header { padding:0 14px; } main { padding:14px; } .left-titlebar { padding-left:4px; } .hero-top, .metric-grid, .pool-grid, .right-top { grid-template-columns:1fr; } }
  </style>
</head>
<body>
  <header>
  </header>
  <main>
    <section class="dash">
      <div class="left-stack">
        <div class="left-titlebar"><div class="brand-lockup"><img class="brand-logo" src="/assets/cszy_ultimate_logo.png" alt="CSZY Ultimate logo" /><h1>CSZY Ultimate V1</h1></div><button class="refresh-btn" onclick="loadAll()">刷新</button></div>
        <div class="panel capital-hero">
          <div class="hero-top">
            <div class="mode-card">
              <div>
                <div class="label">资金模式</div>
                <div class="value" id="modeValue">--</div>
              </div>
              <div class="small-muted" id="modeHint">等待账户数据</div>
            </div>
            <div class="metric-grid" id="metrics"></div>
          </div>
          <div class="risk-strip">
            <div>
              <h2>风险状态</h2>
              <div class="risk-line" id="risk"></div>
            </div>
            <div class="risk-actions">
              <button class="clear-btn" onclick="openClearModal()">清仓</button>
              <span class="risk-badge" id="riskBadge">--</span>
            </div>
          </div>
          <div class="exposure-card">
            <div class="exposure-head">
              <span>总持仓比例</span>
              <span class="exposure-value" id="exposureValue">--</span>
            </div>
            <div class="exposure-bar"><div class="exposure-fill" id="exposureFill"></div></div>
          </div>
          <div class="pool-grid" id="pools"></div>
        </div>
      </div>
      <div class="right-stack">
        <div class="right-top">
          <div class="panel donut-panel">
            <h2>资金比例</h2>
            <div class="donut-wrap">
              <canvas id="capitalDonut" width="220" height="220"></canvas>
              <div class="legend" id="donutLegend"></div>
            </div>
          </div>
          <div class="panel bot-panel">
            <h2>机器人</h2>
            <div class="bot-grid" id="botLights"></div>
          </div>
        </div>
        <div class="panel chart-panel">
          <div class="chart-head">
            <div class="chart-title"><h2>收益曲线</h2><span class="today-pnl" id="todayPnl">今日收益 --</span></div>
            <div class="tabs">
              <button class="tab active" data-period="week">周</button>
              <button class="tab" data-period="month">月</button>
              <button class="tab" data-period="year">年</button>
              <button class="tab" data-period="all">所有</button>
            </div>
          </div>
          <canvas id="equityChart" width="760" height="260"></canvas>
        </div>
      </div>
    </section>
    <section class="panel holdings-panel">
      <div class="section-head holding-head">
        <h2>持仓</h2>
        <div class="holding-tabs">
          <button class="holding-tab active" data-holding="ALL">总</button>
          <button class="holding-tab" data-holding="A">A</button>
          <button class="holding-tab" data-holding="C">C</button>
          <button class="holding-tab" data-holding="B">B</button>
          <button class="holding-tab" data-holding="D">D</button>
        </div>
        <button class="sync-positions-btn" onclick="syncPositions()">同步仓位</button>
      </div>
      <div class="scroll"><table id="holdings"></table></div>
    </section>
  </main>
  <div class="modal-backdrop" id="clearModal">
    <div class="modal">
      <h2>确认清仓</h2>
      <p>该操作会写入清仓命令。请输入操作密码确认。</p>
      <input id="clearPassword" type="password" placeholder="操作密码" />
      <div class="modal-actions">
        <button onclick="closeClearModal()">取消</button>
        <button class="danger-action" onclick="submitClearPosition()">确认清仓</button>
      </div>
    </div>
  </div>
  <script>
    const money = v => Number(v || 0).toLocaleString(undefined, {style:'currency', currency:'USD'});
    const pct = v => `${(Number(v || 0) * 100).toFixed(2)}%`;
    const cls = v => Number(v || 0) < 0 ? 'neg' : Number(v || 0) > 0 ? 'pos' : '';
    const colors = {A:'#2563eb', B:'#d97706', C:'#15936a', D:'#7c3aed'};
    let currentPeriod = 'week';
    let currentHolding = 'ALL';
    let latestHoldings = [];
    async function api(path) { const r = await fetch(path); return await r.json(); }
    async function postJson(path, body) { const r = await fetch(path, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body || {})}); return await r.json(); }
    function metric(label, value) { return `<div class="metric"><div class="metric-label">${label}</div><div class="metric-value">${value}</div></div>`; }
    function poolCard(g, cap) {
      const target = Number(cap.targets[g] || 0), used = Number(cap.used[g] || 0), av = Number(cap.available[g] || 0);
      const w = target > 0 ? Math.min(100, used / target * 100) : 0;
      const basePct = Number(cap.base_percents?.[g] || 0) * 100;
      const riskPct = Number(cap.total_risk_percent || 0) * Number(cap.pool_risk_percents?.[g] || 0) * 100;
      return `<div class="pool-card"><div class="pool-head"><div><div class="pool-name">${g} 资金池</div><div class="small-muted">月度 ${basePct.toFixed(1)}% · 可用 ${riskPct.toFixed(0)}%</div></div><div class="small-muted">${w.toFixed(1)}%</div></div><div class="pool-value">${money(used)}</div><div class="pool-amounts"><span>target ${money(target)}</span><span>实时额度 ${money(av)}</span></div><div class="bar"><div class="fill" style="width:${w}%;background:${colors[g]}"></div></div></div>`;
    }
    function drawDonut(cap) {
      const canvas = document.getElementById('capitalDonut'), ctx = canvas.getContext('2d');
      const entries = ['A','B','C','D'].map(g => [g, Number(cap.targets?.[g] || 0)]).filter(x => x[1] > 0);
      const total = entries.reduce((s, x) => s + x[1], 0) || 1;
      ctx.clearRect(0,0,canvas.width,canvas.height);
      let start = -Math.PI / 2;
      entries.forEach(([g, value]) => {
        const a = value / total * Math.PI * 2;
        ctx.beginPath(); ctx.moveTo(110,110); ctx.arc(110,110,92,start,start+a); ctx.closePath(); ctx.fillStyle = colors[g]; ctx.fill(); start += a;
      });
      ctx.beginPath(); ctx.arc(110,110,58,0,Math.PI*2); ctx.fillStyle = '#fff'; ctx.fill();
      ctx.fillStyle = '#17202a'; ctx.font = '700 20px system-ui'; ctx.textAlign='center'; ctx.fillText(money(cap.equity || 0).replace('.00',''),110,106);
      ctx.fillStyle = '#667085'; ctx.font = '12px system-ui'; ctx.fillText('equity',110,126);
      document.getElementById('donutLegend').innerHTML = entries.map(([g,v]) => `<div class="legend-row"><span class="swatch" style="background:${colors[g]}"></span><span>${g}</span><span>${((v/total)*100).toFixed(1)}%</span></div>`).join('');
    }
    function renderBots(bots, controls) {
      const known = ['dashboard_bot','risk_bot','ac_bot','b_buy_bot','b_sell_bot','d_buy_bot','d_sell_bot'];
      const byName = Object.fromEntries((bots || []).map(b => [b.bot_name, b]));
      const processMap = Object.fromEntries(((window.latestBotProcesses || [])).map(b => [b.bot_name, b]));
      const controlMap = Object.fromEntries((controls || []).map(b => [b.bot_name, Number(b.enabled) === 1]));
      document.getElementById('botLights').innerHTML = known.map(name => {
        const b = byName[name];
        const p = processMap[name];
        const ok = Boolean(p && p.running) || Boolean(b && b.status === 'running');
        const controllable = controlMap[name] !== undefined;
        const enabled = controlMap[name] !== false;
        const title = b ? `${name} ${b.status} pid=${p?.pid || '-'} ${b.last_seen_at || ''} ${b.last_message || ''}` : `${name} no heartbeat pid=${p?.pid || '-'}`;
        return `<div class="bot-row" title="${title}"><span class="bot-name">${name}</span><span class="bot-dot ${ok ? '' : 'bad'}"></span>${controllable ? `<button class="bot-switch ${enabled ? 'on' : ''}" onclick="toggleBot('${name}', ${enabled ? 'false' : 'true'})"></button>` : '<span></span>'}</div>`;
      }).join('');
    }
    function parseDateOnly(s) {
      if (!s) return null;
      const [y,m,d] = String(s).slice(0,10).split('-').map(Number);
      return new Date(y, m - 1, d);
    }
    function dayDiff(a,b) { return Math.round((b-a)/86400000); }
    function mmdd(d) { return `${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`; }
    function drawChart(curve) {
      const canvas = document.getElementById('equityChart'), ctx = canvas.getContext('2d');
      const rect = canvas.getBoundingClientRect();
      if (rect.width > 0 && rect.height > 0) {
        canvas.width = Math.floor(rect.width * window.devicePixelRatio);
        canvas.height = Math.floor(rect.height * window.devicePixelRatio);
        ctx.setTransform(window.devicePixelRatio, 0, 0, window.devicePixelRatio, 0, 0);
      }
      const w = rect.width || canvas.width, h = rect.height || canvas.height, pad = 34;
      ctx.clearRect(0,0,w,h);
      ctx.fillStyle = '#fff'; ctx.fillRect(0,0,w,h);
      const rows = curve.rows || [];
      const startDate = parseDateOnly(curve.start_date) || parseDateOnly(rows[0]?.snapshot_date || rows[0]?.created_at);
      const endDate = parseDateOnly(curve.end_date) || parseDateOnly(rows[rows.length-1]?.snapshot_date || rows[rows.length-1]?.created_at);
      const totalDays = startDate && endDate ? Math.max(1, dayDiff(startDate, endDate)) : 1;
      const points = rows.map(r => ({d:parseDateOnly(r.snapshot_date || r.created_at), t:r.snapshot_date || r.created_at, y:Number(r.equity || r.portfolio_value || 0)})).filter(p => p.d);
      if (points.length === 0) {
        ctx.fillStyle = '#667085'; ctx.font='14px system-ui'; ctx.textAlign='center'; ctx.fillText('暂无收益曲线数据，等待 dashboard_bot 记录账户快照', w/2, h/2);
        if (startDate && endDate) {
          ctx.fillStyle = '#667085'; ctx.font='11px system-ui'; ctx.fillText(`${mmdd(startDate)} 到 ${mmdd(endDate)}`, w/2, h-12);
        }
        return;
      }
      const ys = points.map(p => p.y), min = Math.min(...ys), max = Math.max(...ys), span = Math.max(1, max-min);
      ctx.strokeStyle = '#d7dde5'; ctx.lineWidth = 1;
      ctx.fillStyle = '#667085'; ctx.font='11px system-ui'; ctx.textAlign='right';
      for (let i=0;i<4;i++){
        const y=pad+i*(h-pad*2)/3;
        const value = max - i*span/3;
        ctx.beginPath(); ctx.moveTo(pad,y); ctx.lineTo(w-pad,y); ctx.stroke();
        ctx.fillText(`${(value/1000).toFixed(0)}k`, pad-7, y+4);
      }
      ctx.beginPath();
      points.forEach((p,i) => {
        const offset = startDate ? Math.max(0, Math.min(totalDays, dayDiff(startDate, p.d))) : i;
        const x = startDate ? pad + offset*(w-pad*2)/totalDays : (points.length === 1 ? w/2 : pad + i*(w-pad*2)/(points.length-1));
        const y = h-pad - ((p.y-min)/span)*(h-pad*2);
        if (i===0) ctx.moveTo(x,y); else ctx.lineTo(x,y);
      });
      ctx.strokeStyle = points[points.length-1].y >= points[0].y ? '#15936a' : '#c62828';
      ctx.lineWidth = 3; ctx.stroke();
      const last = points[points.length-1].y, first = points[0].y, diff = last-first;
      ctx.fillStyle = diff >= 0 ? '#15936a' : '#c62828';
      ctx.font='700 16px system-ui'; ctx.textAlign='left'; ctx.fillText(`${money(last)}  ${diff>=0?'+':''}${money(diff)}`, pad, 22);
      ctx.fillStyle = '#667085'; ctx.font='11px system-ui'; ctx.textAlign='center';
      const firstLabel = startDate ? mmdd(startDate) : String(points[0].t || '').slice(5,10);
      const lastLabel = endDate ? mmdd(endDate) : String(points[points.length-1].t || '').slice(5,10);
      ctx.fillText(firstLabel, pad, h-8);
      ctx.fillText(lastLabel, w-pad, h-8);
    }
    function renderTodayPnl(curve) {
      const rows = curve.rows || [];
      const today = new Date();
      const todayKey = `${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,'0')}-${String(today.getDate()).padStart(2,'0')}`;
      const todayRows = rows.filter(r => String(r.snapshot_date || r.created_at || '').slice(0,10) === todayKey);
      const source = todayRows.length > 0 ? todayRows : rows;
      const el = document.getElementById('todayPnl');
      if (!source.length) { el.textContent = '今日收益 --'; el.className = 'today-pnl'; return; }
      const first = Number(source[0].equity || source[0].portfolio_value || 0);
      const last = Number(source[source.length-1].equity || source[source.length-1].portfolio_value || 0);
      const diff = last - first;
      el.textContent = `今日收益 ${diff >= 0 ? '+' : ''}${money(diff)}`;
      el.className = `today-pnl ${diff < 0 ? 'neg' : 'pos'}`;
    }
    async function loadCurve(period=currentPeriod) {
      currentPeriod = period;
      document.querySelectorAll('.tab').forEach(b => b.classList.toggle('active', b.dataset.period === period));
      const curve = await api(`/api/equity_curve?period=${period}`);
      drawChart(curve);
      renderTodayPnl(curve);
    }
    function renderHoldings() {
      const rows = currentHolding === 'ALL'
        ? latestHoldings
        : latestHoldings.filter(r => String(r.strategy_group || '').toUpperCase() === currentHolding);
      document.querySelectorAll('.holding-tab').forEach(b => b.classList.toggle('active', b.dataset.holding === currentHolding));
      const blanks = Array.from({length: Math.max(0, 10 - rows.length)}, () => `<tr><td>&nbsp;</td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr>`).join('');
      document.getElementById('holdings').innerHTML = `<thead><tr>${['代码','策略组','状态','数量','均价','现价','市值','浮盈亏','浮盈亏%','已实现','持仓天数','更新时间'].map(h=>`<th>${h}</th>`).join('')}</tr></thead><tbody>` +
        rows.map(r => `<tr><td><b>${r.symbol}</b></td><td>${r.strategy_group}</td><td><span class="status ${r.status}">${r.status}</span></td><td>${Number(r.qty||0).toFixed(4)}</td><td>${money(r.avg_entry_price)}</td><td>${money(r.current_price)}</td><td>${money(r.market_value)}</td><td class="${cls(r.unrealized_pnl)}">${money(r.unrealized_pnl)}</td><td class="${cls(r.unrealized_pnl_pct)}">${pct(r.unrealized_pnl_pct)}</td><td class="${cls(r.realized_pnl)}">${money(r.realized_pnl)}</td><td>${r.holding_days || 0}</td><td>${r.last_update_time || ''}</td></tr>`).join('') +
        blanks + `</tbody>`;
    }
    async function loadAll() {
      const refreshBtn = document.querySelector('.refresh-btn');
      if (refreshBtn) refreshBtn.classList.add('loading');
      try {
      const [cap, risk, holdings, state] = await Promise.all([api('/api/capital'), api('/api/risk'), api('/api/holdings'), api('/api/state')]);
      if (cap.ok) {
        document.getElementById('modeValue').textContent = cap.mode;
        document.getElementById('modeHint').textContent = `cash ${money(cap.cash)} / portfolio ${money(cap.portfolio_value)}`;
        document.getElementById('metrics').innerHTML = [
          metric('Equity', money(cap.equity)), metric('Buying Power', money(cap.buying_power)), metric('Portfolio', money(cap.portfolio_value)), metric('Cash', money(cap.cash))
        ].join('');
        document.getElementById('pools').innerHTML = ['A','B','C','D'].map(g => poolCard(g, cap)).join('');
        const usedTotal = Number(cap.used_total || 0);
        const usableTotal = Number(cap.usable_total || 0);
        const exposurePct = usableTotal > 0 ? Math.min(999, usedTotal / usableTotal * 100) : 0;
        const totalRiskPct = Number(cap.total_risk_percent || 0) * 100;
        document.getElementById('exposureValue').textContent = `${exposurePct.toFixed(1)}% / 可用${totalRiskPct.toFixed(0)}% / ${money(usedTotal)}`;
        document.getElementById('exposureFill').style.width = `${Math.min(100, exposurePct)}%`;
        drawDonut(cap);
      } else {
        document.getElementById('modeValue').textContent = 'ERROR';
        document.getElementById('metrics').innerHTML = metric('账户', cap.error || '不可用');
      }
      document.getElementById('riskBadge').textContent = risk.suggest_mode ? `建议 ${risk.suggest_mode}` : '正常';
      document.getElementById('risk').innerHTML = [
        `risk=${Number(risk.risk_multiplier).toFixed(2)}`, `daily=${pct(risk.daily_pnl_pct)}`,
        `loss=${risk.loss_days}`, `drawdown=${pct(risk.max_drawdown)}`, `B=${risk.block_b?'停':'开'}`, `D=${risk.block_d?'停':'开'}`
      ].map(x => `<span>${x}</span>`).join('');
      window.latestBotProcesses = state.bot_processes || [];
      renderBots(state.bot_heartbeats || [], state.bot_controls || []);
      latestHoldings = holdings.rows || [];
      renderHoldings();
      await loadCurve(currentPeriod);
      } finally {
        if (refreshBtn) refreshBtn.classList.remove('loading');
      }
    }
    document.querySelectorAll('.tab').forEach(b => b.addEventListener('click', () => loadCurve(b.dataset.period)));
    document.querySelectorAll('.holding-tab').forEach(b => b.addEventListener('click', () => { currentHolding = b.dataset.holding; renderHoldings(); }));
    function openClearModal() {
      document.getElementById('clearPassword').value = '';
      document.getElementById('clearModal').classList.add('show');
      setTimeout(() => document.getElementById('clearPassword').focus(), 50);
    }
    function closeClearModal() { document.getElementById('clearModal').classList.remove('show'); }
    async function submitClearPosition() {
      const password = document.getElementById('clearPassword').value;
      const result = await postJson('/api/clear_position', {password});
      if (!result.ok) { alert(result.error || '清仓命令失败'); return; }
      closeClearModal();
      alert('清仓命令已写入');
    }
    async function toggleBot(botName, enabled) {
      const result = await postJson('/api/bot_control', {bot_name:botName, enabled});
      if (!result.ok) { alert(result.error || '开关失败'); return; }
      await loadAll();
    }
    async function syncPositions() {
      const btn = document.querySelector('.sync-positions-btn');
      const oldText = btn ? btn.textContent : '';
      if (btn) { btn.classList.add('loading'); btn.textContent = '同步中'; }
      try {
        const result = await postJson('/api/sync_positions', {});
        if (!result.ok) { alert(result.error || '同步仓位失败'); return; }
        await loadAll();
      } finally {
        if (btn) { btn.classList.remove('loading'); btn.textContent = oldText || '同步仓位'; }
      }
    }
    loadAll();
    setInterval(loadAll, 30000);
  </script>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, payload: dict | list, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=_json_default).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self) -> None:
        body = INDEX_HTML.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_asset(self, path: str) -> None:
        """发送项目内静态资源，目前用于 logo。"""
        asset_root = Path(__file__).resolve().parent / "assets"
        name = Path(path).name
        asset_path = asset_root / name
        if not asset_path.exists() or not asset_path.is_file():
            self._send_json({"ok": False, "error": "asset_not_found"}, 404)
            return
        content_type = "image/png" if asset_path.suffix.lower() == ".png" else "application/octet-stream"
        body = asset_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "public, max-age=3600")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args) -> None:
        print(f"[WEB] {self.address_string()} {fmt % args}", flush=True)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8")
        return json.loads(raw or "{}")

    def _check_password(self, payload: dict) -> bool:
        password = str(payload.get("password") or "")
        expected = env_str("DASHBOARD_ACTION_PASSWORD", env_str("MOBILE_CONTROL_TOKEN", ""))
        return bool(expected and password == expected)

    def do_GET(self) -> None:
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            if path == "/":
                self._send_html()
            elif path.startswith("/assets/"):
                self._send_asset(path)
            elif path == "/api/capital":
                self._send_json(_allocation_payload())
            elif path == "/api/risk":
                self._send_json(_risk_payload())
            elif path == "/api/holdings":
                self._send_json(_holdings_payload())
            elif path == "/api/state":
                self._send_json(_state_payload())
            elif path == "/api/equity_curve":
                period = parse_qs(parsed.query).get("period", ["week"])[0]
                self._send_json(_curve_payload(period))
            elif path == "/api/rebalance":
                self._send_json({"ok": True, "rows": generate_rebalance_report()})
            else:
                self._send_json({"ok": False, "error": "not_found"}, 404)
        except Exception as exc:
            self._send_json({"ok": False, "error": str(exc)}, 500)

    def do_POST(self) -> None:
        try:
            path = urlparse(self.path).path
            payload = self._read_json()
            if path == "/api/clear_position":
                if not self._check_password(payload):
                    self._send_json({"ok": False, "error": "密码错误或未配置操作密码"}, 403)
                    return
                add_command("d_sell_bot", "flatten_all", {"source": "web_dashboard"})
                self._send_json({"ok": True, "message": "清仓命令已写入"})
            elif path == "/api/bot_control":
                bot_name = str(payload.get("bot_name") or "")
                enabled_raw = payload.get("enabled")
                if bot_name not in managed_bot_names():
                    self._send_json({"ok": False, "error": "不支持的机器人"}, 400)
                    return
                enabled = bool(enabled_raw is True or str(enabled_raw).lower() in {"1", "true", "yes", "on"})
                set_bot_runtime(bot_name, enabled)
                self._send_json({"ok": True, "bot_name": bot_name, "enabled": enabled})
            elif path == "/api/sync_positions":
                ok = sync_position_holdings()
                if not ok:
                    detail = last_sync_error()
                    self._send_json({"ok": False, "error": f"券商仓位同步失败：{detail or '请检查 Alpaca 配置和服务日志'}"}, 500)
                    return
                self._send_json({"ok": True, "message": "仓位已同步"})
            else:
                self._send_json({"ok": False, "error": "not_found"}, 404)
        except Exception as exc:
            self._send_json({"ok": False, "error": str(exc)}, 500)


def run() -> None:
    """启动内置 HTTP 服务。"""
    s = settings()
    from .main import startup

    startup()
    sync_from_controls()
    server = ThreadingHTTPServer((s.web_host, s.web_port), Handler)
    print(f"[WEB] http://127.0.0.1:{s.web_port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    run()
