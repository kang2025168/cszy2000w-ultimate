# -*- coding: utf-8 -*-
"""
monitor/api.py  v3
改进：
1) 闪崩保护状态显示剩余等待时间
2) 机器人心跳（最后交易时间，超30分钟标红）
3) 待买入队列显示预估初始止损位
4) 最近交易记录展示
"""

import os
import time
import traceback
import requests as req
from datetime import datetime, time as dt_time

import pymysql
import pymysql.cursors
from flask import Flask, jsonify, Response, request
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

OPS_TABLE        = os.getenv("OPS_TABLE", "stock_operations")
MIN_BUYING_POWER = float(os.getenv("MIN_BUYING_POWER", "2100"))
APCA_KEY         = os.getenv("APCA_API_KEY_ID", "") or os.getenv("ALPACA_KEY", "")
APCA_SECRET      = os.getenv("APCA_API_SECRET_KEY", "") or os.getenv("ALPACA_SECRET", "")
ALPACA_TRADE_URL = "https://api.alpaca.markets" if TRADE_ENV == "live" else "https://paper-api.alpaca.markets"
FLASH_CRASH_WAIT_MINUTES = int(os.getenv("FLASH_CRASH_WAIT_MINUTES", "10"))

MARKET_OPEN  = dt_time(6, 40)
MARKET_CLOSE = dt_time(13, 0)

def now_la():
    return datetime.now(LA_TZ) if LA_TZ else datetime.now()

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
    return {"APCA-API-KEY-ID": APCA_KEY, "APCA-API-SECRET-KEY": APCA_SECRET}

def get_conn():
    return pymysql.connect(**DB)

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
            "buying_power":    round(safe_float(d.get("buying_power")), 2),
            "cash":            round(safe_float(d.get("cash")), 2),
            "equity":          round(safe_float(d.get("equity")), 2),
            "portfolio_value": round(safe_float(d.get("portfolio_value")), 2),
            "bp_ok":           safe_float(d.get("buying_power")) >= MIN_BUYING_POWER,
        }
        _acct_cache["ts"] = now
        _acct_cache["val"] = result
        return result
    except Exception as e:
        traceback.print_exc()
        return {"error": str(e), "buying_power": 0, "bp_ok": False}

_pos_cache = {"ts": 0, "val": None}
def get_alpaca_positions():
    now = time.time()
    if now - _pos_cache["ts"] < 10 and _pos_cache["val"] is not None:
        return _pos_cache["val"]
    try:
        r = req.get(f"{ALPACA_TRADE_URL}/v2/positions", headers=alpaca_headers(), timeout=8)
        if r.status_code != 200:
            return {"__error__": f"HTTP {r.status_code}: {r.text[:200]}"}
        result = {}
        for p in r.json():
            code = (p.get("symbol") or "").strip().upper()
            if not code:
                continue
            result[code] = {
                "qty":             safe_int(p.get("qty", 0)),
                "cost":            safe_float(p.get("avg_entry_price", 0)),
                "price":           safe_float(p.get("current_price", 0)),
                "market_value":    safe_float(p.get("market_value", 0)),
                "unrealized_pl":   safe_float(p.get("unrealized_pl", 0)),
                "unrealized_plpc": safe_float(p.get("unrealized_plpc", 0)) * 100,
                "side":            p.get("side", ""),
            }
        _pos_cache["ts"] = now
        _pos_cache["val"] = result
        return result
    except Exception as e:
        traceback.print_exc()
        return {"__error__": str(e)}

# ─── 前端 HTML ─────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">
<title>TradeBot Monitor</title>
<style>
*{box-sizing:border-box;margin:0;padding:0;}
body{font-family:'SF Mono',monospace;background:#0d1117;color:#e2e8f0;font-size:13px;padding:12px;}
.top-bar{display:flex;align-items:center;gap:8px;margin-bottom:12px;flex-wrap:wrap;}
.title{font-size:15px;font-weight:600;color:#f1f5f9;letter-spacing:.05em;}
.env{font-size:10px;padding:2px 7px;border-radius:4px;background:#3d2e0a;color:#f59e0b;font-weight:600;}
.env.live{background:#3b0f0f;color:#f87171;}
.dot{width:7px;height:7px;border-radius:50%;display:inline-block;margin-right:4px;}
.dot-g{background:#22c55e;box-shadow:0 0 5px #22c55e88;}
.dot-a{background:#f59e0b;}
.dot-r{background:#ef4444;box-shadow:0 0 5px #ef444488;}
.ts{font-size:10px;color:#475569;margin-left:auto;}
.btn{font-size:11px;padding:5px 10px;cursor:pointer;font-family:inherit;background:#1e293b;border:1px solid #334155;color:#94a3b8;border-radius:5px;}
.btn:active{background:#334155;}
.err{font-size:11px;color:#fca5a5;background:#3b0f0f;padding:7px 10px;border-radius:5px;margin-bottom:10px;display:none;border-left:3px solid #ef4444;}

/* 心跳 */
.heartbeat{display:flex;align-items:center;gap:6px;font-size:11px;padding:6px 12px;border-radius:6px;margin-bottom:10px;background:#1e293b;}
.hb-ok{color:#4ade80;}
.hb-warn{color:#fbbf24;}
.hb-dead{color:#f87171;background:#3b0f0f;}

.metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:7px;margin-bottom:10px;}
.metric{background:#1e293b;border-radius:7px;padding:10px 12px;}
.ml{font-size:10px;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px;}
.mv{font-size:18px;font-weight:600;color:#f1f5f9;}
.ms{font-size:10px;color:#475569;margin-top:2px;}
.cg{color:#4ade80;} .cr{color:#f87171;} .ca{color:#fbbf24;}
.gates{display:flex;gap:7px;margin-bottom:10px;flex-wrap:wrap;}
.gate{flex:1;min-width:110px;background:#1e293b;border:1px solid #1e293b;border-radius:7px;padding:8px 12px;}
.gl{font-size:10px;color:#64748b;text-transform:uppercase;letter-spacing:.04em;}
.gv{font-size:12px;font-weight:600;margin-top:3px;}
.gon{color:#4ade80;} .goff{color:#f87171;}
.sec{font-size:10px;font-weight:600;color:#64748b;letter-spacing:.08em;text-transform:uppercase;margin-bottom:7px;margin-top:4px;}
.tw{overflow-x:auto;border:1px solid #1e293b;border-radius:8px;margin-bottom:14px;}
table{width:100%;border-collapse:collapse;min-width:520px;}
th{background:#161d2d;color:#64748b;font-weight:500;padding:7px 9px;text-align:left;font-size:10px;letter-spacing:.05em;text-transform:uppercase;border-bottom:1px solid #1e293b;}
td{padding:8px 9px;border-bottom:1px solid #111827;color:#cbd5e1;vertical-align:middle;}
tr:last-child td{border-bottom:none;}
tr:hover td{background:#161d2d;}
.pp{color:#4ade80;} .pn{color:#f87171;} .pf{color:#64748b;}
.sp{display:inline-block;font-size:9px;padding:1px 5px;border-radius:99px;background:#1e3a5f;color:#60a5fa;}
.sp.m{background:#3d2e0a;color:#fbbf24;} .sp.h{background:#14391f;color:#4ade80;}
.pending-badge{display:inline-block;font-size:9px;padding:1px 5px;border-radius:99px;background:#3b0f0f;color:#f87171;margin-left:3px;animation:blink 1.5s infinite;}
@keyframes blink{0%,100%{opacity:1;}50%{opacity:.4;}}
.sb{display:flex;align-items:center;gap:4px;}
.sbg{flex:1;height:3px;background:#334155;border-radius:2px;min-width:30px;overflow:hidden;}
.sbf{height:100%;border-radius:2px;}
.sok{background:#22c55e;} .sw{background:#f59e0b;} .sd{background:#ef4444;}
.sv{font-size:10px;color:#64748b;min-width:36px;}
.tk{font-weight:600;font-size:12px;color:#f1f5f9;}
.nodb{font-size:9px;color:#64748b;margin-left:3px;}
.lm{font-size:10px;color:#475569;padding:14px;text-align:center;}
.side-buy{color:#4ade80;font-weight:600;}
.side-sell{color:#f87171;font-weight:600;}
.side-sync{color:#64748b;}
.footer{font-size:10px;color:#334155;text-align:center;padding-top:10px;}
</style>
</head>
<body>
<div class="top-bar">
  <span class="title">TradeBot</span>
  <span class="env" id="envBadge">—</span>
  <span><span class="dot dot-a" id="dot"></span><span id="stxt" style="font-size:12px;color:#94a3b8;">加载中</span></span>
  <span class="ts" id="ts"></span>
  <button class="btn" onclick="load()">↺</button>
</div>
<div class="err" id="err"></div>

<!-- 心跳 -->
<div class="heartbeat" id="heartbeat">
  <span id="hbDot">⚙️</span>
  <span id="hbText" style="color:#475569;">检查机器人状态...</span>
</div>

<div class="metrics" id="metrics"><div class="metric"><div class="ml">状态</div><div class="mv" style="font-size:13px;color:#475569;">连接中...</div></div></div>
<div class="gates" id="gates"></div>

<div class="sec">持仓列表</div>
<div class="tw"><table><thead><tr><th>股票</th><th>数量</th><th>成本</th><th>现价</th><th>浮盈%</th><th>止损</th><th>距止损</th><th>阶段</th></tr></thead><tbody id="hb"><tr><td colspan="8" class="lm">—</td></tr></tbody></table></div>

<div class="sec">待买入队列</div>
<div class="tw"><table><thead><tr><th>股票</th><th>类型</th><th>触发价</th><th>现价</th><th>当日涨幅</th><th>预估止损</th><th>压力位日期</th><th>最后操作</th></tr></thead><tbody id="bb"><tr><td colspan="8" class="lm">—</td></tr></tbody></table></div>

<div class="sec">最近交易记录</div>
<div class="tw"><table><thead><tr><th>时间</th><th>股票</th><th>方向</th><th>操作详情</th><th>数量</th><th>成本</th><th>止损</th><th>阶段</th></tr></thead><tbody id="tb"><tr><td colspan="8" class="lm">—</td></tr></tbody></table></div>

<div class="footer">自动刷新 30s &nbsp;|&nbsp; <span id="fenv">—</span></div>

<script>
function fp(v){return (v>=0?'+':'')+Number(v).toFixed(2)+'%';}
function fc(v){return '$'+Math.round(v).toLocaleString('en');}
function pc(v){return v>0.3?'pp':v<-0.3?'pn':'pf';}
function sc(s){return s>=7?'sp h':s>=4?'sp m':'sp';}
function slc(d){return d<0?'sd':d<3?'sw':'sok';}
function ft(s){return s?String(s).slice(5,16):'—';}
function fd(s){return s?String(s).slice(0,10):'—';}

async function load(){
  try{
    const [st,ho,bq,tr]=await Promise.all([
      fetch('/api/status').then(r=>r.json()),
      fetch('/api/holdings').then(r=>r.json()),
      fetch('/api/buy_queue').then(r=>r.json()),
      fetch('/api/recent_trades').then(r=>r.json()),
    ]);
    document.getElementById('err').style.display='none';
    renderStatus(st);
    renderHeartbeat(tr);
    renderHoldings(ho);
    renderQueue(bq);
    renderTrades(tr);
  }catch(e){
    const eb=document.getElementById('err');
    eb.style.display='block';
    eb.textContent='连接失败：'+e.message;
    document.getElementById('dot').className='dot dot-r';
    document.getElementById('stxt').textContent='连接失败';
  }
}

// ─── 心跳 ───────────────────────────────────────────────────────────
function renderHeartbeat(trades){
  const hb=document.getElementById('heartbeat');
  const hbText=document.getElementById('hbText');
  const hbDot=document.getElementById('hbDot');

  if(!Array.isArray(trades)||trades.length===0){
    hb.className='heartbeat hb-warn';
    hbDot.textContent='⚠️';
    hbText.textContent='无交易记录，无法判断机器人状态';
    return;
  }

  const last=trades[0];
  const lastTime=last.time?new Date(last.time.replace(' ','T')):null;
  if(!lastTime){
    hb.className='heartbeat hb-warn';
    hbDot.textContent='⚠️';
    hbText.textContent='时间解析失败';
    return;
  }

  const nowMs=Date.now();
  const diffMin=Math.floor((nowMs-lastTime.getTime())/60000);

  if(diffMin<30){
    hb.className='heartbeat';
    hbDot.textContent='✅';
    hbText.innerHTML=`机器人运行正常 &nbsp;·&nbsp; 最后操作：<strong style="color:#f1f5f9">${last.code}</strong> ${last.side} &nbsp;·&nbsp; ${diffMin}分钟前`;
  } else if(diffMin<120){
    hb.className='heartbeat hb-warn';
    hbDot.textContent='⚠️';
    hbText.innerHTML=`机器人可能异常 &nbsp;·&nbsp; 距上次操作已 <strong style="color:#fbbf24">${diffMin}分钟</strong>`;
  } else {
    hb.className='heartbeat hb-dead';
    hbDot.textContent='🔴';
    hbText.innerHTML=`机器人可能已停止 &nbsp;·&nbsp; 距上次操作已 <strong>${Math.floor(diffMin/60)}小时${diffMin%60}分钟</strong>`;
  }
}

// ─── 状态 ────────────────────────────────────────────────────────────
function renderStatus(s){
  const env=(s.env||'—').toUpperCase();
  const eb=document.getElementById('envBadge');
  eb.textContent=env;
  eb.className='env'+(env==='LIVE'?' live':'');
  document.getElementById('fenv').textContent=env;
  document.getElementById('dot').className='dot '+(s.trading_time?'dot-g':'dot-a');
  document.getElementById('stxt').textContent=s.trading_time?'交易时段':'休市';
  document.getElementById('ts').textContent=s.server_time_la||'';
  const bp=s.buying_power||0;
  const eq=s.equity||0;
  document.getElementById('metrics').innerHTML=`
    <div class="metric"><div class="ml">购买力</div><div class="mv ${bp>=(s.min_buying_power||2100)?'cg':'cr'}">${fc(bp)}</div><div class="ms">阈值 ${fc(s.min_buying_power||2100)}</div></div>
    <div class="metric"><div class="ml">账户净值</div><div class="mv">${fc(eq)}</div><div class="ms">portfolio</div></div>
    <div class="metric" id="hm"><div class="ml">持仓数</div><div class="mv">—</div></div>
    <div class="metric" id="pm"><div class="ml">总浮盈</div><div class="mv">—</div></div>
    <div class="metric" id="qm"><div class="ml">待买入</div><div class="mv ca">—</div></div>
  `;
  const bpOk=s.bp_ok,mg=s.market_gate===1,ba=s.buy_allowed;
  document.getElementById('gates').innerHTML=`
    <div class="gate"><div class="gl">资金</div><div class="gv ${bpOk?'gon':'goff'}">${bpOk?'✓ 开':'✗ 关'} ${fc(bp)}</div></div>
    <div class="gate"><div class="gl">大盘 QQQ</div><div class="gv ${mg?'gon':'goff'}">${mg?'✓ open=1':'✗ open=0'}</div></div>
    <div class="gate"><div class="gl">买入开关</div><div class="gv ${ba?'gon':'goff'}">${ba?'✓ 允许':'✗ 禁止'}</div></div>
  `;
}

// ─── 持仓 ────────────────────────────────────────────────────────────
function renderHoldings(rows){
  if(!Array.isArray(rows)){
    document.getElementById('hb').innerHTML=`<tr><td colspan="8" class="lm" style="color:#f87171">${rows?.error||'加载失败'}</td></tr>`;
    return;
  }
  let tc=0,tv=0;
  rows.forEach(h=>{tc+=h.cost*h.qty;tv+=h.price*h.qty;});
  const pl=tv-tc,plp=tc>0?pl/tc*100:0;
  const hm=document.getElementById('hm');
  if(hm)hm.innerHTML=`<div class="ml">持仓数</div><div class="mv">${rows.length}</div><div class="ms">只股票</div>`;
  const pm=document.getElementById('pm');
  if(pm)pm.innerHTML=`<div class="ml">总浮盈</div><div class="mv ${pl>=0?'cg':'cr'}">${pl>=0?'+':''}$${Math.abs(pl).toFixed(0)}</div><div class="ms">${fp(plp)}</div>`;

  document.getElementById('hb').innerHTML=rows.length===0
    ?'<tr><td colspan="8" class="lm">暂无持仓</td></tr>'
    :rows.map(h=>{
      const d=h.dist_to_sl_pct,bp=Math.max(0,Math.min(100,Math.abs(d)*6));

      // ✅ 改进1：闪崩保护显示剩余时间
      let stageCell='—';
      if(h.stage>0) stageCell=`<span class="${sc(h.stage)}">S${h.stage}</span>`;
      if(h.pending_stop){
        const leftMin=h.pending_left_min!==undefined?h.pending_left_min:'?';
        stageCell+=`<span class="pending-badge">观察 ${leftMin}m</span>`;
      }

      return`<tr>
        <td><span class="tk">${h.code}</span>${!h.in_db?'<span class="nodb">*</span>':''}</td>
        <td>${h.qty}${h.base_qty>0?` <span style="color:#475569;font-size:9px">(${h.base_qty})</span>`:''}</td>
        <td>$${Number(h.cost).toFixed(2)}</td>
        <td>$${Number(h.price).toFixed(2)}</td>
        <td class="${pc(h.up_pct)}">${fp(h.up_pct)}</td>
        <td>${h.sl>0?'$'+Number(h.sl).toFixed(2):'—'}</td>
        <td><div class="sb"><div class="sbg"><div class="sbf ${slc(d)}" style="width:${bp.toFixed(0)}%"></div></div><span class="sv ${d<0?'pn':''}">${Number(d).toFixed(1)}%</span></div></td>
        <td>${stageCell}</td>
      </tr>`;
    }).join('');
}

// ─── 待买入队列 ───────────────────────────────────────────────────────
function renderQueue(rows){
  const qm=document.getElementById('qm');
  if(qm)qm.innerHTML=`<div class="ml">待买入</div><div class="mv ca">${Array.isArray(rows)?rows.length:'—'}</div><div class="ms">队列中</div>`;
  document.getElementById('bb').innerHTML=!Array.isArray(rows)
    ?'<tr><td colspan="8" class="lm" style="color:#f87171">加载失败</td></tr>'
    :rows.length===0?'<tr><td colspan="8" class="lm">暂无队列</td></tr>'
    :rows.map(b=>{
      const upCls=b.up_pct>0?'pp':b.up_pct<0?'pn':'pf';
      const priceVsTrigger=b.price>0&&b.trigger>0?(b.price>=b.trigger?'cg':'cr'):'';
      // ✅ 改进3：预估初始止损 = max(trigger, price*0.97)
      const estSl=b.trigger>0&&b.price>0?Math.max(b.trigger,b.price*0.97):0;
      const estSlStr=estSl>0?'$'+estSl.toFixed(2):'—';
      return`<tr>
        <td><span class="tk">${b.code}</span></td>
        <td>${b.type}</td>
        <td>$${Number(b.trigger).toFixed(2)}</td>
        <td class="${priceVsTrigger}">${b.price>0?'$'+Number(b.price).toFixed(2):'—'}</td>
        <td class="${upCls}">${b.up_pct!==undefined?fp(b.up_pct):'—'}</td>
        <td style="color:#64748b;font-size:11px;">${estSlStr}</td>
        <td style="color:#64748b;font-size:11px;">${b.entry_date||'—'}</td>
        <td style="color:#475569;font-size:10px;">${b.last_order_side||'—'} ${ft(b.last_order_time)}</td>
      </tr>`;
    }).join('');
}

// ─── 最近交易记录 ─────────────────────────────────────────────────────
function renderTrades(rows){
  if(!Array.isArray(rows)){
    document.getElementById('tb').innerHTML=`<tr><td colspan="8" class="lm" style="color:#f87171">加载失败</td></tr>`;
    return;
  }
  document.getElementById('tb').innerHTML=rows.length===0
    ?'<tr><td colspan="8" class="lm">暂无记录</td></tr>'
    :rows.slice(0,20).map(r=>{
      const sideCls=r.side==='buy'?'side-buy':r.side==='sell'?'side-sell':'side-sync';
      const sideLabel=r.side==='buy'?'买入':r.side==='sell'?'卖出':r.side||'—';
      return`<tr>
        <td style="color:#475569;font-size:11px;">${ft(r.time)}</td>
        <td><span class="tk">${r.code}</span></td>
        <td><span class="${sideCls}">${sideLabel}</span></td>
        <td style="color:#64748b;font-size:10px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${r.intent||'—'}</td>
        <td>${r.qty||'—'}</td>
        <td>${r.cost>0?'$'+Number(r.cost).toFixed(2):'—'}</td>
        <td>${r.sl>0?'$'+Number(r.sl).toFixed(2):'—'}</td>
        <td>${r.stage>0?`<span class="${sc(r.stage)}">S${r.stage}</span>`:'—'}</td>
      </tr>`;
    }).join('');
}

load();
setInterval(load,30000);
</script>
</body>
</html>"""

@app.route("/")
def index():
    return Response(HTML, mimetype="text/html")

@app.route("/health")
def health():
    return jsonify({"ok": True, "env": TRADE_ENV, "time": now_la().strftime("%H:%M:%S")})

@app.route("/api/status")
def api_status():
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(f"SELECT entry_open FROM `{OPS_TABLE}` WHERE stock_code='QQQ' AND stock_type='N' LIMIT 1")
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
    try:
        alpaca_pos = get_alpaca_positions()
        if "__error__" in alpaca_pos:
            return jsonify({"error": alpaca_pos["__error__"]}), 500
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT stock_code, stock_type, stop_loss_price, b_stage, base_qty,
                       last_order_time, last_order_side, last_order_intent,
                       b_stop_pending_since, b_stop_pending_sl
                FROM `{OPS_TABLE}` WHERE stock_type IN ('A','B','C','D','E')
            """)
            db_rows = cur.fetchall() or []
        conn.close()
        db_map = {(r.get("stock_code") or "").strip().upper(): r for r in db_rows}
        result = []
        now = now_la().replace(tzinfo=None)
        for code, pos in alpaca_pos.items():
            db = db_map.get(code, {})
            price = pos["price"]
            sl = safe_float(db.get("stop_loss_price"))
            dist_to_sl = (price - sl) / price * 100 if price > 0 and sl > 0 else 0

            # ✅ 改进1：计算闪崩保护剩余时间
            pending_stop = False
            pending_left_min = 0
            pending_since_raw = db.get("b_stop_pending_since")
            if pending_since_raw:
                try:
                    if isinstance(pending_since_raw, datetime):
                        pending_dt = pending_since_raw
                    else:
                        pending_dt = datetime.fromisoformat(str(pending_since_raw).replace("Z",""))
                    elapsed = (now - pending_dt.replace(tzinfo=None)).total_seconds()
                    wait_sec = FLASH_CRASH_WAIT_MINUTES * 60
                    if elapsed < wait_sec:
                        pending_stop = True
                        pending_left_min = max(0, int((wait_sec - elapsed) / 60))
                except Exception:
                    pending_stop = True
                    pending_left_min = 0

            result.append({
                "code": code,
                "type": (db.get("stock_type") or "—").strip().upper(),
                "qty": pos["qty"],
                "base_qty": safe_int(db.get("base_qty")),
                "cost": round(pos["cost"], 2),
                "price": round(price, 2),
                "market_value": round(pos["market_value"], 2),
                "unrealized_pl": round(pos["unrealized_pl"], 2),
                "sl": round(sl, 2),
                "stage": safe_int(db.get("b_stage")),
                "up_pct": round(pos["unrealized_plpc"], 2),
                "dist_to_sl_pct": round(dist_to_sl, 2),
                "pending_stop": pending_stop,
                "pending_left_min": pending_left_min,
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
                       close_price, entry_date,
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
            code = (r.get("stock_code") or "").strip().upper()
            trigger = round(safe_float(r.get("trigger_price")), 2)
            db_close = safe_float(r.get("close_price"))
            price = 0.0
            try:
                snap_r = req.get(
                    f"https://data.alpaca.markets/v2/stocks/{code}/snapshot",
                    headers=alpaca_headers(),
                    params={"feed": os.getenv("B_DATA_FEED", "iex")},
                    timeout=5,
                )
                if snap_r.status_code == 200:
                    js = snap_r.json()
                    lt = js.get("latestTrade") or {}
                    if lt.get("p"):
                        price = safe_float(lt["p"])
                    if price == 0:
                        lq = js.get("latestQuote") or {}
                        bid = safe_float(lq.get("bp"))
                        ask = safe_float(lq.get("ap"))
                        if bid > 0 and ask > 0:
                            price = (bid + ask) / 2
                    pb = js.get("prevDailyBar") or {}
                    if pb.get("c"):
                        db_close = safe_float(pb["c"])
            except Exception:
                pass
            up_pct = (price - db_close) / db_close * 100 if db_close > 0 and price > 0 else 0.0
            entry_date = r.get("entry_date")
            result.append({
                "code": code,
                "type": (r.get("stock_type") or "").strip().upper(),
                "trigger": trigger,
                "price": round(price, 2),
                "up_pct": round(up_pct, 2),
                "entry_date": str(entry_date)[:10] if entry_date else "—",
                "last_order_time": str(r.get("last_order_time") or ""),
                "last_order_side": r.get("last_order_side") or "",
            })
        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route("/api/recent_trades")
def api_recent_trades():
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT stock_code, stock_type, last_order_side, last_order_intent,
                       last_order_id, last_order_time, qty, cost_price, stop_loss_price, b_stage
                FROM `{OPS_TABLE}`
                WHERE last_order_time IS NOT NULL AND stock_type IN ('A','B','C','D','E')
                ORDER BY last_order_time DESC LIMIT 50
            """)
            rows = cur.fetchall() or []
        conn.close()
        return jsonify([{
            "code":     (r.get("stock_code") or "").strip().upper(),
            "type":     (r.get("stock_type") or "").strip().upper(),
            "side":     r.get("last_order_side") or "",
            "intent":   r.get("last_order_intent") or "",
            "order_id": r.get("last_order_id") or "",
            "time":     str(r.get("last_order_time") or ""),
            "qty":      safe_int(r.get("qty")),
            "cost":     round(safe_float(r.get("cost_price")), 2),
            "sl":       round(safe_float(r.get("stop_loss_price")), 2),
            "stage":    safe_int(r.get("b_stage")),
        } for r in rows])
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.getenv("MONITOR_PORT", "5050"))
    print(f"[Monitor API] :{port} env={TRADE_ENV} url={ALPACA_TRADE_URL}", flush=True)
    app.run(host="0.0.0.0", port=port, debug=False)

# ─── 分析页面路由 ──────────────────────────────────────────────────────
ANALYSIS_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>交易分析</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/lightweight-charts/4.1.3/lightweight-charts.standalone.production.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0;}
body{font-family:'SF Mono',monospace;background:#0d1117;color:#e2e8f0;font-size:13px;padding:16px;}
.top-bar{display:flex;align-items:center;gap:10px;margin-bottom:16px;flex-wrap:wrap;}
.title{font-size:15px;font-weight:600;color:#f1f5f9;}
.back-btn{font-size:11px;padding:4px 10px;cursor:pointer;font-family:inherit;background:#1e293b;border:1px solid #334155;color:#94a3b8;border-radius:5px;text-decoration:none;}
.env{font-size:10px;padding:2px 7px;border-radius:4px;background:#3b0f0f;color:#f87171;font-weight:600;}
.filter-bar{display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;align-items:center;}
.filter-bar select,.filter-bar input{padding:6px 9px;background:#1e293b;border:1px solid #334155;color:#e2e8f0;border-radius:5px;font-family:inherit;font-size:12px;}
.filter-bar button{padding:6px 14px;font-size:12px;cursor:pointer;font-family:inherit;background:#1d4ed8;border:none;color:#fff;border-radius:5px;}
.metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:8px;margin-bottom:16px;}
.metric{background:#1e293b;border-radius:8px;padding:12px 14px;}
.ml{font-size:10px;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px;}
.mv{font-size:20px;font-weight:600;color:#f1f5f9;}
.ms{font-size:10px;color:#475569;margin-top:2px;}
.cg{color:#4ade80;} .cr{color:#f87171;} .ca{color:#fbbf24;}
.sec{font-size:10px;font-weight:600;color:#64748b;letter-spacing:.08em;text-transform:uppercase;margin-bottom:8px;margin-top:4px;}
.chart-box{background:#111827;border:1px solid #1e293b;border-radius:8px;padding:4px;margin-bottom:14px;}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:14px;}
.tw{border:1px solid #1e293b;border-radius:8px;overflow:hidden;margin-bottom:14px;overflow-x:auto;}
table{width:100%;border-collapse:collapse;min-width:600px;}
th{background:#161d2d;color:#64748b;font-weight:500;padding:7px 9px;text-align:left;font-size:10px;letter-spacing:.05em;text-transform:uppercase;border-bottom:1px solid #1e293b;}
td{padding:8px 9px;border-bottom:1px solid #111827;color:#cbd5e1;font-size:12px;}
tr:last-child td{border-bottom:none;}
tr:hover td{background:#161d2d;}
.buy-tag{color:#4ade80;font-weight:600;}
.sell-tag{color:#f87171;font-weight:600;}
.bar-wrap{display:flex;align-items:center;gap:6px;}
.bar-bg{flex:1;height:6px;background:#1e293b;border-radius:3px;overflow:hidden;min-width:60px;}
.bar-fill{height:100%;border-radius:3px;}
.bar-pos{background:#4ade80;}
.bar-neg{background:#f87171;}
.loading{text-align:center;padding:40px;color:#475569;font-size:13px;}
.err{color:#f87171;background:#3b0f0f;padding:10px;border-radius:6px;margin-bottom:12px;display:none;}
.sym-chip{display:inline-block;padding:3px 8px;border-radius:99px;font-size:11px;font-weight:600;margin:2px;}
.chip-pos{background:#14391f;color:#4ade80;}
.chip-neg{background:#3b0f0f;color:#f87171;}
</style>
</head>
<body>
<div class="top-bar">
  <a class="back-btn" href="/">← 返回监控</a>
  <span class="title">交易分析</span>
  <span class="env" id="envBadge">LIVE</span>
  <span style="font-size:11px;color:#475569;" id="lastUpdate"></span>
</div>
<div class="err" id="err"></div>

<div class="filter-bar">
  <select id="period">
    <option value="30">近30天</option>
    <option value="90" selected>近90天</option>
    <option value="180">近180天</option>
    <option value="365">近1年</option>
    <option value="0">全部</option>
  </select>
  <button onclick="load()">刷新</button>
</div>

<div id="content"><div class="loading">加载中...</div></div>

<script>
let equityChart = null;

async function load(){
  const days = document.getElementById('period').value;
  document.getElementById('err').style.display='none';
  document.getElementById('content').innerHTML='<div class="loading">⏳ 拉取 Alpaca 成交数据...</div>';
  try{
    const data = await fetch(`/api/analysis?days=${days}`).then(r=>r.json());
    if(data.error){ showErr(data.error); return; }
    render(data);
    document.getElementById('lastUpdate').textContent = new Date().toLocaleTimeString('zh-CN');
  }catch(e){
    showErr('请求失败：'+e.message);
  }
}

function showErr(msg){
  const e=document.getElementById('err');
  e.style.display='block';
  e.textContent=msg;
  document.getElementById('content').innerHTML='';
}

function fp(v,d=2){return (v>=0?'+':'')+Number(v).toFixed(d)+'%';}
function fm(v){return (v>=0?'+$':'-$')+Math.abs(v).toFixed(2);}
function fd(s){return s?String(s).slice(0,16).replace('T',' '):'—';}

function render(d){
  const s = d.stats;
  const html = `
    <div class="metrics">
      <div class="metric"><div class="ml">总盈亏</div><div class="mv ${s.total_pnl>=0?'cg':'cr'}">${fm(s.total_pnl)}</div><div class="ms">${fp(s.total_pnl_pct)} 总收益率</div></div>
      <div class="metric"><div class="ml">胜率</div><div class="mv ${s.win_rate>=50?'cg':'ca'}">${s.win_rate}%</div><div class="ms">${s.win_count}胜 / ${s.loss_count}负</div></div>
      <div class="metric"><div class="ml">平均盈利</div><div class="mv cg">${fm(s.avg_win)}</div><div class="ms">${fp(s.avg_win_pct)} 每笔</div></div>
      <div class="metric"><div class="ml">平均亏损</div><div class="mv cr">${fm(s.avg_loss)}</div><div class="ms">${fp(s.avg_loss_pct)} 每笔</div></div>
      <div class="metric"><div class="ml">盈亏比</div><div class="mv ${s.profit_factor>=1.5?'cg':'ca'}">${s.profit_factor}</div><div class="ms">盈/亏倍数</div></div>
      <div class="metric"><div class="ml">总交易笔数</div><div class="mv">${s.total_trades}</div><div class="ms">已平仓</div></div>
      <div class="metric"><div class="ml">最大单笔盈利</div><div class="mv cg">${fm(s.max_win)}</div><div class="ms">${s.max_win_sym}</div></div>
      <div class="metric"><div class="ml">最大单笔亏损</div><div class="mv cr">${fm(s.max_loss)}</div><div class="ms">${s.max_loss_sym}</div></div>
    </div>

    <div class="sec">累计收益曲线</div>
    <div class="chart-box"><div id="equityChart" style="height:220px;"></div></div>

    <div class="grid2">
      <div>
        <div class="sec">盈利最多的股票</div>
        <div>${d.top_winners.map(x=>`<span class="sym-chip chip-pos">${x.symbol} ${fm(x.pnl)}</span>`).join('')}</div>
      </div>
      <div>
        <div class="sec">亏损最多的股票</div>
        <div>${d.top_losers.map(x=>`<span class="sym-chip chip-neg">${x.symbol} ${fm(x.pnl)}</span>`).join('')}</div>
      </div>
    </div>

    <div class="sec">按股票统计</div>
    <div class="tw"><table>
      <thead><tr><th>股票</th><th>交易次数</th><th>总盈亏</th><th>胜率</th><th>平均持仓天数</th><th>盈亏分布</th></tr></thead>
      <tbody>${d.by_symbol.map(x=>{
        const barW = Math.min(100, Math.abs(x.pnl)/Math.max(...d.by_symbol.map(s=>Math.abs(s.pnl)))*100);
        return`<tr>
          <td style="font-weight:600;color:#f1f5f9;">${x.symbol}</td>
          <td>${x.count}</td>
          <td class="${x.pnl>=0?'cg':'cr'}">${fm(x.pnl)}</td>
          <td class="${x.win_rate>=50?'cg':'ca'}">${x.win_rate}%</td>
          <td style="color:#64748b;">${x.avg_hold_days}天</td>
          <td><div class="bar-wrap"><div class="bar-bg"><div class="bar-fill ${x.pnl>=0?'bar-pos':'bar-neg'}" style="width:${barW.toFixed(0)}%"></div></div></div></td>
        </tr>`;
      }).join('')}</tbody>
    </table></div>

    <div class="sec">全部成交记录（${d.trades.length} 笔）</div>
    <div class="tw"><table>
      <thead><tr><th>时间</th><th>股票</th><th>方向</th><th>数量</th><th>成交价</th><th>盈亏</th><th>盈亏%</th><th>持仓天数</th></tr></thead>
      <tbody>${d.trades.map(t=>`<tr>
        <td style="color:#475569;">${fd(t.filled_at)}</td>
        <td style="font-weight:600;color:#f1f5f9;">${t.symbol}</td>
        <td>${t.side==='buy'?'<span class="buy-tag">买入</span>':'<span class="sell-tag">卖出</span>'}</td>
        <td>${t.qty}</td>
        <td>$${Number(t.fill_price).toFixed(2)}</td>
        <td class="${(t.pnl||0)>=0?'cg':'cr'}">${t.pnl!==null?fm(t.pnl):'—'}</td>
        <td class="${(t.pnl_pct||0)>=0?'cg':'cr'}">${t.pnl_pct!==null?fp(t.pnl_pct):'—'}</td>
        <td style="color:#64748b;">${t.hold_days!==null?t.hold_days+'天':'—'}</td>
      </tr>`).join('')}</tbody>
    </table></div>
  `;
  document.getElementById('content').innerHTML = html;

  // 收益曲线
  setTimeout(()=>{
    const el = document.getElementById('equityChart');
    if(!el||!d.equity_curve||d.equity_curve.length===0) return;
    if(equityChart){ try{equityChart.remove();}catch(e){} }
    const chart = LightweightCharts.createChart(el,{
      layout:{background:{color:'#111827'},textColor:'#64748b'},
      grid:{vertLines:{color:'#1e293b'},horzLines:{color:'#1e293b'}},
      rightPriceScale:{borderColor:'#1e293b'},
      timeScale:{borderColor:'#1e293b'},
      width:el.clientWidth, height:220,
    });
    equityChart = chart;
    const area = chart.addAreaSeries({
      lineColor:'#3b82f6',topColor:'#3b82f633',bottomColor:'#3b82f600',lineWidth:2,
    });
    // 零线
    const base = chart.addLineSeries({color:'#334155',lineWidth:1,lineStyle:2});
    const startVal = d.equity_curve[0]?.value||0;
    base.setData([
      {time:d.equity_curve[0].time, value:0},
      {time:d.equity_curve[d.equity_curve.length-1].time, value:0},
    ]);
    area.setData(d.equity_curve);
    chart.timeScale().fitContent();
  },50);
}

load();
</script>
</body>
</html>"""

@app.route("/analysis")
def analysis_page():
    return Response(ANALYSIS_HTML, mimetype="text/html")

@app.route("/api/analysis")
def api_analysis():
    """
    从 Alpaca 拉历史成交，配对买卖单，计算盈亏分析
    """
    try:
        days = int(request.args.get("days", 90))

        # 拉 Alpaca 历史成交
        params = {"limit": 500, "direction": "desc"}
        if days > 0:
            from datetime import timedelta
            since = (now_la() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
            params["after"] = since

        r = req.get(
            f"{ALPACA_TRADE_URL}/v2/account/activities/FILL",
            headers=alpaca_headers(),
            params=params,
            timeout=15,
        )
        if r.status_code != 200:
            return jsonify({"error": f"Alpaca HTTP {r.status_code}: {r.text[:200]}"}), 500

        activities = r.json()
        if not isinstance(activities, list):
            return jsonify({"error": "Unexpected response format"}), 500

        # 整理成交记录
        fills = []
        for a in activities:
            sym       = (a.get("symbol") or "").strip().upper()
            side      = (a.get("side") or "").lower()
            qty       = safe_float(a.get("qty", 0))
            price     = safe_float(a.get("price", 0))
            filled_at = a.get("transaction_time") or a.get("date") or ""
            if not sym or not side or qty <= 0 or price <= 0:
                continue
            fills.append({
                "symbol":    sym,
                "side":      side,
                "qty":       qty,
                "fill_price": price,
                "filled_at": filled_at,
                "pnl":       None,
                "pnl_pct":   None,
                "hold_days": None,
            })

        # 按股票配对买卖（FIFO）
        buy_queues = {}  # symbol -> [(price, qty, filled_at)]
        paired_trades = []

        # 先按时间正序处理
        fills_asc = sorted(fills, key=lambda x: x["filled_at"])
        fills_desc = sorted(fills, key=lambda x: x["filled_at"], reverse=True)

        for f in fills_asc:
            sym   = f["symbol"]
            side  = f["side"]
            qty   = f["qty"]
            price = f["fill_price"]
            fat   = f["filled_at"]

            if side == "buy":
                buy_queues.setdefault(sym, []).append({
                    "price": price, "qty": qty, "at": fat
                })
            elif side == "sell":
                queue = buy_queues.get(sym, [])
                remaining = qty
                total_cost = 0
                total_qty  = 0
                buy_at     = fat

                while remaining > 0 and queue:
                    b = queue[0]
                    use = min(b["qty"], remaining)
                    total_cost += use * b["price"]
                    total_qty  += use
                    buy_at      = b["at"]
                    b["qty"]   -= use
                    remaining  -= use
                    if b["qty"] <= 0:
                        queue.pop(0)

                if total_qty > 0:
                    avg_cost = total_cost / total_qty
                    pnl      = (price - avg_cost) * total_qty
                    pnl_pct  = (price - avg_cost) / avg_cost * 100
                    # 持仓天数
                    try:
                        buy_dt  = datetime.fromisoformat(buy_at.replace("Z",""))
                        sell_dt = datetime.fromisoformat(fat.replace("Z",""))
                        hold_days = (sell_dt - buy_dt).days
                    except Exception:
                        hold_days = None

                    # 更新 fills 里对应 sell 的盈亏
                    for fl in fills_desc:
                        if fl["symbol"]==sym and fl["side"]=="sell" and fl["filled_at"]==fat and fl["pnl"] is None:
                            fl["pnl"]       = round(pnl, 2)
                            fl["pnl_pct"]   = round(pnl_pct, 2)
                            fl["hold_days"] = hold_days
                            break

        # 统计
        sell_fills = [f for f in fills if f["side"]=="sell" and f["pnl"] is not None]
        win_fills  = [f for f in sell_fills if f["pnl"] > 0]
        loss_fills = [f for f in sell_fills if f["pnl"] <= 0]

        total_pnl  = sum(f["pnl"] for f in sell_fills)
        win_count  = len(win_fills)
        loss_count = len(loss_fills)
        win_rate   = round(win_count / len(sell_fills) * 100, 1) if sell_fills else 0
        avg_win    = sum(f["pnl"] for f in win_fills) / win_count if win_count else 0
        avg_loss   = sum(f["pnl"] for f in loss_fills) / loss_count if loss_count else 0
        avg_win_pct  = sum(f["pnl_pct"] for f in win_fills) / win_count if win_count else 0
        avg_loss_pct = sum(f["pnl_pct"] for f in loss_fills) / loss_count if loss_count else 0
        profit_factor = round(abs(avg_win / avg_loss), 2) if avg_loss != 0 else 0
        max_win_f  = max(sell_fills, key=lambda x: x["pnl"]) if sell_fills else None
        max_loss_f = min(sell_fills, key=lambda x: x["pnl"]) if sell_fills else None

        # 估算总收益率（用总盈亏 / 初始资金近似）
        acct = get_alpaca_account()
        portfolio_val = acct.get("portfolio_value", 1) or 1
        total_pnl_pct = round(total_pnl / portfolio_val * 100, 2)

        # 按股票统计
        sym_data = {}
        for f in sell_fills:
            sym = f["symbol"]
            if sym not in sym_data:
                sym_data[sym] = {"symbol": sym, "count": 0, "pnl": 0,
                                 "wins": 0, "hold_days_list": []}
            sym_data[sym]["count"] += 1
            sym_data[sym]["pnl"]   += f["pnl"]
            if f["pnl"] > 0:
                sym_data[sym]["wins"] += 1
            if f["hold_days"] is not None:
                sym_data[sym]["hold_days_list"].append(f["hold_days"])

        by_symbol = []
        for sym, sd in sym_data.items():
            avg_hold = round(sum(sd["hold_days_list"]) / len(sd["hold_days_list"]), 1) \
                       if sd["hold_days_list"] else 0
            by_symbol.append({
                "symbol":        sym,
                "count":         sd["count"],
                "pnl":           round(sd["pnl"], 2),
                "win_rate":      round(sd["wins"] / sd["count"] * 100, 1),
                "avg_hold_days": avg_hold,
            })
        by_symbol.sort(key=lambda x: x["pnl"], reverse=True)

        top_winners = sorted(by_symbol, key=lambda x: x["pnl"], reverse=True)[:5]
        top_losers  = sorted(by_symbol, key=lambda x: x["pnl"])[:5]

        # 收益曲线（按时间累计盈亏）
        equity_curve = []
        cumulative = 0
        for f in sorted(sell_fills, key=lambda x: x["filled_at"]):
            cumulative += f["pnl"]
            date_str = f["filled_at"][:10]
            if equity_curve and equity_curve[-1]["time"] == date_str:
                equity_curve[-1]["value"] = round(cumulative, 2)
            else:
                equity_curve.append({"time": date_str, "value": round(cumulative, 2)})

        return jsonify({
            "stats": {
                "total_pnl":     round(total_pnl, 2),
                "total_pnl_pct": total_pnl_pct,
                "win_rate":      win_rate,
                "win_count":     win_count,
                "loss_count":    loss_count,
                "avg_win":       round(avg_win, 2),
                "avg_loss":      round(avg_loss, 2),
                "avg_win_pct":   round(avg_win_pct, 2),
                "avg_loss_pct":  round(avg_loss_pct, 2),
                "profit_factor": profit_factor,
                "total_trades":  len(sell_fills),
                "max_win":       round(max_win_f["pnl"], 2) if max_win_f else 0,
                "max_win_sym":   max_win_f["symbol"] if max_win_f else "—",
                "max_loss":      round(max_loss_f["pnl"], 2) if max_loss_f else 0,
                "max_loss_sym":  max_loss_f["symbol"] if max_loss_f else "—",
            },
            "by_symbol":    by_symbol,
            "top_winners":  top_winners,
            "top_losers":   top_losers,
            "equity_curve": equity_curve,
            "trades":       sorted(fills, key=lambda x: x["filled_at"], reverse=True),
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
