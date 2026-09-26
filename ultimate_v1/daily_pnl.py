"""Read-only broker observations, persisted independently of dashboard visits."""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from threading import Event, Thread
from zoneinfo import ZoneInfo

from .db import db_conn, fetch_all

NY = ZoneInfo('America/New_York')


def today_key():
    return datetime.now(NY).date().isoformat()


def ensure_table():
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('''CREATE TABLE IF NOT EXISTS daily_pnl_reports (
                report_date DATE PRIMARY KEY,
                payload LONGTEXT NOT NULL,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4''')


def attribute(positions, fills, closes):
    """Day attribution: closing inventory + sales - purchases, all vs prior close.

    This is NOT realized P&L; no historical cost is inferred for sold holdings.
    """
    rows = {}
    for p in positions:
        symbol = p['symbol']
        rows[symbol] = {'symbol': symbol, 'qty': float(p['qty']),
                        'market_value': float(p['market_value']),
                        'unrealized_pnl': float(p['unrealized_pl']),
                        'price': float(p['current_price']), 'trades': []}
    for fill in fills:
        symbol = fill['symbol']
        row = rows.setdefault(symbol, {'symbol': symbol, 'qty': 0, 'market_value': 0,
                                       'unrealized_pnl': 0, 'price': 0, 'trades': []})
        row['trades'].append(fill)
    for symbol, row in rows.items():
        close = closes.get(symbol)
        row['previous_close'] = close
        row['daily_pnl'] = None
        # Full same-day round trips can be valued even without previous close.
        opening_qty = row['qty'] + sum(float(f['qty']) * (1 if f['side'] == 'sell' else -1)
                                       for f in row['trades'])
        if re.search(r'\d{6}[CP]\d{8}$', symbol):
            row['daily_pnl'] = None  # Options require contract multiplier and separate valuation.
            continue
        if close is not None or (abs(opening_qty) < 1e-8 and abs(row['qty']) < 1e-8):
            base = close or 0
            pnl = row['qty'] * (row['price'] - base)
            pnl += sum(float(f['qty']) * (float(f['price']) - base) *
                       (1 if f['side'] == 'sell' else -1) for f in row['trades'])
            row['daily_pnl'] = round(pnl, 2)
    return sorted(rows.values(), key=lambda r: (r['daily_pnl'] is None, r['daily_pnl'] or 0))


def _collect_profile(now=None, profile="trading", client=None, account=None, allowed_groups=None):
    from .alpaca_gateway import trading_client, stock_data_client
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame
    now = now or datetime.now(NY)
    day = now.date().isoformat()
    client = client or trading_client(profile=profile)
    account = account or client.get_account()
    positions = [{k: str(getattr(p, k)) for k in
                  ('symbol', 'qty', 'market_value', 'unrealized_pl', 'current_price')}
                 for p in client.get_all_positions()]
    fills, seen, token = [], set(), None
    while True:
        args = {'date': day, 'direction': 'asc', 'page_size': 100}
        if token:
            args['page_token'] = token
        page = client.get('/account/activities/FILL', data=args)
        if not isinstance(page, list):
            raise ValueError('Invalid broker activity response')
        new = [f for f in page if f['id'] not in seen]
        if page and not new:
            raise ValueError('Broker activity pagination stalled')
        for f in new:
            seen.add(f['id'])
            if f.get('side') not in ('buy', 'sell'):
                raise ValueError('Unsupported fill side')
            fills.append({k: f.get(k) for k in
                          ('id', 'symbol', 'side', 'qty', 'price', 'transaction_time', 'order_id')})
        if len(page) < 100:
            break
        token = page[-1]['id']
    symbols = sorted(s for s in ({p['symbol'] for p in positions} | {f['symbol'] for f in fills})
                     if not re.search(r'\d{6}[CP]\d{8}$', s))
    closes, notes = {}, []
    if symbols:
        try:
            midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
            bars = stock_data_client(profile=profile).get_stock_bars(StockBarsRequest(
                symbol_or_symbols=symbols, timeframe=TimeFrame.Day,
                start=midnight - timedelta(days=10), end=midnight - timedelta(microseconds=1), feed='iex'))
            for symbol, values in bars.data.items():
                eligible = [b for b in values if b.timestamp.astimezone(NY).date() < now.date()]
                if eligible:
                    closes[symbol] = float(max(eligible, key=lambda b: b.timestamp).close)
        except Exception:
            notes.append('昨收行情获取失败，缺失标的不估算当日盈亏。')
    rows = attribute(positions, fills, closes)
    ops = fetch_all('''SELECT stock_code,stock_type,cost_price,b_peak_price
                       FROM stock_operations WHERE is_bought=1''')
    if allowed_groups is not None:
        ops = [op for op in ops if op['stock_type'] in allowed_groups]
    groups = {}
    for op in ops:
        groups.setdefault(op['stock_code'], set()).add(op['stock_type'])
    equity, previous = float(account.equity), float(account.last_equity)
    for row in rows:
        row['strategy_hint'] = 'A' if allowed_groups == {'A'} else '/'.join(sorted(groups.get(row['symbol'], []))) or '未归类'
        row['account_profile'] = profile
        row['equity_weight'] = abs(row['market_value']) / equity if equity > 0 else None
    if rows:
        known = [r for r in rows if r['daily_pnl'] is not None]
        if known:
            worst, best = min(known, key=lambda r:r['daily_pnl']), max(known, key=lambda r:r['daily_pnl'])
            if worst['daily_pnl'] < 0:
                notes.append(f"主要拖累：{worst['symbol']}，当日估算 {worst['daily_pnl']:+.2f} 美元。")
            if best['daily_pnl'] > 0:
                notes.append(f"主要贡献：{best['symbol']}，当日估算 {best['daily_pnl']:+.2f} 美元。")
        largest = max(rows, key=lambda r:abs(r['market_value']))
        if largest['equity_weight'] and largest['equity_weight'] > .3:
            notes.append(f"仓位集中：{largest['symbol']} 市值占净值 {largest['equity_weight']:.1%}。")
    for op in ops:
        if op['stock_type'] == 'B' and float(op['cost_price'] or 0) > 0:
            peak = float(op['b_peak_price'] or 0) / float(op['cost_price']) - 1
            if 0 <= peak < .05:
                notes.append(f"B / {op['stock_code']} 记录最高涨幅 {peak:.2%}，未达到 5% 回撤保护启动线。")
    # Prevent a report from spanning the broker's overnight accounting rollover.
    end = datetime.now(NY)
    if end.date() != now.date() or end.hour >= 20:
        raise ValueError('Outside report accounting window')
    return {'date': day, 'as_of': now.isoformat(), 'equity': equity,
            'previous_equity': previous, 'equity_change': round(equity - previous, 2),
            'rows': rows, 'fills': fills, 'notes': notes,
            'estimated_total': round(sum(r['daily_pnl'] or 0 for r in rows), 2),
            'methodology': '保证金交易账户（B/C/D/F及期权），不含A养老金。净值变化未剔除出入金；逐股当日盈亏按IEX昨收、当前持仓和券商实际成交估算，非已实现盈亏。期权不套用股票计算，缺失项显示待核对。累计浮盈亏单列；策略归属仅为当前数据库标签，同名跨策略可能不准确。行情口径、费用及资金变动会造成与净值变化的差额。'}


def collect_report(now=None):
    from .account_config import profile_for_pool
    from .alpaca_gateway import trading_client
    now = now or datetime.now(NY)
    profiles = {}
    for group in ('A','B','C','D','F'):
        profiles.setdefault(profile_for_pool(group), set()).add(group)
    accounts = {}
    for profile, groups in profiles.items():
        client = trading_client(profile=profile)
        account = client.get_account()
        identity = str(account.id)
        if identity in accounts:
            accounts[identity]['groups'].update(groups)
        else:
            accounts[identity] = dict(profile=profile,groups=set(groups),client=client,account=account)
    reports = [_collect_profile(now, x['profile'],x['client'],x['account'],x['groups']) for x in accounts.values()]
    if not reports:
        raise ValueError('No account reports')
    equity = sum(r['equity'] for r in reports)
    previous = sum(r['previous_equity'] for r in reports)
    rows = [row for r in reports for row in r['rows']]
    for row in rows:
        row['equity_weight'] = abs(row['market_value']) / equity if equity > 0 else None
    return dict(date=now.date().isoformat(),as_of=now.isoformat(),equity=equity,
                previous_equity=previous,equity_change=round(equity-previous,2),
                rows=sorted(rows,key=lambda r:(r['daily_pnl'] is None,r['daily_pnl'] or 0)),
                fills=[dict(f,account_profile=x['profile']) for x,r in zip(accounts.values(),reports) for f in r['fills']],
                accounts=[dict(profile=x['profile'],groups=sorted(x['groups']),equity=r['equity'],equity_change=r['equity_change']) for x,r in zip(accounts.values(),reports)],
                notes=[x['profile']+'：'+n for x,r in zip(accounts.values(),reports) for n in r['notes'] if not n.startswith('仓位集中')],
                estimated_total=round(sum(r['estimated_total'] for r in reports),2),
                methodology='账户合计包含 A 养老金，按券商账户去重；同名股票按账户分开。净值变化含入出金；股票当日估算按昨收、持仓和实际成交计算，非已实现收益。期权、缺行情项待核对，估算合计仅包含可估值股票；未扣费用。旧日报保留原采集口径。')


def save_report(report):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('''INSERT INTO daily_pnl_reports(report_date,payload) VALUES (%s,%s)
                           ON DUPLICATE KEY UPDATE payload=VALUES(payload),updated_at=NOW()''',
                        (report['date'], json.dumps(report, ensure_ascii=False)))


def report_payload(day=None):
    from datetime import date
    day = day or today_key()
    if date.fromisoformat(day).isoformat() != day:
        raise ValueError('Invalid date')
    dates = [str(r['report_date']) for r in fetch_all('SELECT report_date FROM daily_pnl_reports ORDER BY report_date DESC')]
    rows = fetch_all('SELECT payload FROM daily_pnl_reports WHERE report_date=%s', (day,))
    return {'ok': True, 'date': day, 'today': today_key(), 'dates': dates,
            'report': json.loads(rows[0]['payload']) if rows else None}


def start_collector():
    stop = Event()
    def loop():
        initialized = False
        while not stop.is_set():
            try:
                if not initialized:
                    ensure_table()
                    initialized = True
                now = datetime.now(NY)
                # Broker last_equity rolls after 20:00 ET. Keep the last valid
                # observation instead of replacing it with next-day accounting.
                if 4 <= now.hour < 20:
                    save_report(collect_report(now))
            except Exception as exc:
                print(f'[DAILY PNL] refresh failed: {type(exc).__name__}', flush=True)
            stop.wait(300)
    Thread(target=loop, name='daily-pnl-archive', daemon=True).start()
    return stop
