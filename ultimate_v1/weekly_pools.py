"""Weekly strategy equity ledger. Broker reads only; never rebalance positions.

Inventory value + signed executed cash flow measures profit, including open lots.
Unknown ownership/inventory discrepancies block budget increases, not guessed P&L.
"""
import json
import math
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from .db import db_conn

WEIGHTS = {'B': .4, 'C': .4, 'D': .2}
NY = ZoneInfo('America/New_York')


def calculate(initial, current):
    result = {}
    flow = current['flow'] - initial['flow']
    for group, weight in WEIGHTS.items():
        start = initial['equity'] * weight
        profit = (current['values'][group] - initial['values'][group]
                  + current['cash'][group] - initial['cash'][group])
        basis = start + flow * weight
        result[group] = dict(initial=start, net_flow=flow*weight, pnl=profit,
                             equity=basis+profit, return_pct=profit/basis if basis > 0 else None)
    return result


def _pages(client, after):
    rows, seen, token = [], set(), None
    while True:
        args = dict(after=after, direction='asc', page_size=100)
        if token:
            args['page_token'] = token
        page = client.get('/account/activities/FILL', data=args)
        if not isinstance(page, list):
            raise ValueError('成交流水暂不可用')
        for item in page:
            if not item.get('id') or item['id'] in seen:
                raise ValueError('成交分页重复')
            seen.add(item['id'])
            rows.append(item)
        if len(page) < 100:
            return rows
        token = page[-1]['id']


def _observe(cur, client, after, previous=None):
    from .adjusted_returns import _activities
    # Persist order ownership before stock_operations overwrites its last order id.
    sources = [
        'SELECT order_id, pool AS g FROM execution_orders',
        'SELECT order_id, strategy_group AS g FROM manual_trade_records',
        "SELECT order_id, 'D' AS g FROM d_grid_events WHERE order_id IS NOT NULL",
        "SELECT order_id, 'C' AS g FROM strategy_c_core_buys WHERE order_id IS NOT NULL",
        'SELECT exit_order_id AS order_id, strategy_group AS g FROM ac_t_cycle_results',
        'SELECT last_order_id AS order_id, COALESCE(NULLIF(strategy_group,\'\'),stock_type) AS g FROM stock_operations',
    ]
    for query in sources:
        cur.execute(query)
        for row in cur.fetchall():
            if row['order_id'] and row['g'] in WEIGHTS:
                cur.execute('INSERT IGNORE INTO weekly_pool_orders (order_id,pool) VALUES (%s,%s)',
                            (row['order_id'],row['g']))
    cur.execute('SELECT order_id,pool FROM weekly_pool_orders')
    owners = {r['order_id']:r['pool'] for r in cur.fetchall()}
    fills = _pages(client, after)
    cash = dict.fromkeys(WEIGHTS, 0.)
    quantities = {g:{} for g in WEIGHTS}
    unknown = []
    baseline_ids = set(previous.get('fill_ids', [])) if previous else {f['id'] for f in fills}
    for fill in fills:
        if fill['id'] in baseline_ids:
            continue
        group = owners.get(fill['order_id'])
        if group is None:
            order = client.get_order_by_id(fill['order_id'])
            cid = str(getattr(order, 'client_order_id', '') or '')
            if cid.startswith(('pool-B-', 'pool-C-', 'pool-D-')):
                group = cid.split('-')[1]
            elif cid.startswith('dgrid-'):
                group = 'D'
            if group:
                owners[fill['order_id']] = group
                cur.execute('INSERT IGNORE INTO weekly_pool_orders (order_id,pool) VALUES (%s,%s)', (fill['order_id'],group))
        symbol = fill['symbol']
        if group is None or fill['side'] not in ('buy','sell'):
            unknown.append(fill['id'])
            continue
        # Option legs need signed contract ownership; keep them unallocated for now.
        import re
        if re.search(r'\d{6}[CP]\d{8}$', symbol):
            unknown.append(fill['id'])
            continue
        qty, price = float(fill['qty']), float(fill['price'])
        if not math.isfinite(qty*price) or qty <= 0 or price <= 0:
            raise ValueError('成交数值无效')
        sign = 1 if fill['side'] == 'buy' else -1
        cash[group] -= sign * qty * price
        quantities[group][symbol] = quantities[group].get(symbol,0) + sign*qty
    positions = {p.symbol:p for p in client.get_all_positions()}
    cur.execute("SELECT symbol,strategy_group,qty FROM position_holdings WHERE status='open' AND strategy_group IN ('B','C','D')")
    inventory = {g:{} for g in WEIGHTS}
    for row in cur.fetchall():
        group, symbol = row['strategy_group'], row['symbol']
        inventory[group][symbol] = inventory[group].get(symbol,0) + float(row['qty'] or 0)
    values = dict.fromkeys(WEIGHTS,0.)
    for group in WEIGHTS:
        for symbol, qty in inventory[group].items():
            if qty:
                p = positions.get(symbol)
                if p is None or not math.isfinite(float(p.current_price)):
                    raise ValueError('持仓或行情尚未同步')
                total = sum(inventory[g].get(symbol,0) for g in WEIGHTS)
                if abs(total-float(p.qty)) > 1e-5:
                    raise ValueError('跨策略持仓份额待核对')
                values[group] += qty*float(p.current_price)
    if previous:
        for g in WEIGHTS:
            symbols = set(inventory[g]) | set(previous['inventory'][g]) | set(quantities[g]) | set(previous['quantities'][g])
            for s in symbols:
                expected = previous['inventory'][g].get(s,0) + quantities[g].get(s,0)-previous['quantities'][g].get(s,0)
                if abs(expected-inventory[g].get(s,0)) > 1e-5:
                    raise ValueError('成交与策略持仓待同步，暂不更新额度')
        if set(unknown)-set(previous['unknown']):
            raise ValueError('新增成交归属待核对，暂不更新额度')
    account = client.get_account()
    if not math.isfinite(float(account.equity)) or float(account.equity) <= 0:
        raise ValueError('账户净资产不可用')
    flow = sum(amount for _,amount in _activities(client,after))
    return dict(equity=float(account.equity), account_id=str(account.id), flow=flow,
                values=values,cash=cash,inventory=inventory,quantities=quantities,unknown=unknown,fill_ids=[f['id'] for f in fills])


def refresh(broker_snaps):
    """Shared persistent cache; once per minute across web/buy/sell workers."""
    from .account_config import profile_for_pool
    from .alpaca_gateway import trading_client
    now = datetime.now(NY)
    monday = now.date()-timedelta(days=now.weekday())
    profiles = {profile_for_pool(g) for g in WEIGHTS}
    if len(profiles) != 1 or profile_for_pool('A') in profiles:
        raise ValueError('周分配需要独立的 A 账户及共用的 B/C/D 账户')
    profile = profiles.pop()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('''CREATE TABLE IF NOT EXISTS weekly_pool_ledger (
                week_start DATE PRIMARY KEY, payload LONGTEXT NOT NULL,
                updated_at DATETIME NOT NULL) ENGINE=InnoDB''')
            cur.execute('''CREATE TABLE IF NOT EXISTS weekly_pool_orders (
                order_id VARCHAR(128) PRIMARY KEY,pool VARCHAR(8) NOT NULL) ENGINE=InnoDB''')
            cur.execute("SELECT GET_LOCK('weekly_pool_ledger',10) AS acquired")
            if int((cur.fetchone() or {}).get('acquired') or 0) != 1:
                raise ValueError('周资金账本正在同步')
            try:
                cur.execute('SELECT payload FROM weekly_pool_ledger WHERE week_start=%s',(monday,))
                row = cur.fetchone()
                state = json.loads(row['payload']) if row else None
                if state and (now-datetime.fromisoformat(state['checked_at'])).total_seconds() < 60:
                    return state
                # A new installation on weekends previews Monday, without backdating a baseline.
                if not state and now.weekday() >= 5:
                    equity = float(broker_snaps[profile].equity)
                    return dict(week_start=str(monday+timedelta(days=7)),preview=True,
                        groups={g:dict(initial=equity*w,equity=equity*w,pnl=0,net_flow=0,return_pct=0) for g,w in WEIGHTS.items()})
                try:
                    current = _observe(cur,trading_client(profile=profile),monday.isoformat(),state['initial'] if state else None)
                    if state and (state['profile'] != profile or state['initial']['account_id'] != current['account_id']):
                        raise ValueError('账户发生变化，周账本需核对')
                    initial = state['initial'] if state else current
                    groups = calculate(initial,current)
                    state = dict(week_start=str(monday),profile=profile,initial=initial,
                        started_at=state['started_at'] if state else now.isoformat(),
                        as_of=now.isoformat(),groups=groups,
                        unallocated=current['equity']-sum(g['equity'] for g in groups.values()))
                except Exception as exc:
                    if not state:
                        raise
                    state['error'] = str(exc)[:100]
                state['checked_at'] = now.isoformat()
                cur.execute('''INSERT INTO weekly_pool_ledger (week_start,payload,updated_at) VALUES (%s,%s,NOW())
                    ON DUPLICATE KEY UPDATE payload=VALUES(payload),updated_at=NOW()''',
                    (monday,json.dumps(state,ensure_ascii=False)))
                conn.commit()
                return state
            finally:
                cur.execute("SELECT RELEASE_LOCK('weekly_pool_ledger')")
