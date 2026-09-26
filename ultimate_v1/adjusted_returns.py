"""Fresh performance epoch with broker cash transfers excluded from returns."""
import json
import math
import threading
import time
from datetime import datetime, date, timedelta
from .db import db_conn
from .state_store import equity_curve_bounds
from zoneinfo import ZoneInfo

RESET_DAY = date(2026, 9, 26)
TRACKING_START = date(2026, 9, 28)
RESET_KEY = "RETURN_BASELINE_20260928"

def tracking_today():
    return datetime.now(ZoneInfo("America/Los_Angeles")).date()

def tracking_bounds(period):
    from calendar import monthrange
    today = max(tracking_today(), TRACKING_START)
    if period == "week":
        monday = today - timedelta(days=today.weekday())
        return monday, monday + timedelta(days=4)
    if period == "month":
        return today.replace(day=1), today.replace(day=monthrange(today.year,today.month)[1])
    if period == "year":
        return date(today.year,1,1), date(today.year,12,31)
    return RESET_DAY, max(tracking_today(), RESET_DAY)

_lock = threading.Lock()
_last_attempt = 0
_error = None


def metrics(first, last):
    base = float(first['equity']) + float(last['net_flow']) - float(first['net_flow'])
    profit = float(last['equity']) - base
    return {'basis': base, 'profit': profit, 'return_fraction': profit/base if base > 0 else None}


def _activities(client, after):
    rows, seen, token = [], set(), None
    while True:
        args = {'activity_types':'CSD,CSW', 'after':after, 'direction':'asc', 'page_size':100}
        if token:
            args['page_token'] = token
        page = client.get('/account/activities', data=args)
        if not isinstance(page, list):
            raise ValueError('Invalid cash transfer response')
        for row in page:
            if not row.get('id') or row['id'] in seen:
                raise ValueError('Invalid/repeated cash transfer id')
            seen.add(row['id'])
            if row.get('activity_type') not in ('CSD','CSW'):
                raise ValueError('Unexpected cash transfer type')
            amount = float(row['net_amount'])
            if not math.isfinite(amount):
                raise ValueError('Invalid transfer amount')
            rows.append((row['id'], amount))
        if len(page) < 100:
            return rows
        token = page[-1]['id']


def collect():
    """Read only broker requests. Persist a point only when every account succeeds."""
    global _last_attempt, _error
    with _lock:
        if time.monotonic() - _last_attempt < 300:
            return
        _last_attempt = time.monotonic()
        try:
            _collect()
            _error = None
        except Exception:
            _error = '资金流水同步未完成，保留上次有效数据'


def _collect():
    from .account_config import profile_for_pool
    from .alpaca_gateway import trading_client
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('''CREATE TABLE IF NOT EXISTS adjusted_return_points (
                id BIGINT AUTO_INCREMENT PRIMARY KEY, created_at DATETIME(6) NOT NULL,
                equity DECIMAL(20,6) NOT NULL, net_flow DECIMAL(20,6) NOT NULL,
                INDEX idx_return_time(created_at))''')
            cur.execute("SELECT setting_value FROM app_settings WHERE setting_key='RETURN_EPOCH_V1'")
            record = cur.fetchone()
    epoch = json.loads(record['setting_value']) if record else None
    after = epoch['after'] if epoch else (date.today()-timedelta(days=1)).isoformat()
    accounts, equity, flow, initial_ids = set(), 0., 0., {}
    for profile in sorted({profile_for_pool(p) for p in ('A','B','C','D')}):
        client = trading_client(profile=profile)
        account = client.get_account()
        account_id = str(account.id)
        if account_id in accounts:
            continue
        accounts.add(account_id)
        activities = _activities(client, after)
        initial_ids[account_id] = [i for i,_ in activities]
        excluded = set(epoch['ids'].get(account_id, [])) if epoch else set(initial_ids[account_id])
        flow += sum(amount for i,amount in activities if i not in excluded)
        equity += float(client.get_account().equity)
    if not accounts or not math.isfinite(equity) or equity <= 0:
        raise ValueError('Missing account equity')
    if epoch and accounts != set(epoch['ids']):
        raise ValueError('Account mapping changed; return epoch needs reconciliation')
    # Freeze the initial transfer IDs and the first equity sample together.
    with db_conn() as conn:
        with conn.cursor() as cur:
            if epoch is None:
                cur.execute("INSERT INTO app_settings (setting_key,setting_value,updated_at) VALUES ('RETURN_EPOCH_V1',%s,NOW())", (json.dumps({'after':after,'ids':initial_ids}),))
            cur.execute('INSERT INTO adjusted_return_points (created_at,equity,net_flow) VALUES (NOW(6),%s,%s)', (equity,flow))


def curve(period, bounds=None, refresh=True):
    if refresh:
        collect()
    start, end = bounds or tracking_bounds(period)
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT * FROM adjusted_return_points ORDER BY created_at,id')
                points = cur.fetchall()
                cur.execute("SELECT setting_value FROM app_settings WHERE setting_key=%s", (RESET_KEY,))
                saved = cur.fetchone()
                if not saved and points:
                    candidates = [p for p in points if p['created_at'].date() <= RESET_DAY]
                    anchor = candidates[-1] if candidates else points[0]
                    cur.execute("INSERT IGNORE INTO app_settings (setting_key,setting_value,updated_at) VALUES (%s,%s,NOW())", (RESET_KEY,str(anchor['id'])))
                    cur.execute("SELECT setting_value FROM app_settings WHERE setting_key=%s", (RESET_KEY,))
                    saved = cur.fetchone()
                if saved:
                    anchor_id = int(saved['setting_value'])
                    points = [p for p in points if p['id'] >= anchor_id]
    except Exception:
        points = []
    # Preserve the reset baseline, then start measuring on the requested Monday.
    if period != 'all' and points:
        points = [points[0]] + [p for p in points[1:] if p['created_at'].date() >= TRACKING_START]
    eligible = [p for p in points if end is None or p['created_at'].date() <= end]
    if start and eligible:
        prior = [p for p in eligible if p['created_at'].date() < start]
        eligible = ([prior[-1]] if prior else []) + [p for p in eligible if p['created_at'].date() >= start]
    # Keep the exact baseline, then the final sample of each day.
    rows = []
    if eligible:
        by_day = {}
        for p in eligible[1:]:
            by_day[p['created_at'].date()] = p
        rows = [eligible[0]] + list(by_day.values())
    rows = [dict(p, snapshot_date=max(start, min(end, p['created_at'].date())).isoformat()) for p in rows]
    payload = {'tracking_start':TRACKING_START.isoformat(), 'period':period,'start_date':start.isoformat() if start else (rows[0]['created_at'].date().isoformat() if rows else None),
               'end_date':end.isoformat() if end else date.today().isoformat(), 'rows':rows,
               'adjusted':True,'warning':_error,'profit':None,'return_fraction':None}
    if rows:
        payload.update(metrics(rows[0],rows[-1]))
    return payload


def start_collector():
    def run():
        while True:
            collect()
            time.sleep(300)
    thread = threading.Thread(target=run, name='adjusted-return-collector', daemon=True)
    thread.start()
    return thread
