"""Read-only valuation and separate hypothetical option positions. Never submits orders."""
import json
import math
import re
import threading
import time
import uuid
from datetime import date
from .db import db_conn


def ensure_tables():
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('''CREATE TABLE IF NOT EXISTS option_tracking (
                id VARCHAR(80) PRIMARY KEY, source VARCHAR(12) NOT NULL,
                payload LONGTEXT NOT NULL, state VARCHAR(30) NOT NULL DEFAULT 'OPEN',
                error VARCHAR(255), created_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')
            cur.execute('''CREATE TABLE IF NOT EXISTS option_tracking_points (
                id BIGINT AUTO_INCREMENT PRIMARY KEY, tracking_id VARCHAR(80) NOT NULL,
                pnl DECIMAL(20,6) NOT NULL, value DECIMAL(20,6) NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_tracking(tracking_id,created_at))''')


def validate(payload):
    symbol = str(payload.get('symbol','')).upper()
    expiry = date.fromisoformat(str(payload.get('expiry','')))
    if expiry < date.today():
        raise ValueError('不能模拟买入已到期期权')
    mode = payload.get('mode')
    if mode not in ('BULL_CALL','BEAR_PUT','BULL_PUT','BEAR_CALL'):
        raise ValueError('不支持的组合类型')
    raw_qty = float(payload.get('qty',1))
    if not math.isfinite(raw_qty) or not raw_qty.is_integer():
        raise ValueError('组数必须为整数')
    qty = int(raw_qty)
    if not 1 <= qty <= 99:
        raise ValueError('组数需为1至99')
    legs = []
    for name,side in [('buy','BUY'),('sell','SELL')]:
        leg = (payload.get('row') or {}).get(name) or {}
        code = str(leg.get('option_symbol',''))
        match = re.fullmatch(r'([A-Z]+)(\d{6})([CP])(\d{8})',code)
        if not match or match[1] != symbol or match[2] != expiry.strftime('%y%m%d'):
            raise ValueError('期权代码、标的或到期日不匹配')
        legs.append(dict(option_symbol=code,side=side,strike=int(match[4])/1000,cp=match[3]))
    buy,sell = legs
    cp = 'C' if mode in ('BULL_CALL','BEAR_CALL') else 'P'
    ascending = mode in ('BULL_CALL','BULL_PUT')
    if any(l['cp'] != cp for l in legs) or (buy['strike'] < sell['strike']) != ascending or buy['strike'] == sell['strike']:
        raise ValueError('组合方向或行权价不匹配')
    return dict(symbol=symbol,expiry=expiry.isoformat(),mode=mode,qty=qty,legs=legs)


def signed_value(legs, quotes, opening=False):
    value = 0.
    for leg in legs:
        quote = quotes.get(leg['option_symbol'])
        bid,ask = float(getattr(quote,'bid',0)),float(getattr(quote,'ask',0))
        if not all(math.isfinite(x) for x in (bid,ask)) or bid < 0 or ask <= 0 or ask < bid:
            raise ValueError('期权报价缺失或无效，未记录估值')
        buy = leg['side'].upper() == 'BUY'
        value += (ask if opening else bid) if buy else -(bid if opening else ask)
    return value


def simulate(payload):
    from app.strategy_q import _get_option_quotes
    record = validate(payload)
    ensure_tables()
    quotes = _get_option_quotes([l['option_symbol'] for l in record['legs']])
    entry = signed_value(record['legs'],quotes,True)
    width = abs(record['legs'][0]['strike']-record['legs'][1]['strike'])
    debit = record['mode'] in ('BULL_CALL','BEAR_PUT')
    if not (0 < entry < width if debit else -width < entry < 0):
        raise ValueError('组合报价超出价差范围，无法模拟')
    record.update(entry=entry,risk=(entry if debit else width+entry)*100*record['qty'])
    request_id = str(payload.get('request_id',''))
    uuid.UUID(request_id)
    identity = 'sim:'+request_id
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT IGNORE INTO option_tracking (id,source,payload) VALUES (%s,'SIM',%s)", (identity,json.dumps(record)))
            if cur.rowcount:
                value = signed_value(record['legs'],quotes)
                cur.execute('INSERT INTO option_tracking_points (tracking_id,pnl,value) VALUES (%s,%s,%s)',(identity,(value-entry)*100*record['qty'],value))
    return {'ok':True,'id':identity}


def _import_real():
    from app import strategy_q as q
    # Import actual fills only. Pending/rejected orders never become purchases here.
    conn = q._connect()
    try:
        spreads = q._load_q_spreads(conn,('SUBMITTED','OPEN','CLOSE_SUBMITTED','CLOSED'))
        for spread in spreads:
            identity = 'real:'+str(spread['id'])
            with db_conn() as db:
                with db.cursor() as cur:
                    cur.execute('SELECT id FROM option_tracking WHERE id=%s',(identity,))
                    exists = cur.fetchone()
            if exists or not spread.get('order_id'):
                continue
            order = q._get_trading_client().get_order_by_id(str(spread['order_id']))
            if q._order_status_text(order) != 'filled':
                continue
            entry = abs(float(order.filled_avg_price))
            if spread['mode'] in q.CREDIT_MODES:
                entry = -entry
            record=dict(symbol=spread['underlying'],expiry=str(spread['expiry'])[:10],mode=spread['mode'],qty=int(float(order.filled_qty)),entry=entry,
                        legs=q.load_spread_legs(conn,spread['id']),risk=float(spread.get('max_loss') or 0),order_id=str(order.id),filled_at=str(order.filled_at))
            with db_conn() as db:
                with db.cursor() as cur:
                    cur.execute("INSERT IGNORE INTO option_tracking (id,source,payload) VALUES (%s,'REAL',%s)",(identity,json.dumps(record,default=str)))
                    if cur.rowcount and order.filled_at:
                        from zoneinfo import ZoneInfo
                        filled_at = order.filled_at.astimezone(ZoneInfo('America/Los_Angeles')).replace(tzinfo=None)
                        cur.execute('INSERT INTO option_tracking_points (tracking_id,pnl,value,created_at) VALUES (%s,0,%s,%s)', (identity,entry,filled_at))
    finally:
        conn.close()
    return spreads


def collect():
    from app import strategy_q as q
    ensure_tables()
    try:
        spreads = _import_real()
    except Exception as exc:
        print("[OPTION TRACKING] real import unavailable:",type(exc).__name__,flush=True)
        spreads = []
    spread_map = {'real:'+str(s['id']):s for s in spreads}
    with db_conn() as db:
        with db.cursor() as cur:
            cur.execute("SELECT * FROM option_tracking WHERE state='OPEN'")
            records = cur.fetchall()
    for item in records:
        try:
            record=json.loads(item['payload']); state='OPEN'
            spread=spread_map.get(item['id'])
            if spread and spread.get('close_order_id'):
                order=q._get_trading_client().get_order_by_id(str(spread['close_order_id']))
                if q._order_status_text(order)=='filled':
                    if float(order.filled_qty) != record['qty']:
                        raise ValueError('存在部分平仓，待核对完整成交后结算')
                    value=abs(float(order.filled_avg_price)) * (-1 if record['entry']<0 else 1)
                    state='CLOSED'
            if state!='CLOSED':
                if item['source'] == 'REAL' and (not spread or spread.get('status') == 'CLOSED'):
                    raise ValueError('真实订单结算信息暂不可用，等待核对')
                if date.fromisoformat(record['expiry']) < date.today():
                    raise ValueError('已到期，等待核对结算；最后报价不代表最终收益')
                value=signed_value(record['legs'],q._get_option_quotes([l['option_symbol'] for l in record['legs']]))
            pnl=(value-record['entry'])*100*record['qty']
            with db_conn() as db:
                with db.cursor() as cur:
                    cur.execute('INSERT INTO option_tracking_points (tracking_id,pnl,value) VALUES (%s,%s,%s)',(item['id'],pnl,value))
                    cur.execute('UPDATE option_tracking SET state=%s,error=NULL WHERE id=%s',(state,item['id']))
        except Exception as exc:
            with db_conn() as db:
                with db.cursor() as cur:
                    cur.execute('UPDATE option_tracking SET error=%s WHERE id=%s',(str(exc)[:255],item['id']))


def history():
    ensure_tables()
    with db_conn() as db:
        with db.cursor() as cur:
            cur.execute('SELECT * FROM option_tracking ORDER BY created_at DESC')
            records=cur.fetchall()
            for item in records:
                item['combo']=json.loads(item.pop('payload'))
                cur.execute('SELECT pnl,value,created_at FROM option_tracking_points WHERE tracking_id=%s ORDER BY id',(item['id'],))
                item['points']=cur.fetchall()
    return {'ok':True,'records':records}


def start_collector():
    def run():
        while True:
            try: collect()
            except Exception as exc: print('[OPTION TRACKING]',type(exc).__name__,flush=True)
            time.sleep(300)
    threading.Thread(target=run,name='option-tracking',daemon=True).start()
