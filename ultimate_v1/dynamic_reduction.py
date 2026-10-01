"""Strategy-owned reduction consumers. Never invoked by the web or central planner.
B/C use durable intents and cumulative fills; D retains its cycle executor.
"""
from __future__ import annotations
import json
import math
import time
import uuid
from .dynamic_leverage import enabled, read_state
from .state_store import set_app_setting

TERMINAL={'filled','canceled','cancelled','expired','rejected','replaced'}


def reduction_qty(owned, broker_qty, excess, price, emergency=False):
    if min(owned,broker_qty,excess,price) <= 0:
        return 0.0
    # Ceiling in tenths avoids leaving a persistent excess below one lot.
    desired=math.ceil(excess/price*10-1e-9)/10
    return round(min(owned,broker_qty,desired),6)


def report(pool, status, **values):
    payload=dict(status=status,checked_at=time.time(),**values)
    set_app_setting('DYNAMIC_REDUCTION_'+pool,json.dumps(payload,ensure_ascii=False,default=str))
    return payload


def pool_excess(pool):
    if not enabled():
        return 0.,None
    state=read_state()
    # Missing evidence blocks buys, never initiates blind liquidations.
    if not state.get('fresh') or not state.get('valid'):
        return 0.,None
    from .capital_manager import get_capital_allocation
    allocation=get_capital_allocation()
    if allocation is None or allocation.weekly.get('error'):
        return 0.,None
    return max(0.,float(allocation.used.get(pool,0))-allocation.target_for(pool)),allocation


def consume(pool, symbol=None, *, locked=False):
    if not enabled() or pool not in {'B','C'}:
        return None
    from .account_config import profile_for_pool
    from .order_journal import execution_lock
    if not locked:
        with execution_lock(profile_for_pool(pool)):
            return consume(pool,symbol,locked=True)
    try:
        return _consume_locked(pool,symbol)
    except Exception as exc:
        report(pool,'ERROR',reason=str(exc)[:220])
        # No ordinary strategy order while an uncertain risk order is unresolved.
        return 'dynamic_reduction_error'


def _consume_locked(pool, symbol):
    from . import alpaca_gateway as broker, order_journal as journal
    from .account_config import profile_for_pool
    from .db import fetch_all
    from .config import settings
    from .manual_ledger import apply_order
    from .trade_history import _record_manual_trade
    from alpaca.trading.requests import GetOrdersRequest, LimitOrderRequest, MarketOrderRequest
    from alpaca.trading.enums import QueryOrderStatus, OrderSide, TimeInForce
    profile=profile_for_pool(pool); client=broker.trading_client(pool=pool)
    active=fetch_all("""SELECT * FROM execution_orders WHERE profile=%s AND pool=%s
        AND client_order_id LIKE 'cszy-dyn-%%'
        AND (state NOT IN ('filled','canceled','cancelled','expired','rejected','replaced') OR response_json IS NULL)
        ORDER BY created_at""",(profile,pool))
    for intent in active:
        if intent['state']=='prepared':
            data=json.loads(intent['request_json'])
            req_type=MarketOrderRequest if data['market'] else LimitOrderRequest
            kwargs=dict(symbol=intent['symbol'],qty=data['qty'],side=OrderSide.SELL,
                        time_in_force=TimeInForce.DAY,client_order_id=intent['client_order_id'])
            if not data['market']: kwargs['limit_price']=data['price']
            order=journal.submit_prepared(client,req_type(**kwargs),intent)
        else:
            order=client.get_order_by_client_id(intent['client_order_id'])
        result=apply_order(intent['client_order_id'],order)
        data=json.loads(intent["request_json"])
        _record_manual_trade(dict(source="动态降仓",symbol=intent["symbol"],pool=pool,side="sell",qty=data["qty"],price=data["price"],order_type="market" if data["market"] else "limit",order_id=str(order.id),**result))
        journal.update(intent['client_order_id'],response_json=json.dumps(result))
        status=str(result['status'])
        if status not in TERMINAL:
            data=json.loads(intent['request_json'])
            if not data['market'] and time.time()-float(data['started_at']) >= 60:
                if status != 'pending_cancel':
                    client.cancel_order_by_id(str(order.id))
                report(pool,'CANCEL_PENDING',order_id=str(order.id))
            else:
                report(pool,'SELL_WORKING',order_id=str(order.id),filled_qty=result['filled_qty'])
            return 'dynamic_sell_pending'
    excess,allocation=pool_excess(pool)
    if allocation is None:
        report(pool,'WAITING_DATA')
        return None
    state=read_state()
    tolerance=0.01 if state.get('emergency') else max(10.0, allocation.target_for(pool)*0.01)
    raw_excess=excess
    if excess <= tolerance:
        excess=0.
    report(pool,'REDUCING' if excess else 'WITHIN_LIMIT',excess=raw_excess,tolerance=tolerance,target=allocation.target_for(pool))
    if not client.get_clock().is_open:
        report(pool,'WAITING_MARKET',excess=excess)
        return 'dynamic_waiting_market'
    # Open orders cannot be overwritten: cancel only orders owned by this pool.
    open_orders=client.get_orders(filter=GetOrdersRequest(status=QueryOrderStatus.OPEN)) or []
    owned_ids={str(r['order_id']) for r in fetch_all(
        'SELECT order_id FROM execution_orders WHERE profile=%s AND pool=%s AND order_id IS NOT NULL',(profile,pool))}
    lots=fetch_all(f"SELECT stock_code,qty,last_order_id,ac_t_state FROM `{settings().ops_table}` WHERE stock_type=%s AND qty>0",(pool,))
    owned_ids.update(str(r.get('last_order_id')) for r in lots if r.get('last_order_id'))
    own_pending=[o for o in open_orders if str(o.id) in owned_ids or str(getattr(o,'client_order_id','')).startswith('pool-'+pool+'-')]
    cancel_orders = [o for o in own_pending if excess > .01 or (str(getattr(o,'side','')).split('.')[-1]=='buy' and not read_state().get('allow_buy'))]
    if cancel_orders:
        for order in cancel_orders:
            if str(getattr(order,'status','')).split('.')[-1]!='pending_cancel':
                client.cancel_order_by_id(str(order.id))
        return 'dynamic_cancel_own_orders'
    if excess <= .01:
        report(pool,'WITHIN_LIMIT',excess=0)
        return None
    # Other strategies on the same symbol may have a live order; wait rather than
    # cancel it or double-sell account inventory. Their normal reconciliation runs.
    busy={str(o.symbol).upper() for o in open_orders}
    positions={str(p.symbol).upper():p for p in client.get_all_positions()}
    same_account=[g for g in ('A','B','C','D','F') if profile_for_pool(g)==profile]
    placeholders=','.join(['%s']*len(same_account))
    all_lots=fetch_all(f"SELECT stock_code, SUM(qty) AS qty FROM `{settings().ops_table}` WHERE qty>0 AND stock_type IN ({placeholders}) GROUP BY stock_code",tuple(same_account))
    allocated={r['stock_code']:float(r['qty']) for r in all_lots}
    candidates=[r for r in lots if (symbol is None or r['stock_code']==symbol) and r['stock_code'] not in busy]
    lot_tolerance=False
    for lot in candidates:
        sym=lot['stock_code']; position=positions.get(sym)
        if not position: continue
        if abs(allocated.get(sym,0)-float(position.qty)) > .00001:
            report(pool,"WAITING_RECONCILIATION",symbol=sym,reason="策略数量与券商数量不一致")
            continue
        # Active C T cycles must settle using their own state machine first.
        if pool=='C' and str(lot.get('ac_t_state') or 'IDLE')!='IDLE':
            continue
        quote=broker.get_latest_stock_quote(sym,pool=pool)
        ts=quote.quote_timestamp if quote.bid else quote.trade_timestamp
        from datetime import datetime,timezone
        if isinstance(ts,str): ts=datetime.fromisoformat(ts.replace('Z','+00:00'))
        if not ts or not ts.tzinfo or not 0 <= (datetime.now(timezone.utc)-ts).total_seconds() <= 180:
            continue
        price=float(quote.bid or quote.last or 0)
        # Ordinary reductions must justify at least one strategy trading lot.
        minimum_lot = 1.0 if pool == 'B' else .1
        if not state.get('emergency') and excess < minimum_lot*price:
            lot_tolerance=True
            report(pool,'WITHIN_TOLERANCE',excess=excess,tolerance=max(tolerance,minimum_lot*price))
            continue
        qty=reduction_qty(float(lot['qty']),float(position.qty),excess,price)
        if pool == "B":
            qty=min(float(lot["qty"]),float(position.qty),float(math.ceil(qty)))
        if qty<=0: continue
        state=read_state()
        market=bool(state.get('emergency'))
        cid=f'cszy-dyn-{pool}-{uuid.uuid4().hex[:28]}'
        data=dict(qty=qty,price=broker.stock_limit_price(price),market=market,started_at=time.time(),reason='dynamic_ceiling_reduction')
        intent=journal.prepare(cid,profile,pool,sym,'sell',data)
        req_type=MarketOrderRequest if market else LimitOrderRequest
        kwargs=dict(symbol=sym,qty=qty,side=OrderSide.SELL,time_in_force=TimeInForce.DAY,client_order_id=cid)
        if not market: kwargs['limit_price']=data['price']
        order=journal.submit_prepared(client,req_type(**kwargs),intent)
        result=apply_order(cid,order)
        _record_manual_trade(dict(source="动态降仓",symbol=sym,pool=pool,side="sell",qty=qty,price=data["price"],order_type="market" if market else "limit",order_id=str(order.id),**result))
        journal.update(cid,response_json=json.dumps(result))
        report(pool,'SELL_WORKING',excess=excess,order_id=str(order.id),filled_qty=result['filled_qty'])
        return 'dynamic_sell_submitted'
    if not lot_tolerance:
        report(pool,'WAITING_STRATEGY',excess=excess,reason='持仓/挂单/报价或做T周期待处理')
    return None


def reconcile_pending():
    """Read/settle existing intents only; risk_bot never submits or cancels orders."""
    if not enabled():
        return
    from .db import fetch_all
    from . import alpaca_gateway as broker, order_journal as journal
    from .manual_ledger import apply_order
    from .trade_history import _record_manual_trade
    rows=fetch_all("""SELECT client_order_id,profile,pool FROM execution_orders
        WHERE client_order_id LIKE 'cszy-dyn-%%' AND state<>'prepared'
        AND (state NOT IN ('filled','canceled','cancelled','expired','rejected','replaced') OR response_json IS NULL)""")
    for row in rows:
        try:
            with journal.execution_lock(row['profile']):
                intent=journal.get_intent(row['client_order_id'])
                order=broker.trading_client(profile=row['profile']).get_order_by_client_id(row['client_order_id'])
                result=apply_order(row['client_order_id'],order)
                data=json.loads(intent['request_json'])
                _record_manual_trade(dict(source='动态降仓',symbol=intent['symbol'],pool=row['pool'],side='sell',qty=data['qty'],price=data['price'],order_type='market' if data['market'] else 'limit',order_id=str(order.id),**result))
                journal.update(row['client_order_id'],response_json=json.dumps(result))
        except Exception as exc:
            report(row['pool'],'RECONCILIATION_PENDING',reason=str(exc)[:220])
