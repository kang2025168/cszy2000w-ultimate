"""B holding age is based on confirmed fills, never sync/heartbeat timestamps."""
from datetime import datetime, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo

NY = ZoneInfo('America/New_York')


def _aware(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if value is None:
        return None
    # b_entry_at is stored as UTC without timezone.
    return value.replace(tzinfo=ZoneInfo('UTC')) if value.tzinfo is None else value


@lru_cache(maxsize=128)
def _sessions(start, end):
    from alpaca.trading.requests import GetCalendarRequest
    from .alpaca_gateway import trading_client
    return trading_client(pool='B').get_calendar(GetCalendarRequest(start=start, end=end))


def time_exit_due(entry, peak_gain, now, sessions):
    if entry is None or peak_gain >= 0.05 - 1e-9:
        return False
    entry_day = _aware(entry).astimezone(NY).date()
    today = now.astimezone(NY).date()
    held = sorted((s for s in sessions if entry_day <= s.date <= today), key=lambda s: s.date)
    if len(held) < 3:
        return False
    today_session = next((s for s in held if s.date == today), None)
    if today_session is None:
        return False
    close = today_session.close
    opening = today_session.open
    if close.tzinfo is None:
        close = close.replace(tzinfo=NY)
    if opening.tzinfo is None:
        opening = opening.replace(tzinfo=NY)
    return opening <= now < close and (len(held) > 3 or now >= close - timedelta(minutes=10))


def confirmed_entry(client, row, journal_rows):
    """Recover the current B lot from B order IDs; refuse ambiguous history."""
    from .order_fills import status_text
    ids = {str(r['order_id']) for r in journal_rows if r.get('order_id')}
    if str(row.get('last_order_side') or '').lower() == 'buy' and row.get('last_order_id'):
        ids.add(str(row['last_order_id']))
    fills = []
    for oid in ids:
        order = client.get_order_by_id(oid)
        qty = float(getattr(order, 'filled_qty', 0) or 0)
        stamp = getattr(order, 'filled_at', None)
        if qty <= 0:
            continue
        if stamp is None:
            return None
        fills.append((_aware(stamp), status_text(order.side), qty))
    balance = 0.0
    entry = None
    for stamp, side, qty in sorted(fills):
        if side == 'buy':
            if balance <= 1e-8:
                entry = stamp
            balance += qty
        elif side == 'sell':
            balance -= qty
            if balance < -1e-8:
                return None
            if balance <= 1e-8:
                entry = None
    if abs(balance - float(row.get('qty') or 0)) > 1e-6:
        return None
    return entry


def holding_exit_due(conn, row, symbol, table, peak_gain):
    if peak_gain >= 0.05 - 1e-9:
        return False
    entry = _aware(row.get('b_entry_at'))
    if entry is None:
        from .alpaca_gateway import trading_client
        from .account_config import profile_for_pool
        with conn.cursor() as cur:
            cur.execute("""SELECT order_id FROM execution_orders
                WHERE pool='B' AND profile=%s AND symbol=%s AND order_id IS NOT NULL""",
                (profile_for_pool('B'), symbol))
            rows = list(cur.fetchall() or [])
            cur.execute("SELECT order_id FROM manual_trade_records WHERE strategy_group='B' AND symbol=%s", (symbol,))
            rows.extend(cur.fetchall() or [])
        entry = confirmed_entry(trading_client(pool='B'), row, rows)
        if entry is None:
            print(f'[B TIME EXIT] {symbol}: original fill time unavailable; cannot infer holding age', flush=True)
            return False
        with conn.cursor() as cur:
            cur.execute(f"UPDATE `{table}` SET b_entry_at=%s WHERE stock_code=%s AND stock_type='B' AND b_entry_at IS NULL",
                        (entry.astimezone(ZoneInfo('UTC')).replace(tzinfo=None), symbol))
    now = datetime.now(NY)
    return time_exit_due(entry, peak_gain, now, _sessions(entry.astimezone(NY).date(), now.date()))
