"""D entry gate using 20 observed Alpaca prices over approximately five minutes."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import json
import math
import time
from threading import Lock

PREFIX = 'D_PRICE_SAMPLES:'
INTERVAL = 15
WINDOW = 300
COUNT = 20
_tick_lock = Lock()
_last_tick = 0.0


def evaluate_entry(current_price, previous_close, samples, *, now=None):
    now = now or datetime.now(timezone.utc)
    failed = lambda reason: {'ok':False, 'reason':reason}
    if not all(math.isfinite(v) and v > 0 for v in (current_price, previous_close)):
        return failed('invalid_price')
    if current_price <= previous_close * 1.03:
        return failed('daily_gain_not_above_3pct')
    market = ZoneInfo('America/New_York')
    samples = sorted((t,p) for t,p in samples if 0 <= now.timestamp()-t <= WINDOW
                     and datetime.fromtimestamp(t,market).date() == now.astimezone(market).date())[-COUNT:]
    if len(samples) < COUNT:
        return failed('collecting_prices')
    if now.timestamp()-samples[-1][0] > 30:
        return failed('stale_samples')
    if any(not math.isfinite(p) or p <= 0 for _,p in samples):
        return failed('invalid_samples')
    if any(not 10 <= samples[i][0]-samples[i-1][0] <= 30 for i in range(1,COUNT)):
        return failed('interrupted_sampling')
    times = [t-samples[0][0] for t,p in samples]
    prices = [p for t,p in samples]
    mean_t, mean_p = sum(times)/COUNT, sum(prices)/COUNT
    slope = sum((t-mean_t)*(p-mean_p) for t,p in zip(times,prices))/sum((t-mean_t)**2 for t in times)
    first, last = sum(prices[:5])/5, sum(prices[-5:])/5
    rising = slope > 0 and last > first and current_price >= last
    return dict(ok=rising, reason='ok' if rising else 'not_rising',
                day_gain_pct=round((current_price/previous_close-1)*100,4),
                slope=slope, sample_count=COUNT, span_seconds=times[-1], price=current_price)


def read_observation(symbol):
    from .state_store import get_app_setting
    raw = get_app_setting(PREFIX+symbol, '')
    return json.loads(raw) if raw else {}


def update_observation(old, snapshot, now, feed):
    """Ignore repeated/stale trade timestamps; never manufacture historical points."""
    trade = snapshot.latest_trade
    timestamp = trade.timestamp
    if timestamp.tzinfo is None:
        return old
    epoch = timestamp.timestamp()
    price = float(trade.price)
    market = ZoneInfo('America/New_York')
    if not math.isfinite(price) or price <= 0 or not 0 <= now.timestamp()-epoch <= 30:
        return old
    previous = snapshot.previous_daily_bar
    if previous is None or not 1 <= (now.astimezone(market).date()-previous.timestamp.astimezone(market).date()).days <= 5:
        return old
    prev = float(previous.close)
    if not math.isfinite(prev) or prev <= 0:
        return old
    if old.get('feed') != feed:
        old = {}
    points = [p for p in old.get('samples',[]) if 0 <= now.timestamp()-p[0] <= WINDOW]
    result = dict(old, feed=feed, price=price, previous_close=prev, quote_epoch=epoch,
                  day_high=float(getattr(snapshot.daily_bar,'high',0) or 0),
                  day_volume=float(getattr(snapshot.daily_bar,'volume',0) or 0), samples=points)
    if epoch <= float(old.get('last_sample_trade_epoch',0)):
        return result
    if points and int(now.timestamp() // INTERVAL) <= int(points[-1][0] // INTERVAL):
        return result
    result['samples'] = (points+[[now.timestamp(),price]])[-COUNT:]
    result['last_sample_trade_epoch'] = epoch
    return result


def collect_prices(symbols, *, now=None):
    from alpaca.data.requests import StockSnapshotRequest
    from .alpaca_gateway import stock_data_client
    from .config import env_str
    from .state_store import set_app_setting
    now = now or datetime.now(timezone.utc)
    symbols = list(dict.fromkeys(symbols))
    if not symbols:
        return
    feed = env_str('D_SAMPLE_DATA_FEED','iex')
    snapshots = stock_data_client(pool='D').get_stock_snapshot(
        StockSnapshotRequest(symbol_or_symbols=symbols,feed=feed))
    for symbol in symbols:
        snapshot = snapshots.get(symbol)
        if snapshot is None or snapshot.latest_trade is None:
            continue
        old = read_observation(symbol)
        updated = update_observation(old,snapshot,now,feed)
        if updated != old:
            set_app_setting(PREFIX+symbol,json.dumps(updated))


def sample_tick():
    """One batched request per 15 seconds, independent of hourly symbol rotation."""
    global _last_tick
    now = datetime.now(timezone.utc)
    local = now.astimezone(ZoneInfo('America/New_York'))
    if local.weekday() >= 5 or not (570 <= local.hour*60+local.minute < 960):
        return
    if not _tick_lock.acquire(blocking=False):
        return
    try:
        slot = int(time.monotonic() // INTERVAL)
        if slot == _last_tick:
            return
        _last_tick = slot
        from .db import fetch_all
        candidates = fetch_all('SELECT symbol FROM d_candidate_pool WHERE enabled=1 ORDER BY signal_date DESC,base_score DESC,signal_dollar_volume DESC LIMIT 120')
        enabled = fetch_all('SELECT symbol FROM d_grid_symbols WHERE enabled=1')
        collect_prices([str(r['symbol']).upper() for r in candidates+enabled],now=now)
    except Exception as exc:
        print(f'[D SAMPLES] unavailable: {type(exc).__name__}',flush=True)
    finally:
        _tick_lock.release()


def check_entry(symbol, current_price=None, *, refresh=False):
    try:
        now = datetime.now(timezone.utc)
        if refresh:
            collect_prices([symbol],now=now)
        observation = read_observation(symbol)
        if not 0 <= now.timestamp()-float(observation.get('quote_epoch',0)) <= 30:
            return {'ok':False,'reason':'stale_quote'}
        # Both daily gain and trend use the same timestamped Alpaca trade source.
        return evaluate_entry(float(observation.get('price',0)),float(observation.get('previous_close',0)),
                              observation.get('samples',[]),now=now)
    except Exception as exc:
        return {'ok':False,'reason':'market_data_unavailable','error_type':type(exc).__name__}
