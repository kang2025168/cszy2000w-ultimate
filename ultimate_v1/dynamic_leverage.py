"""Persistent account ceiling. Only risk_bot advances the state machine."""
from __future__ import annotations
import json
import math
from datetime import datetime
from zoneinfo import ZoneInfo
from .config import env_bool, settings
from .state_store import get_app_setting, set_app_setting

KEY = 'DYNAMIC_LEVERAGE_STATE_V1'
SAMPLE_SECONDS = 300
CONFIRMATIONS = 3
MAX_STATE_AGE = 180


def enabled():
    return env_bool('DYNAMIC_RISK_ENABLED', True)


def candidate(risk):
    trend, vix = risk.market_trend, float(risk.vix)
    loss, dd = int(risk.loss_days), float(risk.max_drawdown)
    circuit = bool(risk.block_all_new) or float(risk.daily_pnl_pct) <= -abs(settings().daily_loss_limit_pct)
    if circuit or dd >= .10 or loss >= 3 or (trend == '向下' and vix >= 28):
        base, reason = .5, '熔断'
    elif trend == '向下' or vix >= 24 or loss >= 2:
        base, reason = .75, '防守'
    elif vix >= 20:
        base, reason = 1., '谨慎'
    elif trend == '横盘':
        base, reason = 1.2, '中性'
    elif trend == '向上' and vix < 16 and float(risk.qqq_change_pct) >= 0:
        base, reason = 1.5, '进攻'
    else:
        base, reason = 1.4, '偏强'
    factor = {'激进':1.,'中性':.9,'保守':.75}.get(risk.risk_preference,.9)
    return round(max(.5,min(1.5,base*factor)),4), reason, circuit


def advance(previous, *, target, reason, circuit, now, market_open, valid):
    """No clock/DB/broker effects; caller provides a fresh observation."""
    stamp=now.timestamp(); day=now.astimezone(ZoneInfo('America/New_York')).date().isoformat()
    state=dict(previous or {})
    current=float(state.get('ceiling',min(1.,target)))
    if not math.isfinite(target) or not .5 <= target <= 1.5:
        valid=False
    state.update(checked_at=stamp,market_open=market_open,valid=valid)
    if circuit:
        state['circuit_day']=day
    if not valid:
        state.update(count=0,pending=None,status='DATA_UNAVAILABLE',reason='行情/账户数据不可核验，暂停新增买入')
        state.setdefault('ceiling',.5)
        return state
    if circuit:
        state['circuit_day']=day
    if state.get('circuit_day') == day:
        target=.5; reason='日亏损熔断，当日禁止恢复'
    if not market_open:
        state.update(count=0,pending=None,status='MARKET_CLOSED',reason='休市等待有效行情')
        state.setdefault('ceiling',current)
        return state
    state['candidate']=target
    state['reason']=reason
    state['emergency']=target <= .5
    if target < current:
        state.update(ceiling=target,count=0,pending=None,last_sample=stamp,status='REDUCING')
        state['changed_at']=stamp
    elif target > current:
        next_step=min(target, 1.0 if current < 1.0 else 1.25 if current < 1.25 else 1.5)
        # Confirmation requires the same next step, not merely three loop calls.
        if state.get('pending') != next_step or stamp-float(state.get('last_sample',0)) > SAMPLE_SECONDS*2:
            state.update(pending=next_step,count=1,last_sample=stamp)
        elif stamp-float(state.get('last_sample',0)) >= SAMPLE_SECONDS:
            state.update(count=int(state.get('count',0))+1,last_sample=stamp)
        state['status']='CONFIRMING'
        if state['count'] >= CONFIRMATIONS:
            state.update(ceiling=next_step,count=0,pending=None,status='CAPACITY_RELEASED',changed_at=stamp)
    else:
        state.update(count=0,pending=None,status='STEADY')
    state.setdefault('ceiling',current)
    return state


def read_state(now=None):
    now=now or datetime.now(ZoneInfo('UTC'))
    try:
        state=json.loads(get_app_setting(KEY,'{}') or '{}')
        age=now.timestamp()-float(state.get('checked_at',0))
        ceiling=float(state.get('ceiling',.5))
        if not math.isfinite(ceiling) or not .5 <= ceiling <= 1.5:
            raise ValueError('bad ceiling')
        state['initialized']='ceiling' in state and 'checked_at' in state
        state['age_seconds']=max(0,age)
        state['fresh']=state['initialized'] and 0 <= age <= MAX_STATE_AGE
        state['allow_buy']=bool(state.get('valid') and state['fresh'] and state.get('market_open') and
                                state.get('circuit_day') != now.astimezone(ZoneInfo('America/New_York')).date().isoformat())
        return state
    except (ValueError,TypeError,AttributeError):
        return dict(initialized=False,fresh=False,valid=False,allow_buy=False,status='DATA_UNAVAILABLE',reason='尚未取得有效风险状态')


def refresh(risk):
    from . import alpaca_gateway
    from .order_journal import execution_lock
    from .alpaca_gateway import get_latest_stock_quote
    now=datetime.now(ZoneInfo('UTC'))
    valid=False; market_open=False; candidate_valid=True
    try:
        target,reason,circuit=candidate(risk)
    except (ValueError,TypeError,AttributeError):
        target,reason,circuit=.5,"数据无效",False
        candidate_valid=False
    try:
        clock=alpaca_gateway.trading_client(pool='B').get_clock()
        market_open=bool(clock.is_open)
        quote=get_latest_stock_quote('QQQ',pool='B')
        quote_time=quote.trade_timestamp if quote.last else quote.quote_timestamp
        if isinstance(quote_time,str):
            quote_time=datetime.fromisoformat(quote_time.replace('Z','+00:00'))
        now=datetime.now(ZoneInfo('UTC'))
        age=(now-quote_time).total_seconds() if quote_time and quote_time.tzinfo else float('inf')
        valid=(candidate_valid and 0 <= age <= 180 and float(quote.last or quote.bid or 0)>0
               and risk.market_trend in ('向上','横盘','向下')
               and not any(x in str(risk.market_reason) for x in ('不足','未启用','失败','环境变量','手动'))
               and float(risk.vix)>0
               and risk.vix_source == 'Yahoo实时/延迟'
               and str(risk.account_metrics_source).startswith('Alpaca实时账户')
               and all(math.isfinite(float(v)) for v in (risk.vix,risk.daily_pnl_pct,risk.max_drawdown)))
    except Exception:
        pass
    with execution_lock('dynamic_leverage_state'):
        try:
            old=json.loads(get_app_setting(KEY,'{}') or '{}')
            if not isinstance(old,dict) or not .5 <= float(old.get('ceiling',.5)) <= 1.5:
                raise ValueError('bad persisted ceiling')
        except (ValueError,TypeError):
            old={}
        state=advance(old,target=target,reason=reason,circuit=circuit,now=now,market_open=market_open,valid=valid)
        set_app_setting(KEY,json.dumps(state,ensure_ascii=False))
    return state
