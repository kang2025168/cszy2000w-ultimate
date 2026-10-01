"""Read-only interpretation of controls, process observations and heartbeats."""
from datetime import datetime
from zoneinfo import ZoneInfo

LABELS = {'dashboard_bot':'行情与持仓同步', 'risk_bot':'风险控制',
          'rebalance_bot':'资金调仓', 'ac_bot':'A/C 长期策略',
          'b_buy_bot':'B 买入', 'b_sell_bot':'B 卖出', 'd_grid_bot':'D 日内循环',
          'q_sell_bot':'期权卖出监督', 'f_buy_bot':'F 买入', 'f_sell_bot':'F 卖出'}


def build_monitor(beats, controls, processes, *, timezone, intervals=None, risk=None, now=None):
    now = now or datetime.now(ZoneInfo(timezone))
    intervals = intervals or {}
    risk = risk or {}
    by_name = lambda rows: {r['bot_name']:r for r in rows}
    beats, controls, processes = map(by_name, (beats, controls, processes))
    rows = []
    for name in sorted(set(LABELS) | set(beats) | set(controls) | set(processes)):
        hb, ctl, proc = beats.get(name, {}), controls.get(name), processes.get(name)
        enabled = bool(int(ctl['enabled'])) if ctl is not None else None
        age = None
        try:
            stamp = hb.get('last_seen_at')
            if isinstance(stamp, str):
                stamp = datetime.fromisoformat(stamp.replace('Z', '+00:00'))
            if stamp is not None:
                stamp = stamp if stamp.tzinfo else stamp.replace(tzinfo=ZoneInfo(timezone))
                age = (now - stamp).total_seconds()
        except (ValueError, TypeError):
            pass
        stale_after = max(180, int(intervals.get(name, 60)) * 2 + 60)
        status, label = 'ok', '运行正常'
        message = str(hb.get('last_message') or '')
        heartbeat_status = str(hb.get('status') or '').lower()
        if enabled is False:
            status, label = 'off', '已关闭'
        elif proc is not None and not proc.get('running'):
            status, label = 'error', '进程未运行'
        elif age is None or age < -5:
            status, label = 'unknown', '心跳待核验'
        elif age > stale_after:
            status, label = 'error', '心跳超时'
        elif heartbeat_status in {'error','failed','stopped'}:
            status, label = 'error', '执行异常'
        elif heartbeat_status in {'warning','paused'}:
            status, label = 'warn', '需要关注'
        elif heartbeat_status != 'running':
            status, label = 'unknown', '状态待核验'
        elif any(x in message.lower() for x in ('market_closed','market closed','outside_entry_window','outside trading')):
            status, label = 'idle', '时段外待机'
        restriction = ''
        groups = {'b_buy_bot':['b'], 'd_grid_bot':['d'], 'ac_bot':['a','c']}.get(name, [])
        blocked = [g.upper() for g in groups if risk.get('block_all_new') or risk.get('block_'+g) or risk.get('block_'+g+'_buy')]
        if blocked:
            restriction = '/'.join(blocked) + ' 新开仓受风控限制（非进程故障）'
        rows.append(dict(bot_name=name, name=LABELS.get(name,name), enabled=enabled,
                         status=status,label=label,age_seconds=None if age is None else max(0,int(age)),
                         stale_after_seconds=stale_after,last_seen_at=str(hb.get('last_seen_at') or ''),
                         message=message,restriction=restriction,
                         process='运行中' if proc and proc.get('running') else '未运行' if proc else '独立进程，依据心跳'))
    counts = {s:sum(r['status']==s for r in rows) for s in ('ok','idle','off','warn','error','unknown')}
    return dict(ok=True,checked_at=now.isoformat(),rows=rows,counts=counts)


def monitor_from_state(state):
    from .bot_supervisor import BOT_SPECS
    from .config import settings
    intervals = {}
    for name, spec in BOT_SPECS.items():
        args = spec.args
        if '--interval' in args:
            intervals[name] = int(args[args.index('--interval')+1])
    return build_monitor(state['bot_heartbeats'],state['bot_controls'],state['bot_processes'],
                         timezone=settings().timezone,intervals=intervals,risk=state.get('risk_state'))
