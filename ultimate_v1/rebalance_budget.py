"""Display budget ceilings, never promises an executable trade."""
import math


def buying_budget(allocation, risk):
    def number(value):
        try:
            value = float(value or 0)
            return max(0.0, value) if math.isfinite(value) else 0.0
        except (ValueError, TypeError):
            return 0.0
    pools, profile_totals = {}, {}
    for group in ('B', 'C', 'D'):
        profile = allocation.pool_brokers.get(group)
        snapshot = allocation.broker_snapshots.get(profile, {})
        blocked = (getattr(risk, 'block_all_new', False) or getattr(risk, 'block_'+group.lower(), False)
                   or not allocation.pool_enabled.get(group, False)
                   or not snapshot or any(snapshot.get(k) for k in
                       ('account_blocked','trading_blocked','trade_suspended_by_user')))
        amount = 0.0 if blocked else number(allocation.available.get(group))
        pools[group] = amount
        profile_totals[profile] = profile_totals.get(profile, 0) + amount
    # Shared B/C/D buying power must only be counted once.
    for profile, total in profile_totals.items():
        power = number(allocation.broker_snapshots.get(profile, {}).get('buying_power'))
        ratio = min(1.0, power / total) if total else 0
        for group in pools:
            if allocation.pool_brokers.get(group) == profile:
                pools[group] *= ratio
    return dict(pools=pools, allowed_total=sum(pools.values()), executable_total=0.0,
                execution_mode='STRATEGY_ONLY',
                note='可用额度不是买入指令；订单由各策略信号及执行检查决定')
