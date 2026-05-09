from __future__ import annotations

"""月度再平衡报告：只输出建议，不自动卖出长期仓位。"""

from .capital_manager import get_capital_allocation, get_strategy_used_capital


def generate_rebalance_report() -> list[dict]:
    """计算各策略组当前超配/低配金额，供人工决策。"""
    allocation = get_capital_allocation()
    if allocation is None:
        print("[REBALANCE] skipped account_snapshot_failed", flush=True)
        return []
    rows = []
    print(f"[REBALANCE] equity={allocation.equity:.2f} buying_power={allocation.buying_power:.2f}", flush=True)
    for group in ("A", "B", "C", "D"):
        target = allocation.target_for(group)
        used = get_strategy_used_capital(group)
        diff = target - used
        action = "increase" if diff > 0 else "reduce"
        row = {
            "strategy_group": group,
            "target": round(target, 2),
            "used": round(used, 2),
            "diff": round(diff, 2),
            "suggestion": action,
        }
        rows.append(row)
        print(
            f"[REBALANCE] group={group} target={target:.2f} used={used:.2f} "
            f"diff={diff:.2f} suggest={action}",
            flush=True,
        )
    return rows


if __name__ == "__main__":
    generate_rebalance_report()
