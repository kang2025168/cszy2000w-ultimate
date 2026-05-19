from __future__ import annotations

"""统一下单守门口：任何策略买入前都应该先调用这里。"""

from .capital_manager import can_open_new_position
from .risk_controller import can_open


def can_open_position(strategy_group: str, estimated_notional: float) -> tuple[bool, str]:
    """记录风控提示，再过资金池；资金池允许才可以新开仓。"""
    risk_allow, risk_reason = can_open(strategy_group)
    if not risk_allow:
        return False, f"risk:{risk_reason}"
    capital_allow, capital_reason = can_open_new_position(strategy_group, estimated_notional)
    if not capital_allow:
        return False, f"capital:{capital_reason}"
    return True, "allow"
