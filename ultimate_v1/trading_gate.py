from __future__ import annotations

"""统一下单守门口：任何策略买入前都应该先调用这里。"""

from .capital_manager import can_open_new_position
from .risk_controller import can_open


def can_open_position(
    strategy_group: str,
    estimated_notional: float,
    available_override: float | None = None,
) -> tuple[bool, str]:
    """记录风控提示，再过资金池；资金池允许才可以新开仓。"""
    risk_allow, risk_reason = can_open(strategy_group)
    if not risk_allow:
        return False, f"risk:{risk_reason}"
    if available_override is not None:
        available = max(0.0, float(available_override or 0.0))
        request = max(0.0, float(estimated_notional or 0.0))
        if request <= available:
            print(
                f"[CAPITAL CHECK] strategy={strategy_group.upper()} "
                f"available_override={available:.2f} request={request:.2f} allow=True",
                flush=True,
            )
            return True, "allow_override"
        return False, f"capital:override_available_insufficient:{available:.2f}"
    capital_allow, capital_reason = can_open_new_position(strategy_group, estimated_notional)
    if not capital_allow:
        return False, f"capital:{capital_reason}"
    return True, "allow"
