"""Shared manual order quantity and protection policies."""

def _manual_strategy_b_stop_loss(entry_price: float) -> float:
    try:
        from app.strategy_b import B_INITIAL_STOP_MULT

        mult = float(B_INITIAL_STOP_MULT)
    except Exception:
        mult = 0.95
    return round(max(0.0, float(entry_price or 0.0) * mult), 2)


def _manual_stop_policy(price: float, pool: str, side: str) -> dict:
    """Return the protection policy recorded for a manual buy.

    B already has an automatic sell worker. A/C/D levels are recorded as
    protective references until their dedicated exit workers are implemented.
    """
    if str(side or "").lower() != "buy":
        return {}
    pool = str(pool or "").upper()
    policies = {
        "A": (0.85, "A 灾难保护 -15%", "长期养老金仓；仅记录保护线，不做日内自动止损", False),
        "B": (None, "B 初始止损 -5%", "策略 B 卖出机器人自动接管", True),
        "C": (0.88, "C 结构保护 -12%", "长期成长仓；仅记录保护线，等待专用退出规则确认", False),
        "D": (0.97, "D 日内保护 -3%", "记录日内保护线，并保留收盘前强制平仓", False),
    }
    policy = policies.get(pool)
    if not policy:
        return {}
    mult, rule, note, automated = policy
    stop_loss = _manual_strategy_b_stop_loss(price) if pool == "B" else round(max(0.0, float(price or 0) * mult), 2)
    if stop_loss <= 0:
        return {}
    return {
        "auto_stop_loss": automated,
        "protection_recorded": True,
        "stop_loss_price": stop_loss,
        "stop_loss_rule": rule,
        "stop_loss_note": note,
    }


def _manual_stock_qty(raw_qty: float, price: float, full_qty: float | None = None) -> float:
    """手动股票数量：高价股允许 0.1 股，普通股仍按整股。"""
    raw = max(0.0, float(raw_qty or 0.0))
    if full_qty is not None:
        full = max(0.0, float(full_qty or 0.0))
        if abs(raw - full) < 1e-9:
            return round(full, 4)
    if price > 50:
        return round(int(raw * 10) / 10, 1)
    return float(int(raw))


