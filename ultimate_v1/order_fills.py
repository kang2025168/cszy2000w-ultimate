"""Order execution quantities must never be inferred from aggregate positions."""
from __future__ import annotations

import math
import time

TERMINAL = {"filled", "canceled", "cancelled", "expired", "rejected", "replaced"}


def status_text(value) -> str:
    return str(getattr(value, "value", value) or "").lower().rsplit(".", 1)[-1]


def finite_number(value) -> float:
    try:
        number = float(value or 0)
        return number if math.isfinite(number) else 0.0
    except (ValueError, TypeError):
        return 0.0


def wait_for_fill(client, order_id: str, wait_sec: float = 6.0) -> tuple[float, float, str]:
    deadline = time.monotonic() + max(0.0, wait_sec)
    qty = price = 0.0
    status = "unknown"
    while True:
        try:
            order = client.get_order_by_id(str(order_id))
            status = status_text(getattr(order, "status", ""))
            current_qty = max(0.0, finite_number(getattr(order, "filled_qty", 0)))
            current_price = finite_number(getattr(order, "filled_avg_price", 0))
            if current_qty >= qty and current_price > 0:
                qty, price = current_qty, current_price
            if status in TERMINAL:
                return qty, price, status
        except Exception:
            # An unavailable order is unknown, never evidence of a fill.
            pass
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return qty, price, status
        time.sleep(min(0.4, remaining))
