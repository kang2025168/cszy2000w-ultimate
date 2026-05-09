from __future__ import annotations

"""从 Alpaca 同步真实持仓到 position_holdings。"""

from . import alpaca_gateway
from .config import settings
from .position_holdings import mark_missing_from_alpaca, summary_counts, sync_open_holding_from_position
from .schema import ensure_schema


def sync_position_holdings() -> bool:
    """同步持仓：Alpaca 有但本地没有就补，本地 open 但 Alpaca 没有就标记复核。"""
    if not settings().enable_position_holdings:
        print("[POSITION] disabled=1", flush=True)
        return True
    ensure_schema()
    try:
        positions = alpaca_gateway.list_positions()
        symbols = set()
        for pos in positions:
            symbol = str(getattr(pos, "symbol", "")).upper()
            if not symbol:
                continue
            symbols.add(symbol)
            sync_open_holding_from_position(pos, "B")
        mark_missing_from_alpaca(symbols)
        counts = summary_counts()
        print(
            f"[HOLDING SYNC] open_count={counts.get('open', 0)} "
            f"closed_count={counts.get('closed', 0)} needs_review={counts.get('needs_review', 0)}",
            flush=True,
        )
        return True
    except Exception as exc:
        print(f"[POSITION SYNC ERROR] {exc}", flush=True)
        return False


if __name__ == "__main__":
    sync_position_holdings()
