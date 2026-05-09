from __future__ import annotations

"""Ultimate V1 启动入口：迁移表结构、打印资金/风控日志、同步持仓。"""

from .capital_manager import log_capital_startup
from .config import settings
from .risk_controller import log_risk_state
from .schema import ensure_schema
from .sync_positions import sync_position_holdings


def startup() -> None:
    """执行一次完整启动流程。"""
    s = settings()
    ensure_schema()
    log_capital_startup()
    log_risk_state()
    print(f"[POSITION] enabled={1 if s.enable_position_holdings else 0}", flush=True)
    print(f"[POSITION] sync_on_start={1 if s.position_sync_on_start else 0}", flush=True)
    if s.enable_position_holdings and s.position_sync_on_start:
        sync_position_holdings()


if __name__ == "__main__":
    startup()
