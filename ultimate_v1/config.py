from __future__ import annotations

"""读取 `.env` 和环境变量，集中管理 Ultimate V1 的所有开关。"""

import os
from dataclasses import dataclass
from pathlib import Path


def _load_dotenv() -> None:
    """轻量读取项目根目录 `.env`，避免额外依赖也能在本地直接运行。"""
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return
    for raw in env_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


_load_dotenv()


def env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    db_host: str = env_str("DB_HOST", "127.0.0.1")
    db_port: int = env_int("DB_PORT", 3307)
    db_user: str = env_str("DB_USER", "tradebot")
    db_password: str = env_str("DB_PASS", env_str("DB_PASSWORD", ""))
    db_name: str = env_str("DB_NAME", "cszy2000")
    ops_table: str = env_str("OPS_TABLE", "stock_operations")

    alpaca_mode: str = env_str("ALPACA_MODE", env_str("TRADE_ENV", "paper")).lower()
    capital_mode: str = env_str("CAPITAL_MODE", "NORMAL").upper()
    enable_capital_manager: bool = env_bool("ENABLE_CAPITAL_MANAGER", True)
    enable_risk_controller: bool = env_bool("ENABLE_RISK_CONTROLLER", True)
    enable_position_holdings: bool = env_bool("ENABLE_POSITION_HOLDINGS", True)
    position_sync_on_start: bool = env_bool("POSITION_SYNC_ON_START", True)
    position_sync_interval_sec: int = env_int("POSITION_SYNC_INTERVAL_SEC", 300)

    daily_loss_limit_pct: float = env_float("DAILY_LOSS_LIMIT_PCT", 0.03)
    max_loss_days: int = env_int("MAX_LOSS_DAYS", 3)
    max_drawdown_pct: float = env_float("MAX_DRAWDOWN_PCT", 0.08)
    default_risk_multiplier: float = env_float("DEFAULT_RISK_MULTIPLIER", 1.0)
    min_risk_multiplier: float = env_float("MIN_RISK_MULTIPLIER", 0.0)
    max_risk_multiplier: float = env_float("MAX_RISK_MULTIPLIER", 1.0)

    enable_d_intraday: bool = env_bool("ENABLE_D_INTRADAY", True)
    d_use_margin: bool = env_bool("D_USE_MARGIN", True)
    market_close_flatten_time: str = env_str("MARKET_CLOSE_FLATTEN_TIME", "12:50")
    timezone: str = env_str("TIMEZONE", "America/Los_Angeles")

    web_host: str = env_str("ULTIMATE_WEB_HOST", "0.0.0.0")
    web_port: int = env_int("ULTIMATE_WEB_PORT", 8060)


def settings() -> Settings:
    return Settings()


def alpaca_credentials(s: Settings | None = None) -> tuple[str, str, bool]:
    """根据 paper/live 模式选择 Alpaca 密钥。"""
    s = s or settings()
    paper = s.alpaca_mode != "live"
    if paper:
        key = env_str("PAPER_APCA_API_KEY_ID", env_str("APCA_API_KEY_ID", env_str("ALPACA_KEY", "")))
        secret = env_str("PAPER_APCA_API_SECRET_KEY", env_str("APCA_API_SECRET_KEY", env_str("ALPACA_SECRET", "")))
    else:
        key = env_str("LIVE_APCA_API_KEY_ID", env_str("APCA_API_KEY_ID", env_str("ALPACA_KEY", "")))
        secret = env_str("LIVE_APCA_API_SECRET_KEY", env_str("APCA_API_SECRET_KEY", env_str("ALPACA_SECRET", "")))
    return key, secret, paper
