
# =====================================================
# Backward-compatible helper
# - strategy_a.py expects: from app.common.config import load_settings
# =====================================================
def load_settings():
    """
    兼容层：优先调用你已有的配置入口（get_settings/load_config/Settings）。
    如果都不存在，就从环境变量构造一个最小可用对象（保证策略能跑起来）。
    """
    # 1) 若你已有 get_settings()
    if "get_settings" in globals() and callable(globals().get("get_settings")):
        return globals()["get_settings"]()

    # 2) 若你已有 load_config()
    if "load_config" in globals() and callable(globals().get("load_config")):
        return globals()["load_config"]()

    # 3) 若你定义了 Settings 类（pydantic/dataclass）
    if "Settings" in globals():
        try:
            return globals()["Settings"]()
        except Exception:
            pass

    # 4) 最小兜底：从 env 读取
    import os

    class _S:
        pass

    S = _S()

    # 你的项目里常用的是 TRADE_ENV / DB_PASS 等，所以这里按你实际 env 命名映射
    S.APP_ENV = (os.getenv("TRADE_ENV") or os.getenv("APP_ENV") or "paper").strip().lower()

    S.DB_HOST = os.getenv("DB_HOST", "mysql")
    S.DB_PORT = int(os.getenv("DB_PORT", "3306"))
    S.DB_USER = os.getenv("DB_USER", "tradebot")
    S.DB_PASSWORD = os.getenv("DB_PASS", os.getenv("DB_PASSWORD", ""))  # ✅ 兼容 DB_PASS
    S.DB_NAME = os.getenv("DB_NAME", "cszy2000")

    # Alpaca
    S.ALPACA_KEY = os.getenv("ALPACA_KEY", "")
    S.ALPACA_SECRET = os.getenv("ALPACA_SECRET", "")
    S.ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

    return S
