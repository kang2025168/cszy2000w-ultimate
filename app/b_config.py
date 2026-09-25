"""Strategy B parameter defaults. Explicit environment overrides retain priority."""
import os
from zoneinfo import ZoneInfo

B_MIN_UP_PCT = float(os.getenv("B_MIN_UP_PCT", "0.03"))
B_MAX_BUY_UP_PCT = float(os.getenv("B_MAX_BUY_UP_PCT", "0.10"))
B_MAX_ENTRY_UP_PCT = float(os.getenv("B_MAX_ENTRY_UP_PCT", "0.4"))
B_MIN_PRICE = float(os.getenv("B_MIN_PRICE", "5.0"))
B_MAX_ACTIVE_POSITIONS = int(os.getenv("B_MAX_ACTIVE_POSITIONS", "4"))
B_MAX_BELOW_OPEN_PCT = float(os.getenv("B_MAX_BELOW_OPEN_PCT", "0.015"))
B_MAX_PULLBACK_FROM_HIGH_PCT = float(os.getenv("B_MAX_PULLBACK_FROM_HIGH_PCT", "0.03"))
B_REQUIRE_INTRADAY_VOLUME = int(os.getenv("B_REQUIRE_INTRADAY_VOLUME", "0"))
B_MIN_PREV_DAY_VOLUME = int(float(os.getenv("B_MIN_PREV_DAY_VOLUME", "3000000")))
B_READY_LOOKBACK_DAYS = int(os.getenv("B_READY_LOOKBACK_DAYS", "35"))
B_READY_AVG_VOLUME_DAYS = int(os.getenv("B_READY_AVG_VOLUME_DAYS", "20"))
B_READY_MIN_PRICE = float(os.getenv("B_READY_MIN_PRICE", os.getenv("STRONG_MIN_PRICE", "5")))
B_READY_MIN_VOLUME = float(os.getenv("B_READY_MIN_VOLUME", os.getenv("STRONG_MIN_VOLUME", "3000000")))
B_READY_MIN_DAY_VOLUME = float(os.getenv("B_READY_MIN_DAY_VOLUME", os.getenv("B_READY_MIN_VOLUME", "3000000")))
B_READY_MIN_DOLLAR_VOLUME = float(os.getenv("B_READY_MIN_DOLLAR_VOLUME", os.getenv("STRONG_MIN_DOLLAR_VOLUME", "5000000")))
B_READY_MIN_GAIN_PCT = float(os.getenv("B_READY_MIN_GAIN_PCT", os.getenv("STRONG_MIN_GAIN_PCT", "0.05")))
B_READY_MAX_GAIN_PCT = float(os.getenv("B_READY_MAX_GAIN_PCT", os.getenv("STRONG_MAX_GAIN_PCT", "0.15")))
B_READY_MIN_UP_STREAK = int(os.getenv("B_READY_MIN_UP_STREAK", os.getenv("STRONG_MIN_UP_STREAK", "2")))
B_READY_MAX_UP_STREAK = int(os.getenv("B_READY_MAX_UP_STREAK", os.getenv("STRONG_MAX_UP_STREAK", "4")))
B_READY_MIN_CLOSE_POSITION = float(os.getenv("B_READY_MIN_CLOSE_POSITION", os.getenv("STRONG_MIN_CLOSE_POSITION", "0.80")))
B_READY_MIN_VOLUME_RATIO = float(os.getenv("B_READY_MIN_VOLUME_RATIO", os.getenv("STRONG_MIN_VOLUME_RATIO", "1.2")))
B_READY_LIMIT = int(os.getenv("B_READY_LIMIT", "50"))
B_READY_WINDOW_TRADING_DAYS = int(os.getenv("B_READY_WINDOW_TRADING_DAYS", "5"))
B_READY_REPLACE = os.getenv("B_READY_REPLACE", "1").strip().lower() not in {"0", "false", "no", "off"}
B_READY_REQUIRE_GREEN = os.getenv("B_READY_REQUIRE_GREEN", "1").strip().lower() not in {"0", "false", "no", "off"}
B_VOLUME_T1_LA = os.getenv("B_VOLUME_T1_LA", "07:30")
B_VOLUME_T2_LA = os.getenv("B_VOLUME_T2_LA", "09:30")
B_MIN_REALTIME_VOLUME = int(float(os.getenv("B_MIN_REALTIME_VOLUME", "100000")))
B_MIN_REALTIME_DOLLAR_VOLUME = float(os.getenv("B_MIN_REALTIME_DOLLAR_VOLUME", "1000000"))
B_RVOL_EARLY = float(os.getenv("B_RVOL_EARLY", "1.8"))
B_RVOL_MID = float(os.getenv("B_RVOL_MID", "1.4"))
B_RVOL_LATE = float(os.getenv("B_RVOL_LATE", "1.15"))
B_MARKET_OPEN_LA = os.getenv("B_MARKET_OPEN_LA", "06:30")
B_MARKET_CLOSE_LA = os.getenv("B_MARKET_CLOSE_LA", "13:00")
B_SCORE_TABLE = os.getenv("B_SCORE_TABLE", "strategy_b_buy_scores")
B_SCORE_TOP_N = int(os.getenv("B_SCORE_TOP_N", "3"))
B_SCORE_INTERVAL_MINUTES = int(os.getenv("B_SCORE_INTERVAL_MINUTES", "5"))
B_SCORE_CONFIRMATIONS = int(os.getenv("B_SCORE_CONFIRMATIONS", "3"))
B_SCORE_LOOKBACK_MINUTES = int(os.getenv("B_SCORE_LOOKBACK_MINUTES", "30"))
B_SCORE_LOG_EACH_CANDIDATE = int(os.getenv("B_SCORE_LOG_EACH_CANDIDATE", "1"))
B_MARKET_FILTER_ENABLED = int(os.getenv("B_MARKET_FILTER_ENABLED", "1"))
B_MARKET_SCORE_MIN = float(os.getenv("B_MARKET_SCORE_MIN", "55"))
B_MARKET_MAX_VIX = float(os.getenv("B_MARKET_MAX_VIX", "28"))
B_MARKET_WARN_VIX = float(os.getenv("B_MARKET_WARN_VIX", "22"))
B_MARKET_MAX_QQQ_DROP_PCT = float(os.getenv("B_MARKET_MAX_QQQ_DROP_PCT", "-0.80"))
B_MARKET_MAX_DOWNTREND_QQQ_DROP_PCT = float(os.getenv("B_MARKET_MAX_DOWNTREND_QQQ_DROP_PCT", "-0.30"))
B_MIN_BUYING_POWER = float(os.getenv("B_MIN_BUYING_POWER", "1000"))
B_MIN_OPEN_BUYING_POWER = float(os.getenv("B_MIN_OPEN_BUYING_POWER", "1000"))

B_TARGET_NOTIONAL_USD = float(os.getenv("B_TARGET_NOTIONAL_USD", "2500"))
B_MAX_NOTIONAL_USD = float(os.getenv("B_MAX_NOTIONAL_USD", "2500"))
B_USE_DYNAMIC_CAPITAL_SIZING = int(os.getenv("B_USE_DYNAMIC_CAPITAL_SIZING", "1"))
B_AVAILABLE_CAPITAL_MULTIPLIER = float(os.getenv("B_AVAILABLE_CAPITAL_MULTIPLIER", "1.0"))
B_DYNAMIC_MAX_TRADE_NOTIONAL = float(os.getenv("B_DYNAMIC_MAX_TRADE_NOTIONAL", "10000"))
B_DYNAMIC_MIN_TRADE_NOTIONAL = float(os.getenv("B_DYNAMIC_MIN_TRADE_NOTIONAL", "1000"))
B_DYNAMIC_ORDER_MAX_NOTIONAL = float(os.getenv("B_DYNAMIC_ORDER_MAX_NOTIONAL", str(B_DYNAMIC_MAX_TRADE_NOTIONAL)))
B_DYNAMIC_ORDER_TIERS = os.getenv(
    "B_DYNAMIC_ORDER_TIERS",
    "10000:2000,20000:2500,40000:3000,80000:4000,150000:5000,300000:7500,inf:10000",
)
B_REMAINDER_BUY_MIN_NOTIONAL = float(os.getenv("B_REMAINDER_BUY_MIN_NOTIONAL", "1000"))

B_COOLDOWN_MINUTES = int(os.getenv("B_COOLDOWN_MINUTES", "30"))
B_BP_USE_RATIO = float(os.getenv("B_BP_USE_RATIO", "0.98"))
B_ALLOW_EXTENDED = int(os.getenv("B_ALLOW_EXTENDED", "0"))
B_DEBUG = int(os.getenv("B_DEBUG", "0"))
HTTP_TIMEOUT = float(os.getenv("B_HTTP_TIMEOUT", "6"))

B_MONSTER_MIN_PEAK_GAIN_PCT = float(os.getenv("B_MONSTER_MIN_PEAK_GAIN_PCT", "0.03"))

B_BP_USE_CASH = int(os.getenv("B_BP_USE_CASH", "0"))  # 0=buying_power,1=cash
B_BUY_WINDOW_START_LA = os.getenv("B_BUY_WINDOW_START_LA", "06:50")
B_BUY_WINDOW_END_LA = os.getenv("B_BUY_WINDOW_END_LA", "10:40")
LA_TZ = ZoneInfo("America/Los_Angeles") if ZoneInfo else None

B_INITIAL_STOP_MULT = float(os.getenv("B_INITIAL_STOP_MULT", "0.95"))
B_TRAIL_LOCK_START_PCT = float(os.getenv("B_TRAIL_LOCK_START_PCT", "0.05"))
B_TRAIL_LOCK_SL_MULT = float(os.getenv("B_TRAIL_LOCK_SL_MULT", "1.00"))
B_INITIAL_STOP_GRACE_SECONDS = int(os.getenv("B_INITIAL_STOP_GRACE_SECONDS", "180"))
B_CATASTROPHIC_STOP_LOSS_PCT = float(os.getenv("B_CATASTROPHIC_STOP_LOSS_PCT", "-0.08"))
B_PEAK_GIVEBACK_RULES = (
    (0.15, 0.04),
    (0.10, 0.03),
    (0.05, 0.02),
)
B_STAGE_SELL_RULES = (
    (1, 0.20, None, None, 0.20),
    (2, 0.35, None, None, 0.20),
    (3, 0.60, None, None, 0.15),
    (4, 0.85, None, None, 0.10),
    (5, 1.20, None, None, 0.10),
)

# 买入后同步 position
B_POS_WAIT_SEC = int(os.getenv("B_POS_WAIT_SEC", "20"))
B_POS_RETRY = int(os.getenv("B_POS_RETRY", "2"))


__all__ = [name for name in globals() if name.startswith("B_") or name in {"HTTP_TIMEOUT", "LA_TZ"}]
