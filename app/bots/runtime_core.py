# -*- coding: utf-8 -*-
from __future__ import annotations

"""独立机器人的运行时公共工具。

本文件只服务 B/F 独立买卖机器人：负责环境注入、日志、DB 连接、
交易时段、全局开关、买入力检查和策略函数导入。它不包含老主循环。
"""

import logging
import os
import signal
import sys
import time as t
import traceback
from datetime import datetime, time as dt_time
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import pymysql

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

TRADE_ENV = (os.getenv("TRADE_ENV") or os.getenv("ALPACA_MODE") or "paper").strip().lower()
if TRADE_ENV not in ("paper", "live"):
    raise RuntimeError(f"非法 TRADE_ENV/ALPACA_MODE={TRADE_ENV}，只能是 paper 或 live")

BOT_PROCESS_NAME = (os.getenv("BOT_PROCESS_NAME") or "split_bot").strip().lower()

if TRADE_ENV == "paper":
    os.environ["ALPACA_BASE_URL"] = os.getenv("PAPER_ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
    os.environ["APCA_API_KEY_ID"] = os.getenv("PAPER_APCA_API_KEY_ID", "")
    os.environ["APCA_API_SECRET_KEY"] = os.getenv("PAPER_APCA_API_SECRET_KEY", "")
else:
    os.environ["ALPACA_BASE_URL"] = os.getenv("LIVE_ALPACA_BASE_URL", "https://api.alpaca.markets")
    os.environ["APCA_API_KEY_ID"] = os.getenv("LIVE_APCA_API_KEY_ID", "")
    os.environ["APCA_API_SECRET_KEY"] = os.getenv("LIVE_APCA_API_SECRET_KEY", "")

os.environ["ALPACA_KEY"] = os.environ.get("APCA_API_KEY_ID", "")
os.environ["ALPACA_SECRET"] = os.environ.get("APCA_API_SECRET_KEY", "")

from app.strategy_b import (  # noqa: E402
    strategy_B_afterhours_add,
    strategy_B_buy,
    strategy_B_extended_record,
    strategy_B_premarket_manage,
    strategy_B_rank_and_confirm,
    strategy_B_sell,
)
from app.strategy_f import (  # noqa: E402
    strategy_F_buy,
    strategy_F_extended_record,
    strategy_F_premarket_manage,
    strategy_F_refresh_candidates,
    strategy_F_sell,
)
try:  # noqa: E402
    from app.strategy_f import strategy_F_afterhours_add  # type: ignore
except ImportError:  # noqa: E402
    def strategy_F_afterhours_add(code: str) -> bool:
        log.warning(f"[F AFTERHOURS] {code} strategy_F_afterhours_add 未实现，跳过")
        return False

LA_TZ_NAME = os.getenv("TZ", "America/Los_Angeles")
LA_TZ = ZoneInfo(LA_TZ_NAME) if ZoneInfo else None

PREMARKET_OPEN = dt_time(4, 0)
PREOPEN_RECORD_START = dt_time(6, 30)
MARKET_OPEN = dt_time(6, 40)
MARKET_CLOSE = dt_time(13, 0)
AFTERHOURS_CLOSE = dt_time(17, 0)

DB = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASS", "mlp009988"),
    database=os.getenv("DB_NAME", "cszy2000"),
    cursorclass=pymysql.cursors.DictCursor,
    charset="utf8mb4",
    autocommit=True,
)

TABLE = os.getenv("OPS_TABLE", "stock_operations")
SLEEP_BETWEEN_SYMBOLS = float(os.getenv("SLEEP_BETWEEN_SYMBOLS", "0.2"))
SLEEP_BETWEEN_ROUNDS = float(os.getenv("SLEEP_BETWEEN_ROUNDS", "10"))
ERROR_BACKOFF_MIN = int(os.getenv("ERROR_BACKOFF_MIN", "3"))
ERROR_BACKOFF_MAX = int(os.getenv("ERROR_BACKOFF_MAX", "15"))
ROUND_JITTER_MAX = float(os.getenv("ROUND_JITTER_MAX", "1.2"))
MIN_BUYING_POWER = float(os.getenv("MIN_BUYING_POWER", "2500"))
BUYPOWER_REFRESH_SECS = int(os.getenv("BUYPOWER_REFRESH_SECS", "300"))

_STOP = False
_last_bp_ts = 0.0
_cached_buying_power = None
_buy_allowed = True
_alpaca_client = None

_CONTROL = {
    "global_buy_enabled": 1,
    "strategy_b_enabled": 1,
    "strategy_f_enabled": 1,
    "sell_only_mode": 0,
    "emergency_stop": 0,
}


def setup_logger():
    """创建当前机器人独立日志。"""
    logger = logging.getLogger(f"{BOT_PROCESS_NAME}_{TRADE_ENV}")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    log_dir = os.getenv("LOG_DIR", "/tmp/logs")
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_name = os.path.join(log_dir, f"AAA_{BOT_PROCESS_NAME}_{TRADE_ENV}.log")

    file_handler = TimedRotatingFileHandler(
        log_name,
        when="midnight",
        interval=1,
        backupCount=14,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)

    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console)
    return logger


log = setup_logger()
log.info(
    f"[ENV] bot={BOT_PROCESS_NAME} env={TRADE_ENV} "
    f"key_prefix={(os.environ.get('APCA_API_KEY_ID', '')[:5] or '<EMPTY>')}"
)


def _handle_signal(sig, frame):
    global _STOP
    _STOP = True
    log.warning(f"收到退出信号 {sig}，准备安全退出...（本轮结束后退出）")


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def now_la():
    """返回美西时间。"""
    if LA_TZ:
        return datetime.now(LA_TZ)
    return datetime.now()


def get_trade_phase(now=None) -> str:
    """按美西时间判断当前交易阶段。"""
    if now is None:
        now = now_la()
    if now.weekday() >= 5:
        return "closed"
    tnow = now.time()
    if PREMARKET_OPEN <= tnow < PREOPEN_RECORD_START:
        return "premarket_sell"
    if PREOPEN_RECORD_START <= tnow < MARKET_OPEN:
        return "preopen_record"
    if MARKET_OPEN <= tnow <= MARKET_CLOSE:
        return "regular"
    if MARKET_CLOSE < tnow <= AFTERHOURS_CLOSE:
        return "afterhours_add"
    return "closed"


def get_conn():
    """创建数据库连接。"""
    return pymysql.connect(**DB)


def ensure_conn_alive(conn):
    """确认 DB 连接可用，断线时自动重连。"""
    try:
        conn.ping(reconnect=True)
        return conn
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        log.warning("DB 连接失效，正在重连...")
        return get_conn()


def _get_alpaca_client():
    global _alpaca_client
    if _alpaca_client is not None:
        return _alpaca_client
    from alpaca.trading.client import TradingClient

    key = os.environ.get("APCA_API_KEY_ID", "")
    secret = os.environ.get("APCA_API_SECRET_KEY", "")
    _alpaca_client = TradingClient(key, secret, paper=(TRADE_ENV == "paper"))
    return _alpaca_client


def get_buying_power() -> float:
    """读取 Alpaca buying power。失败时返回上一次缓存值。"""
    global _cached_buying_power
    try:
        log.info(f"[BP] using key_prefix={(os.environ.get('APCA_API_KEY_ID', '')[:5] or '<EMPTY>')} env={TRADE_ENV}")
        client = _get_alpaca_client()
        acct = client.get_account()
        bp = getattr(acct, "buying_power", None)
        if bp is None:
            bp = getattr(acct, "cash", None)
        return float(bp or 0.0)
    except Exception as exc:
        log.error(f"[BP] 获取购买力失败：{exc}")
        return float(_cached_buying_power or 0.0)


def refresh_buy_gate(force: bool = False) -> bool:
    """刷新账户买入力开关。"""
    global _last_bp_ts, _cached_buying_power, _buy_allowed
    now = t.time()
    if (not force) and (now - _last_bp_ts < BUYPOWER_REFRESH_SECS):
        return _buy_allowed

    bp = get_buying_power()
    _cached_buying_power = bp
    _last_bp_ts = now

    new_allowed = bp >= MIN_BUYING_POWER
    if new_allowed != _buy_allowed:
        log.warning(f"[BUY_GATE] 状态变化：buy_allowed={new_allowed} (bp={bp:.2f}, threshold={MIN_BUYING_POWER})")
    else:
        log.info(f"[BUY_GATE] buy_allowed={new_allowed} (bp={bp:.2f}, threshold={MIN_BUYING_POWER})")
    _buy_allowed = new_allowed
    return _buy_allowed


def ensure_bot_control_table(conn):
    """确保机器人总控表存在。"""
    sql = """
    CREATE TABLE IF NOT EXISTS bot_control (
        id INT NOT NULL PRIMARY KEY DEFAULT 1,
        global_buy_enabled TINYINT NOT NULL DEFAULT 1,
        strategy_b_enabled TINYINT NOT NULL DEFAULT 1,
        strategy_f_enabled TINYINT NOT NULL DEFAULT 1,
        sell_only_mode TINYINT NOT NULL DEFAULT 0,
        emergency_stop TINYINT NOT NULL DEFAULT 0,
        note VARCHAR(255) NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute("INSERT IGNORE INTO bot_control (id) VALUES (1);")


def load_bot_control(conn):
    """读取机器人总控开关。"""
    global _CONTROL
    ensure_bot_control_table(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM bot_control WHERE id=1 LIMIT 1;")
        row = cur.fetchone() or {}
        cur.execute(
            """
            SELECT bot_name, enabled
            FROM bot_controls
            WHERE bot_name IN ('b_buy_bot', 'f_buy_bot')
            """
        )
        process_controls = {
            str(r.get("bot_name") or ""): int(r.get("enabled") or 0)
            for r in (cur.fetchall() or [])
        }

    _CONTROL = {
        "global_buy_enabled": int(row.get("global_buy_enabled") or 0),
        "strategy_b_enabled": int(row.get("strategy_b_enabled") or 0),
        "strategy_f_enabled": int(row.get("strategy_f_enabled") or 0),
        "sell_only_mode": int(row.get("sell_only_mode") or 0),
        "emergency_stop": int(row.get("emergency_stop") or 0),
    }
    if "b_buy_bot" in process_controls:
        _CONTROL["strategy_b_enabled"] = process_controls["b_buy_bot"]
    if "f_buy_bot" in process_controls:
        _CONTROL["strategy_f_enabled"] = process_controls["f_buy_bot"]
    if _CONTROL["strategy_b_enabled"] == 1 or _CONTROL["strategy_f_enabled"] == 1:
        _CONTROL["global_buy_enabled"] = 1
    return _CONTROL


def _strategy_buy_enabled(stype: str) -> bool:
    """判断某个策略的买入开关是否打开。"""
    stype = (stype or "").strip().upper()
    if stype == "B":
        return _CONTROL.get("strategy_b_enabled", 1) == 1
    if stype == "F":
        return _CONTROL.get("strategy_f_enabled", 1) == 1
    return True


def load_rows(conn, mode: str):
    """读取 stock_operations 中可买或可卖的股票。"""
    if mode == "sell":
        sql = f"""
        SELECT stock_code, stock_type, is_bought, can_sell, can_buy
        FROM `{TABLE}`
        WHERE stock_type IN ('A','B','D','E','F')
          AND is_bought=1 AND can_sell=1
        ORDER BY stock_type, stock_code
        """
    elif mode == "buy":
        sql = f"""
        SELECT stock_code, stock_type, is_bought, can_sell, can_buy
        FROM `{TABLE}`
        WHERE stock_type IN ('A','B','D','E','F')
          AND can_buy=1 AND (is_bought IS NULL OR is_bought<>1)
        ORDER BY stock_type, stock_code
        """
    else:
        return []

    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def safe_call(fn, *args, **kwargs):
    """保护性调用策略函数，避免单只股票异常拖垮机器人。"""
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        log.error(f"[策略异常] {getattr(fn, '__name__', str(fn))} args={args} err={exc}")
        traceback.print_exc()
        return None


def get_market_gate(conn) -> int:
    """读取 QQQ 大盘开仓闸门。"""
    sql = f"""
    SELECT entry_open
    FROM `{TABLE}`
    WHERE stock_code='QQQ' AND stock_type='N'
    LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone() or {}

    try:
        return int(float(row.get("entry_open") or 0))
    except Exception:
        return 0
