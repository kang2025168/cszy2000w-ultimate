# -*- coding: utf-8 -*-
import os
import sys
import time as t
import random
import traceback
import signal
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, time
from pathlib import Path
import atexit

from app.strategy_a import *
from app.strategy_b import *
from app.strategy_c import *
from app.strategy_d import *
from app.strategy_e import *

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# =========================
# 0) 读取环境开关（唯一来源）
# =========================
TRADE_ENV = os.getenv("TRADE_ENV", "paper").strip().lower()
if TRADE_ENV not in ("paper", "live"):
    raise RuntimeError(f"❌ 非法 TRADE_ENV={TRADE_ENV}，只能是 paper 或 live")
print(f"===== 当前运行环境: {TRADE_ENV} =====", flush=True)

# =========================
# 1) 根据环境选择 Alpaca Key
#    ✅ 建议：未来改成从 env 读取；这里先保持你原样（最小改动）
# =========================
if TRADE_ENV == "paper":
    ALPACA_BASE_URL = "https://paper-api.alpaca.markets"
    ALPACA_KEY = "PKU4Z37Z272D7RKRES2R77ZOY6"
    ALPACA_SECRET = "BdY5DwFMwNHtEXm7bX2C3HFrmga4n9rqqf1F9PyMHFUC"
else:
    ALPACA_BASE_URL = "https://api.alpaca.markets"
    ALPACA_KEY = "AKMUKOBY5QQG54OIYZDKVOR3JM"
    ALPACA_SECRET = "Cji6QtUqexq9TYpZFKwPmCN71jinJC21tKcYr6etbsyU"

print("ALPACA_BASE_URL =", ALPACA_BASE_URL, flush=True)
print("KEY_PREFIX =", ALPACA_KEY[:5], flush=True)

# =========================
# 2) 强制 stdout/stderr UTF-8
# =========================
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
    sys.stderr.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception:
    pass

# =========================
# 3) 单实例锁（按环境）
# =========================
PID_FILE = f"/tmp/tradebot_{TRADE_ENV}.pid"

def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False

def _write_pid():
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, "r", encoding="utf-8") as f:
                old_pid = int((f.read().strip() or "0"))
        except Exception:
            old_pid = 0

        if _pid_alive(old_pid):
            print(f"[LOCK] already running env={TRADE_ENV} pid={old_pid}", flush=True)
            raise SystemExit(0)

    with open(PID_FILE, "w", encoding="utf-8") as f:
        f.write(str(os.getpid()))

def _cleanup_pid():
    try:
        if os.path.exists(PID_FILE):
            with open(PID_FILE, "r", encoding="utf-8") as f:
                cur = int((f.read().strip() or "0"))
            if cur == os.getpid():
                os.remove(PID_FILE)
    except Exception:
        pass

_write_pid()
atexit.register(_cleanup_pid)

# =========================
# 4) 加入项目根目录
# =========================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# =========================
# 5) imports
# =========================
import pymysql

from strategy_a_pick import *
# from B策略买卖方法 import *
# from C策略买卖方法 import *
# from D策略买卖方法 import *
# from E策略买卖方法 import *

# =========================
# 6) 交易时间（美西）
#    ✅ Docker/云端默认可能是 UTC，这里强制用 LA
# =========================
LA_TZ_NAME = os.getenv("TZ", "America/Los_Angeles")
LA_TZ = ZoneInfo(LA_TZ_NAME) if ZoneInfo else None

MARKET_OPEN = time(6, 30)
MARKET_CLOSE = time(13, 0)

def now_la():
    if LA_TZ:
        return datetime.now(LA_TZ)
    return datetime.now()

def is_trading_time(now=None) -> bool:
    if now is None:
        now = now_la()
    if now.weekday() >= 5:
        return False
    tnow = now.time()
    return MARKET_OPEN <= tnow <= MARKET_CLOSE

# =========================
# 7) DB 配置（✅最小改动：支持 env，默认 docker-compose 的 mysql 服务名）
# =========================
DB = dict(
    host=os.getenv("DB_HOST", "localhost"),  # docker 里通常是 mysql
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASS", "mlp009988"),
    database=os.getenv("DB_NAME", "cszy2000"),
    cursorclass=pymysql.cursors.DictCursor,
    charset="utf8mb4",
    autocommit=True,
)

TABLE = os.getenv("OPS_TABLE", "stock_operations")

# =========================
# 8) 运行参数
# =========================
SLEEP_BETWEEN_SYMBOLS = float(os.getenv("SLEEP_BETWEEN_SYMBOLS", "0.2"))
SLEEP_BETWEEN_ROUNDS  = float(os.getenv("SLEEP_BETWEEN_ROUNDS", "10"))
ERROR_BACKOFF_MIN     = int(os.getenv("ERROR_BACKOFF_MIN", "3"))
ERROR_BACKOFF_MAX     = int(os.getenv("ERROR_BACKOFF_MAX", "15"))

# 每轮增加一点抖动，减少固定频率被风控（尤其 yfinance）
ROUND_JITTER_MAX = float(os.getenv("ROUND_JITTER_MAX", "1.2"))

# =========================
# 9) 全局停止标记
# =========================
_STOP = False

# =========================
# 10) Logger（按环境区分日志）
# =========================
def setup_logger():
    logger = logging.getLogger(f"trade_bot_{TRADE_ENV}")
    logger.setLevel(logging.INFO)
    logger.propagate = False  # ✅ 防止重复输出

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    log_dir = os.getenv("LOG_DIR", ".")
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_name = os.path.join(log_dir, f"AAA_trade_bot_{TRADE_ENV}.log")

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

# =========================
# 11) 信号处理
# =========================
def _handle_signal(sig, frame):
    global _STOP
    _STOP = True
    log.warning(f"收到退出信号 {sig}，准备安全退出...（本轮结束后退出）")

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# =========================
# 12) DB 连接
# =========================
def get_conn():
    return pymysql.connect(**DB)

def ensure_conn_alive(conn):
    try:
        conn.ping(reconnect=True)  # ✅ pymysql 自带重连
        return conn
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        log.warning("DB 连接失效，正在重连...")
        return get_conn()

def load_rows(conn):
    """
    ✅优化：只拉“可能需要动作”的行
    - 要卖：is_bought=1 AND can_sell=1
    - 要买：can_buy=1 AND is_bought<>1
    """
    sql = f"""
    SELECT stock_code, stock_type, is_bought, can_sell, can_buy
    FROM {TABLE}
    WHERE stock_type IN ('A','B','C','D','E')
      AND (
            (is_bought=1 AND can_sell=1)
         OR (can_buy=1 AND (is_bought IS NULL OR is_bought<>1))
      )
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()

# =========================
# 13) 策略分发
# =========================
def safe_call(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        log.error(f"[策略异常] {getattr(fn, '__name__', str(fn))} args={args} err={e}")
        traceback.print_exc()
        return None

def dispatch_one(code, stype, is_bought, can_sell, can_buy):
    if stype == "A":
        if is_bought == 1 and can_sell == 1:
            safe_call(strategy_A_sell, code)
        elif can_buy == 1:
            safe_call(strategy_A_buy, code)

    elif stype == "B":
        if is_bought == 1 and can_sell == 1:
            safe_call(strategy_B_sell, code)
        elif can_buy == 1:
            safe_call(strategy_B_buy, code)

    elif stype == "C":
        if is_bought == 1 and can_sell == 1:
            safe_call(strategy_C_sell, code)
        elif can_buy == 1:
            safe_call(strategy_C_buy, code)

    elif stype == "D":
        if is_bought == 1 and can_sell == 1:
            safe_call(strategy_D_sell, code)
        elif can_buy == 1:
            safe_call(strategy_D_buy, code)

    elif stype == "E":
        if is_bought == 1 and can_sell == 1:
            safe_call(strategy_E_sell, code)
        elif can_buy == 1:
            safe_call(strategy_E_buy, code)

def one_round(conn):
    conn = ensure_conn_alive(conn)
    rows = load_rows(conn) or []

    if not rows:
        log.info("本轮 rows=0")
        return conn

    for row in rows:
        if _STOP:
            break

        code = (row.get("stock_code") or "").strip().upper()
        stype = (row.get("stock_type") or "").strip().upper()
        is_bought = int(row.get("is_bought") or 0)
        can_sell  = int(row.get("can_sell") or 0)
        can_buy   = int(row.get("can_buy") or 0)

        if not code or stype not in ("A", "B", "C", "D", "E"):
            continue

        dispatch_one(code, stype, is_bought, can_sell, can_buy)

        # ✅ 每个 symbol 之间的 sleep 加一点抖动，降低固定频率
        t.sleep(SLEEP_BETWEEN_SYMBOLS + random.uniform(0, 0.08))

    return conn

# =========================
# 14) 主循环
# =========================
def main_loop():
    log.info(f"===== 稳定主循环启动 ===== env={TRADE_ENV}")
    log.info(f"pid={os.getpid()} pid_file={PID_FILE}")
    log.info(f"sys.executable={sys.executable}")
    log.info(f"TZ={LA_TZ_NAME} DB={DB.get('host')}:{DB.get('port')} user={DB.get('user')} db={DB.get('database')} table={TABLE}")

    conn = None

    while not _STOP:
        try:
            # 如果你要只在交易时段跑，就打开下面注释
            # if not is_trading_time():
            #     log.info("非交易时段，休眠 60s...（仅 06:30~13:00 PT 运行）")
            #     t.sleep(60)
            #     continue

            if conn is None:
                conn = get_conn()
                log.info("DB 已连接")

            conn = one_round(conn)

            # ✅ 每轮 sleep + jitter
            sleep_s = SLEEP_BETWEEN_ROUNDS + random.uniform(0, ROUND_JITTER_MAX)
            t.sleep(sleep_s)

        except Exception as e:
            log.error(f"[主循环异常] {e}")
            traceback.print_exc()

            backoff = random.randint(ERROR_BACKOFF_MIN, ERROR_BACKOFF_MAX)
            log.warning(f"退避 {backoff}s 后继续...")
            t.sleep(backoff)

            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None

    try:
        if conn:
            conn.close()
    except Exception:
        pass

    log.info("===== 已安全退出 =====")

if __name__ == "__main__":
    main_loop()