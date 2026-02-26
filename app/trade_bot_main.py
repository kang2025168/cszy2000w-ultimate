# -*- coding: utf-8 -*-
import os
import sys
import time as t
import random
import traceback
import signal
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, time as dt_time
from pathlib import Path
import atexit

# =========================
# 0) 把项目根目录加入 sys.path
# =========================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# =========================
# 1) 读取运行环境（TRADE_ENV / ALPACA_MODE 兼容）
# =========================
TRADE_ENV = (os.getenv("TRADE_ENV") or os.getenv("ALPACA_MODE") or "paper").strip().lower()
if TRADE_ENV not in ("paper", "live"):
    raise RuntimeError(f"❌ 非法 TRADE_ENV/ALPACA_MODE={TRADE_ENV}，只能是 paper 或 live")
print(f"===== 当前运行环境: {TRADE_ENV} =====", flush=True)

# =========================
# ✅ 强制注入：把 PAPER/LIVE 的 key 写进通用变量名（必须在 import strategy 之前）
# =========================
if TRADE_ENV == "paper":
    os.environ["ALPACA_BASE_URL"] = os.getenv("PAPER_ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
    os.environ["APCA_API_KEY_ID"] = os.getenv("PAPER_APCA_API_KEY_ID", "")
    os.environ["APCA_API_SECRET_KEY"] = os.getenv("PAPER_APCA_API_SECRET_KEY", "")
else:
    os.environ["ALPACA_BASE_URL"] = os.getenv("LIVE_ALPACA_BASE_URL", "https://api.alpaca.markets")
    os.environ["APCA_API_KEY_ID"] = os.getenv("LIVE_APCA_API_KEY_ID", "")
    os.environ["APCA_API_SECRET_KEY"] = os.getenv("LIVE_APCA_API_SECRET_KEY", "")

# 兼容老变量名（同样强制覆盖）
os.environ["ALPACA_KEY"] = os.environ.get("APCA_API_KEY_ID", "")
os.environ["ALPACA_SECRET"] = os.environ.get("APCA_API_SECRET_KEY", "")

print(f"[ENV] key_prefix={os.environ.get('APCA_API_KEY_ID','')[:5]} env={TRADE_ENV}", flush=True)

# ✅ 现在再 import strategy
from app.strategy_a import *
from app.strategy_b import *
from app.strategy_c import *
from app.strategy_d import *
from app.strategy_e import *
# =========================
# 4) 强制 stdout/stderr UTF-8
# =========================
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
    sys.stderr.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception:
    pass

# =========================
# 5) 单实例锁（按环境）
#    ✅ Docker 里建议禁用 PID 文件锁：DISABLE_PID_LOCK=1
# =========================
DISABLE_PID_LOCK = int(os.getenv("DISABLE_PID_LOCK", "1"))  # Docker 默认禁用
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
    if DISABLE_PID_LOCK == 1:
        print("[LOCK] PID lock disabled by env", flush=True)
        return

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
    if DISABLE_PID_LOCK == 1:
        return
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
# 6) imports (DB + pick)
# =========================
import pymysql
from app.strategy_a_pick import *  # noqa 你自己的 pick 模块

# =========================
# 7) 交易时间（美西）
# =========================
LA_TZ_NAME = os.getenv("TZ", "America/Los_Angeles")
LA_TZ = ZoneInfo(LA_TZ_NAME) if ZoneInfo else None

MARKET_OPEN = dt_time(6, 30)
MARKET_CLOSE = dt_time(13, 0)

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
# 8) DB 配置
# =========================
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

# =========================
# 9) 运行参数
# =========================
SLEEP_BETWEEN_SYMBOLS = float(os.getenv("SLEEP_BETWEEN_SYMBOLS", "0.2"))
SLEEP_BETWEEN_ROUNDS  = float(os.getenv("SLEEP_BETWEEN_ROUNDS", "10"))
ERROR_BACKOFF_MIN     = int(os.getenv("ERROR_BACKOFF_MIN", "3"))
ERROR_BACKOFF_MAX     = int(os.getenv("ERROR_BACKOFF_MAX", "15"))
ROUND_JITTER_MAX      = float(os.getenv("ROUND_JITTER_MAX", "1.2"))

MIN_BUYING_POWER = float(os.getenv("MIN_BUYING_POWER", "900"))
BUYPOWER_REFRESH_SECS = int(os.getenv("BUYPOWER_REFRESH_SECS", "300"))

# =========================
# 10) 全局停止标记
# =========================
_STOP = False

# =========================
# 11) Logger（按环境区分日志）
# =========================
def setup_logger():
    logger = logging.getLogger(f"trade_bot_{TRADE_ENV}")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    log_dir = os.getenv("LOG_DIR", "/tmp/logs")
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
# 12) 信号处理
# =========================
def _handle_signal(sig, frame):
    global _STOP
    _STOP = True
    log.warning(f"收到退出信号 {sig}，准备安全退出...（本轮结束后退出）")

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# =========================
# 13) DB 连接
# =========================
def get_conn():
    return pymysql.connect(**DB)

def ensure_conn_alive(conn):
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

# =========================
# 14) Alpaca buying power（每5分钟刷新）
# =========================
_last_bp_ts = 0.0
_cached_buying_power = None
_buy_allowed = True
_alpaca_client = None

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
    global _cached_buying_power
    try:
        log.info(f"[BP] using key_prefix={(os.environ.get('APCA_API_KEY_ID','')[:5] or '<EMPTY>')} env={TRADE_ENV}")
        client = _get_alpaca_client()
        acct = client.get_account()
        bp = getattr(acct, "buying_power", None)
        if bp is None:
            bp = getattr(acct, "cash", None)
        return float(bp or 0.0)
    except Exception as e:
        log.error(f"[BP] 获取购买力失败：{e}")
        return float(_cached_buying_power or 0.0)

def refresh_buy_gate(force: bool = False) -> bool:
    global _last_bp_ts, _cached_buying_power, _buy_allowed
    now = t.time()
    if (not force) and (now - _last_bp_ts < BUYPOWER_REFRESH_SECS):
        return _buy_allowed

    bp = get_buying_power()
    _cached_buying_power = bp
    _last_bp_ts = now

    new_allowed = (bp >= MIN_BUYING_POWER)
    if new_allowed != _buy_allowed:
        log.warning(f"[BUY_GATE] 状态变化：buy_allowed={new_allowed} (bp={bp:.2f}, threshold={MIN_BUYING_POWER})")
    else:
        log.info(f"[BUY_GATE] buy_allowed={new_allowed} (bp={bp:.2f}, threshold={MIN_BUYING_POWER})")
    _buy_allowed = new_allowed
    return _buy_allowed

# =========================
# 15) load_rows：允许买 vs 禁买（禁买时只扫可卖持仓）
# =========================
def load_rows(conn, buy_allowed: bool):
    if buy_allowed:
        sql = f"""
        SELECT stock_code, stock_type, is_bought, can_sell, can_buy
        FROM {TABLE}
        WHERE stock_type IN ('A','B','C','D','E')
          AND (
                (is_bought=1 AND can_sell=1)
             OR (can_buy=1 AND (is_bought IS NULL OR is_bought<>1))
          )
        """
    else:
        sql = f"""
        SELECT stock_code, stock_type, is_bought, can_sell, can_buy
        FROM {TABLE}
        WHERE stock_type IN ('A','B','C','D','E')
          AND is_bought=1 AND can_sell=1
        """
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()

# =========================
# 16) 策略分发
# =========================
def safe_call(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        log.error(f"[策略异常] {getattr(fn, '__name__', str(fn))} args={args} err={e}")
        traceback.print_exc()
        return None

def dispatch_one(code, stype, is_bought, can_sell, can_buy, buy_allowed: bool):
    if stype == "A":
        if is_bought == 1 and can_sell == 1:
            safe_call(strategy_A_sell, code)
        elif buy_allowed and can_buy == 1:
            safe_call(strategy_A_buy, code)
            refresh_buy_gate(force=True)

    elif stype == "B":
        if is_bought == 1 and can_sell == 1:
            safe_call(strategy_B_sell, code)
        elif buy_allowed and can_buy == 1:
            safe_call(strategy_B_buy, code)
            refresh_buy_gate(force=True)

    elif stype == "C":
        if is_bought == 1 and can_sell == 1:
            safe_call(strategy_C_sell, code)
        elif buy_allowed and can_buy == 1:
            safe_call(strategy_C_buy, code)
            refresh_buy_gate(force=True)

    elif stype == "D":
        if is_bought == 1 and can_sell == 1:
            safe_call(strategy_D_sell, code)
        elif buy_allowed and can_buy == 1:
            safe_call(strategy_D_buy, code)
            refresh_buy_gate(force=True)

    elif stype == "E":
        if is_bought == 1 and can_sell == 1:
            safe_call(strategy_E_sell, code)
        elif buy_allowed and can_buy == 1:
            safe_call(strategy_E_buy, code)
            refresh_buy_gate(force=True)

def one_round(conn, buy_allowed: bool):
    conn = ensure_conn_alive(conn)
    rows = load_rows(conn, buy_allowed) or []

    if not rows:
        log.info(f"本轮 rows=0 (buy_allowed={buy_allowed})")
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

        dispatch_one(code, stype, is_bought, can_sell, can_buy, buy_allowed)
        t.sleep(SLEEP_BETWEEN_SYMBOLS + random.uniform(0, 0.08))

    return conn

# =========================
# 17) 主循环
# =========================
def main_loop():
    log.info(f"===== 稳定主循环启动 ===== env={TRADE_ENV}")
    log.info(f"pid={os.getpid()} pid_file={PID_FILE}")
    log.info(f"sys.executable={sys.executable}")
    log.info(f"TZ={LA_TZ_NAME} DB={DB.get('host')}:{DB.get('port')} user={DB.get('user')} db={DB.get('database')} table={TABLE}")
    log.info(f"BUY_GATE: MIN_BUYING_POWER={MIN_BUYING_POWER} refresh={BUYPOWER_REFRESH_SECS}s")

    conn = None

    refresh_buy_gate(force=True)

    while not _STOP:
        try:
            if not is_trading_time():
                log.info("非交易时段，休眠 600s...")
                t.sleep(60)
                continue

            if conn is None:
                conn = get_conn()
                log.info("DB 已连接")

            buy_allowed = refresh_buy_gate(force=False)
            conn = one_round(conn, buy_allowed)

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