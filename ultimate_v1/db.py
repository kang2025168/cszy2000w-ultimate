from __future__ import annotations

"""MySQL 连接工具，所有 V1 模块统一从这里拿数据库连接。"""

from contextlib import contextmanager
from typing import Iterator

import pymysql
from pymysql.cursors import DictCursor

from .config import Settings, settings


from queue import LifoQueue, Empty
from threading import BoundedSemaphore, Lock
import time
from .config import env_int
from .metrics import record

_POOLS = {}
_POOLS_LOCK = Lock()


def _pool(s):
    key = (s.db_host, s.db_port, s.db_user, s.db_password, s.db_name)
    with _POOLS_LOCK:
        if key not in _POOLS:
            size = max(2, min(32, env_int("DB_POOL_SIZE", 8)))
            _POOLS[key] = (LifoQueue(maxsize=size), BoundedSemaphore(size))
        return _POOLS[key]


@contextmanager
def db_conn(s: Settings | None = None) -> Iterator[pymysql.connections.Connection]:
    """Bounded connection reuse, commit on success, rollback before returning."""
    s = s or settings()
    idle, slots = _pool(s)
    if not slots.acquire(timeout=10):
        raise TimeoutError("Database connection pool exhausted")
    conn = None
    reusable = True
    started = time.monotonic()
    try:
        try:
            conn = idle.get_nowait()
            conn.ping(reconnect=False)
        except (Empty, pymysql.Error):
            if conn is not None:
                conn.close()
            conn = pymysql.connect(
                host=s.db_host, port=s.db_port, user=s.db_user,
                password=s.db_password, database=s.db_name,
                charset="utf8mb4", cursorclass=DictCursor, autocommit=False,
                connect_timeout=10, read_timeout=30, write_timeout=30,
            )
            record("db_connect", time.monotonic() - started)
        try:
            yield conn
            conn.commit()
        except BaseException:
            try:
                conn.rollback()
            except Exception:
                reusable = False
            raise
    finally:
        if conn is not None:
            if reusable and conn.open:
                idle.put_nowait(conn)
            else:
                conn.close()
        slots.release()
        record("db_transaction", time.monotonic() - started)


def fetch_one(sql: str, args: tuple | dict | None = None) -> dict | None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            return cur.fetchone()


def fetch_all(sql: str, args: tuple | dict | None = None) -> list[dict]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            return list(cur.fetchall())
