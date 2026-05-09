from __future__ import annotations

"""MySQL 连接工具，所有 V1 模块统一从这里拿数据库连接。"""

from contextlib import contextmanager
from typing import Iterator

import pymysql
from pymysql.cursors import DictCursor

from .config import Settings, settings


@contextmanager
def db_conn(s: Settings | None = None) -> Iterator[pymysql.connections.Connection]:
    """提供带事务的数据库连接：正常提交，异常回滚。"""
    s = s or settings()
    conn = pymysql.connect(
        host=s.db_host,
        port=s.db_port,
        user=s.db_user,
        password=s.db_password,
        database=s.db_name,
        charset="utf8mb4",
        cursorclass=DictCursor,
        autocommit=False,
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


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
