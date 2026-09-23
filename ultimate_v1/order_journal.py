"""Durable order intents. Uncertain submissions are queried, never blindly retried."""
from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager

from .db import db_conn, fetch_one
from .order_fills import status_text


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS execution_orders (
            client_order_id VARCHAR(64) PRIMARY KEY,
            profile VARCHAR(32) NOT NULL,
            pool VARCHAR(8) NOT NULL,
            symbol VARCHAR(64) NOT NULL,
            side VARCHAR(16) NOT NULL,
            request_json JSON NOT NULL,
            request_hash CHAR(64) NOT NULL,
            state VARCHAR(32) NOT NULL DEFAULT 'prepared',
            order_id VARCHAR(128) NULL,
            reserved_notional DECIMAL(20,6) NOT NULL DEFAULT 0,
            accounted_qty DECIMAL(20,6) NOT NULL DEFAULT 0,
            accounted_value DECIMAL(20,6) NOT NULL DEFAULT 0,
            response_json JSON NULL,
            last_error VARCHAR(512) NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_execution_pool (pool, reserved_notional),
            INDEX idx_execution_state (state, updated_at)
        ) ENGINE=InnoDB""")


@contextmanager
def execution_lock(scope: str):
    # Session lock only: no row locks or uncommitted order intent during HTTP.
    name = "execution:" + hashlib.sha256(scope.encode()).hexdigest()[:48]
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT GET_LOCK(%s, 10) AS acquired", (name,))
            if int((cur.fetchone() or {}).get("acquired") or 0) != 1:
                raise RuntimeError("另一笔交易正在处理，请使用原请求重试")
            try:
                yield
            finally:
                cur.execute("SELECT RELEASE_LOCK(%s)", (name,))


def get_intent(client_order_id: str) -> dict | None:
    return fetch_one("SELECT * FROM execution_orders WHERE client_order_id=%s", (client_order_id,))


def encode_request(payload: dict) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def prepare(client_order_id: str, profile: str, pool: str, symbol: str, side: str,
            payload: dict, reserved: float = 0.0) -> dict:
    encoded = encode_request(payload)
    digest = hashlib.sha256(encoded.encode()).hexdigest()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT IGNORE INTO execution_orders
                (client_order_id,profile,pool,symbol,side,request_json,request_hash,reserved_notional)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (client_order_id, profile, pool, symbol, side, encoded, digest, reserved))
    row = get_intent(client_order_id)
    if not row or row["request_hash"] != digest or row["profile"] != profile:
        raise ValueError("幂等请求标识已用于另一笔订单")
    return row


def update(client_order_id: str, **values) -> None:
    allowed = {"state", "order_id", "last_error", "response_json", "reserved_notional"}
    if not values or not set(values) <= allowed:
        raise ValueError("invalid journal update")
    with db_conn() as conn:
        with conn.cursor() as cur:
            fields = ",".join(f"{key}=%s" for key in values)
            cur.execute(f"UPDATE execution_orders SET {fields} WHERE client_order_id=%s",
                        (*values.values(), client_order_id))


def submit_prepared(client, request, row: dict):
    cid = row["client_order_id"]
    if row["state"] != "prepared":
        # Unknown means query-only: a failed lookup never permits resubmission.
        order = client.get_order_by_client_id(cid)
    else:
        update(cid, state="submitting")
        try:
            order = client.submit_order(order_data=request)
        except Exception as exc:
            update(cid, state="unknown", last_error=type(exc).__name__)
            try:
                order = client.get_order_by_client_id(cid)
            except Exception:
                raise RuntimeError(f"订单状态待核对，请保留请求标识 {cid}；系统不会重复提交") from exc
    order_id = str(getattr(order, "id", "") or "")
    if not order_id:
        update(cid, state="unknown", last_error="missing_order_id")
        raise RuntimeError("券商未返回订单编号，等待核对")
    update(cid, order_id=order_id, state=status_text(getattr(order, "status", "submitted")), last_error=None)
    return order


def reserved_for_pool(pool: str) -> float:
    row = fetch_one("SELECT COALESCE(SUM(reserved_notional),0) AS total FROM execution_orders WHERE pool=%s", (pool,))
    return max(0.0, float((row or {}).get("total") or 0))
