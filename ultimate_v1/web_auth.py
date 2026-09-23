"""Bounded, expiring dashboard sessions and login throttling."""
from __future__ import annotations

import hashlib
import hmac
import secrets
import threading
import time
from collections import OrderedDict

from .config import env_bool, env_int, env_str

COOKIE_NAME = "cszy_ultimate_auth"
_lock = threading.Lock()
_attempts: OrderedDict[str, list[float]] = OrderedDict()


def login_password() -> str:
    value = env_str("DASHBOARD_LOGIN_PASSWORD", env_str("ULTIMATE_LOGIN_PASSWORD", env_str("DASHBOARD_ACTION_PASSWORD", env_str("MOBILE_CONTROL_TOKEN", ""))))
    return "" if value.upper().startswith("CHANGE_ME") else value


def _signature(body: str) -> str:
    password = login_password()
    secret = env_str("DASHBOARD_AUTH_SECRET", password)
    return hmac.new(secret.encode(), f"{password}:{body}".encode(), hashlib.sha256).hexdigest()


def issue_token() -> str:
    if not login_password():
        return ""
    expires = int(time.time()) + max(60, env_int("DASHBOARD_SESSION_SECONDS", 28800))
    body = f"{expires}.{secrets.token_hex(24)}"
    return f"{body}.{_signature(body)}"


def verify_token(token: str) -> bool:
    if not login_password() or len(token) > 256:
        return False
    try:
        expires, nonce, signature = token.split(".")
        now = int(time.time())
        ttl = max(60, env_int("DASHBOARD_SESSION_SECONDS", 28800))
        return (len(nonce) == 48 and now < int(expires) <= now + ttl
                and hmac.compare_digest(signature, _signature(f"{expires}.{nonce}")))
    except (ValueError, TypeError):
        return False


def cookie(token: str) -> str:
    age = max(60, env_int("DASHBOARD_SESSION_SECONDS", 28800)) if token else 0
    secure = "; Secure" if env_bool("DASHBOARD_COOKIE_SECURE", False) else ""
    return f"{COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={age}{secure}"


def allow_login(address: str) -> bool:
    now = time.monotonic()
    with _lock:
        recent = [v for v in _attempts.pop(address, []) if now - v < 60]
        allowed = len(recent) < 10
        if allowed:
            recent.append(now)
        _attempts[address] = recent
        while len(_attempts) > 4096:
            _attempts.popitem(last=False)
        return allowed
