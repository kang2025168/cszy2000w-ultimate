"""Thread-local SDK clients with bounded HTTP calls and account identity checks."""
from __future__ import annotations

import threading
import requests
import time

from .config import env_str

_local = threading.local()
_identity_lock = threading.Lock()
_identities = {}


class TimeoutSession(requests.Session):
    def request(self, method, url, **kwargs):
        kwargs.setdefault("timeout", (5, 20))
        from .metrics import record
        started = time.monotonic()
        try:
            return super().request(method, url, **kwargs)
        finally:
            record("broker_request", time.monotonic() - started)


def cached_client(kind, profile, credentials, factory):
    key, secret, paper = credentials
    cache = getattr(_local, "clients", {})
    slot = (kind, profile)
    signature = (key, secret, paper)
    old = cache.get(slot)
    if old and old[0] == signature:
        return old[1]
    if old:
        old[1]._session.close()
    client = factory(key, secret, paper)
    # alpaca-py 0.43.2 uses this requests session for REST calls.
    client._session.close()
    client._session = TimeoutSession()
    if kind == "trading":
        account = client.get_account()
        account_id = str(getattr(account, "id", "") or "")
        if not account_id:
            raise RuntimeError("Broker account identity unavailable")
        expected = env_str(f"{profile.upper()}_EXPECTED_ACCOUNT_ID", "")
        if expected and account_id != expected:
            raise RuntimeError(f"Unexpected broker account for {profile}")
        with _identity_lock:
            other = "trading" if profile == "retirement" else "retirement"
            if _identities.get((other, paper)) == account_id:
                raise RuntimeError("Account profiles resolve to the same broker account")
            _identities[(profile, paper)] = account_id
    cache[slot] = (signature, client)
    _local.clients = cache
    return client
