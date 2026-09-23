"""Background display snapshots. Execution never reads this cache."""
from __future__ import annotations

from copy import deepcopy
from threading import Event, Lock, Thread
import time


class DashboardCache:
    def __init__(self, loaders: dict, interval: float = 15, max_age: float = 60):
        self.loaders, self.interval, self.max_age = loaders, interval, max_age
        self._lock, self._stop = Lock(), Event()
        self._values = {}
        self._thread = None

    def refresh(self):
        for key, loader in self.loaders.items():
            try:
                value = loader()
                if isinstance(value, dict) and value.get("ok") is False:
                    continue
                with self._lock:
                    self._values[key] = (time.monotonic(), time.time(), deepcopy(value))
            except Exception as exc:
                print(f"[SNAPSHOT] {key} refresh failed: {type(exc).__name__}", flush=True)

    def get(self, key):
        with self._lock:
            saved = self._values.get(key)
            if not saved or time.monotonic() - saved[0] > self.max_age:
                return {"ok": False, "error": "snapshot_unavailable_or_stale"}
            value = deepcopy(saved[2])
            if isinstance(value, dict):
                value["snapshot_at"] = saved[1]
            return value

    def start(self):
        def loop():
            while not self._stop.is_set():
                self.refresh()
                self._stop.wait(self.interval)
        self._thread = Thread(target=loop, name="dashboard-snapshots", daemon=True)
        self._thread.start()

    def close(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
