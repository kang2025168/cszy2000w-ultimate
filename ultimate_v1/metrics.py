"""Bounded in-process operational metrics; no request payloads or credentials."""
from collections import defaultdict, deque
from threading import Lock

_lock = Lock()
_samples = defaultdict(lambda: deque(maxlen=512))
_counts = defaultdict(int)


def record(name: str, seconds: float):
    with _lock:
        _counts[name] += 1
        _samples[name].append(seconds * 1000)


def snapshot() -> dict:
    with _lock:
        return {name: {"count": _counts[name], "p50_ms": round(sorted(values)[int((len(values)-1)*0.5)], 2),
                       "p95_ms": round(sorted(values)[int((len(values)-1)*0.95)], 2)}
                for name, values in _samples.items() if values}
