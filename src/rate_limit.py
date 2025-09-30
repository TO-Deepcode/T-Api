import time
from threading import Lock
from typing import Dict


class RateLimitError(Exception):
    """Raised when rate limit budget is exhausted for a host."""


_last_call: Dict[str, float] = {}
_lock = Lock()


def enforce_rate_limit(key: str, min_interval: float) -> None:
    if min_interval <= 0:
        return
    now = time.monotonic()
    with _lock:
        last = _last_call.get(key)
        if last is None or now - last >= min_interval:
            _last_call[key] = now
            return
    raise RateLimitError(f"rate limit triggered for {key}")
