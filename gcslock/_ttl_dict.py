import time
from threading import RLock
from typing import Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class TTLDict(Generic[K, V]):
    def __init__(self, default_ttl: int = 60):
        self._ttl_dict: dict[K, tuple[V, float]] = dict()
        self._default_ttl = default_ttl
        self._lock = RLock()

    def __getitem__(self, key: K):
        return self.get(key)

    def __setitem__(self, key: K, value: V):
        self.set(key, value)

    def __delitem__(self, key: K):
        with self._lock:
            self._ttl_dict.pop(key, None)

    def get(self, key: K):
        with self._lock:
            value, expires_at = self._ttl_dict.get(key, (None, 0))
            if time.monotonic() < expires_at:
                return value
            else:
                self._ttl_dict.pop(key, None)
                raise KeyError(f"{key} is expired (or not found)")

    def set(self, key: K, value: V, ttl: int = None):
        with self._lock:
            if ttl is None:
                ttl = self._default_ttl
            expires_at = time.monotonic() + ttl
            self._ttl_dict[key] = (value, expires_at)
