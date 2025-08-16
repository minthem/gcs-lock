import time
from collections.abc import MutableMapping
from threading import RLock
from typing import Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class TTLDict(Generic[K, V], MutableMapping[K, V]):
    def __init__(self, default_ttl: int = 60):
        self._ttl_dict: dict[K, tuple[V, float]] = {}
        self._default_ttl = default_ttl
        self._lock = RLock()

    def __getitem__(self, key: K) -> V:
        with self._lock:
            try:
                value, expires_at = self._ttl_dict[key]
            except KeyError:
                raise KeyError(f"{key} not found")
            if time.monotonic() < expires_at:
                return value
            self._ttl_dict.pop(key, None)
            raise KeyError(f"{key} is expired")

    def __setitem__(self, key: K, value: tuple[V, int] | V) -> None:
        if isinstance(value, tuple):
            value, ttl = value
        else:
            value, ttl = value, None

        if ttl is None:
            ttl = self._default_ttl

        with self._lock:
            expires_at = time.monotonic() + ttl
            self._ttl_dict[key] = (value, expires_at)

    def __delitem__(self, key: K) -> None:
        with self._lock:
            self._ttl_dict.pop(key, None)

    def __iter__(self):
        return iter(self._ttl_dict)

    def __len__(self) -> int:
        return len(self._ttl_dict)
