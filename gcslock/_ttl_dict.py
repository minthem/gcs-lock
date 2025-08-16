"""
A lightweight thread-safe TTL (time-to-live) dictionary.

Values are stored alongside their expiration timestamp. Reads return only
non-expired values; expired entries are purged lazily upon access.
"""

import time
from collections.abc import MutableMapping
from threading import RLock
from typing import Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class TTLDict(Generic[K, V], MutableMapping[K, V]):
    """
    A thread-safe mapping that automatically expires entries after a TTL.

    - Each stored value can have an explicit TTL (in seconds) or use the default.
    - Expiration is evaluated lazily on read; expired entries are removed on access.
    - The mapping is protected by an RLock for basic thread-safety.
    """

    def __init__(self, default_ttl: int = 60):
        """
        Initialize the TTL dictionary.

        Args:
            default_ttl: Default time-to-live (in seconds) applied to new entries
                         when no explicit TTL is provided on assignment.
        """
        self._ttl_dict: dict[K, tuple[V, float]] = {}
        self._default_ttl = default_ttl
        self._lock = RLock()

    def __getitem__(self, key: K) -> V:
        """
        Retrieve a non-expired value for the given key.

        Returns:
            The stored value if it exists and has not expired.

        Raises:
            KeyError: If the key is absent or the entry has expired.
        """
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
        """
        Store a value with an optional TTL.

        You can provide:
          - value directly, which will use the default TTL, or
          - a tuple (value, ttl_seconds) to set a custom TTL for this entry.

        Args:
            key: Entry key.
            value: Either the value or a (value, ttl_seconds) tuple.
        """
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
        """
        Remove the key from the dictionary if present.

        Silently does nothing if the key does not exist.
        """
        with self._lock:
            self._ttl_dict.pop(key, None)

    def __iter__(self):
        """
        Iterate over keys currently stored (may include expired keys until accessed).

        Note:
            Expired entries are cleaned up lazily on access (e.g., __getitem__).
        """
        return iter(self._ttl_dict)

    def __len__(self) -> int:
        """
        Return the number of entries currently tracked.

        Note:
            This count may include expired entries that have not been accessed
            and therefore not yet purged.
        """
        return len(self._ttl_dict)
