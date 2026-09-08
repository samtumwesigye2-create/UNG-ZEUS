"""Bounded in-process LRU byte cache for hot ZEUS reads.

This is intentionally a cache, never the source of truth. Durable bytes remain in the
configured storage backend and integrity is still checked before data enters cache.
"""
from collections import OrderedDict
from threading import RLock


class ByteLRUCache:
    def __init__(self, max_bytes: int = 64 * 1024 * 1024):
        self.max_bytes = max(0, int(max_bytes))
        self._items = OrderedDict()
        self._bytes = 0
        self._lock = RLock()

    def get(self, key):
        with self._lock:
            value = self._items.get(key)
            if value is None:
                return None
            self._items.move_to_end(key)
            return value

    def put(self, key, value: bytes):
        if self.max_bytes <= 0 or len(value) > self.max_bytes:
            return
        with self._lock:
            old = self._items.pop(key, None)
            if old is not None:
                self._bytes -= len(old)
            self._items[key] = value
            self._bytes += len(value)
            while self._bytes > self.max_bytes and self._items:
                _, evicted = self._items.popitem(last=False)
                self._bytes -= len(evicted)

    def invalidate(self, key):
        with self._lock:
            old = self._items.pop(key, None)
            if old is not None:
                self._bytes -= len(old)

    def clear(self):
        with self._lock:
            self._items.clear()
            self._bytes = 0

    def stats(self):
        with self._lock:
            return {"entries": len(self._items), "bytes": self._bytes, "max_bytes": self.max_bytes}
