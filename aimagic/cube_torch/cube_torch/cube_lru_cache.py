import time
import threading
from collections import defaultdict


class LRUCache:
    def __init__(self, timeout):
        self.timeout = timeout
        self.cache = defaultdict()
        self.lock = threading.Lock()

    def put(self, key, value):
        with self.lock:
            self.cache[key] = (value, time.time())

    def pop(self, key):
        with self.lock:
            if key in self.cache:
                return self.cache.pop(key)
            else:
                return None

    def cache_length(self):
        with self.lock:
            return len(self.cache)

    def _is_expired(self, timestamp):
        return time.time() - timestamp > self.timeout

    def cleanup_expired_items(self):
        with self.lock:
            expired_keys = []
            for key, (_, timestamp) in self.cache.items():
                if self._is_expired(timestamp):
                    expired_keys.append(key)
            for key in expired_keys:
                self.cache.pop(key)
