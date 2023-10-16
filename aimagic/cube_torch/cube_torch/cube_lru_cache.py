import time
import threading
from collections import OrderedDict


class LRUCache:
    def __init__(self, max_size, timeout):
        self.max_size = max_size
        self.timeout = timeout
        self.cache = OrderedDict()
        self.lock = threading.Lock()

    def get(self, key):
        with self.lock:
            if key in self.cache:
                value, timestamp = self.cache[key]
                if self._is_expired(timestamp):
                    del self.cache[key]
                    return None
                else:
                    self.cache.move_to_end(key)
                    return value
            else:
                return None

    def put(self, key, value):
        with self.lock:
            if key in self.cache:
                # 更新值和访问时间
                self.cache[key] = (value, time.time())
                self.cache.move_to_end(key)
            else:
                # 插入新的缓存项
                self.cache[key] = (value, time.time())
                if len(self.cache) > self.max_size:
                    # 如果超过最大容量，则删除最旧的缓存项
                    self.cache.popitem(last=False)

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
        if self.cache_length() < self.max_size * 0.2:
            return
        with self.lock:
            expired_keys = []
            for key, (_, timestamp) in self.cache.items():
                if self._is_expired(timestamp):
                    expired_keys.append(key)
            for key in expired_keys:
                del self.cache[key]

