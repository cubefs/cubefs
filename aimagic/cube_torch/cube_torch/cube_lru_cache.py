import io
import time
import threading


class CubeStream(io.BytesIO):
    def __init__(self, _fpath, _fcontent):
        self.file_path = _fpath
        self.content = _fcontent
        self.content_size = len(_fcontent)
        self.put_time = time.time()
        super().__init__(_fcontent)

    def get_path(self):
        return self.file_path

    def get_content(self):
        return self.content

    def get_content_size(self):
        return self.content_size

    def __del__(self):
        del self.content
        if not self.closed:
            self.close()


class LRUCache:
    def __init__(self, timeout):
        self.timeout = timeout
        self.cache = {}
        self.lock = threading.Lock()

    def put(self, key, value):
        with self.lock:
            self.cache[key] = value

    def pop(self, key):
        with self.lock:
            return self.cache.pop(key, None)

    def cache_length(self):
        with self.lock:
            return len(self.cache)

    def delete_keys(self, keys):
        with self.lock:
            for key in keys:
                self.cache.pop(key, None)

    def get_expired_key(self):
        expired_key = []
        with self.lock:
            for key, stream in self.cache.items():
                if self._is_expired(stream.put_time):
                    expired_key.append(key)
        return expired_key

    def _is_expired(self, timestamp):
        return time.time() - timestamp > self.timeout
