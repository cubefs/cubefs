import io
import time
import threading


def get_current_time():
    return int(time.perf_counter())


class CubeStream(io.BytesIO):
    def __init__(self, _fpath, _fcontent, put_time):
        self.file_path = _fpath
        self.content = _fcontent
        self.content_size = len(_fcontent)
        self.put_time = put_time
        super().__init__(_fcontent)

    def get_path(self):
        return self.file_path

    def get_content(self):
        return self.content

    def get_content_size(self):
        return self.content_size


class LRUCache:
    def __init__(self, timeout):
        self.timeout = timeout
        self.cache = {}
        self.lock = threading.Lock()
        self.last_check_time = time.time()

    def put(self, key, value):
        with self.lock:
            self.cache[key] = value

    def pop(self, key):
        with self.lock:
            return self.cache.pop(key, None)

    def cache_length(self):
        with self.lock:
            return len(self.cache)

    def clean_expired_key(self):
        current_time = get_current_time()
        if current_time - self.last_check_time < 30 * 60:
            return
        with self.lock:
            for key, stream in self.cache.items():
                if current_time - stream.put_time > self.timeout:
                    self.cache.pop(key, None)
                    print("key{} has expires,so auto expires,current_time:{} put_time:{}".format(key, current_time,
                                                                                                 stream.put_time))
        self.last_check_time = time.time()

    def _is_expired(self, current_time, timestamp):
        return current_time - timestamp > self.timeout
