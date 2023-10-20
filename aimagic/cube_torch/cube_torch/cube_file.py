import asyncio
import builtins
import io
import json
import os
import queue
import threading
from functools import wraps
import time

import requests
import torch
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from cube_torch.cube_file_open_interceptor import CubeFileOpenInterceptor
from cube_torch.cube_lru_cache import LRUCache, CubeStream

global_interceptionIO = None
global_cube_rootdir_path = None
builtins_open = builtins.open
builtins_torch_load = torch.load


def set_global_cube_rootdir_path(rootdir):
    global global_cube_rootdir_path
    global_cube_rootdir_path = rootdir


def set_global_interception_io(io):
    global global_interceptionIO
    global_interceptionIO = io


def is_prefix_cube_file(string):
    global global_cube_rootdir_path
    prefix_length = len(global_cube_rootdir_path)
    return string[:prefix_length] == global_cube_rootdir_path


class InterceptionIO:
    def __init__(self, storage_info):
        cube_root_dir, wait_download_queue, batch_download_addr, batch_size, free_memory_addr = storage_info
        self.cube_root_dir = cube_root_dir
        self.files_cache = LRUCache(timeout=60)
        self.batch_download_addr = batch_download_addr
        self.storage_session = requests.Session()
        self.wait_download_queue = wait_download_queue
        self.batch_size = batch_size
        self.free_memory_addr = free_memory_addr
        self.download_event = threading.Event()
        self._lock = threading.Lock()
        retry_strategy = Retry(
            total=1,  # 最大重试次数
            backoff_factor=0.5,  # 重试之间的时间间隔因子
            status_forcelist=[429, 500, 502, 503, 504]  # 触发重试的 HTTP 状态码
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.storage_session.mount('http://', adapter)
        self.download_thread = threading.Thread(target=self._loop_download_worker, args=(self.download_event,))
        self.download_thread.daemon = True
        self.download_thread.start()

    def add_stream(self, file_path, stream):
        self.files_cache.put(file_path, stream)

    def get_stream(self, file_name):
        stream = self.files_cache.pop(file_name)
        if stream is None:
            CubeFileOpenInterceptor.add_count(False, 0)
            return None
        current_time = time.time()
        CubeFileOpenInterceptor.add_count(True, current_time - stream.put_time)
        return stream

    def get_event_and_thread(self):
        return self.download_thread, self.download_event

    def _loop_download_worker(self, event):
        loop_index = 0
        while not event.is_set():
            try:
                loop_index += 1
                files = self.wait_download_queue.get(timeout=5)
                if files is None:
                    break
                self.batch_download_async([files])
                if loop_index % 100 == 0:
                    CubeFileOpenInterceptor.print_hit_rate()
                    expired_keys = self.files_cache.get_expired_key()
                    self.files_cache.delete_keys(expired_keys)
            except queue.Empty:
                continue
        event.set()
        print("pid:{} loop_downloader_worker ready exit".format(os.getpid()))

    def intercept_open(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            file_path = args[0]
            if is_prefix_cube_file(file_path):
                result = CubeFile(*args, **kwargs)
                return result
            result = builtins_open(*args, **kwargs)
            return result

        return wrapper

    def intercept_torch_load(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            file_path = args[0]
            if is_prefix_cube_file(file_path):
                stream = self.get_stream(file_path)
                if stream:
                    result = builtins_torch_load(stream, **kwargs)
                    return result
            result = builtins_torch_load(*args, **kwargs)
            return result

        return wrapper

    def batch_download(self, index_list):
        try:
            data = json.dumps(index_list)
            with requests.post(self.batch_download_addr, data=data, stream=True, timeout=100,
                               headers={'Content-Type': 'application/octet-stream'}) as response:
                if response.status_code != 200:
                    raise ValueError(
                        "unavalid http reponse code:{} response:{}".format(response.status_code, response.text))
                self.stream_parse_content(self.batch_download_addr, response)
            del data, index_list
        except Exception as e:
            print('pid:{} url:{} Error:{} '.format(os.getpid(), self.batch_download_addr, e))
            pass

    def free_memory_async(self):
        try:
            requests.get(self.free_memory_addr)
        except Exception as e:
            pass

    def batch_download_async(self, index_list):
        loop = asyncio.new_event_loop()
        loop.run_in_executor(None, self.batch_download, index_list)

    def free_os_memory_async(self):
        loop = asyncio.new_event_loop()
        loop.run_in_executor(None, self.free_memory_async)

    def stream_parse_content(self, url, response):
        version = response.raw.read(8)
        version = int.from_bytes(version, byteorder='big')
        count = response.raw.read(8)
        count = int.from_bytes(count, byteorder='big')
        for i in range(count):
            file_path_size_body = response.raw.read(8)
            file_path_size = int.from_bytes(file_path_size_body, byteorder='big')
            filename = response.raw.read(file_path_size)
            filename = filename.decode()
            content_length_body = response.raw.read(8)
            content_length = int.from_bytes(content_length_body, byteorder='big')
            if content_length == 0:
                print("file_name:{} content_length:{} content_length_body:{}".format(filename, content_length,
                                                                                     len(content_length_body)))
                break
            content = response.raw.read(content_length)
            stream = CubeStream(filename, content)
            self.add_stream(filename, stream)
        response.raw.close()


class CubeFile(io.FileIO):
    @property
    def name(self):
        return self._name

    def __init__(self, *args, **kwargs):
        self.name = args[0]
        global global_interceptionIO
        self._is_cached = False
        self._cube_stream = None
        stream = global_interceptionIO.get_stream(self.name)
        if stream is None:
            super().__init__(*args, **kwargs)
            return
        else:
            self._cube_stream = stream
            self._is_cached = True

        return

    def __del__(self):
        if self._is_cached:
            del self._cube_stream
        del self._name

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def close(self, *args, **kwargs):  # real signature unknown
        if self._is_cached:
            return self._cube_stream.close()
        return super().close()

    def flush(self, *args, **kwargs):  # real signature unknown
        if self._is_cached:
            return self._cube_stream.flush()
        return super().flush()

    def read(self, *args, **kwargs):  # real signature unknown
        if self._is_cached:
            return self._cube_stream.read(*args, **kwargs)
        return super().read(*args, **kwargs)

    def fileno(self):  # real signature unknown
        if self._is_cached:
            return self._cube_stream.fileno()
        return super().fileno()

    def isatty(self):  # real signature unknown
        if self._is_cached:
            return self._cube_stream.isatty()
        return super().isatty()

    def readable(self):  # real signature unknown
        if self._is_cached:
            return self._cube_stream.readable()
        return super().readable()

    def readline(self, *args, **kwargs):  # real signature unknown
        if self._is_cached:
            return self._cube_stream.readline(*args, **kwargs)
        return super().readline(*args, **kwargs)

    def readlines(self, *args, **kwargs):  # real signature unknown
        if self._is_cached:
            return self._cube_stream.readlines(*args, **kwargs)
        return super().readlines(*args, **kwargs)

    def seek(self, *args, **kwargs):  # real signature unknown
        if self._is_cached:
            return self._cube_stream.seek(*args, **kwargs)
        return super().seek(*args, **kwargs)

    def seekable(self):  # real signature unknown
        if self._is_cached:
            return self._cube_stream.seekable()
        return super().seekable()

    def tell(self):  # real signature unknown
        if self._is_cached:
            return self._cube_stream.tell()
        return super().tell()

    def truncate(self, *args, **kwargs):  # real signature unknown
        if self._is_cached:
            return self._cube_stream.truncate(*args, **kwargs)
        return super().truncate(*args, **kwargs)

    def writable(self):  # real signature unknown
        if self._is_cached:
            return self._cube_stream.writable()
        return super().writable()

    def writelines(self, *args, **kwargs):  # real signature unknown
        if self._is_cached:
            return self._cube_stream.writelines(*args, **kwargs)
        return super().writelines(*args, **kwargs)

    @name.setter
    def name(self, value):
        self._name = value
