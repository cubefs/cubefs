import builtins
import copy
import io
from functools import wraps

import torch

from cube_torch.cube_batch_download import CubeStream
from cube_torch.cube_file_open_interceptor import CubeFileOpenInterceptor

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

    def __init__(self, file_path_metas, shared_memory, free_memory_queues):
        self.file_path_metas = file_path_metas
        self.shared_memory = shared_memory
        self.free_memory_queues = free_memory_queues

    def get_file_path_meta(self, file_path):
        try:
            if file_path in self.file_path_metas:
                return self.file_path_metas.pop(file_path)
        except Exception as e:
            return None

        return None

    def get_cube_file_stream_by_meta(self, file_path):
        file_meta = self.get_file_path_meta(file_path)
        if file_meta is None:
            return None
        m_worker_id, m_file_path, m_offset, m_size = file_meta
        data = bytes(self.shared_memory[m_offset:m_offset + m_size])
        item_offset = 0
        item_offset += 8
        file_path_size = int.from_bytes(data[item_offset:item_offset + 8], byteorder='big')
        item_offset += 8
        actual_file_path = data[item_offset:item_offset + file_path_size].decode()
        if file_path != actual_file_path:
            print("expect_file_path:{} actual_file_path:{} item_meta:{}".format(file_path, actual_file_path,
                                                                                file_meta))
            return None
        item_offset += file_path_size
        file_content_size = int.from_bytes(data[item_offset:item_offset + 8], byteorder='big')
        item_offset += 8
        content = bytes(data[item_offset:item_offset + file_content_size])
        free_item_meta=actual_file_path, m_offset, m_size
        self.free_memory_queues.put(free_item_meta)
        return CubeStream(file_path, content, file_meta)


    def intercept_open(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            file_path = args[0]
            if is_prefix_cube_file(file_path):
                return CubeFile(*args,**kwargs)
            result = builtins_open(*args, **kwargs)
            return result

        return wrapper

    def intercept_torch_load(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            file_path = args[0]
            if is_prefix_cube_file(file_path):
                stream = self.get_cube_file_stream_by_meta(file_path)
                CubeFileOpenInterceptor.add_count(stream is not None)
                if stream:
                    stream.seek(0)
                    return builtins_torch_load(stream, **kwargs)
            result = builtins_torch_load(*args, **kwargs)
            return result

        return wrapper


class CubeFile(io.FileIO):
    @property
    def name(self):
        return self._name

    def __init__(self, *args, **kwargs):
        self.name = args[0]
        global global_interceptionIO
        self._is_cached = False
        stream = global_interceptionIO.get_cube_file_stream_by_meta(self.name)
        CubeFileOpenInterceptor.add_count(stream is not None)
        if stream is None:
            super().__init__(*args, **kwargs)
            return
        else:
            self._cube_stream = stream
            self._is_cached = True
        return

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def close(self, *args, **kwargs):  # real signature unknown
        if self._is_cached:
            return self._cube_stream.close(*args, **kwargs)
        return super().close()

    def flush(self, *args, **kwargs):  # real signature unknown
        if self._is_cached:
            return self._cube_stream.flush(*args, **kwargs)
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
