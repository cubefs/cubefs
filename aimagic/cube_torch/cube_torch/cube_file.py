import builtins
import io
import os
from functools import wraps

import torch

from cube_torch.cube_batch_download import init_cube_batch_downloader
from cube_torch.cube_file_open_interceptor import CubeFileOpenInterceptor

global_cube_batch_downloader = None
global_cube_rootdir_path = None
builtins_open = builtins.open


def set_global_cube_batch_downloader(downloader):
    global global_cube_batch_downloader
    global_cube_batch_downloader = downloader


def set_global_cube_rootdir_path(rootdir):
    global global_cube_rootdir_path
    global_cube_rootdir_path = rootdir


def is_prefix_cube_file(string):
    global global_cube_rootdir_path
    prefix_length = len(global_cube_rootdir_path)
    return string[:prefix_length] == global_cube_rootdir_path


def intercept_open(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if is_prefix_cube_file(args[0]):
            result=CubeFile(*args, **kwargs)
        else:
            result=builtins_open(*args, **kwargs)
        return result

    return wrapper


def intercept_read(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


class CubeFile(io.FileIO):
    @property
    def name(self):
        return self._name

    def __init__(self, filename, mode='r'):
        global global_cube_batch_downloader
        self._cube_item = global_cube_batch_downloader.get_cube_path_item(filename)
        if self._cube_item is None:
            super().__init__(filename, mode)
            self._is_cube_item = False
        else:
            self._is_cube_item = True
        CubeFileOpenInterceptor.add_count(self._is_cube_item)
        self.name = filename

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self, *args, **kwargs):  # real signature unknown
        if self._is_cube_item:
            return self._cube_item.close(*args, **kwargs)
        return super().close()

    def flush(self, *args, **kwargs):  # real signature unknown
        if self._is_cube_item:
            return self._cube_item.flush(*args, **kwargs)
        return super().flush()

    def read(self, *args, **kwargs):  # real signature unknown
        if self._is_cube_item:
            return self._cube_item.read(*args, **kwargs)
        return super().read(*args, **kwargs)

    def fileno(self):  # real signature unknown
        if self._is_cube_item:
            return self._cube_item.fileno()
        return super().fileno()

    def isatty(self):  # real signature unknown
        if self._is_cube_item:
            return self._cube_item.isatty()
        return super().isatty()

    def readable(self):  # real signature unknown
        if self._is_cube_item:
            return self._cube_item.readable()
        return super().readable()

    def readline(self, *args, **kwargs):  # real signature unknown
        if self._is_cube_item:
            return self._cube_item.readline(*args, **kwargs)
        return super().readline(*args, **kwargs)

    def readlines(self, *args, **kwargs):  # real signature unknown
        if self._is_cube_item:
            return self._cube_item.readlines(*args, **kwargs)
        return super().readlines(*args, **kwargs)

    def seek(self, *args, **kwargs):  # real signature unknown
        if self._is_cube_item:
            return self._cube_item.seek(*args, **kwargs)
        return super().seek(*args, **kwargs)

    def seekable(self):  # real signature unknown
        if self._is_cube_item:
            return self._cube_item.seekable()
        return super().seekable()

    def tell(self):  # real signature unknown
        if self._is_cube_item:
            return self._cube_item.tell()
        return super().tell()

    def truncate(self, *args, **kwargs):  # real signature unknown
        if self._is_cube_item:
            return self._cube_item.truncate(*args, **kwargs)
        return super().truncate(*args, **kwargs)

    def writable(self):  # real signature unknown
        if self._is_cube_item:
            return self._cube_item.writable()
        return super().writable()

    def writelines(self, *args, **kwargs):  # real signature unknown
        if self._is_cube_item:
            return self._cube_item.writelines(*args, **kwargs)
        return super().writelines(*args, **kwargs)

    @name.setter
    def name(self, value):
        self._name = value


def intercept_torch_load(func):
    def wrapper(*args, **kwargs):
        file_path = args[0]
        global global_cube_batch_downloader
        cube_item = global_cube_batch_downloader.get_cube_path_item(file_path)
        is_cache=cube_item is not None
        CubeFileOpenInterceptor.add_count(is_cache)
        if cube_item is None:
            result = func(*args, **kwargs)
        else:
            result = func(cube_item, **kwargs)
        return result

    return wrapper



if __name__ == '__main__':
    global_cube_batch_downloader, jpeg_files = init_cube_batch_downloader()
    title_path = '/home/guowl/testdata/1.bertids'
    global_cube_batch_downloader.add_cube_path_item(title_path)
    open = intercept_open(open)
    file_path = jpeg_files[0]
    with open(file_path, 'rb') as f:
        data = f.read()
        print("data is {}".format(len(data)))

    torch.load = intercept_torch_load(torch.load)
    t = torch.load(title_path)
    print("torch_load file_path:{} len:{}".format(title_path, t))
