import builtins
import io
import os
from functools import wraps

import torch

from cube_torch.cube_batch_download import CubeBatchDownloader, init_cube_batch_downloader

_cube_batch_downloader = None


def intercept_open(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        print("intercepted open:{}".format(args))
        if args[1] == 'r' or args[1] == 'rb':
            return CubeFile(*args, **kwargs)
        return builtins.open(*args, **kwargs)

    return wrapper


def intercept_read(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        print("intercepted read")
        return func(*args, **kwargs)

    return wrapper


class CubeFile(io.FileIO):
    @property
    def name(self):
        return self._name

    def __init__(self, filename, mode='r'):
        global _cube_batch_downloader
        self._cube_item = _cube_batch_downloader.get_cube_path_item(filename)
        if self._cube_item is None:
            super().__init__(filename, mode)
            self._is_cube_item = False
        else:
            self._is_cube_item = True
        self.name=filename

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        super().close()

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
        cube_item=_cube_batch_downloader.get_cube_path_item(file_path)
        if cube_item is None:
            result = func(*args, **kwargs)
        else:
            result = func(cube_item, **kwargs)
        return result

    return wrapper



if __name__ == '__main__':
    _cube_batch_downloader, jpeg_files = init_cube_batch_downloader()
    title_path = '/home/guowl/testdata/1.bertids'
    _cube_batch_downloader.add_cube_path_item(title_path)
    open = intercept_open(open)
    file_path = jpeg_files[0]
    with open(file_path, 'rb') as f:
        data = f.read()
        print("data is {}".format(len(data)))

    torch.load = intercept_torch_load(torch.load)
    t = torch.load(title_path)
    print("torch_load file_path:{} len:{}".format(title_path, t))
