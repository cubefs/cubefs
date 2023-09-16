import builtins
import io
from functools import wraps

import torch

from cube_torch.cube_file_open_interceptor import CubeFileOpenInterceptor

global_cube_batch_downloader = None
global_cube_rootdir_path = None
builtins_open = builtins.open
builtins_torch_load = torch.load


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
            result = CubeFile(*args, **kwargs)
        else:
            result = builtins_open(*args, **kwargs)
        return result

    return wrapper


def intercept_torch_load(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        file_path = args[0]
        global global_cube_batch_downloader
        cube_stream = global_cube_batch_downloader.get_cube_path_item(file_path)
        is_cube_stream = cube_stream is not None
        CubeFileOpenInterceptor.add_count(is_cube_stream)
        if not is_cube_stream:
            result = builtins_torch_load(*args, **kwargs)
        else:
            result = builtins_torch_load(cube_stream, **kwargs)
            global_cube_batch_downloader.free_cube_stream(cube_stream)
        return result

    return wrapper


class CubeFile(io.FileIO):
    @property
    def name(self):
        return self._name

    def __init__(self, filename, mode='r'):
        global global_cube_batch_downloader
        self._cube_stream = global_cube_batch_downloader.get_cube_path_item(filename)
        if self._cube_stream is None:
            super().__init__(filename, mode)
            self._is_cube_stream = False
        else:
            self._is_cube_stream = True
        CubeFileOpenInterceptor.add_count(self._is_cube_stream)
        self.name = filename

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def close(self, *args, **kwargs):  # real signature unknown
        if self._is_cube_stream:
            self._cube_stream.close(*args, **kwargs)
            global_cube_batch_downloader.free_cube_stream(self._cube_stream)
            return
        return super().close()

    def flush(self, *args, **kwargs):  # real signature unknown
        if self._is_cube_stream:
            return self._cube_stream.flush(*args, **kwargs)
        return super().flush()

    def read(self, *args, **kwargs):  # real signature unknown
        if self._is_cube_stream:
            return self._cube_stream.read(*args, **kwargs)
        return super().read(*args, **kwargs)

    def fileno(self):  # real signature unknown
        if self._is_cube_stream:
            return self._cube_stream.fileno()
        return super().fileno()

    def isatty(self):  # real signature unknown
        if self._is_cube_stream:
            return self._cube_stream.isatty()
        return super().isatty()

    def readable(self):  # real signature unknown
        if self._is_cube_stream:
            return self._cube_stream.readable()
        return super().readable()

    def readline(self, *args, **kwargs):  # real signature unknown
        if self._is_cube_stream:
            return self._cube_stream.readline(*args, **kwargs)
        return super().readline(*args, **kwargs)

    def readlines(self, *args, **kwargs):  # real signature unknown
        if self._is_cube_stream:
            return self._cube_stream.readlines(*args, **kwargs)
        return super().readlines(*args, **kwargs)

    def seek(self, *args, **kwargs):  # real signature unknown
        if self._is_cube_stream:
            return self._cube_stream.seek(*args, **kwargs)
        return super().seek(*args, **kwargs)

    def seekable(self):  # real signature unknown
        if self._is_cube_stream:
            return self._cube_stream.seekable()
        return super().seekable()

    def tell(self):  # real signature unknown
        if self._is_cube_stream:
            return self._cube_stream.tell()
        return super().tell()

    def truncate(self, *args, **kwargs):  # real signature unknown
        if self._is_cube_stream:
            return self._cube_stream.truncate(*args, **kwargs)
        return super().truncate(*args, **kwargs)

    def writable(self):  # real signature unknown
        if self._is_cube_stream:
            return self._cube_stream.writable()
        return super().writable()

    def writelines(self, *args, **kwargs):  # real signature unknown
        if self._is_cube_stream:
            return self._cube_stream.writelines(*args, **kwargs)
        return super().writelines(*args, **kwargs)

    @name.setter
    def name(self, value):
        self._name = value


if __name__ == '__main__':
    from cube_torch.cube_batch_download import init_cube_batch_downloader

    global_cube_batch_downloader, jpeg_files = init_cube_batch_downloader()
    title_path = '/home/guowl/testdata/1.bertids'
    global_cube_batch_downloader.add_test_env_item(title_path)
    open = intercept_open(open)
    file_path = jpeg_files[0]
    with open(file_path, 'rb') as f:
        data = f.read()
        print("data is {}".format(len(data)))

    torch.load = intercept_torch_load(torch.load)
    t = torch.load(title_path)
    print("torch_load file_path:{} len:{}".format(title_path, t))
