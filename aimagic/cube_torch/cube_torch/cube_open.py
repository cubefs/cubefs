import builtins
from functools import wraps

import torch

from cube_torch.cube_batch_download import CubeDownloadItem


def intercept_open(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        print("intercepted open:{}".format(args))
        if args[1]=='r' or args[1]=='rb':
            return CubeFile(*args, **kwargs)
        return builtins.open(*args,**kwargs)

    return wrapper


def intercept_read(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        print("intercepted read")
        return func(*args, **kwargs)

    return wrapper


class CubeFile():
    def __init__(self, filename, mode='r'):
        print(f'opening {filename}')
        self.file = builtins.open(filename, mode)

    def __enter__(self):
        print('entering context')
        return self

    def update_file_content(self,file_content):
        self.file=file_content

    def __exit__(self, exc_type, exc_value, traceback):
        if isinstance(self.file, CubeDownloadItem):
            return
        self.file.close()

    @intercept_read
    def read(self):
        if isinstance(self.file,CubeDownloadItem):
            return self.file.get_file_content()
        return self.file.read()


if __name__ == '__main__':
    open = intercept_open(open)
    file_path='/home/guowl/testdata/data0/n01440764/n01440764_8938.JPEG'
    with open(file_path,'rb') as f:
        data = f.read()
        print("data is {}".format(len(data)))

    title_path='/home/guowl/testdata/1.bertids'
    t=torch.load(title_path)
    print("torch_load file_path:{} len:{}".format(title_path,t))