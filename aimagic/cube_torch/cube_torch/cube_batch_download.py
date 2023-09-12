import asyncio
import ctypes
import io
import json
import multiprocessing
import os
import time

import requests
import torch
import xxhash
from PIL import Image
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from cube_torch import get_manager
from cube_torch.cube_file import set_global_cube_rootdir_path


class CubeStream(io.BytesIO):
    def __init__(self, _fpath, _fcontent):
        self.file_path = _fpath
        self.content = _fcontent
        self.content_size = len(_fcontent)
        super().__init__(_fcontent)

    def get_path(self):
        return self.file_path

    def get_content(self):
        return self.content

    def get_content_size(self):
        return self.content_size


class CubeDownloadItem:
    def __init__(self, file_path, file_content, avali_time):
        self.file_path = file_path
        self.file_path_size = len(file_path)
        self.file_content = file_content
        self.file_content_size = len(file_content)
        self.avali_time = avali_time

    def get_path(self):
        return self.file_path

    def get_content(self):
        return self.file_content

    def get_content_size(self):
        return self.file_content_size

    def encode(self):
        content = b''
        content += self.avali_time.to_bytes(8, byteorder='big')
        content += self.file_path_size.to_bytes(8, byteorder='big')
        content += self.file_path.encode()
        content += self.file_content_size.to_bytes(8, byteorder='big')
        content += self.file_content
        return content


class CubeBatchDownloader:
    def __init__(self, url, shard_mem):
        self.batch_download_addr = url
        manager = get_manager()
        self.cube_content_cache = shard_mem
        manager.cube_batch_downloader = self
        self.storage_session = requests.Session()
        retry_strategy = Retry(
            total=1,  # 最大重试次数
            backoff_factor=0.5,  # 重试之间的时间间隔因子
            status_forcelist=[429, 500, 502, 503, 504]  # 触发重试的 HTTP 状态码
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.storage_session.mount('http://', adapter)

    def check_content(self, item):
        if item.get_path().endswith('.JPEG') or item.get_path().endswith('.jpg'):
            with open(item.get_path(), "rb") as f:
                img = Image.open(f)
                img.convert("RGB")

        else:
            torch.load(item.get_path())

    def parse_content(self, content):
        version = int.from_bytes(content[:8], byteorder='big')
        path_count = int.from_bytes(content[8:16], byteorder='big')
        start = 16
        current_time = int(time.time())
        for i in range(path_count):
            path_size = int.from_bytes(content[start:start + 8], byteorder='big')
            start += 8
            file_path = content[start:start + path_size].decode()
            start += path_size
            content_size = int.from_bytes(content[start:start + 8], byteorder='big')
            start += 8
            if content_size > 0:
                file_content = content[start:start + content_size]
                start += content_size
                item = CubeDownloadItem(file_path, file_content, current_time)
                self.add_cube_item(item)
                self.check_content(item)
            else:
                print('file_path:{}  content_size:{} Content is empty'.format(ctypes.string_at(file_path, path_size),
                                                                              content_size))

    def add_cube_item(self, cube_item: CubeDownloadItem):
        self.cube_content_cache.insert_cube_item(cube_item)

    def encode_by_paths(self, path_list):
        content = b''
        content += int(0).to_bytes(8, byteorder='big')
        path_count = len(path_list)
        content += path_count.to_bytes(8, byteorder='big')
        for path in path_list:
            with open(path, 'rb') as f:
                file_content = f.read()
            path_bytes = path.encode()
            content += len(path_bytes).to_bytes(8, byteorder='big')
            content += path_bytes
            content += len(file_content).to_bytes(8, byteorder='big')
            content += file_content
        return content

    def batch_download(self, index_list):
        try:
            data = json.dumps(index_list)
            response = self.storage_session.post(self.batch_download_addr, data=data)
            content = response.content
            if response.status_code != 200:
                raise ValueError("unavalid http reponse code:{} response:{}".format(response.status_code, content))
            self.parse_content(content)
        except Exception as e:
            print('pid:{} url:{} Error:{} data:{}:'.format(os.getpid(), self.batch_download_addr, e, data[0][0]))
            pass

    def batch_download_async(self, index_list):
        loop = asyncio.new_event_loop()
        loop.run_in_executor(None, self.batch_download, index_list)

    def get_cube_path_item(self, file_path):
        try:
            data = self.cube_content_cache.get_cube_item(file_path)
            return data
        except Exception as e:
            raise e

    def add_test_env_item(self, file_path):
        from cube_torch.cube_file import builtins_open
        with builtins_open(file_path, 'rb') as f:
            content_ptr = f.read()
            item = CubeDownloadItem(file_path, content_ptr, int(time.time()))
            self.add_cube_item(item)

    def add_cube_dataset(self, path_items):
        for item in path_items:
            self.add_test_env_item(item)


def init_cube_batch_downloader():
    data_dir = '/home/guowl/testdata/data0/n01440764/'
    jpg_files = []
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith('.JPEG'):
                file_path = os.path.join(root, file)
                jpg_files.append(file_path)
                if len(jpg_files) >= 100:
                    break
        if len(jpg_files) >= 100:
            break
    from cube_torch.cube_shard_memory import ShardMemory
    shard_memory = ShardMemory()
    cube_downloader = CubeBatchDownloader("http://127.0.0.1", shard_memory)
    content = cube_downloader.encode_by_paths(jpg_files)
    cube_downloader.parse_content(content)
    return cube_downloader, jpg_files


def check_jpg_contents(jpg_files):
    for path in jpg_files:
        with open(path, 'rb') as f:
            file_content = f.read()
            item = cube_downloader.get_cube_path_item(path)
            if item is None:
                continue
            assert file_content == item.get_content()
            assert path == item.get_path()
            assert len(file_content) == item.get_content_size()
            print("file_path:{} file_size:{} file_content is same".format(path, len(file_content)))


if __name__ == '__main__':
    set_global_cube_rootdir_path("/mnt/cfs/chubaofs_tech_data-test")
    cube_downloader, jpg_files = init_cube_batch_downloader()
    w = multiprocessing.Process(target=check_jpg_contents, args=(jpg_files,))
    w.daemon = False
    w.start()
