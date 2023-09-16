import asyncio
import copy
import ctypes
import io
import json
import multiprocessing
import os
import struct
import time

import requests
import torch
from PIL import Image
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from cube_torch import get_manager
from cube_torch.cube_file import set_global_cube_rootdir_path
from cube_torch.cube_shard_memory import ShardMemory


class CubeStream(io.BytesIO):
    def __init__(self, _fpath, _fcontent, item_meta):
        self.file_path = _fpath
        self.content = _fcontent
        self.content_size = len(_fcontent)
        self.item_meta = item_meta
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
    def __init__(self, url, cache_size, batch_download_workers):
        self.batch_download_addr = url
        manager = get_manager()
        self.cube_content_cache = ShardMemory(cache_size, batch_download_workers)
        manager.cube_batch_downloader = self
        self.storage_session = requests.Session()
        self.file_path_item_meta = manager.dict()
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

    def stream_parse_content(self, url, response, batch_download_worker_idx):
        version = response.raw.read(8)
        version = int.from_bytes(version, byteorder='big')
        count = response.raw.read(8)
        count = int.from_bytes(count, byteorder='big')
        current_time = int(time.time())
        for i in range(count):
            file_path_size = response.raw.read(8)
            file_path_size = int.from_bytes(file_path_size, byteorder='big')

            filename = response.raw.read(file_path_size)
            filename = filename.decode()

            content_length = response.raw.read(8)
            content_length = int.from_bytes(content_length, byteorder='big')

            content = response.raw.read(content_length)
            item = CubeDownloadItem(filename, content, current_time)
            self.add_cube_item(item, batch_download_worker_idx)
        response.raw.close()

    def parse_content(self, url, content):
        sum_content = len(content)
        try:
            start = 0
            version = int.from_bytes(content[start:start + 8], byteorder='big')
            start += 8
            path_count = int.from_bytes(content[start:start + 8], byteorder='big')
            start += 8
            current_time = int(time.time())
            for i in range(path_count):
                if start >= sum_content:
                    break
                path_size = int.from_bytes(content[start:start + 8], byteorder='big')
                start += 8
                file_path = content[start:start + path_size].decode()
                d_file_path = copy.deepcopy(file_path)
                start += path_size
                content_size = int.from_bytes(content[start:start + 8], byteorder='big')
                start += 8
                file_content = content[start:start + content_size]
                d_file_content = copy.deepcopy(file_content)
                start += content_size
                item = CubeDownloadItem(d_file_path, d_file_content, current_time)
                # self.add_cube_item(item)
                print("item path is {}".format(item.get_path()))

        except Exception as e:
            print("url:{} parse content error:{} start:{} sum_content:{}".format(url, e, start, sum_content))

    def add_cube_item(self, cube_item: CubeDownloadItem, batch_download_worker_idx):
        encode_data = cube_item.encode()
        if cube_item.file_path in self.file_path_item_meta:
            return None
        item_meta = self.cube_content_cache.insert_cube_item(cube_item.file_path, encode_data,
                                                             batch_download_worker_idx)
        if item_meta is None:
            return None
        self.file_path_item_meta[cube_item.file_path] = item_meta

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

    def batch_download(self, index_list, batch_download_worker_idx):
        try:
            data = json.dumps(index_list)
            with requests.post(self.batch_download_addr, data=data, stream=True, timeout=100,
                               headers={'Content-Type': 'application/octet-stream'}) as response:
                if response.status_code != 200:
                    raise ValueError(
                        "unavalid http reponse code:{} response:{}".format(response.status_code, response.text))
                self.stream_parse_content(self.batch_download_addr, response, batch_download_worker_idx)
        except Exception as e:
            print('pid:{} url:{} Error:{} reponse_raw_is_closed:{} '.format(os.getpid(), self.batch_download_addr, e,
                                                                            response.raw.closed))
            pass

    def batch_download_async(self, index_list, batch_download_worker_idx):
        loop = asyncio.new_event_loop()
        loop.run_in_executor(None, self.batch_download, index_list, batch_download_worker_idx)

    def get_cube_path_item(self, file_path):
        try:
            if not file_path in self.file_path_item_meta:
                # print("file_path:{} not in self.file_path_item_meta".format(file_path))
                return None
            item_meta = self.file_path_item_meta.pop(file_path)
            item = self.cube_content_cache.get_cube_item(file_path, item_meta)
            if item is None:
                return None
            stream = CubeStream(item.file_path, item.file_content, item_meta)
            return stream
        except Exception as e:
            print("get file_path:{} item_meta:{} Exception:{}".format(file_path,item_meta, e))
            return None

    def free_cube_stream(self,cube_stream):
        self.cube_content_cache.free_item_meta(cube_stream.item_meta)

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
    data_dir = '/mnt/cfs/chubaofs_tech_data-test/sangqingyuan1/vlp/000/0'
    jpg_files = []
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith('.jpg'):
                file_path = os.path.join(root, file)
                jpg_files.append(file_path)
                if len(jpg_files) >= 100:
                    break
        if len(jpg_files) >= 100:
            break
    cube_downloader = CubeBatchDownloader("http://127.0.0.1", 2 * 1024 * 1024 * 1024, 1)
    content = cube_downloader.encode_by_paths(jpg_files)
    cube_downloader.parse_content("http://127.0.0.1", content)
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
    ctx = multiprocessing.get_context("fork")
    w = ctx.Process(target=check_jpg_contents, args=(jpg_files,))
    w.daemon = False
    w.start()
    w.join()
