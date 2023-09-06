import asyncio
import ctypes
import io
import json
import multiprocessing
import os
import time
from datetime import datetime
from typing import Optional

import requests
import xxhash
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from cube_torch import get_manager


class CubeDownloadItemList(ctypes.Structure):
    _fields_ = [("items", ctypes.py_object)]


class CubeDownloadItem(io.BytesIO):

    def __init__(self, file_path, file_size, file_content):
        self.train_file_path = file_path
        self.train_file_size = file_size
        self.train_file_content = file_content
        super().__init__(file_content)


class CubeBatchDownloader:
    def __init__(self, url,dataset_cnt):
        self.batch_download_addr = url
        manager = get_manager()
        self.dataset_size = dataset_cnt // 2
        self.cube_content_cache = multiprocessing.Array(CubeDownloadItemList, self.dataset_size)
        for i in range(self.dataset_size):
            cube_item = CubeDownloadItemList()
            cube_item.items = manager.list()
            self.cube_content_cache[i] = cube_item
        manager.cube_batch_downloader = self
        self.storage_session = requests.Session()
        retry_strategy = Retry(
            total=1,  # 最大重试次数
            backoff_factor=0.5,  # 重试之间的时间间隔因子
            status_forcelist=[429, 500, 502, 503, 504]  # 触发重试的 HTTP 状态码
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.storage_session.mount('http://', adapter)

    def parse_content(self, content):
        version = int.from_bytes(content[:8], byteorder='big')
        path_count = int.from_bytes(content[8:16], byteorder='big')
        start = 16
        for i in range(path_count):
            path_size_size = int.from_bytes(content[start:start + 8], byteorder='big')
            start += 8
            file_path = content[start:start + path_size_size].decode()
            start += path_size_size
            file_size = int.from_bytes(content[start:start + 8], byteorder='big')
            start += 8
            if file_size > 0:
                file_content = content[start:start + file_size]
                start += file_size
                item = CubeDownloadItem(file_path, file_size, file_content)
                self.add_cube_item(item)
            else:
                print('file_path:{}  content_size:{} Content is empty'.format(file_path, file_size))

    def add_cube_item(self, item: CubeDownloadItem):
        slot_idx = self.get_slot_idx(item.train_file_path)
        slot_items=self.cube_content_cache[slot_idx].items
        slot_items.append(item)
        current_time = datetime.now()
        formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")
        print("||pid:{} time:{} add path:{} slot_idx:{} slot_idx_items_id:{} "
              "len(slot_idx_items):{} ".format(os.getpid(), formatted_time,item.train_file_path,
               slot_idx, id(slot_items), len(slot_items)))

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

    def get_slot_idx(self, text):
        hash_value = xxhash.xxh64(text).hexdigest()
        slot_idx = int(hash_value, 16) % self.dataset_size

        return slot_idx

    def batch_download(self, index_list):
        try:
            data = json.dumps(index_list)
            response = self.storage_session.post(self.batch_download_addr, data=data)
            content = response.content
            if response.status_code != 200:
                raise ValueError("unavalid http reponse code:{} response:{}".format(response.status_code, content))
            self.parse_content(content)
        except Exception as e:
            print('pid:{} url:{} Error{}:'.format(os.getpid(), self.batch_download_addr, e))
            pass

    def batch_download_async(self, index_list):
        loop = asyncio.new_event_loop()
        loop.run_in_executor(None, self.batch_download, index_list)

    def get_cube_path_item(self, file_path, is_free_memory: Optional[bool] = True):
        slot_idx = self.get_slot_idx(file_path)
        slot_items=self.cube_content_cache[slot_idx].items
        current_time = datetime.now()
        formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")
        print("|| pid:{} time:{} file_path:{} slot_idx:{}  slot_idx_items_id:{} "
              "len(slot_idx_items):{} is_free_memory:{}".format(os.getpid(),formatted_time,file_path,slot_idx,
            id(slot_items),len(slot_items),is_free_memory))
        data=None
        for index,_ in  enumerate(slot_items):
            item=slot_items[index]
            print("find file_path:{} item:{} is same:{}".format(file_path,item.train_file_path,file_path==item.train_file_path))
            if item.train_file_path == file_path:
                if is_free_memory:
                    data = slot_items.pop(index)
                else:
                    data=slot_items[index]
                break
        return data


    def add_test_env_item(self, file_path):
        from cube_torch.cube_file import builtins_open
        with builtins_open(file_path, 'rb') as f:
            data = f.read()
            size = len(data)
            item = CubeDownloadItem(file_path, size, data)
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
                if len(jpg_files) >= 30:
                    break
        if len(jpg_files) >= 30:
            break
    cube_downloader = CubeBatchDownloader("http://127.0.0.1")
    content = cube_downloader.encode_by_paths(jpg_files)
    cube_downloader.parse_content(content)
    return cube_downloader, jpg_files


if __name__ == '__main__':
    cube_downloader, jpg_files = init_cube_batch_downloader()
    for path in jpg_files:
        with open(path, 'rb') as f:
            file_content = f.read()
            item = cube_downloader.get_cube_path_item(path)
            assert file_content == item.train_file_content
            assert path == item.train_file_path
            assert len(file_content) == item.train_file_size
            print("file_path:{} file_size:{} file_content is same".format(path, len(file_content)))
