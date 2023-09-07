import asyncio
import ctypes
import json
import multiprocessing
import os

import requests
import xxhash
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from cube_torch import get_manager


class CubeDownloadItem(ctypes.Structure):
    _fields_ = [('path_ptr', ctypes.c_char_p),
                ('path_size', ctypes.c_int),
                ('content_ptr', ctypes.c_char_p),
                ('content_size', ctypes.c_int)]

    def get_path(self):
        return ctypes.string_at(self.path_ptr, self.path_size).decode()

    def get_content(self):
        return ctypes.string_at(self.content_ptr, self.content_size)

    def get_content_size(self):
        return self.content_size


class CubeBatchDownloader:
    def __init__(self, url, dataset_size: int = 1000000000):
        self.batch_download_addr = url
        manager = get_manager()
        self.dataset_size = dataset_size
        self.cube_content_cache=multiprocessing.Array(CubeDownloadItem,self.dataset_size)
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
        content=bytearray(content)
        ptr = (ctypes.c_char * len(content)).from_buffer(content)
        version = int.from_bytes(content[:8], byteorder='big')
        path_count = int.from_bytes(content[8:16], byteorder='big')
        start = 16
        for i in range(path_count):
            path_size = int.from_bytes(content[start:start + 8], byteorder='big')
            start += 8
            path_ptr=ptr[start:start+path_size]
            start += path_size
            content_size = int.from_bytes(content[start:start + 8], byteorder='big')
            start += 8
            if content_size > 0:
                content_ptr = ptr[start:start + content_size]
                start += content_size
                item = CubeDownloadItem(path_ptr,path_size,content_ptr,content_size)
                if i==0:
                    print("path:{} item_content_ptr:{} content_ptr:{}".format(item.get_path(),item.content_ptr,content_ptr))
                    print("i:{} path:{} content_size:{} content:{}".format(i,item.get_path(),item.get_content_size(),ctypes.string_at(item.content_ptr, content_size)))
                self.add_cube_item(item)

            else:
                print('file_path:{}  content_size:{} Content is empty'.format(ctypes.string_at(path_ptr,path_size), content_size))

    def add_cube_item(self,cube_item:CubeDownloadItem):
        slot_idx = self.get_slot(cube_item.get_path())
        self.cube_content_cache[slot_idx]=cube_item


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

    def get_slot(self, text):
        hash_value = xxhash.xxh64(text).hexdigest()
        slot_idx= int(hash_value, 16) % self.dataset_size
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

    def get_cube_path_item(self, file_path):
        slot_idx=self.get_slot(file_path)
        slot_value=self.cube_content_cache[slot_idx]
        if slot_value.get_path()==file_path:
            return slot_value
        return None


    def add_test_env_item(self, file_path):
        from cube_torch.cube_file import builtins_open
        with builtins_open(file_path, 'rb') as f:
            data = f.read()
            size = len(data)
            item = CubeDownloadItem(file_path,size, data)
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
    cube_downloader = CubeBatchDownloader("http://127.0.0.1", len(jpg_files)*100)
    content = cube_downloader.encode_by_paths(jpg_files)
    cube_downloader.parse_content(content)
    return cube_downloader, jpg_files


if __name__ == '__main__':
    cube_downloader, jpg_files = init_cube_batch_downloader()
    for path in jpg_files:
        with open(path, 'rb') as f:
            file_content = f.read()
            item = cube_downloader.get_cube_path_item(path)
            if item is None:
                continue
            print("path is {} ,file_size:{} file_content is :{}".format(path,len(file_content),file_content))
            print("item.get_content type: {}".format(item.get_content()))
            assert file_content == item.get_content()
            assert path == item.get_path()
            assert len(file_content) == item.get_train_file_size()
            print("file_path:{} file_size:{} file_content is same".format(path, len(file_content)))
