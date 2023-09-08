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


def string_to_c_void_p(input_string):
    byte_array = input_string.encode()
    shared_memory = multiprocessing.Array(ctypes.c_byte, len(byte_array))
    byte_array_ct = ctypes.create_string_buffer(byte_array)
    ctypes.memmove(shared_memory.get_obj(), byte_array_ct, len(byte_array))
    return ctypes.cast(shared_memory.get_obj(), ctypes.c_void_p)


def bytes_to_c_void_p(byte_array):
    shared_memory = multiprocessing.Array(ctypes.c_byte, len(byte_array))
    byte_array_ct = ctypes.create_string_buffer(byte_array)
    ctypes.memmove(shared_memory.get_obj(), byte_array_ct, len(byte_array))
    print("shard_memory.get_obj addr:{}".format(shared_memory.get_obj()))
    return ctypes.cast(shared_memory.get_obj(), ctypes.c_void_p)


def c_void_p_to_bytes(c_void_p, size):
    ptr_type = ctypes.POINTER(ctypes.c_byte * size)
    ptr = ctypes.cast(c_void_p, ptr_type)
    byte_array = bytes(ptr.contents)

    return byte_array


def c_void_p_to_string(c_void_p, size):
    ptr_type = ctypes.POINTER(ctypes.c_byte * size)
    ptr = ctypes.cast(c_void_p, ptr_type)
    byte_array = bytes(ptr.contents).decode()

    return byte_array



class CubeDownloadItem(ctypes.Structure):
    _fields_ = [('path_ptr', ctypes.c_void_p),
                ('path_size', ctypes.c_int),
                ('content_ptr', ctypes.c_void_p),
                ('content_size', ctypes.c_int),
                ('avali_time', ctypes.c_int)]

    def get_path(self):
        return c_void_p_to_string(self.path_ptr, self.path_size)

    def get_content(self):
        return c_void_p_to_bytes(self.content_ptr, self.content_size)

    def get_content_size(self):
        return self.content_size

    def is_avali_item(self):
        return int(time.time()) - self.avali_time < 150


class CubeBatchDownloader:
    def __init__(self, url):
        self.batch_download_addr = url
        manager = get_manager()
        self.slot_cnt = 1000000
        self.cube_content_cache = multiprocessing.Array(CubeDownloadItem, self.slot_cnt * 10)
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
        content = bytearray(content)
        ptr = (ctypes.c_char * len(content)).from_buffer(content)
        version = int.from_bytes(content[:8], byteorder='big')
        path_count = int.from_bytes(content[8:16], byteorder='big')
        start = 16
        current_time = int(time.time())
        for i in range(path_count):
            path_size = int.from_bytes(content[start:start + 8], byteorder='big')
            start += 8
            path_ptr = ptr[start:start + path_size]
            start += path_size
            content_size = int.from_bytes(content[start:start + 8], byteorder='big')
            start += 8
            if content_size > 0:
                content_ptr = ptr[start:start + content_size]
                start += content_size
                item = CubeDownloadItem(bytes_to_c_void_p(path_ptr), path_size, bytes_to_c_void_p(content_ptr),
                                        content_size, current_time)
                self.add_cube_item(item)
                self.check_content(item)
            else:
                print('file_path:{}  content_size:{} Content is empty'.format(ctypes.string_at(path_ptr, path_size),
                                                                              content_size))

    def add_cube_item(self, cube_item: CubeDownloadItem):
        slot_idx = self.get_slot(cube_item.get_path())
        if not self.cube_content_cache[slot_idx].is_avali_item():
            self.cube_content_cache[slot_idx] = cube_item

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
        slot_idx = int(hash_value, 16) % self.slot_cnt
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
            print('pid:{} url:{} Error:{} data:{}:'.format(os.getpid(), self.batch_download_addr, e, data[0][0]))
            pass

    def batch_download_async(self, index_list):
        loop = asyncio.new_event_loop()
        loop.run_in_executor(None, self.batch_download, index_list)

    def get_cube_path_item(self, file_path):

        try:
            slot_idx = self.get_slot(file_path)
            slot_value = self.cube_content_cache[slot_idx]
            if slot_value.is_avali_item() and slot_value.get_path() == file_path:
                stream = CubeStream(slot_value.get_path(), slot_value.get_content())
                slot_value.avali_time = 0
                return stream
        except Exception as e:
            raise e
        return None

    def covert_to_item(self, file_path, content):
        path_ptr = file_path.encode()
        path_size = len(path_ptr)
        content_size = len(content)
        item = CubeDownloadItem(path_ptr, path_size, bytes_to_c_void_p(content), content_size, int(time.time()))
        return item

    def add_test_env_item(self, file_path):
        from cube_torch.cube_file import builtins_open
        with builtins_open(file_path, 'rb') as f:
            content_ptr = f.read()
            path_ptr = file_path.encode()
            path_size = len(path_ptr)
            content_size = len(content_ptr)
            item = CubeDownloadItem(path_ptr, path_size, bytes_to_c_void_p(content_ptr), content_size, int(time.time()))
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
    cube_downloader = CubeBatchDownloader("http://127.0.0.1")
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
