import asyncio
import io
import json
import os

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from cube_torch import get_manager


class CubeDownloadItem(io.BytesIO):
    def __init__(self, file_size, content, file_path):
        self._file_size = file_size
        self._file_content = content
        self._file_path = file_path
        super().__init__(self._file_content)

    def get_train_file_content(self):
        return self._file_content

    def get_train_file_path(self):
        return self._file_path

    def get_train_file_size(self):
        return self._file_size


class CubeBatchDownloader:
    def __init__(self, url):
        self.batch_download_addr = url
        manager = get_manager()
        self.cube_content_cache = manager.dict()
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
            path_size = int.from_bytes(content[start:start + 8], byteorder='big')
            start += 8
            path = content[start:start + path_size].decode()
            start += path_size
            content_size = int.from_bytes(content[start:start + 8], byteorder='big')
            start += 8
            if content_size > 0:
                file_content = content[start:start + content_size]
                start += content_size
                cube_content_item = CubeDownloadItem(content_size, file_content, path)
                self.cube_content_cache[path] = cube_content_item
            else:
                print('path:{}  content_size:{} Content is empty'.format(path,content_size))

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
            print('pid:{} url:{} Error{}:'.format(os.getpid(),self.batch_download_addr, e))
            pass

    def batch_download_async(self, index_list):
        loop = asyncio.new_event_loop()
        loop.run_in_executor(None, self.batch_download, index_list)


    def get_cube_path_item(self, path):
        if self.cube_content_cache.get(path) is None:
            return None
        try:
            item=self.cube_content_cache.pop(path)
            return item
        except Exception:
            return None

    def add_cube_path_item(self, file_path):
        from cube_torch.cube_file import builtins_open
        with builtins_open(file_path, 'rb') as f:
            data = f.read()
            size = len(data)
            self.cube_content_cache[file_path] = CubeDownloadItem(size, data, file_path)

    def add_cube_dataset(self, path_items):
        for item in path_items:
            self.add_cube_path_item(item)




def init_cube_batch_downloader():
    cube_downloader = CubeBatchDownloader("http://127.0.0.1")
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
    content = cube_downloader.encode_by_paths(jpg_files)
    cube_downloader.parse_content(content)
    return cube_downloader, jpg_files


if __name__ == '__main__':
    cube_downloader, jpg_files = init_cube_batch_downloader()
    for path in jpg_files:
        with open(path, 'rb') as f:
            file_content = f.read()
            cube_content_item = cube_downloader.get_cube_path_item(path)
            assert file_content == cube_content_item.get_train_file_content()
            assert path == cube_content_item.get_train_file_path()
            assert len(file_content) == cube_content_item.get_train_file_size()
            print("file_path:{} file_size:{} file_content is same".format(path, len(file_content)))
