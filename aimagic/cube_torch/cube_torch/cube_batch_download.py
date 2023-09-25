import asyncio
import io
import json
import os
import threading
import time

import requests
import torch
from PIL import Image
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from cube_torch.mem_manager import MemoryAllocater


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


def encode_item_to_bytes(item_meta):
    filename, content, content_length, current_time = item_meta
    result = b''
    result += current_time.to_bytes(8, byteorder='big')
    result += len(filename).to_bytes(8, byteorder='big')

    result += filename.encode()
    result += content_length.to_bytes(8, byteorder='big')
    result += content
    return result


class CubeBatchDownloader:
    def __init__(self, downloader_info):
        download_url, dataset_id, batch_download_idx, file_path_metas, shared_memory, sub_shared_memory_start, sub_shared_memory_end, free_memory_qeueue = downloader_info
        self.batch_download_addr = download_url
        self.file_path_metas = file_path_metas
        self.dataset_id = dataset_id
        self.sub_shared_memory_start = sub_shared_memory_start
        self.sub_shared_memory_end = sub_shared_memory_end
        self.memory_allocate = MemoryAllocater(sub_shared_memory_start, sub_shared_memory_end)
        self.batch_download_idx = batch_download_idx
        self.shard_memory = shared_memory
        self.storage_session = requests.Session()
        self._lock = threading.Lock()
        retry_strategy = Retry(
            total=1,  # 最大重试次数
            backoff_factor=0.5,  # 重试之间的时间间隔因子
            status_forcelist=[429, 500, 502, 503, 504]  # 触发重试的 HTTP 状态码
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.storage_session.mount('http://', adapter)
        e=threading.Event()
        t=threading.Thread(target=self.background_free_memory,args=(free_memory_qeueue,e))
        t.daemon=True
        t.start()
        print("CubeBatchDownloader init on :{}".format(batch_download_idx))

    def check_content(self, item):
        if item.get_path().endswith('.JPEG') or item.get_path().endswith('.jpg'):
            with open(item.get_path(), "rb") as f:
                img = Image.open(f)
                img.convert("RGB")

        else:
            torch.load(item.get_path())

    def background_free_memory(self, free_item_meta_queue, event):
        while not event.is_set():
            request = free_item_meta_queue.get()
            m_worker_id,actual_file_path, free_offset, size = request
            self.memory_allocate.mem_manager.free(free_offset - self.sub_shared_memory_start, size)

    def stream_parse_content(self, url, response):
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
            item_meta = (filename, content, content_length, current_time)
            self.add_cube_item(item_meta)
        response.raw.close()

    def add_cube_item(self, item_meta):
        filename, content, content_length, current_time = item_meta
        encode_data = encode_item_to_bytes(item_meta)
        write_size = len(encode_data)
        request = (self.batch_download_idx, filename, write_size)
        response = self.memory_allocate.allocate_memory(request)
        m_worker_idx, m_file_path, m_offset, m_size = response
        if m_worker_idx != self.batch_download_idx:
            raise ValueError("add_cube_item:{} for batch_download_idx:{} input_queue:{} "
                             "out_queue:{}".format(os.getpid(), self.batch_download_idx, request, response))
        if m_offset is None:
            return None

        if m_size != write_size:
            return None

        if m_file_path != filename:
            raise ValueError("add_cube_item:{} for batch_download_idx:{} input_queue:{} "
                             "out_queue:{}".format(os.getpid(), self.batch_download_idx, request, response))

        self.shard_memory[m_offset:m_offset + m_size] = encode_data
        self.file_path_metas[filename] = response

    def batch_download(self, index_list):
        try:
            data = json.dumps(index_list)
            with requests.post(self.batch_download_addr, data=data, stream=True, timeout=100,
                               headers={'Content-Type': 'application/octet-stream'}) as response:
                if response.status_code != 200:
                    raise ValueError(
                        "unavalid http reponse code:{} response:{}".format(response.status_code, response.text))
                self.stream_parse_content(self.batch_download_addr, response)
        except Exception as e:
            print('pid:{} url:{} Error:{} reponse_raw_is_closed:{} '.format(os.getpid(), self.batch_download_addr, e,
                                                                            response.raw.closed))
            pass

    def batch_download_async(self, index_list):
        loop = asyncio.new_event_loop()
        loop.run_in_executor(None, self.batch_download, index_list)
