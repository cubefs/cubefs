import ctypes
import multiprocessing
import os
import threading
import time

from cube_torch import get_manager
from cube_torch.mem_manager import MemoryManager


class ShardMemory:
    def __init__(self, cache_size, batch_download_workers):
        self._min_obj_size = 128 * 1024
        self.default_avali_value = [0, 0, 0, 0, 0, 0, 0, 0]
        self.cache_size = cache_size
        self.mem_manager = MemoryManager(cache_size)
        self.batch_download_worker_num = batch_download_workers
        self.shard_bytes_arr = multiprocessing.RawArray(ctypes.c_ubyte, cache_size)
        self.slot_compute_offset_threads = []
        self.compute_offset_input_queues = []
        self.compute_offset_result_queues = []
        self.compute_offset_threads = []
        self.compute_offset_events = []
        manager = get_manager()
        for i in range(batch_download_workers):
            e = threading.Event()
            input_queue = manager.Queue(maxsize=200)
            result_queue = manager.Queue(maxsize=200)
            self.compute_offset_input_queues.append(input_queue)
            self.compute_offset_result_queues.append(result_queue)
            t = threading.Thread(target=self.background_compute_offset, args=(input_queue,result_queue, e, i))
            t.daemon = True
            t.start()
            self.compute_offset_threads.append(t)
            self.compute_offset_events.append(e)

    def is_empty_avali_time(self, avali_time):
        if avali_time == self.default_avali_value:
            return True
        return False

    def insert_cube_item(self, file_path, encode_bytes, batch_download_worker_idx):
        write_size = len(encode_bytes)
        self.compute_offset_input_queues[batch_download_worker_idx].put((file_path, batch_download_worker_idx, write_size))
        r = self.compute_offset_result_queues[batch_download_worker_idx].get()
        actual_file_path, actual_write_offset, actual_size = r
        if actual_write_offset is None:
            return
        if actual_size != write_size:
            return
        if actual_file_path != file_path:
            return
        write_offset = actual_write_offset
        self.shard_bytes_arr[write_offset:write_offset + write_size] = encode_bytes
        item_meta = (write_offset, write_size)
        # print("child_pid:{} file_path:{} insert_cube_item:{}".format(os.getpid(),file_path, item_meta))

        return item_meta

    def background_compute_offset(self, input_queue,out_queue, event, idx):
        while not event.is_set():
            items = input_queue.get()
            file_path, worker_idx, write_size = items
            write_offset, size = self.mem_manager.allocate(write_size)
            if write_offset is None:
                out_queue.put((file_path, None, None))
            else:
                item = (file_path, write_offset, size)
                out_queue.put(item)
                # print("paraent_pid:{} idx:{} allocate item:{}".format(os.getpid(),idx, item))

    def get_cube_item(self, expect_file_path, item_meta):
        from cube_torch.cube_batch_download import CubeDownloadItem
        offset, size = item_meta
        data = bytes(self.shard_bytes_arr[offset:offset + size])
        item_offset = 0
        item_offset += 8
        file_path_size = int.from_bytes(data[item_offset:item_offset + 8], byteorder='big')
        item_offset += 8
        actual_file_path = data[item_offset:item_offset + file_path_size].decode()
        if expect_file_path != actual_file_path:
            print("expect_file_path:{} actual_file_path:{} item_meta:{}".format(expect_file_path, actual_file_path,
                                                                                item_meta))
            return None
        item_offset += file_path_size
        file_content_size = int.from_bytes(data[item_offset:item_offset + 8], byteorder='big')
        item_offset += 8
        content = bytes(data[item_offset:item_offset + file_content_size])
        item = CubeDownloadItem(actual_file_path, content, int(time.time()))

        return item

    def free_item_meta(self,item_meta):
        self.mem_manager.free(item_meta[0],item_meta[1])
