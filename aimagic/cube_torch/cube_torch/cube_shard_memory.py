import ctypes
import multiprocessing
import time
from cube_torch import get_manager
from cube_torch.cube_batch_download import CubeDownloadItem


class SlotCache:
    def __init__(self, slot_index, max_obj_size, cache_size):
        self.max_obj_size = max_obj_size
        self.cache_size = cache_size
        self.default_avali_value = [0, 0, 0, 0, 0, 0, 0, 0]
        self.shard_bytes_arr = multiprocessing.RawArray(ctypes.c_byte, cache_size)
        for i in range(cache_size):
            self.shard_bytes_arr[i] = 0
        self._lock = multiprocessing.Lock()
        self.slot_offset = 0
        self.slot_index = slot_index

    def is_empty_avali_time(self, avali_time):
        return avali_time == self.default_avali_value

    def insert_cube_item(self, encode_bytes):
        write_size = len(encode_bytes)
        with self._lock:
            if self.slot_offset + write_size >= self.cache_size:
                self.slot_offset = 0
            write_offset = self.slot_offset
            if not self.is_empty_avali_time(self.shard_bytes_arr[write_offset:write_offset + 8]):
                return
            self.slot_offset += write_size
        self.shard_bytes_arr[write_offset:write_offset + write_size] = encode_bytes
        item_meta = (self.slot_index, write_offset, write_size)
        return item_meta

    def get_cube_item(self, expect_file_path, item_meta):
        slot_index, offset, size = item_meta
        data = bytes(self.shard_bytes_arr[offset:size])
        offset = 0
        if self.is_empty_avali_time(data[offset:offset + 8]):
            return None

        offset += 8
        file_path_size = int.from_bytes(data[offset:offset + 8], byteorder='big')
        offset += 8
        actual_file_path = data[offset:offset + file_path_size].decode()
        if expect_file_path != actual_file_path:
            return None
        offset += file_path_size
        file_content_size = int.from_bytes(data[offset:offset + 8], byteorder='big')
        offset += 8
        content = bytes(data[offset:offset + file_content_size])
        item = CubeDownloadItem(actual_file_path, content, int(time.time()))
        return item


class ShardMemory:
    def __init__(self, slot_cnt=2, cache_size=10 * 1024 * 1024):
        self.slot_cnt = slot_cnt
        self.slot_caches = []
        manager = get_manager()
        self.file_path_item_meta = multiprocessing.Manager().dict()
        self._min_obj_size = 128 * 1024
        self.cache_size = cache_size
        self._slot_cache_size = cache_size // slot_cnt
        for i in range(self.slot_cnt):
            cache = SlotCache(i,(i + 1) * self._min_obj_size, self._slot_cache_size)
            self.slot_caches.append(cache)

    def get_slot_cache(self, item_size):
        for cache in self.slot_caches:
            if cache.max_obj_size >= item_size:
                return cache

        return None

    def insert_cube_item(self, item: CubeDownloadItem):
        encode_bytes = item.encode()
        slot_cache = self.get_slot_cache(len(encode_bytes))
        if slot_cache is None:
            return
        item_meta = slot_cache.insert_cube_item(encode_bytes)
        print("insert item:{} to shard_memory,item_meta:{}".format(item.get_path(),item_meta))
        self.file_path_item_meta[item.get_path()] = item_meta


    def get_cube_item(self,file_path):
        if file_path not in self.file_path_item_meta:
            return None
        item_meta=self.file_path_item_meta.pop(file_path)
        if item_meta is None:
            return None
        slot_index,offset,size=item_meta
        return self.slot_caches[slot_index].get_cube_item(file_path,item_meta)
