import ctypes
import multiprocessing
import os
import time

slot_cache_lock = multiprocessing.Lock()


class SlotCache:
    def __init__(self, slot_index, max_obj_size, cache_size):
        self.max_obj_size = max_obj_size
        self.cache_size = cache_size
        self.default_avali_value = [0, 0, 0, 0, 0, 0, 0, 0]
        self.shard_bytes_arr = multiprocessing.RawArray(ctypes.c_ubyte, cache_size)
        self.slot_offset = multiprocessing.Value("i", 0)
        self.slot_index = slot_index
        self.last_slot_offset = 0

    def is_empty_avali_time(self, avali_time):
        if avali_time == self.default_avali_value:
            return True
        insert_time = int.from_bytes(avali_time[0:8], byteorder='big')
        if int(time.time()) - insert_time > 200:
            return True
        return False

    def check_has_exsit_avali_item(self, offset):
        avali_time = int.from_bytes(self.shard_bytes_arr[offset:offset + 8])
        offset += 8
        path_size = int.from_bytes(self.shard_bytes_arr[offset:offset + 8])
        offset += 8
        file_path = bytes(self.shard_bytes_arr[offset:offset + path_size]).decode()
        return avali_time, file_path

    def insert_cube_item(self, file_path, encode_bytes):
        write_size = len(encode_bytes)
        with slot_cache_lock:
            if self.slot_offset.value + write_size >= self.cache_size:
                self.slot_offset.value = 0
            write_offset = self.slot_offset.value
            if not self.is_empty_avali_time(self.shard_bytes_arr[write_offset:write_offset + 8]):
                exsit_avali_time, exsit_file_path = self.check_has_exsit_avali_item(write_offset)
                print("slot_index:{} write_offset:{} has exsit "
                      "item:[avali_time:{},path:{}]".format(self.slot_index, write_offset, exsit_avali_time,
                                                            exsit_file_path))
                return None

            self.slot_offset.value = write_offset + write_size
            self.last_slot_offset = self.slot_offset.value
        self.shard_bytes_arr[write_offset:write_offset + write_size] = encode_bytes
        item_meta = (self.slot_index, write_offset, write_size)
        return item_meta

    def get_cube_item(self, expect_file_path, item_meta):
        from cube_torch.cube_batch_download import CubeDownloadItem
        slot_index, offset, size = item_meta
        data = bytes(self.shard_bytes_arr[offset:offset + size])
        item_offset = 0
        if self.is_empty_avali_time(data[item_offset:item_offset + 8]):
            print("expect_file_path:{} avali_time:{} "
                  "".format(expect_file_path, int.from_bytes(data[item_offset:item_offset + 8], byteorder='big')))
            return None

        item_offset += 8
        file_path_size = int.from_bytes(data[item_offset:item_offset + 8], byteorder='big')
        item_offset += 8
        actual_file_path = data[item_offset:item_offset + file_path_size].decode()
        if expect_file_path != actual_file_path:
            print("expect_file_path:{} actual_file_path:{}".format(expect_file_path, actual_file_path))
            return None
        item_offset += file_path_size
        file_content_size = int.from_bytes(data[item_offset:item_offset + 8], byteorder='big')
        item_offset += 8
        content = bytes(data[item_offset:item_offset + file_content_size])
        item = CubeDownloadItem(actual_file_path, content, int(time.time()))
        self.shard_bytes_arr[offset:offset + 8] = self.default_avali_value


        return item


def get_file_path_item_key(dataset_id):
    return "file_path_item_{}".format(dataset_id)


class ShardMemory:
    def __init__(self, cache_size):
        self.slot_caches = []
        self._min_obj_size = 128 * 1024
        self.cache_size = cache_size
        self.slot_caches.append(SlotCache(0, self._min_obj_size, int(self.cache_size * 0.4)))
        self.slot_caches.append(SlotCache(1, self._min_obj_size * 2, int(self.cache_size * 0.4)))
        self.slot_caches.append(SlotCache(2, self._min_obj_size * 3, int(self.cache_size * 0.2)))

    def get_slot_cache(self, item_size):
        for cache in self.slot_caches:
            if cache.max_obj_size >= item_size:
                return cache

        return self.slot_caches[0]

    def insert_cube_item(self, file_path, encode_bytes):
        slot_cache = self.get_slot_cache(len(encode_bytes))
        if slot_cache is None:
            return
        item_meta = slot_cache.insert_cube_item(file_path, encode_bytes)
        return item_meta

    def get_cube_item(self, file_path, item_meta):
        slot_index, offset, size = item_meta
        return self.slot_caches[slot_index].get_cube_item(file_path, item_meta)
