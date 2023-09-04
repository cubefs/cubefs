import copy
import io
import os
import ctypes
import multiprocessing
import random

import psutil

from cube_torch.cube_batch_download import CubeBatchDownloader, CubeDownloadItem

dataset_size=10000000

downloader=CubeBatchDownloader("http://127.0.0.1",dataset_size)

array_size = 128 * 1024
byte_array = bytearray(array_size)
for i in range(array_size):
    random_byte = random.randint(0, 255)  # 生成0到255范围内的随机整数
    byte_array[i] = random_byte

push_items=[]
for t in range(dataset_size):
    array_size = 128 * 1024
    copied_byte_array = copy.deepcopy(byte_array)
    path="guowl/{}".format(t)
    item=CubeDownloadItem(array_size,copied_byte_array,path)
    downloader.add_cube_item(item)
    push_items.append(path)
    if t %100==0:
        process = psutil.Process()
        memory_info = process.memory_info()
        print("当前进程的内存使用情况：")
        print(f"常驻内存大小（RSS）：{memory_info.rss} bytes")
        print(f"虚拟内存大小（VMS）：{memory_info.vms} bytes")
    if len(push_items)>1000:
        for path in push_items:
            downloader.get_cube_path_item(path)
        push_items=[]
        print("当前进程的内存使用情况：")
        print(f"常驻内存大小（RSS）：{memory_info.rss} bytes")
        print(f"虚拟内存大小（VMS）：{memory_info.vms} bytes")
