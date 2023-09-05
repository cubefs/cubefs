import copy
import random

import psutil

from cube_torch.cube_batch_download import CubeBatchDownloader, CubeDownloadItem

dataset_size=10000000

downloader=CubeBatchDownloader("http://127.0.0.1",dataset_size)

file_size = 128 * 1024
byte_array = bytearray(file_size)
for i in range(file_size):
    random_byte = random.randint(0, 255)  # 生成0到255范围内的随机整数
    byte_array[i] = random_byte

push_items=[]
for t in range(dataset_size):
    file_size = 128 * 1024
    file_content = copy.deepcopy(byte_array)
    file_name= "guowl/{}".format(t)
    item=CubeDownloadItem(file_name, file_size, file_content)
    downloader.add_cube_item(item)
    push_items.append(file_name)
    if t %100==0:
        process = psutil.Process()
        memory_info = process.memory_info()
        print("当前进程的内存使用情况：")
        print(f"常驻内存大小（RSS）：{memory_info.rss} bytes")
        print(f"虚拟内存大小（VMS）：{memory_info.vms} bytes")
    if len(push_items)>1000:
        for file_name in push_items:
            downloader.get_cube_path_item(file_name)
        push_items=[]
        print("当前进程的内存使用情况：")
        print(f"常驻内存大小（RSS）：{memory_info.rss} bytes")
        print(f"虚拟内存大小（VMS）：{memory_info.vms} bytes")
