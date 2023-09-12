import multiprocessing
import random
import time

from cube_torch.cube_batch_download import CubeDownloadItem, bytes_to_c_void_p


def generate_random_indexes(num_indexes, max_index):
    indexes = []
    for _ in range(num_indexes):
        index = random.randint(0, max_index)
        indexes.append(index)
    return indexes


def read_index_file(filename):
    with open(filename, 'r') as f:
        lines = f.readlines()
    return [line.strip() for line in lines]


def write_process(cube_content_cache, index_files, indexes_queue):
    while True:
        indexes = generate_random_indexes(10, len(index_files) - 1)
        for index in indexes:
            file_path = index_files[index].encode()
            file_content = read_local_file(file_path)
            item = CubeDownloadItem(bytes_to_c_void_p(file_path), len(file_path), bytes_to_c_void_p(file_content),
                                    len(file_content), int(time.time()))
            cube_content_cache[index] = item
            print("write_process cube_content_cache addr:{}  item.path_ptr:{} item.path_size:{} item addr:{}"
                  " item.avali_time:{}".format(cube_content_cache, item.path_ptr, item.path_size, item,
                                               item.avali_time))
            indexes_queue.put(index)
            time.sleep(1)


def read_process(cube_content_cache, index_files, indexes_queue):
    while True:
        index = indexes_queue.get()
        file_path = index_files[index]
        item = cube_content_cache[index]
        print("read_process cube_content_cache addr:{}  item.path_ptr:{} item.path_size:{} item addr:{}"
              " item.avali_time:{}".format(cube_content_cache, item.path_ptr, item.path_size, item, item.avali_time))
        if item.is_avali_item() and item.get_path() == file_path:
            # 读取本地文件内容
            file_content = read_local_file(file_path)
            # 比对 cube_item.get_content() 和本地文件内容
            if item.get_content() == file_content:
                print("Content matches.")
            else:
                print("Content does not match.")


def read_local_file(file_path):
    with open(file_path, "rb") as f:
        return f.read()


if __name__ == "__main__":
    index_files = read_index_file("0_0_1w.txt")
    cube_content_cache = multiprocessing.Array(CubeDownloadItem, 100000,lock=False)
    indexes_queue = multiprocessing.Queue()
    write_process = multiprocessing.Process(target=write_process, args=(cube_content_cache, index_files, indexes_queue))
    write_process.daemon=True
    read_process1 = multiprocessing.Process(target=read_process,args=(cube_content_cache, index_files, indexes_queue))
    read_process1.daemon=True
    read_process2 = multiprocessing.Process(target=read_process,args=(cube_content_cache, index_files, indexes_queue))
    read_process2.daemon=True
    write_process.start()
    read_process1.start()
    read_process2.start()

    # 等待进程结束
    write_process.join()
    read_process1.join()
    read_process2.join()
    print("main thread exit")