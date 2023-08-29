import os
import shutil
import threading
import time
import warnings

from cube_torch import get_manager

warnings.filterwarnings("ignore", category=UserWarning, module="multiprocessing")
from cube_torch.cube_dataset_info import CubeDataSetInfo

CubeFS_CACHE_DIR = 'CubeFS_CACHE_DIR'
CubeFS_CACHE_SIZE = 'CubeFS_CACHE_SIZE'


def get_free_space(path):
    os.makedirs(path, exist_ok=True)
    st = os.statvfs(path)
    free_bytes = st.f_bavail * st.f_frsize
    return free_bytes


class CubeDiskDataSetInfo(CubeDataSetInfo):
    def __init__(self, cube_loader):
        super().__init__(cube_loader)
        self.cubefs_cache_dir = os.environ.get(CubeFS_CACHE_DIR)
        self.cubefs_cache_size = os.environ.get(CubeFS_CACHE_SIZE)
        self.check_evn()
        self.init_cache_dir()
        self.THRESHOLD_90 = self.cubefs_cache_size * 0.9
        self.HALF_THRESHOLD = self.cubefs_cache_size * 0.5
        self.get_train_file_name_lists()
        t = threading.Thread(target=self._loop_clean_disk_data, daemon=True)
        t.daemon = True
        t.start()
        dataset_id = id(cube_loader.dataset)
        get_manager().__dict__[dataset_id] = self

    def init_cache_dir(self):
        for filename in os.listdir(self.cubefs_cache_dir):
            file_path = os.path.join(self.cubefs_cache_dir, filename)
            try:
                if os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                else:
                    os.remove(file_path)
            except Exception as e:
                continue

    def get_cubefs_cache_dir(self):
        return self.cubefs_cache_dir

    def check_evn(self):
        if self.cubefs_cache_dir is None:
            raise ValueError("{} not set on os environ ".format(CubeFS_CACHE_DIR))
        if self.cubefs_cache_size is None:
            raise ValueError("{} not set on os environ ".format(CubeFS_CACHE_SIZE))
        try:
            self.cubefs_cache_size = float(self.cubefs_cache_size)
        except Exception as e:
            raise ValueError("{} is wrong Exception {} ".format(CubeFS_CACHE_SIZE, e))
        free_space = get_free_space(self.cubefs_cache_dir)
        if free_space < self.cubefs_cache_size * 1.2:
            raise ValueError("{} free_space {} ,cubefs_cache_size {}"
                             " too smaller".format(self.cubefs_cache_dir, free_space, self.cubefs_cache_size))
        self.check_cube_queue_size_on_worker()

    def _loop_clean_disk_data(self):
        while not self.stop_event.is_set():
            try:
                self.clear_dir()
                time.sleep(10)
            except KeyboardInterrupt:
                return
            except Exception:
                continue

    def do_clean(self, maxThreshold, minThreshold):
        total_size = 0
        files = []
        dir_path = self.cubefs_cache_dir

        # Batch retrieve file information
        file_info_cache = {}
        for root, dirs, filenames in os.walk(dir_path):
            for filename in filenames:
                full_path = os.path.join(root, filename)
                try:
                    if full_path in file_info_cache:
                        stat = file_info_cache[full_path]
                    else:
                        stat = os.stat(full_path)
                        file_info_cache[full_path] = stat
                except OSError:
                    # Handle case where file was deleted by another process
                    continue

                size = stat.st_size
                total_size += size
                access_time = stat.st_atime
                files.append((access_time, full_path, size))

        max_size_limit = self.cubefs_cache_size * maxThreshold
        min_size_limit = self.cubefs_cache_size * minThreshold
        if total_size < max_size_limit:
            return

        files.sort()
        clean_size = 0
        files_to_delete = []

        # Find files to delete
        for access_time, delete_file_name, size in files:
            if total_size <= min_size_limit:
                break
            try:
                os.unlink(delete_file_name)
                total_size -= size
                clean_size += size
                files_to_delete.append(delete_file_name)
            except OSError:
                # Handle case where file was deleted by another process
                continue

        print("dir_path {} cubefs_cache_size{} total_size{} min_size_limit{} "
              "max_size_limit{} delete_size{}".format(dir_path, self.cubefs_cache_size, total_size, min_size_limit,
                                                      max_size_limit, clean_size))

    def clear_dir(self):
        lock_file_name = os.path.join(self.cubefs_cache_dir, ".lock")
        try:
            if os.path.exists(lock_file_name):
                last_access_time = os.path.getmtime(lock_file_name)
                time_diff = time.time() - last_access_time
                if time_diff > 10:
                    os.remove(lock_file_name)
                return
            f = open(lock_file_name, "w", )
            self.do_clean(0.9, 0.5)
            f.write("nihao")
            f.close()
            os.remove(lock_file_name)
        except Exception as e:
            os.remove(lock_file_name)
            pass
