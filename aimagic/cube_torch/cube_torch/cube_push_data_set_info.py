import json
import os
import re
import threading
import time

import requests

from cube_torch import get_manager
from cube_torch.cube_dataset_info import CubeDataSetInfo, CubeFS_ROOT_DIR

USE_PREFETCH = 'USE_PREFETCH'
VOL_NAME = 'VOL_NAME'
LOCAL_IP = 'localIP'
PREFETCH_THREAD_NUM = 'PREFETCH_THREAD_NUM'
avali_dataset_time = 60 * 5
default_shared_memory_size = 4 * 1024 * 1024 * 1024

Min_PREFETCH_THREAD_NUM = 1
Max_PREFETCH_THREAD_NUM = 5


class CubePushDataSetInfo(CubeDataSetInfo):
    def __init__(self, cube_loader):
        super().__init__(cube_loader)
        self.prefetch_file_url = ""
        self.prefetch_read_url = ""
        self.register_pid_addr = ""
        self.shared_memory_size = 0
        self.prof_port = ""
        self.vol_name = os.environ.get(VOL_NAME)
        self._is_use_batch_download = True
        self.prefetch_thread_num = os.environ.get(PREFETCH_THREAD_NUM)
        self.cube_prefetch_ttl = 30
        self.check_evn()
        self._dataset_cnt = cube_loader.real_sample_size
        self.cubefs_mount_point = ""
        self.local_ip = ""
        self.read_cube_torch_config_file_for_v3()
        self.dataset_dir = os.path.join(self.cubefs_mount_point, '.cube_torch', self.local_ip, '.dataset')
        self.storage_seesion = requests.Session()
        self.get_train_file_name_lists()
        self.register_pid_addr = "http://127.0.0.1:{}/register/pid".format(self.prof_port)
        self.unregister_pid_addr = "http://127.0.0.1:{}/unregister/pid".format(self.prof_port)
        self.prefetch_file_url = "http://127.0.0.1:{}/prefetch/pathAdd".format(self.prof_port)
        self.prefetch_read_url = "http://127.0.0.1:{}/prefetch/read?dataset_cnt={}".format(self.prof_port,
                                                                                           self._dataset_cnt)
        self.batch_download_addr = "http://127.0.0.1:{}/batchdownload/path".format(self.prof_port)
        self.clean_old_dataset_file(self.dataset_dir)

        if not self.is_use_batch_download():
            t = threading.Thread(target=self._renew_ttl_loop, daemon=True)
            t.daemon = True
            t.start()

        dataset_id = id(cube_loader.dataset)
        get_manager().__dict__[dataset_id] = self

    def get_cube_prefetch_addr(self):
        return self.prefetch_read_url

    def get_register_pid_addr(self):
        return self.register_pid_addr

    def get_unregister_pid_addr(self):
        return self.unregister_pid_addr

    def covert_index_list_to_filename(self, index_list):
        train_file_name_lists = []
        for index in index_list:
            if self.train_list_dimensional==2:
                train_file_name_lists.extend(self.train_list[:, index])
            else:
                train_file_name_lists.append(self.train_list[index])
        return train_file_name_lists

    def is_use_batch_download(self):
        return self._is_use_batch_download

    def check_evn(self):
        if self.vol_name is None:
            raise ValueError('VOL_NAME env not set,please set VOL_NAME env')

        if self.prefetch_thread_num is None:
            self.prefetch_thread_num = Min_PREFETCH_THREAD_NUM

        try:
            thread_num = int(self.prefetch_thread_num)
        except Exception:
            thread_num = Min_PREFETCH_THREAD_NUM
        self.prefetch_thread_num = thread_num
        prefetch = os.environ.get(USE_PREFETCH)
        if prefetch is not None:
            self._is_use_batch_download = False
        self.check_cube_queue_size_on_worker()
        self._init_env_fininsh = True

    def read_cube_torch_config_file_for_v3(self):
        config = '/tmp/cube_torch.config.{}'.format(self.vol_name)
        try:
            with open(config) as f:
                cube_info = json.load(f)
                if 'prof' not in cube_info:
                    raise ValueError(".cube_info {} cannot find prof info".format(config))
                self.prof_port = cube_info['prof']
                if type(self.prof_port) is not int:
                    raise ValueError(".cube_info {} not set prof prof info".format(config))
                if 'mount_point' not in cube_info:
                    raise ValueError(".cube_info {} cannot find mount_point info".format(config))
                self.cubefs_mount_point = cube_info['mount_point']
                if not self.is_cubefs_mount_point(self.cubefs_mount_point):
                    raise ValueError("{} is not cubefs client mount point".format(self.cubefs_mount_point))
                if not self.is_use_batch_download():
                    if 'local_ip' not in cube_info:
                        raise ValueError(".cube_info {} cannot find prof info".format(config))
                    self.local_ip = cube_info['local_ip']
                    if not self.is_valid_ip(self.local_ip):
                        raise ValueError(".cube_info {} cannot find valid  ip".format(config))

        except Exception as e:
            raise e

    def is_valid_ip(self, address):
        pattern = r'^(\d{1,3}\.){3}\d{1,3}$'
        match = re.match(pattern, address)
        return match is not None

    def load_train_name_check_consistency(self, train_name, train_data):
        def read_file_lines(filename):
            lines = []
            with open(filename, 'r') as file:
                for line in file:
                    lines.append(line.strip())
            return lines

        train_lines = read_file_lines(train_name)

        def compare_arrays(array1, array2):
            if array1 == array2:
                return True
            else:
                return False

        consistency = compare_arrays(train_data, train_lines)
        print("train_name:{} consistency is {}".format(train_name, consistency))
        return consistency

    def start_write_train_file_list(self, check_consistency):
        train_file_list = []
        for idx, train_data in enumerate(self.train_list):
            train_data_len = len(train_data)
            train_name = '{}/{}_{}.txt'.format(self.dataset_dir, train_data_len, idx)
            self._write_train_list(train_name, train_data)
            train_file_list.append((train_name, train_data_len))
            if check_consistency and not self.load_train_name_check_consistency(train_name, train_data):
                raise ValueError("train_file_name:{} consistency check failed".format(train_name))

        return train_file_list

    def set_cube_prefetch_file_list_by_datasets(self):
        if len(self.cube_prefetch_file_list) != 0:
            return self.cube_prefetch_file_list
        if self.train_list is None or len(self.train_list) == 0:
            return

        self.cube_prefetch_file_list = self.start_write_train_file_list(False)
        print("write_cube_train_files set cube_prefetch_files is {}".format(self.cube_prefetch_file_list))
        return self.cube_prefetch_file_list

    def _renew_ttl_loop(self):
        while not self.stop_event.is_set():
            try:
                self.set_cube_prefetch_file_list_by_datasets()
                if len(self.cube_prefetch_file_list) != 0:
                    self.renew_ttl_on_prefetch_files()
                self.clean_old_dataset_file(self.dataset_dir)
                time.sleep(60)
            except Exception as e:
                print("_renew_ttl_loop expect {}".format(e))
                time.sleep(60)
                continue

    def clean_old_dataset_file(self, folder):
        if self.is_use_batch_download():
            return
        try:
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                if os.path.isfile(file_path):
                    stat = os.stat(file_path)
                    if time.time() - stat.st_atime > avali_dataset_time:
                        print(f'Deleting file {file_path} due to not accessed over a day')
                        os.remove(file_path)
        except Exception as e:
            raise ValueError("clean_old_dataset_file expection:{}".format(e))

    def renew_ttl_on_prefetch_files(self):

        for train_name, dataset_cnt in self.cube_prefetch_file_list:
            url = "{}?path={}&ttl={}&dataset_cnt={}".format(self.prefetch_file_url, train_name, self.cube_prefetch_ttl,
                                                            self._dataset_cnt)
            try:
                with self.storage_seesion.get(url, timeout=1) as response:
                    if response.status_code != 200:
                        raise ValueError("unavaliResponse{}".format(response.text))
            except Exception as e:
                raise ValueError("renew_ttl_on_prefetch_files {} error{}".format(url, e))

    def _write_train_list(self, train_name, train_data):
        try:
            if os.path.exists(train_name + ".tmp"):
                return train_name
            if os.path.exists(train_name):
                return train_name
            tmp_file_name = train_name + ".tmp"
            batch_size = 3000
            num_batches = len(train_data) // batch_size
            if len(train_data) % batch_size != 0:
                num_batches += 1
            with open(tmp_file_name, "w") as f:
                for i in range(num_batches):
                    start_index = i * batch_size
                    end_index = (i + 1) * batch_size
                    if end_index >= len(train_data):
                        end_index = len(train_data)
                    batch_array = train_data[start_index:end_index]
                    if len(batch_array) == 0:
                        continue
                    f.write('\n'.join(batch_array))
                    if i < num_batches:
                        f.write('\n')
            os.rename(tmp_file_name, train_name)
            return train_name
        except Exception as e:
            raise ValueError("pid:{} train_name:{} _write_train_list exception:{}".format(os.getpid(), train_name, e))

    def get_batch_download_addr(self):
        return self.batch_download_addr

    def get_notify_storage_worker_num(self):
        if self._is_use_batch_download:
            return 1
        return 1

    def get_cube_prefetch_thread_cnt(self):
        return self.prefetch_thread_num
