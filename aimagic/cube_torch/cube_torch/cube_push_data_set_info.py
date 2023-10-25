import json
import os
import re
import requests
from cube_torch import get_manager
from cube_torch.cube_dataset_info import CubeDataSetInfo

USE_PREFETCH = 'USE_PREFETCH'
VOL_NAME = 'VOL_NAME'


class CubePushDataSetInfo(CubeDataSetInfo):
    def __init__(self, cube_loader):
        super().__init__(cube_loader)
        self.prefetch_read_url = ""
        self.register_pid_addr = ""
        self.shared_memory_size = 0
        self.prof_port = ""
        self.vol_name = os.environ.get(VOL_NAME)
        self._is_use_batch_download = True
        self.check_evn()
        self._dataset_cnt = cube_loader.real_sample_size
        self.cubefs_mount_point = ""
        self.read_cube_torch_config_file_for_v3()
        self.storage_seesion = requests.Session()
        self.get_train_file_name_lists()
        self.register_pid_addr = "http://127.0.0.1:{}/register/pid".format(self.prof_port)
        self.unregister_pid_addr = "http://127.0.0.1:{}/unregister/pid".format(self.prof_port)
        self.prefetch_read_url = "http://127.0.0.1:{}/prefetch/read/path".format(self.prof_port)
        self.batch_download_addr = "http://127.0.0.1:{}/batchdownload/path".format(self.prof_port)
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
            if self.train_list_dimensional == 2:
                train_file_name_lists.extend(self.train_list[:, index])
            else:
                train_file_name_lists.append(self.train_list[index])
        return train_file_name_lists

    def is_use_batch_download(self):
        return self._is_use_batch_download

    def check_evn(self):
        if self.vol_name is None:
            raise ValueError('VOL_NAME env not set,please set VOL_NAME env')
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

    def get_batch_download_addr(self):
        return self.batch_download_addr
