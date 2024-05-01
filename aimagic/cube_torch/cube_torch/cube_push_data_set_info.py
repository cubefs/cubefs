import json
import os
import re

import numpy as np
import requests
from torch.utils.data import ConcatDataset

from cube_torch import get_manager
from cube_torch.cube_dataset_info import CubeDataSetInfo, is_numpy_2d_array

USE_PREFETCH = 'USE_PREFETCH'
USE_BATCH_DOWNLOAD = 'USE_BATCH_DOWNLOAD'
VOL_NAME = 'VOL_NAME'
CUBE_DATASET_FILE = 'DATASET_FILE'


def parse_file(file_path):
    data = []
    element_count = None
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            elements = [float(x.strip()) for x in line.split(',')]
            if element_count is None:
                element_count = len(elements)
            elif len(elements) != element_count:
                raise ValueError(
                    f"每行的元素个数应该相等,但第一行有{element_count}个元素,当前行有{len(elements)}个元素")
            data.append(elements)
    return np.array(data)


class CubePushDataSetInfo(CubeDataSetInfo):
    def __init__(self, cube_loader):
        os.environ[USE_PREFETCH]="1"
        super().__init__(cube_loader)
        self.prefetch_read_url = ""
        self.register_pid_addr = ""
        self.shared_memory_size = 0
        self.prof_port = ""
        self.vol_name = os.environ.get(VOL_NAME)
        self._is_use_batch_download = True
        self._dataset_file = None
        self._train_dataset = None
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

    def parse_dataset_file(self):
        if self._dataset_file is not None:
            self.train_list = parse_file(self._dataset_file)

    def folder_train_file_names(self, index_list):
        train_file_name_lists = []
        is_signal_element = False
        if len(self.train_list[0]) == 1:
            is_signal_element = True
        for index in index_list:
            if is_signal_element:
                train_file_name_lists.append(self.train_list[index])
            else:
                train_file_name_lists.append(self.train_list[index][0])
        return train_file_name_lists

    def numpy_2d_train_file_names(self, index_list):
        train_file_name_lists = []
        for index in index_list:
            train_file_name_lists.extend(self.train_list[:, index])
        return train_file_name_lists

    def numpy_1d_train_file_names(self, index_list):
        train_file_name_lists = []
        for index in index_list:
            train_file_name_lists.extend(self.train_list[index])
        return train_file_name_lists

    def covert_index_list_to_filename(self, index_list):
        if self.is_folder_dataset:
            return self.folder_train_file_names(index_list)
        elif self.train_list_dimensional == 2:
            return self.numpy_2d_train_file_names(index_list)
        elif self.train_list_dimensional == 1:
            return self.numpy_1d_train_file_names(index_list)

    def is_use_batch_download(self):
        return self._is_use_batch_download

    def check_evn(self):
        if self.vol_name is None:
            raise ValueError('VOL_NAME env not set,please set VOL_NAME env')
        prefetch = os.environ.get(USE_PREFETCH)
        if prefetch is not None:
            self._is_use_batch_download = False
        batch_download = os.environ.get(USE_BATCH_DOWNLOAD)
        if batch_download is not None:
            self._is_use_batch_download = True
        dataset_file = os.environ.get(CUBE_DATASET_FILE)
        if dataset_file is not None:
            if not os.path.exists(dataset_file):
                raise ValueError('DATASET_FILE:{} not exist'.format(dataset_file))
            if not os.path.isfile(dataset_file):
                raise ValueError('DATASET_FILE:{} is not file'.format(dataset_file))
            self._dataset_file = dataset_file
        else:
            raise ValueError('DATASET_FILE env not set,please set DATASET_FILE env')
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

    def get_train_file_name_lists(self):
        if len(self.train_list) != 0:
            return self.train_list

        loader = self.cube_loader
        dataset = loader.dataset
        if isinstance(dataset, ConcatDataset):
            file_name_lists = self._concatDataSet_get_samples(dataset)
        else:
            file_name_lists = self._signel_DataSet_get_samples(dataset)
        if is_numpy_2d_array(file_name_lists):
            self.train_list_dimensional = 2
        self.train_list = file_name_lists
