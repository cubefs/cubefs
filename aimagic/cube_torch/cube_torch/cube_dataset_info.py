import gc
import os

import numpy as np
from torch import multiprocessing
from torch.utils.data import ConcatDataset
from torchvision import datasets

CubeFS_ROOT_DIR = 'CubeFS_ROOT_DIR'
TEST_ENV = 'TEST_ENV'
CubeFS_QUEUE_SIZE_ON_WORKER = 'CubeFS_QUEUE_SIZE_ON_WORKER'
Min_QUEUE_SIZE_ON_WORKER = 10
Max_QUEUE_SIZE_ON_WORKER = 20


def is_2d_array(obj):
    if isinstance(obj, list):
        if all(isinstance(sublist, list) for sublist in obj):
            return True
    return False


def is_numpy_2d_array(obj):
    return isinstance(obj, np.ndarray) and len(obj.shape) == 2


def is_numpy_1d_array(obj):
    return isinstance(obj, np.ndarray) and len(obj.shape) == 1


class CubeDataSetInfo:
    def __init__(self, cube_loader):
        self.cube_loader = cube_loader
        self.dataset_id = id(cube_loader.dataset)
        self.cubefs_queue_size_on_worker = os.environ.get(CubeFS_QUEUE_SIZE_ON_WORKER)
        self.cubefs_mount_point = os.environ.get(CubeFS_ROOT_DIR)
        self.cube_prefetch_file_list = []
        self.train_list = []
        self._is_test_env = os.environ.get(TEST_ENV)
        if self._is_test_env is not None:
            self._is_test_env = True
        else:
            self._is_test_env = False
        self.stop_event = multiprocessing.Event()
        self._init_env_fininsh = False
        self.train_list_dimensional = 1
        self.is_folder_dataset = False

    def get_cubefs_root_dir(self):
        return self.cubefs_mount_point

    def is_cubefs_mount_point(self, directory_path):
        stat_info = os.stat(directory_path)
        inode_number = stat_info.st_ino
        inode_number = 1
        return inode_number == 1

    def check_cube_queue_size_on_worker(self):
        if self.cubefs_queue_size_on_worker is None:
            self.cubefs_queue_size_on_worker = Min_QUEUE_SIZE_ON_WORKER
        try:
            queue_size = int(self.cubefs_queue_size_on_worker)
            if queue_size > Max_QUEUE_SIZE_ON_WORKER:
                queue_size = Max_QUEUE_SIZE_ON_WORKER
            if queue_size < Min_QUEUE_SIZE_ON_WORKER:
                queue_size = Min_QUEUE_SIZE_ON_WORKER
            self.cubefs_queue_size_on_worker = queue_size
        except Exception as e:
            raise ValueError("{} set is not a number exception{} ".format(CubeFS_QUEUE_SIZE_ON_WORKER, e))

    def get_dataset_samples(self, dataset):
        result = None
        if isinstance(dataset, datasets.DatasetFolder):
            result = dataset.samples
            self.is_folder_dataset = True
        elif isinstance(dataset, datasets.VOCDetection):
            result = dataset.images
            self.is_folder_dataset = True
        elif isinstance(dataset, datasets.CocoDetection):
            samples_len = dataset.__len__()
            samples_path = []
            for i in range(samples_len):
                path = dataset.coco.loadImgs(i)[0]["file_name"]
                samples_path.append(os.path.join(dataset.root, path))
            result = samples_path
            self.is_folder_dataset = True
        elif hasattr(dataset, 'train_data_list'):
            result = dataset.train_data_list()
        if not self.is_folder_dataset and not is_numpy_2d_array(result) and not is_numpy_1d_array(result):
            raise ValueError("Invalid Custom Dataset train_data_list func, "
                             "Its return value must be a two-dimensional numpy array or one-dimensional numpy array.")

        return result

    def _concat_folder_dataset_array(self, dataset):
        sample_array = []
        for sub_dataset in dataset.datasets:
            sdata_arr = self.get_dataset_samples(sub_dataset)
            sample_array.extend(sdata_arr)
        return sample_array

    def _concat_numpy_array(self, dataset):
        numpy_array = []
        for sdataset in dataset.datasets:
            sdata_arr = self.get_dataset_samples(sdataset)
            numpy_array.append(sdata_arr)
        return numpy_array

    def _concatDataSet_get_samples(self, dataset):
        sdata_arr = self.get_dataset_samples(dataset.datasets[0])
        if self.is_folder_dataset:
            return self._concat_folder_dataset_array(dataset)
        elif is_numpy_2d_array(sdata_arr):
            return np.concatenate(self._concat_numpy_array(dataset), axis=1)
        elif is_numpy_1d_array(sdata_arr):
            return np.concatenate(self._concat_numpy_array(dataset))

    def _signel_DataSet_get_samples(self, dataset):
        return self.get_dataset_samples(dataset)

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

    def get_cube_queue_size_on_worker(self):
        return self.cubefs_queue_size_on_worker

    def get_register_pid_addr(self):
        return ""

    def get_unregister_pid_addr(self):
        return ""

    def get_batch_download_addr(self):
        return ""

    def is_test_env(self):
        return self._is_test_env
