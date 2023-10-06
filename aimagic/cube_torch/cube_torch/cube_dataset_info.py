import os

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

    def get_cubefs_root_dir(self):
        return self.cubefs_mount_point

    def get_cubefs_cache_dir(self):
        return self.cubefs_mount_point

    def is_cubefs_mount_point(self, directory_path):
        stat_info = os.stat(directory_path)
        inode_number = stat_info.st_ino
        inode_number=1
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
        if isinstance(dataset, datasets.DatasetFolder):
            samples = dataset.samples
            return [s[0] for s in samples]
        if isinstance(dataset, datasets.VOCDetection):
            return dataset.images
        if isinstance(dataset, datasets.CocoDetection):
            samples_len = dataset.__len__()
            samples_path = []
            for i in range(samples_len):
                path = dataset.coco.loadImgs(i)[0]["file_name"]
                samples_path.append(os.path.join(dataset.root, path))
            return samples_path
        if hasattr(dataset, 'train_data_list'):
            return dataset.train_data_list()
        raise ValueError("Invalid dataset{} because the custom dataset does not implement "
                         "the train_data_list method or it is not a pytorch dataset supported by CubeTorch.".format(
            dataset))

    def _concatDataSet_get_samples(self, dataset):
        file_name_lists = []
        sdata_arr_is_2d = False
        for sdataset in dataset.datasets:
            sdata_arr = self.get_dataset_samples(sdataset)
            if is_2d_array(sdata_arr):
                sdata_arr_is_2d = True
                file_name_lists.append(sdata_arr)
            else:
                file_name_lists += sdata_arr
        if sdata_arr_is_2d:
            return [sum(row, []) for row in zip(*file_name_lists)]
        else:
            return file_name_lists

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
        if is_2d_array(file_name_lists):
            self.train_list = file_name_lists
        else:
            self.train_list = [file_name_lists]

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
