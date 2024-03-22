from torch import multiprocessing

manager = None
_lock = multiprocessing.Lock()
originDataLoaderNew = None


def get_manager():
    global manager
    with _lock:
        if not manager:
            manager = multiprocessing.Manager()
    return manager


from cube_torch.cube_push_data_set_info import CubePushDataSetInfo
from cube_torch.cube_loader import CubeDataLoader

try:
    import numpy

    HAS_NUMPY = True
except ModuleNotFoundError:
    HAS_NUMPY = False


def _set_python_exit_flag():
    global python_exit_status
    python_exit_status = True


def custom_new_CubeDataloader(cls, *args, **kwargs):
    return CubeDataLoader(*args, **kwargs)


def init_by_pass_DataLoaderClass():
    from torch.utils.data import DataLoader
    DataLoader.__new__ = custom_new_CubeDataloader


init_by_pass_DataLoaderClass()
MP_STATUS_CHECK_INTERVAL = 5.0
