r""""Contains definitions of the methods used by the _BaseDataLoaderIter workers.

These **needs** to be in global scope since Py2 doesn't support serializing
static methods.
"""
import asyncio
import builtins
import json
import logging
import os
import queue
import random
import time
from typing import Union

import requests
import torch
from dataclasses import dataclass
from torch._utils import ExceptionWrapper
from torch.utils.data import _DatasetKind

from cube_torch.cube_batch_download import CubeBatchDownloader
from cube_torch.cube_file import set_global_cube_rootdir_path, InterceptionIO, set_global_interception_io
from cube_torch.cube_file_open_interceptor import CubeFileOpenInterceptor
from cube_torch.mem_manager import MemoryAllocater

logger = logging.getLogger(__name__)
from torch.utils.data._utils.worker import WorkerInfo, _generate_state, HAS_NUMPY, _IterableDatasetStopIteration, \
    MP_STATUS_CHECK_INTERVAL, ManagerWatchdog

_worker_info = None


def get_worker_info():
    r"""Returns the information about the current
    :class:`~torch.utils.data.DataLoader` iterator worker process.

    When called in a worker, this returns an object guaranteed to have the
    following attributes:

    * :attr:`id`: the current worker id.
    * :attr:`num_workers`: the total number of workers.
    * :attr:`seed`: the random seed set for the current worker. This value is
      determined by main process RNG and the worker id. See
      :class:`~torch.utils.data.DataLoader`'s documentation for more details.
    * :attr:`dataset`: the copy of the dataset object in **this** process. Note
      that this will be a different object in a different process than the one
      in the main process.

    When called in the main process, this returns ``None``.

    .. note::
       When used in a :attr:`worker_init_fn` passed over to
       :class:`~torch.utils.data.DataLoader`, this method can be useful to
       set up each worker process differently, for instance, using ``worker_id``
       to configure the ``dataset`` object to only read a specific fraction of a
       sharded dataset, or use ``seed`` to seed other libraries used in dataset
       code.
    """
    return _worker_info


r"""Dummy class used to signal the end of an IterableDataset"""


@dataclass(frozen=True)
class _IterableDatasetStopIteration(object):
    worker_id: int


r"""Dummy class used to resume the fetching when worker reuse is enabled"""


@dataclass(frozen=True)
class _ResumeIteration(object):
    pass


def _post_to_storage_async(index_list, notify_storage_addr):
    loop = asyncio.new_event_loop()
    loop.run_in_executor(None, _post_to_storage, index_list, notify_storage_addr)


def _post_to_storage(index_list, notify_storage_addr):
    if len(index_list) == 0:
        return
    try:
        data = json.dumps(index_list)
        with requests.post(notify_storage_addr, data, timeout=2) as response:
            if response.status_code != 200:
                raise ValueError("unavali request,response:{}".format(response.text))
    except Exception as e:
        print('_post_to_storage{} _post_to_storage error{} index_list{} '.format(notify_storage_addr, e, index_list))
        return


def _register_pid_to_storage(pids, register_storage_pid_addr):
    try:
        if register_storage_pid_addr == "":
            return
        data = json.dumps(pids)
        requests.post(register_storage_pid_addr, data, timeout=1)
    except Exception as e:
        print('register_storage_pid_addr{} _post_to_storage error{} pids{} '.format(register_storage_pid_addr, e, pids))
        return


def _unregister_pid_to_storage(pids, unregister_storage_addr):
    try:
        if unregister_storage_addr == "":
            return
        data = json.dumps(pids)
        requests.post(unregister_storage_addr, data, timeout=1)
    except Exception as e:
        print('unregister_storage_addr{} _post_to_storage error{} pids{} '.format(unregister_storage_addr, e, pids))
        return


def _loop_push_worker(wait_read_train_file_queue, cube_prefetch_addr, is_use_batch_download, downloader_info, event):
    torch.set_num_threads(1)
    if is_use_batch_download:
        downloader = CubeBatchDownloader(downloader_info)
    while not event.is_set():
        try:
            copy_file_indexs = wait_read_train_file_queue.get(timeout=5)
            index_list = [copy_file_indexs]
            if is_use_batch_download:
                downloader.batch_download_async(index_list)
            else:
                _post_to_storage_async(index_list, cube_prefetch_addr)
        except queue.Empty:
            continue
        except KeyboardInterrupt:
            return
        except Exception as e:
            continue


def _loop_allocate_mem_worker(total_memory, batch_download_workers, batch_download_notify_queues, free_item_meta_queue, event):
    torch.set_num_threads(batch_download_workers)
    mem_allocater = MemoryAllocater(total_memory, batch_download_workers, batch_download_notify_queues, free_item_meta_queue)
    while not event.is_set():
        for t in mem_allocater.allocate_memory_threads:
            t.join()


def _worker_loop(dataset_kind, dataset, index_queue, data_queue, done_event,
                 auto_collation, collate_fn, drop_last, base_seed, init_fn, worker_id,
                 num_workers, persistent_workers, cube_root_dir, is_use_batch_download, downloader_info):
    torch.set_num_threads(1)
    inception = None
    if is_use_batch_download:
        set_global_cube_rootdir_path(cube_root_dir)
        CubeFileOpenInterceptor.set_params(cube_root_dir)
        CubeFileOpenInterceptor.start_timer()
        inception = InterceptionIO(downloader_info[0], downloader_info[1], downloader_info[2])
        builtins.open = inception.intercept_open(open)
        torch.load = inception.intercept_torch_load(torch.load)
        set_global_interception_io(inception)

    try:
        seed = base_seed + worker_id
        random.seed(seed)
        torch.manual_seed(seed)
        if HAS_NUMPY:
            np_seed = _generate_state(base_seed, worker_id)
            import numpy as np
            np.random.seed(np_seed)

        global _worker_info
        _worker_info = WorkerInfo(id=worker_id, num_workers=num_workers,
                                  seed=seed, dataset=dataset)

        init_exception = None

        try:
            if init_fn is not None:
                init_fn(worker_id)

            fetcher = _DatasetKind.create_fetcher(dataset_kind, dataset,
                                                  auto_collation, collate_fn, drop_last)
        except Exception:
            init_exception = ExceptionWrapper(
                where="in DataLoader worker process {}".format(worker_id))

        # When using Iterable mode, some worker can exit earlier than others due
        # to the IterableDataset behaving differently for different workers.
        # When such things happen, an `_IterableDatasetStopIteration` object is
        # sent over to the main process with the ID of this worker, so that the
        # main process won't send more tasks to this worker, and will send
        # `None` to this worker to properly exit it.
        #
        # Note that we cannot set `done_event` from a worker as it is shared
        # among all processes. Instead, we set the `iteration_end` flag to
        # signify that the iterator is exhausted. When either `done_event` or
        # `iteration_end` is set, we skip all processing step and just wait for
        # `None`.
        iteration_end = False
        watchdog = ManagerWatchdog()
        fetch_batch_cnt = 0
        while watchdog.is_alive():
            try:
                r = index_queue.get(timeout=MP_STATUS_CHECK_INTERVAL)
            except queue.Empty:
                continue
            if isinstance(r, _ResumeIteration):
                # Acknowledge the main process
                data_queue.put((r, None))
                iteration_end = False
                # Recreate the fetcher for worker-reuse policy
                fetcher = _DatasetKind.create_fetcher(
                    dataset_kind, dataset, auto_collation, collate_fn, drop_last)
                continue
            elif r is None:
                # Received the final signal
                assert done_event.is_set() or iteration_end
                break
            elif done_event.is_set() or iteration_end:
                # `done_event` is set. But I haven't received the final signal
                # (None) yet. I will keep continuing until get it, and skip the
                # processing steps.
                continue
            idx = r[0]
            index = r[1]
            data: Union[_IterableDatasetStopIteration, ExceptionWrapper]
            if init_exception is not None:
                data = init_exception
                init_exception = None
            else:
                try:
                    data = fetcher.fetch(index)
                    fetch_batch_cnt += 1
                except Exception as e:
                    if isinstance(e, StopIteration) and dataset_kind == _DatasetKind.Iterable:
                        data = _IterableDatasetStopIteration(worker_id)
                        iteration_end = True
                    else:
                        data = ExceptionWrapper(
                            where="in DataLoader worker process {}".format(worker_id))

            data_queue.put((idx, data))
            del data, idx, index, r  # save memory
    except KeyboardInterrupt:
        pass

    if done_event.is_set():
        data_queue.cancel_join_thread()
        data_queue.close()
