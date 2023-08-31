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
from concurrent.futures import ThreadPoolExecutor
from typing import Union

import requests
import torch
from dataclasses import dataclass
from torch._utils import ExceptionWrapper

from cube_torch.cache_open_interceptor import CachedOpenInterceptor
from cube_torch.fast_copy import do_copy

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


def _post_to_storage_async(index_list, notify_storage_addr, storage_seesion):
    loop = asyncio.new_event_loop()
    loop.run_in_executor(None, _post_to_storage, index_list, notify_storage_addr, storage_seesion)


def _post_to_storage(index_list, notify_storage_addr, storage_seesion):
    if len(index_list) == 0:
        return
    try:
        data = json.dumps(index_list)
        storage_seesion.post(notify_storage_addr, data, timeout=1)
    except Exception as e:
        print('url{} _post_to_storage error{} index_list{} '.format(notify_storage_addr, e, index_list))
        return


def _register_pid_to_storage(pids, register_storage_pid_addr):
    try:
        if register_storage_pid_addr == "":
            return
        data = json.dumps(pids)
        requests.post(register_storage_pid_addr, data, timeout=1)
    except Exception as e:
        print('url{} _post_to_storage error{} pids{} '.format(register_storage_pid_addr, e, pids))
        return


def _unregister_pid_to_storage(pids, unregister_storage_addr):
    try:
        if unregister_storage_addr == "":
            return
        data = json.dumps(pids)
        requests.post(unregister_storage_addr, data, timeout=1)
    except Exception as e:
        print('url{} _post_to_storage error{} pids{} '.format(unregister_storage_addr, e, pids))
        return


def get_cube_dataset_info_on_worker(dataset_id):
    from cube_torch import get_manager
    manager = get_manager()
    while True:
        try:
            cube_dataset_info = manager.__dict__[dataset_id]
            if cube_dataset_info is None:
                manager.refresh()
                continue
            return cube_dataset_info
        except Exception as e:
            time.sleep(1)
            continue


def _copy_files(files, num_threads):
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        for file in files:
            executor.submit(do_copy, file)


def _copy_worker_loop_for_localdisk(dataset_id, wait_read_train_file_queue, event):
    cube_dataset_info = get_cube_dataset_info_on_worker(dataset_id)
    dst_prefix = cube_dataset_info.get_cubefs_cache_dir()
    train_file_name_lists = cube_dataset_info.get_train_file_name_lists()
    while not event.is_set():
        try:
            copy_file_indexs = wait_read_train_file_queue.get(timeout=5)
            tasks = []
            for index in copy_file_indexs:
                for train_name in train_file_name_lists:
                    src = train_name[index]
                    dst = "{}/{}".format(dst_prefix, src)
                    task = (src, dst)
                    tasks.append(task)
            _copy_files(tasks, 100)
        except queue.Empty:
            continue
        except KeyboardInterrupt:
            return
        except Exception as e:
            continue


def _copy_worker_loop_for_post_client(wait_read_train_file_queue, prefetch_addr, worker_num, event):
    cube_prefetch_addr = prefetch_addr
    storage_seesion = requests.Session()
    while not event.is_set():
        try:
            copy_file_indexs = wait_read_train_file_queue.get(timeout=5)
            _post_to_storage([copy_file_indexs],cube_prefetch_addr, storage_seesion)
        except queue.Empty:
            continue
        except KeyboardInterrupt:
            return
        except Exception as e:
            continue



def _worker_loop(dataset_kind, dataset, index_queue, data_queue, done_event,
                 auto_collation, collate_fn, drop_last, base_seed, init_fn, worker_id,
                 num_workers, persistent_workers, is_use_disk, cubefs_root_dir, cubefs_cache_dir):
    if is_use_disk:
        CachedOpenInterceptor.set_params(cubefs_root_dir, cubefs_cache_dir, is_use_disk)
        builtins.open = CachedOpenInterceptor.open
    torch.set_num_threads(1)
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

        from cube_torch.cube_fetcher import _DatasetKind

        init_exception = None

        try:
            if init_fn is not None:
                init_fn(worker_id)

            fetcher = _DatasetKind.create_fetcher(dataset_kind, dataset, auto_collation, collate_fn, drop_last)
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
                    if is_use_disk and fetch_batch_cnt % 20 == 0:
                        CachedOpenInterceptor.print_hit_rate()
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


