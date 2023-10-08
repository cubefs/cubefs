r"""Definition of the DataLoader and associated iterators that subclass _BaseDataLoaderIter

To support these two classes, in `./_utils` we define many utility methods and
functions to be run in multiprocessing. E.g., the data loading worker loop is
in `./_utils/worker.py`.
"""
import datetime
import itertools
import multiprocessing as python_multiprocessing
import os
import queue
import threading
import time
import warnings
from typing import Iterable, Generic, Sequence, Optional, Union

import torch
import torch.multiprocessing as multiprocessing
import torch.utils.data.graph_settings
from torch import _utils
from torch._utils import ExceptionWrapper
from torch.utils.data import _utils
from torch.utils.data.dataloader import _BaseDataLoaderIter, _DatasetKind, _collate_fn_t, _worker_init_fn_t, T_co, \
    _InfiniteConstantSampler
from torch.utils.data.datapipes.datapipe import (
    IterDataPipe,
)
from torch.utils.data.dataset import (
    Dataset,
    IterableDataset, )
from torch.utils.data.sampler import (
    BatchSampler,
    RandomSampler,
    Sampler,
    SequentialSampler,
)

from cube_torch import CubePushDataSetInfo, get_manager
from cube_torch.cube_worker import _worker_loop, _register_pid_to_storage, \
    _unregister_pid_to_storage


class CubeDataLoader(Generic[T_co]):
    dataset: Dataset[T_co]
    batch_size: Optional[int]
    num_workers: int
    pin_memory: bool
    drop_last: bool
    timeout: float
    sampler: Union[Sampler, Iterable]
    prefetch_factor: int
    _iterator: Optional['_BaseDataLoaderIter']
    __initialized = False

    def __init__(self, dataset: Dataset[T_co], batch_size: Optional[int] = 1,
                 shuffle: Optional[bool] = None, sampler: Union[Sampler, Iterable, None] = None,
                 batch_sampler: Union[Sampler[Sequence], Iterable[Sequence], None] = None,
                 num_workers: int = 1, collate_fn: Optional[_collate_fn_t] = None,
                 pin_memory: bool = False, drop_last: bool = False,
                 timeout: float = 0, worker_init_fn: Optional[_worker_init_fn_t] = None,
                 multiprocessing_context=None, generator=None,
                 *, prefetch_factor: int = 2,
                 persistent_workers: bool = False,
                 pin_memory_device: str = ""):
        torch._C._log_api_usage_once("python.data_loader")
        if num_workers < 0:
            raise ValueError('num_workers option should be non-negative; '
                             'use num_workers=0 to disable multiprocessing.')

        self.cube_dataset_info = None
        if timeout < 0:
            raise ValueError('timeout option should be non-negative')

        assert prefetch_factor > 0

        if persistent_workers and num_workers == 0:
            raise ValueError('persistent_workers option needs num_workers > 0')

        self.prefetch_factor = prefetch_factor

        self.dataset = dataset
        self.num_workers = num_workers

        if self.num_workers == 1:
            self.prefetch_factor = 5

        self.pin_memory = pin_memory
        self.pin_memory_device = pin_memory_device
        self.timeout = timeout
        self.worker_init_fn = worker_init_fn
        self.multiprocessing_context = multiprocessing_context

        # Arg-check cube_torch related before checking samplers because we want to
        # tell users that iterable-style datasets are incompatible with custom
        # samplers first, so that they don't learn that this combo doesn't work
        # after spending time fixing the custom sampler errors.
        if isinstance(dataset, IterableDataset):
            self._dataset_kind = _DatasetKind.Iterable
            # NOTE [ Custom Samplers and IterableDataset ]
            #
            # `IterableDataset` does not support custom `batch_sampler` or
            # `sampler` since the key is irrelevant (unless we support
            # generator-style dataset one day...).
            #
            # For `sampler`, we always create a dummy sampler. This is an
            # infinite sampler even when the dataset may have an implemented
            # finite `__len__` because in multi-process data loading, naive
            # settings will return duplicated data (which may be desired), and
            # thus using a sampler with length matching that of dataset will
            # cause data lost (you may have duplicates of the first couple
            # batches, but never see anything afterwards). Therefore,
            # `Iterabledataset` always uses an infinite sampler, an instance of
            # `_InfiniteConstantSampler` defined above.
            #
            # A custom `batch_sampler` essentially only controls the batch size.
            # However, it is unclear how useful it would be since an iterable-style
            # dataset can handle that within itself. Moreover, it is pointless
            # in multi-process data loading as the assignment order of batches
            # to workers is an implementation detail so users can not control
            # how to batchify each worker's iterable. Thus, we disable this
            # option. If this turns out to be useful in future, we can re-enable
            # this, and support custom samplers that specify the assignments to
            # specific workers.
            if isinstance(dataset, IterDataPipe):
                if shuffle is not None:
                    dataset = torch.utils.data.graph_settings.apply_shuffle_settings(dataset, shuffle=shuffle)
            # We cannot check `shuffle is not None` here, since previously `shuffle=False` was the default.
            elif shuffle not in {False, None}:
                raise ValueError(
                    "DataLoader with IterableDataset: expected unspecified "
                    "shuffle option, but got shuffle={}".format(shuffle))

            if sampler is not None:
                # See NOTE [ Custom Samplers and IterableDataset ]
                raise ValueError(
                    "DataLoader with IterableDataset: expected unspecified "
                    "sampler option, but got sampler={}".format(sampler))
            elif batch_sampler is not None:
                # See NOTE [ Custom Samplers and IterableDataset ]
                raise ValueError(
                    "DataLoader with IterableDataset: expected unspecified "
                    "batch_sampler option, but got batch_sampler={}".format(batch_sampler))
        else:
            shuffle = bool(shuffle)
            self._dataset_kind = _DatasetKind.Map

        if sampler is not None and shuffle:
            raise ValueError('sampler option is mutually exclusive with '
                             'shuffle')

        if batch_sampler is not None:
            # auto_collation with custom batch_sampler
            if batch_size != 1 or shuffle or sampler is not None or drop_last:
                raise ValueError('batch_sampler option is mutually exclusive '
                                 'with batch_size, shuffle, sampler, and '
                                 'drop_last')
            batch_size = None
            drop_last = False
        elif batch_size is None:
            # no auto_collation
            if drop_last:
                raise ValueError('batch_size=None option disables auto-batching '
                                 'and is mutually exclusive with drop_last')

        if sampler is None:  # give default samplers
            if self._dataset_kind == _DatasetKind.Iterable:
                # See NOTE [ Custom Samplers and IterableDataset ]
                sampler = _InfiniteConstantSampler()
            else:  # map-style
                if shuffle:
                    sampler = RandomSampler(dataset, generator=generator)  # type: ignore[arg-type]
                else:
                    sampler = SequentialSampler(dataset)  # type: ignore[arg-type]

        if batch_size is not None and batch_sampler is None:
            # auto_collation without custom batch_sampler
            batch_sampler = BatchSampler(sampler, batch_size, drop_last)

        self.batch_size = batch_size
        self.drop_last = drop_last
        self.sampler = sampler
        self.batch_sampler = batch_sampler
        self.generator = generator

        if collate_fn is None:
            if self._auto_collation:
                collate_fn = _utils.collate.default_collate
            else:
                collate_fn = _utils.collate.default_convert

        self.collate_fn = collate_fn
        self.persistent_workers = persistent_workers

        self.__initialized = True
        self._IterableDataset_len_called = None  # See NOTE [ IterableDataset and __len__ ]

        self._iterator = None

        self.real_sample_size = len(self.dataset)
        self.check_worker_number_rationality()
        self.wait_read_train_file_queue = multiprocessing.Queue()
        self.wait_read_train_file_queue.cancel_join_thread()
        self.is_use_batch_download = False
        self._dataset_id = id(self.dataset)
        self.wait_download_queues=[]
        self.cube_dataset_info = CubePushDataSetInfo(self)
        self.is_use_batch_download = self.cube_dataset_info.is_use_batch_download()
        for i in range(self.num_workers):
            download_queue = multiprocessing.Queue()
            download_queue.cancel_join_thread()
            self.wait_download_queues.append(download_queue)

        torch.set_vital('Dataloader', 'enabled', 'True')  # type: ignore[attr-defined]

    def _get_iterator(self) -> '_BaseDataLoaderIter':
        self.check_worker_number_rationality()
        return CubeMultiProcessingDataLoaderIter(self)

    @property
    def multiprocessing_context(self):
        return self.__multiprocessing_context

    @multiprocessing_context.setter
    def multiprocessing_context(self, multiprocessing_context):
        if multiprocessing_context is not None:
            if self.num_workers > 0:
                if isinstance(multiprocessing_context, str):
                    valid_start_methods = multiprocessing.get_all_start_methods()
                    if multiprocessing_context not in valid_start_methods:
                        raise ValueError(
                            ('multiprocessing_context option '
                             'should specify a valid start method in {!r}, but got '
                             'multiprocessing_context={!r}').format(valid_start_methods, multiprocessing_context))
                    multiprocessing_context = multiprocessing.get_context(multiprocessing_context)

                if not isinstance(multiprocessing_context, python_multiprocessing.context.BaseContext):
                    raise TypeError(('multiprocessing_context option should be a valid context '
                                     'object or a string specifying the start method, but got '
                                     'multiprocessing_context={}').format(multiprocessing_context))
            else:
                raise ValueError(('multiprocessing_context can only be used with '
                                  'multi-process loading (num_workers > 0), but got '
                                  'num_workers={}').format(self.num_workers))

        self.__multiprocessing_context = multiprocessing_context

    def __setattr__(self, attr, val):
        if self.__initialized and attr in (
                'batch_size', 'batch_sampler', 'sampler', 'drop_last', 'cube_torch', 'persistent_workers'):
            raise ValueError('{} attribute should not be set after {} is '
                             'initialized'.format(attr, self.__class__.__name__))

        super(CubeDataLoader, self).__setattr__(attr, val)

    # We quote '_BaseDataLoaderIter' since it isn't defined yet and the definition can't be moved up
    # since '_BaseDataLoaderIter' references 'DataLoader'.
    def __iter__(self) -> '_BaseDataLoaderIter':
        # When using a single worker the returned iterator should be
        # created everytime to avoid reseting its state
        # However, in the case of a multiple workers iterator
        # the iterator is only created once in the lifetime of the
        # DataLoader object so that workers can be reused
        if self.persistent_workers and self.num_workers > 0:
            if self._iterator is None:
                self._iterator = self._get_iterator()
            else:
                self._iterator._reset(self)
            return self._iterator
        else:
            return self._get_iterator()

    @property
    def _auto_collation(self):
        return self.batch_sampler is not None

    @property
    def _index_sampler(self):
        # The actual sampler used for generating indices for `_DatasetFetcher`
        # (see _utils/fetch.py) to read data at each time. This would be
        # `.batch_sampler` if in auto-collation mode, and `.sampler` otherwise.
        # We can't change `.sampler` and `.batch_sampler` attributes for BC
        # reasons.
        if self._auto_collation:
            return self.batch_sampler
        else:
            return self.sampler

    def __len__(self) -> int:
        if self._dataset_kind == _DatasetKind.Iterable:
            # NOTE [ IterableDataset and __len__ ]
            #
            # For `IterableDataset`, `__len__` could be inaccurate when one naively
            # does multi-processing data loading, since the samples will be duplicated.
            # However, no real use case should be actually using that behavior, so
            # it should count as a user error. We should generally trust user
            # code to do the proper thing (e.g., configure each replica differently
            # in `__iter__`), and give us the correct `__len__` if they choose to
            # implement it (this will still throw if the dataset does not implement
            # a `__len__`).
            #
            # To provide a further warning, we track if `__len__` was called on the
            # `DataLoader`, save the returned value in `self._len_called`, and warn
            # if the iterator ends up yielding more than this number of samples.

            # Cannot statically verify that dataset is Sized
            length = self._IterableDataset_len_called = len(self.dataset)  # type: ignore[assignment, arg-type]
            if self.batch_size is not None:  # IterableDataset doesn't allow custom sampler or batch_sampler
                from math import ceil
                if self.drop_last:
                    length = length // self.batch_size
                else:
                    length = ceil(length / self.batch_size)
            return length
        else:
            return len(self._index_sampler)

    def check_worker_number_rationality(self):
        # This function check whether the dataloader's worker number is rational based on
        # current system's resource. Current rule is that if the number of workers this
        # Dataloader will create is bigger than the number of logical cpus that is allowed to
        # use, than we will pop up a warning to let user pay attention.
        #
        # eg. If current system has 2 physical CPUs with 16 cores each. And each core support 2
        #     threads, then the total logical cpus here is 2 * 16 * 2 = 64. Let's say current
        #     DataLoader process can use half of them which is 32, then the rational max number of
        #     worker that initiated from this process is 32.
        #     Now, let's say the created DataLoader has num_works = 40, which is bigger than 32.
        #     So the warning message is triggered to notify the user to lower the worker number if
        #     necessary.
        #
        #
        # [Note] Please note that this function repects `cpuset` only when os.sched_getaffinity is
        #        available (available in most of Linux system, but not OSX and Windows).
        #        When os.sched_getaffinity is not available, os.cpu_count() is called instead, but
        #        it doesn't repect cpuset.
        #        We don't take threading into account since each worker process is single threaded
        #        at this time.
        #
        #        We don't set any threading flags (eg. OMP_NUM_THREADS, MKL_NUM_THREADS, etc)
        #        other than `torch.set_num_threads` to 1 in the worker process, if the passing
        #        in functions use 3rd party modules that rely on those threading flags to determine
        #        how many thread to create (eg. numpy, etc), then it is caller's responsibility to
        #        set those flags correctly.
        def _create_warning_msg(num_worker_suggest, num_worker_created, cpuset_checked):

            suggested_max_worker_msg = ((
                "Our suggested max number of worker in current system is {}{}, which is smaller "
                "than what this DataLoader is going to create.").format(
                num_worker_suggest,
                ("" if cpuset_checked else " (`cpuset` is not taken into account)"))
            ) if num_worker_suggest is not None else (
                "DataLoader is not able to compute a suggested max number of worker in current system.")

            warn_msg = (
                "This DataLoader will create {} worker processes in total. {} "
                "Please be aware that excessive worker creation might get DataLoader running slow or even freeze, "
                "lower the worker number to avoid potential slowness/freeze if necessary.").format(
                num_worker_created,
                suggested_max_worker_msg)
            return warn_msg

        if not self.num_workers or self.num_workers == 0:
            return

        # try to compute a suggested max number of worker based on system's resource
        max_num_worker_suggest = None
        cpuset_checked = False
        if hasattr(os, 'sched_getaffinity'):
            try:
                max_num_worker_suggest = len(os.sched_getaffinity(0))
                cpuset_checked = True
            except Exception:
                pass
        if max_num_worker_suggest is None:
            # os.cpu_count() could return Optional[int]
            # get cpu count first and check None in order to satify mypy check
            cpu_count = os.cpu_count()
            if cpu_count is not None:
                max_num_worker_suggest = cpu_count

        if max_num_worker_suggest is None:
            warnings.warn(_create_warning_msg(
                max_num_worker_suggest,
                self.num_workers,
                cpuset_checked))
            return

        if self.num_workers > max_num_worker_suggest:
            warnings.warn(_create_warning_msg(
                max_num_worker_suggest,
                self.num_workers,
                cpuset_checked))


class CubeMultiProcessingDataLoaderIter(_BaseDataLoaderIter):
    def __init__(self, loader):
        self._shutdown = False
        super(CubeMultiProcessingDataLoaderIter, self).__init__(loader)

        if loader.multiprocessing_context is None:
            multiprocessing_context = multiprocessing
        else:
            multiprocessing_context = loader.multiprocessing_context
        self._sampler_iter_len = self.__len__()
        self._prefetch_factor = loader.prefetch_factor
        self._num_workers = loader.num_workers
        self._worker_init_fn = loader.worker_init_fn
        self._worker_result_queue = multiprocessing_context.Queue()  # gnore[var-annotated]
        self._worker_pids_set = False
        self._workers_done_event = multiprocessing_context.Event()
        self.cube_dataset_info = loader.cube_dataset_info
        self._index_queues = []
        self._register_pid_addr = loader.cube_dataset_info.get_register_pid_addr()
        self._unregister_pid_addr = loader.cube_dataset_info.get_unregister_pid_addr()
        self._workers = []

        self._start_loop_get_index = False
        self._storage_event = threading.Event()
        self._loadindex_event = threading.Event()
        self.is_use_batch_download = loader.is_use_batch_download
        self._wait_download_queues = loader.wait_download_queues
        self._batch_size = loader.batch_size
        self._preload_index_queue = queue.Queue(maxsize=self.cube_dataset_info.get_cube_queue_size_on_worker() * 2)
        self._loadindex_queue = queue.Queue(maxsize=self.cube_dataset_info.get_cube_queue_size_on_worker())

        self._storage_thread = threading.Thread(
            target=self._notify_storage_func, args=(self._storage_event,)
        )
        self._storage_thread.daemon = True
        self._storage_thread.start()

        self._loadindex_thread = threading.Thread(
            target=self._loadindex_func, args=(self._loadindex_event,)
        )
        self._loadindex_thread.daemon = True
        self._loadindex_thread.start()
        is_use_batch_download = self.cube_dataset_info.is_use_batch_download()
        cube_root_dir = self.cube_dataset_info.get_cubefs_root_dir()
        storage_info = None
        for i in range(self._num_workers):
            if is_use_batch_download:
                storage_info = (
                    cube_root_dir, self._wait_download_queues[i], self.cube_dataset_info.get_batch_download_addr(),
                    self._batch_size)
                print("batch download addr  info:{}".format(storage_info))
            else:
                storage_info = (
                    cube_root_dir, self._wait_download_queues[i], self.cube_dataset_info.get_cube_prefetch_addr())
                print("notify storage info:{}".format(storage_info))

            index_queue = multiprocessing_context.Queue()
            index_queue.cancel_join_thread()
            w = multiprocessing_context.Process(
                target=_worker_loop,
                args=(self._dataset_kind, self._dataset, index_queue,
                      self._worker_result_queue, self._workers_done_event,
                      self._auto_collation, self._collate_fn, self._drop_last,
                      self._base_seed, self._worker_init_fn, i, self._num_workers,
                      self._persistent_workers, is_use_batch_download, storage_info))
            w.daemon = True
            w.start()
            self._index_queues.append(index_queue)
            self._workers.append(w)

        self.regiester_worker_pid()

        if self._pin_memory:
            self._pin_memory_thread_done_event = threading.Event()

            # Queue is not type-annotated
            self._data_queue = queue.Queue()  # type: ignore[var-annotated]

            if hasattr(self, '_pin_memory_device'):
                pin_memory_thread = threading.Thread(
                    target=_utils.pin_memory._pin_memory_loop,
                    args=(self._worker_result_queue, self._data_queue,
                          torch.cuda.current_device(),
                          self._pin_memory_thread_done_event, self._pin_memory_device))
            else:
                pin_memory_thread = threading.Thread(
                    target=_utils.pin_memory._pin_memory_loop,
                    args=(self._worker_result_queue, self._data_queue,
                          torch.cuda.current_device(),
                          self._pin_memory_thread_done_event))

            pin_memory_thread.daemon = True
            pin_memory_thread.start()
            # Similar to workers (see comment above), we only register
            # pin_memory_thread once it is started.
            self._pin_memory_thread = pin_memory_thread
        else:
            self._data_queue = self._worker_result_queue

        # In some rare cases, persistent workers (daemonic processes)
        # would be terminated before `__del__` of iterator is invoked
        # when main process exits
        # It would cause failure when pin_memory_thread tries to read
        # corrupted data from worker_result_queue
        # atexit is used to shutdown thread and child processes in the
        # right sequence before main process exits
        if self._persistent_workers and self._pin_memory:
            import atexit
            for w in self._workers:
                atexit.register(CubeMultiProcessingDataLoaderIter._clean_up_worker, w)

        # .pid can be None only before process is spawned (not the case, so ignore)
        _utils.signal_handling._set_worker_pids(id(self), tuple(w.pid for w in self._workers))  # type: ignore[misc]
        _utils.signal_handling._set_SIGCHLD_handler()

        self._worker_pids_set = True
        self._reset(loader, first_iter=True)

    def regiester_worker_pid(self):
        pids = []
        for w in self._workers:
            pids.append(w.pid)
        _register_pid_to_storage(pids, self._register_pid_addr)

    def unregiester_worker_pid(self):
        pids = []
        for w in self._workers:
            pids.append(w.pid)
        _unregister_pid_to_storage(pids, self._unregister_pid_addr)

    def compute_worker_queue_idx(self):
        for _ in range(self._num_workers):  # find the next active worker, if any
            worker_queue_idx = next(self._worker_queue_idx_cycle)
            if self._workers_status[worker_queue_idx]:
                return worker_queue_idx
            else:
                return -1

    def _stop_loop_batch_download_worker(self):
        for i in range(self._num_workers):
            self._wait_download_queues[i].put(None)

    def _notify_storage_func(self, event):
        cnt = 0
        while not event.set():
            try:
                if not self._start_loop_get_index:
                    time.sleep(0.01)
                    continue
                cnt += 1
                worker_queue_idx = self.compute_worker_queue_idx()
                if worker_queue_idx == -1:
                    raise StopIteration
                index = self._next_index()
                if self.is_use_batch_download:
                    train_file_names = self.cube_dataset_info.covert_index_list_to_filename(index)
                    self._wait_download_queues[worker_queue_idx].put(train_file_names)
                else:
                    self._wait_download_queues[worker_queue_idx].put(index)
                r = (worker_queue_idx, index)
                self._preload_index_queue.put(r)
            except StopIteration:
                event.set()
                self._preload_index_queue.put(None)
                self._stop_loop_batch_download_worker()

                return

    def _loadindex_func(self, event):
        while not event.set():
            try:
                if not self._start_loop_get_index:
                    time.sleep(0.01)
                    continue
                r = self._preload_index_queue.get()
                if r is None:
                    event.set()
                    break
                self._loadindex_queue.put(r)
            except StopIteration:
                return

    def _reset(self, loader, first_iter=False):
        super()._reset(loader, first_iter)
        self._worker_queue_idx_cycle = itertools.cycle(range(self._num_workers))

        self._send_idx = 0  # idx of the next task to be sent to workers
        self._rcvd_idx = 0  # idx of the next task to be returned in __next__
        # information about data not yet yielded, i.e., tasks w/ indices in range [rcvd_idx, send_idx).
        # map: task idx => - (worker_id,)        if data isn't fetched (outstanding)
        #                  \ (worker_id, data)   if data is already fetched (out-of-order)
        self._task_info = {}
        self._tasks_outstanding = 0  # always equal to count(v for v in task_info.values() if len(v) == 1)
        self._get_data_cnt = 0
        # A list of booleans representing whether each worker still has work to
        # do, i.e., not having exhausted its iterable dataset object. It always
        # contains all `True`s if not using an iterable-style dataset
        # (i.e., if kind != Iterable).
        # Not that this indicates that a worker still has work to do *for this epoch*.
        # It does not mean that a worker is dead. In case of `_persistent_workers`,
        # the worker will be reset to available in the next epoch.
        self._workers_status = [True for i in range(self._num_workers)]
        # Reset the worker input_queue cycle so it resumes next epoch at worker 0

        # We resume the prefetching in case it was enabled
        if not first_iter:
            for idx in range(self._num_workers):
                self._index_queues[idx].put(_utils.worker._ResumeIteration())
            resume_iteration_cnt = self._num_workers
            while resume_iteration_cnt > 0:
                return_idx, return_data = self._get_data()
                if isinstance(return_idx, _utils.worker._ResumeIteration):
                    assert return_data is None
                    resume_iteration_cnt -= 1
        # prime the prefetch loop

        if not self._start_loop_get_index:
            self._start_loop_get_index = True
        for _ in range(self._prefetch_factor * self._num_workers):
            self._try_put_index()

    def _try_get_data(self, timeout=_utils.MP_STATUS_CHECK_INTERVAL):
        # Tries to fetch data from `self._data_queue` once for a given timeout.
        # This can also be used as inner loop of fetching without timeout, with
        # the sender status as the loop condition.
        #
        # This raises a `RuntimeError` if any worker died expectedly. This error
        # can come from either the SIGCHLD handler in `_utils/signal_handling.py`
        # (only for non-Windows platforms), or the manual check below on errors
        # and timeouts.
        #
        # Returns a 2-tuple:
        #   (bool: whether successfully get data, any: data if successful else None)

        try:
            data = self._data_queue.get(timeout=timeout)
            return (True, data)
        except Exception as e:
            # At timeout and error, we manually check whether any worker has
            # failed. Note that this is the only mechanism for Windows to detect
            # worker failures.
            failed_workers = []
            for worker_id, w in enumerate(self._workers):
                if self._workers_status[worker_id] and not w.is_alive():
                    failed_workers.append(w)
                    self._mark_worker_as_unavailable(worker_id)
            if len(failed_workers) > 0:
                pids_str = ', '.join(str(w.pid) for w in failed_workers)
                raise RuntimeError('DataLoader worker (pid(s) {}) exited unexpectedly'.format(pids_str, e)) from e
            if isinstance(e, queue.Empty):
                return (False, None)
            import tempfile
            import errno
            try:
                # Raise an exception if we are this close to the FDs limit.
                # Apparently, trying to open only one file is not a sufficient
                # test.
                # See NOTE [ DataLoader on Linux and open files limit ]
                fds_limit_margin = 10
                fs = [tempfile.NamedTemporaryFile() for i in range(fds_limit_margin)]
            except OSError as e:
                if e.errno == errno.EMFILE:
                    raise RuntimeError(
                        "Too many open files. Communication with the"
                        " workers is no longer possible. Please increase the"
                        " limit using `ulimit -n` in the shell or change the"
                        " sharing strategy by calling"
                        " `torch.multiprocessing.set_sharing_strategy('file_system')`"
                        " at the beginning of your code") from None
            raise

    # NOTE [ DataLoader on Linux and open files limit ]
    #
    # On Linux when DataLoader is used with multiprocessing we pass the data between
    # the root process and the workers through SHM files. We remove those files from
    # the filesystem as soon as they are created and keep them alive by
    # passing around their file descriptors through AF_UNIX sockets. (See
    # docs/source/multiprocessing.rst and 'Multiprocessing Technical Notes` in
    # the wiki (https://github.com/pytorch/pytorch/wiki).)
    #
    # This sometimes leads us to exceeding the open files limit. When that happens,
    # and the offending file descriptor is coming over a socket, the `socket` Python
    # package silently strips the file descriptor from the message, setting only the
    # `MSG_CTRUNC` flag (which might be a bit misleading since the manpage says that
    # it _indicates that some control data were discarded due to lack of space in
    # the buffer for ancillary data_). This might reflect the C implementation of
    # AF_UNIX sockets.
    #
    # This behaviour can be reproduced with the script and instructions at the
    # bottom of this note.
    #
    # When that happens, the standard Python `multiprocessing` (and not
    # `torch.multiprocessing`) raises a `RuntimeError: received 0 items of ancdata`
    #
    # Sometimes, instead of the FD being stripped, you may get an `OSError:
    # Too many open files`, both in the script below and in DataLoader. However,
    # this is rare and seems to be nondeterministic.
    #
    #
    #   #!/usr/bin/env python3
    #   import sys
    #   import socket
    #   import os
    #   import array
    #   import shutil
    #   import socket
    #
    #
    #   if len(sys.argv) != 4:
    #       print("Usage: ", sys.argv[0], " tmp_dirname iteration (send|recv)")
    #       sys.exit(1)
    #
    #   if __name__ == '__main__':
    #       dirname = sys.argv[1]
    #       sock_path = dirname + "/sock"
    #       iterations = int(sys.argv[2])
    #       def dummy_path(i):
    #           return dirname + "/" + str(i) + ".dummy"
    #
    #
    #       if sys.argv[3] == 'send':
    #           while not os.file_path.exists(sock_path):
    #               pass
    #           client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    #           client.connect(sock_path)
    #           for i in range(iterations):
    #               fd = os.open(dummy_path(i), os.O_WRONLY | os.O_CREAT)
    #               ancdata = array.array('i', [fd])
    #               msg = bytes([i % 256])
    #               print("Sending fd ", fd, " (iteration #", i, ")")
    #               client.sendmsg([msg], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, ancdata)])
    #
    #
    #       else:
    #           assert sys.argv[3] == 'recv'
    #
    #           if os.file_path.exists(dirname):
    #               raise Exception("Directory exists")
    #
    #           os.mkdir(dirname)
    #
    #           print("Opening socket...")
    #           server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    #           server.bind(sock_path)
    #
    #           print("Listening...")
    #           for i in range(iterations):
    #               a = array.array('i')
    #               msg, ancdata, flags, addr = server.recvmsg(1, socket.CMSG_SPACE(a.itemsize))
    #               assert(len(ancdata) == 1)
    #               cmsg_level, cmsg_type, cmsg_data = ancdata[0]
    #               a.frombytes(cmsg_data)
    #               print("Received fd ", a[0], " (iteration #", i, ")")
    #
    #           shutil.rmtree(dirname)
    #
    # Steps to reproduce:
    #
    # 1. Run two shells and set lower file descriptor limit in the receiving one:
    # (shell1) ulimit -n 1020
    # (shell2) ulimit -n 1022
    #
    # 2. Run the script above with the `recv` option in the first shell
    # (shell1) ./test_socket.py sock_tmp 1017 recv
    #
    # 3. Run the script with the `send` option in the second shell:
    # (shell2) ./test_socket.py sock_tmp 1017 send

    def _get_data(self):
        # Fetches data from `self._data_queue`.
        #
        # We check workers' status every `MP_STATUS_CHECK_INTERVAL` seconds,
        # which we achieve by running `self._try_get_data(timeout=MP_STATUS_CHECK_INTERVAL)`
        # in a loop. This is the only mechanism to detect worker failures for
        # Windows. For other platforms, a SIGCHLD handler is also used for
        # worker failure detection.
        #
        # If `pin_memory=True`, we also need check if `pin_memory_thread` had
        # died at timeouts.
        if self._timeout > 0:
            success, data = self._try_get_data(self._timeout)
            if success:
                return data
            else:
                raise RuntimeError('DataLoader timed out after {} seconds'.format(self._timeout))
        elif self._pin_memory:
            while self._pin_memory_thread.is_alive():
                success, data = self._try_get_data()
                if success:
                    return data
            else:
                # while condition is false, i.e., pin_memory_thread died.
                raise RuntimeError('Pin memory thread exited unexpectedly')
            # In this case, `self._data_queue` is a `input_queue.Queue`,. But we don't
            # need to call `.task_done()` because we don't use `.join()`.
        else:
            while True:
                success, data = self._try_get_data()
                if success:
                    return data

    def _next_data(self):
        while True:
            while self._rcvd_idx < self._send_idx:
                info = self._task_info[self._rcvd_idx]
                worker_id = info[0]
                if len(info) == 2 or self._workers_status[worker_id]:  # has data or is still active
                    break
                del self._task_info[self._rcvd_idx]
                self._rcvd_idx += 1
            else:
                if not self._persistent_workers:
                    self._shutdown_workers()
                raise StopIteration

            if len(self._task_info[self._rcvd_idx]) == 2:
                data = self._task_info.pop(self._rcvd_idx)[1]
                return self._process_data(data)

            assert not self._shutdown and self._tasks_outstanding > 0
            idx, data = self._get_data()
            self._tasks_outstanding -= 1
            if self._dataset_kind == _DatasetKind.Iterable:
                if isinstance(data, _utils.worker._IterableDatasetStopIteration):
                    if self._persistent_workers:
                        self._workers_status[data.worker_id] = False
                    else:
                        print("pid{} _try_get_data shutdown worker_id{},"
                              "self._dataset_kind{}".format(os.getpid(), worker_id,
                                                            self._dataset_kind == _DatasetKind.Iterable))
                        self._mark_worker_as_unavailable(data.worker_id)
                    self._try_put_index()
                    continue

            if idx != self._rcvd_idx:
                self._task_info[idx] += (data,)
            else:
                del self._task_info[idx]
                return self._process_data(data)

    def _try_put_index(self):
        assert self._tasks_outstanding < self._prefetch_factor * self._num_workers
        if self._send_idx >= self._sampler_iter_len:
            return
        r = self._loadindex_queue.get()
        if r is None:
            return
        worker_queue_idx, index = r
        self._index_queues[worker_queue_idx].put((self._send_idx, index))
        self._task_info[self._send_idx] = (worker_queue_idx,)
        self._tasks_outstanding += 1
        self._send_idx += 1

    def _process_data(self, data):
        self._rcvd_idx += 1
        self._try_put_index()
        if isinstance(data, ExceptionWrapper):
            data.reraise()
        return data

    def _mark_worker_as_unavailable(self, worker_id, shutdown=False):
        # Mark a worker as having finished its work e.g., due to
        # exhausting an `IterableDataset`. This should be used only when this
        # `_MultiProcessingDataLoaderIter` is going to continue running.

        assert self._workers_status[worker_id] or (self._persistent_workers and shutdown)

        # Signal termination to that specific worker.
        q = self._index_queues[worker_id]
        # Indicate that no more data will be put on this input_queue by the current
        # process.
        q.put(None)

        # Note that we don't actually join the worker here, nor do we remove the
        # worker's pid from C side struct because (1) joining may be slow, and
        # (2) since we don't join, the worker may still raise error, and we
        # prefer capturing those, rather than ignoring them, even though they
        # are raised after the worker has finished its job.
        # Joinning is deferred to `_shutdown_workers`, which it is called when
        # all workers finish their jobs (e.g., `IterableDataset` replicas) or
        # when this iterator is garbage collected.

        self._workers_status[worker_id] = False

        assert self._workers_done_event.is_set() == shutdown

    def _shutdown_workers(self):
        # Called when shutting down this `_MultiProcessingDataLoaderIter`.
        # See NOTE [ Data Loader Multiprocessing Shutdown Logic ] for details on
        # the logic of this function.
        python_exit_status = _utils.python_exit_status
        if python_exit_status is True or python_exit_status is None:
            # See (2) of the note. If Python is shutting down, do no-op.
            return
        # Normal exit when last reference is gone / iterator is depleted.
        # See (1) and the second half of the note.
        self.unregiester_worker_pid()
        if not self._shutdown:
            self._shutdown = True
            try:
                # Normal exit when last reference is gone / iterator is depleted.
                # See (1) and the second half of the note.

                # Exit `pin_memory_thread` first because exiting workers may leave
                # corrupted data in `worker_result_queue` which `pin_memory_thread`
                # reads from.
                if hasattr(self, '_pin_memory_thread'):
                    # Use hasattr in case error happens before we set the attribute.
                    self._pin_memory_thread_done_event.set()
                    # Send something to pin_memory_thread in case it is waiting
                    # so that it can wake up and check `pin_memory_thread_done_event`
                    self._worker_result_queue.put((None, None))
                    self._pin_memory_thread.join()
                    self._worker_result_queue.cancel_join_thread()
                    self._worker_result_queue.close()

                self._storage_event.set()
                self._loadindex_event.set()
                self._storage_thread.join()
                self._loadindex_thread.join()
                self._workers_done_event.set()

                for worker_id in range(len(self._workers)):
                    # Get number of workers from `len(self._workers)` instead of
                    # `self._num_workers` in case we error before starting all
                    # workers.
                    # If we are using workers_status with persistent_workers
                    # we have to shut it down because the worker is paused
                    if self._persistent_workers or self._workers_status[worker_id]:
                        self._mark_worker_as_unavailable(worker_id, shutdown=True)
                for w in self._workers:
                    # We should be able to join here, but in case anything went
                    # wrong, we set a timeout and if the workers fail to join,
                    # they are killed in the `finally` block.
                    w.join(timeout=_utils.MP_STATUS_CHECK_INTERVAL)

                for q in self._index_queues:
                    q.cancel_join_thread()
                    q.close()


            finally:
                # Even though all this function does is putting into batch_download_notify_queues that
                # we have called `cancel_join_thread` on, weird things can
                # happen when a worker is killed by a signal, e.g., hanging in
                # `Event.set()`. So we need to guard this with SIGCHLD handler,
                # and remove pids from the C side data structure only at the
                # end.
                #
                # FIXME: Unfortunately, for Windows, we are missing a worker
                #        error detection mechanism here in this function, as it
                #        doesn't provide a SIGCHLD handler.
                if self._worker_pids_set:
                    _utils.signal_handling._remove_worker_pids(id(self))
                    self._worker_pids_set = False
                for w in self._workers:
                    if w.is_alive():
                        # Existing mechanisms try to make the workers exit
                        # peacefully, but in case that we unfortunately reach
                        # here, which we shouldn't, (e.g., pytorch/pytorch#39570),
                        # we kill the worker.
                        w.terminate()

    # staticmethod is used to remove reference to `_MultiProcessingDataLoaderIter`
    @staticmethod
    def _clean_up_worker(w):
        try:
            w.join(timeout=_utils.MP_STATUS_CHECK_INTERVAL)
        finally:
            if w.is_alive():
                w.terminate()

    def __del__(self):
        self._shutdown_workers()
