import os


class CachedOpenInterceptor:
    _instance = None
    cube_cache_dir = "/tmp"  # 缓存目录
    cube_root_dir = "/tmp"  # 数据目录
    _last_cycle_hit_count = 0  # 命中次数
    _last_cycle_miss_count = 0  # 未命中次数
    built_in_open = open
    is_use_cache = False
    total_count = 0
    total_hit_count = 0
    total_miss_count = 0

    @classmethod
    def set_params(self, cubefs_root_dir, cube_cache_dir, is_use_cache):
        CachedOpenInterceptor.cube_cache_dir = cube_cache_dir
        CachedOpenInterceptor.cube_root_dir = cubefs_root_dir
        CachedOpenInterceptor.is_use_cache = is_use_cache

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass

    @staticmethod
    def open(file, mode='r', buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        if not CachedOpenInterceptor.is_use_cache:
            return CachedOpenInterceptor.built_in_open(file, mode, buffering, encoding, errors, newline,
                                                       closefd, opener)
        if type(file) is str and file.startswith(CachedOpenInterceptor.cube_root_dir):
            cache_file = "{}/{}".format(CachedOpenInterceptor.cube_cache_dir, file)
            try:
                fd = CachedOpenInterceptor.built_in_open(cache_file, mode, buffering, encoding, errors, newline,
                                                         closefd, opener)
                CachedOpenInterceptor._last_cycle_hit_count += 1
                return fd
            except Exception as e:
                pass
            CachedOpenInterceptor._last_cycle_miss_count += 1
        return CachedOpenInterceptor.built_in_open(file, mode, buffering, encoding, errors, newline, closefd, opener)

    @staticmethod
    def print_hit_rate():
        request_count = CachedOpenInterceptor._last_cycle_hit_count + CachedOpenInterceptor._last_cycle_miss_count
        if request_count == 0:
            request_count = 1

        CachedOpenInterceptor.total_count += request_count
        CachedOpenInterceptor.total_hit_count += CachedOpenInterceptor._last_cycle_hit_count
        CachedOpenInterceptor.total_miss_count += CachedOpenInterceptor._last_cycle_miss_count

        last_cycle_hit_rate = (CachedOpenInterceptor._last_cycle_hit_count / request_count) * 100
        last_cycle_miss_rate = (CachedOpenInterceptor._last_cycle_miss_count / request_count) * 100
        total_hit_rate = (CachedOpenInterceptor.total_hit_count / CachedOpenInterceptor.total_count) * 100
        total_miss_rate = (CachedOpenInterceptor.total_miss_count / CachedOpenInterceptor.total_count) * 100
        print_mesg = "pid:{} cube_cache_dir:{} last_cycle_metrics:([request_count:{} hit_count:{} miss_count:{} " \
                     "hit_rate:{:.2f}% miss_rate:{:.2f}% ])  sum_metrics:([request_count:{} hit_count:{} " \
                     "miss_count:{} hit_rate:{:.2f}% miss_rate:{:.2f}%]) " \
                     "".format(os.getpid(), CachedOpenInterceptor.cube_cache_dir, request_count,
                               CachedOpenInterceptor._last_cycle_hit_count,
                               CachedOpenInterceptor._last_cycle_miss_count,
                               last_cycle_hit_rate, last_cycle_miss_rate,
                               CachedOpenInterceptor.total_count,
                               CachedOpenInterceptor.total_hit_count,
                               CachedOpenInterceptor.total_miss_count,
                               total_hit_rate, total_miss_rate)

        print(print_mesg)

        CachedOpenInterceptor._last_cycle_hit_count = 0
        CachedOpenInterceptor._last_cycle_miss_count = 0
