# blobstore bench 工具使用

bench 工具用于评估 blobstore 的性能。支持 put、get、del 性能评估。

## 帮助

```text
# build/bin/blobstore/blobstore-bench  -h

USAGE: build/bin/blobstore/blobstore-bench [OPTIONS]

OPTIONS:
  -b string
        Storage backend. 'blobstore' or 'dummy' (default "blobstore")
  -c string
        Conf file of blobstore
  -d int
        Maximum test duration in seconds <-1 for unlimited> (default 60)
  -db string
        Database mode. 'rocksdb' or 'memory'
  -e int
        Maximum number of errors allowed <-1 for unlimited> (default 3)
  -l int
        Number of times to repeat bench test (default 1)
  -m string
        Run modes in order.  See NOTES for more info (default "pgd")
  -n int
        Maximum number of objects <-1 for unlimited> (default -1)
  -pr string
        Specifies the name of the test run, which also serves as a prefix for generated object names
  -r string
        RocksDB direcotry. Only for blobstore backend
  -s string
        Size of objects in bytes with postfix Ki, Mi, and Gi (default "1Mi")
  -t int
        Number of threads to run (default 1)

NOTES:
  - Valid mode types for the -m mode string are:
    p: put objects in blobstore
    g: get objects from blobstore
    d: delete objects from blobstore
```

## 示例

### 测试 blobstore 的性能

准备配置文件 blob.conf：

```json
{
        "idc": "z0",
        "code_mode_put_quorums": {
                "11": 4
        },
        "code_mode_get_ordered": {
                "11": true
        },
        "cluster_config": {
                "clusters": [{"cluster_id": 1, "hosts": ["http://127.0.0.1:9998"]}]
        }
}
```

执行测试:

```text
# build/bin/blobstore/blobstore-bench -b blobstore -c blob.conf -db rocksdb -r build/db/ -d 5 -m pgd -pr mybench -s 10Ki -t 2 -l 2
2025/07/10 10:45:23 DataSize=10240 MD5=122506ab6d703619d577a93dd6966e80
2025/07/10 10:45:23 BlobStore Benchmark
2025/07/10 10:45:23 Parameters:
2025/07/10 10:45:23 backend=blobstore
2025/07/10 10:45:23 database=rocksdb
2025/07/10 10:45:23 conf=blob.conf
2025/07/10 10:45:23 runName=mybench
2025/07/10 10:45:23 poolName=
2025/07/10 10:45:23 objSize=10Ki
2025/07/10 10:45:23 maxObjCnt=-1
2025/07/10 10:45:23 durationSecs=5
2025/07/10 10:45:23 threads=2
2025/07/10 10:45:23 loops=2
2025/07/10 10:45:23 interval=1.000000
2025/07/10 10:45:23 dbDir=build/db/
2025/07/10 10:45:23 Running Loop 0 Put Test
2025/07/10 10:45:24 Loop: 0, Int: 0, Dur(s): 1.0, Mode: PUT, Ops: 677, MB/s: 6.61, IO/s: 677, Lat(ms): [ min: 1.9, avg: 2.7, 99%: 5.3, max: 27.7 ], Slowdowns: 0
2025/07/10 10:45:25 Loop: 0, Int: 1, Dur(s): 1.0, Mode: PUT, Ops: 482, MB/s: 4.71, IO/s: 482, Lat(ms): [ min: 1.9, avg: 3.8, 99%: 12.2, max: 16.3 ], Slowdowns: 0
2025/07/10 10:45:26 Loop: 0, Int: 2, Dur(s): 1.0, Mode: PUT, Ops: 629, MB/s: 6.14, IO/s: 629, Lat(ms): [ min: 1.7, avg: 2.9, 99%: 11.2, max: 15.5 ], Slowdowns: 0
2025/07/10 10:45:27 Loop: 0, Int: 3, Dur(s): 1.0, Mode: PUT, Ops: 504, MB/s: 4.92, IO/s: 504, Lat(ms): [ min: 1.9, avg: 3.7, 99%: 6.8, max: 10.7 ], Slowdowns: 0
2025/07/10 10:45:28 Loop: 0, Int: 4, Dur(s): 1.0, Mode: PUT, Ops: 531, MB/s: 5.19, IO/s: 531, Lat(ms): [ min: 1.7, avg: 3.5, 99%: 13.3, max: 36.8 ], Slowdowns: 0
2025/07/10 10:45:28 Loop: 0, Int: TOTAL, Dur(s): 5.0, Mode: PUT, Ops: 2825, MB/s: 5.49, IO/s: 562, Lat(ms): [ min: 1.7, avg: 3.3, 99%: 11.1, max: 36.8 ], Slowdowns: 0
2025/07/10 10:45:28 The subsequent '-m g' or '-m d' test should be specified with '-n 2825'
2025/07/10 10:45:28 Running Loop 0 Get Test
2025/07/10 10:45:29 Loop: 0, Int: 0, Dur(s): 1.0, Mode: GET, Ops: 1286, MB/s: 12.56, IO/s: 1286, Lat(ms): [ min: 0.9, avg: 1.5, 99%: 4.1, max: 7.8 ], Slowdowns: 0
2025/07/10 10:45:30 Loop: 0, Int: 1, Dur(s): 1.0, Mode: GET, Ops: 1339, MB/s: 13.08, IO/s: 1339, Lat(ms): [ min: 0.7, avg: 1.5, 99%: 3.9, max: 11.2 ], Slowdowns: 0
2025/07/10 10:45:30 Loop: 0, Int: TOTAL, Dur(s): 2.1, Mode: GET, Ops: 2825, MB/s: 12.91, IO/s: 1322, Lat(ms): [ min: 0.7, avg: 1.5, 99%: 4.0, max: 11.2 ], Slowdowns: 0
2025/07/10 10:45:30 Running Loop 0 Del Test
2025/07/10 10:45:31 Loop: 0, Int: 0, Dur(s): 1.0, Mode: DEL, Ops: 2215, MB/s: 21.63, IO/s: 2215, Lat(ms): [ min: 0.4, avg: 0.8, 99%: 1.9, max: 26.3 ], Slowdowns: 0
2025/07/10 10:45:31 Loop: 0, Int: TOTAL, Dur(s): 1.2, Mode: DEL, Ops: 2825, MB/s: 22.20, IO/s: 2273, Lat(ms): [ min: 0.4, avg: 0.7, 99%: 1.9, max: 26.3 ], Slowdowns: 0
2025/07/10 10:45:31 Running Loop 1 Put Test
2025/07/10 10:45:32 Loop: 1, Int: 0, Dur(s): 1.0, Mode: PUT, Ops: 720, MB/s: 7.03, IO/s: 720, Lat(ms): [ min: 1.8, avg: 2.5, 99%: 3.9, max: 5.0 ], Slowdowns: 0
2025/07/10 10:45:33 Loop: 1, Int: 1, Dur(s): 1.0, Mode: PUT, Ops: 603, MB/s: 5.89, IO/s: 603, Lat(ms): [ min: 1.8, avg: 3.0, 99%: 9.5, max: 27.8 ], Slowdowns: 0
2025/07/10 10:45:34 Loop: 1, Int: 2, Dur(s): 1.0, Mode: PUT, Ops: 728, MB/s: 7.11, IO/s: 728, Lat(ms): [ min: -3.2, avg: 2.5, 99%: 4.3, max: 5.5 ], Slowdowns: 0
2025/07/10 10:45:35 Loop: 1, Int: 3, Dur(s): 1.0, Mode: PUT, Ops: 703, MB/s: 6.87, IO/s: 703, Lat(ms): [ min: 1.7, avg: 2.6, 99%: 5.1, max: 9.8 ], Slowdowns: 0
2025/07/10 10:45:36 Loop: 1, Int: TOTAL, Dur(s): 5.0, Mode: PUT, Ops: 3485, MB/s: 6.81, IO/s: 698, Lat(ms): [ min: -3.2, avg: 2.6, 99%: 5.8, max: 27.8 ], Slowdowns: 0
2025/07/10 10:45:36 The subsequent '-m g' or '-m d' test should be specified with '-n 3485'
2025/07/10 10:45:36 Running Loop 1 Get Test
2025/07/10 10:45:37 Loop: 1, Int: 0, Dur(s): 1.0, Mode: GET, Ops: 1332, MB/s: 13.01, IO/s: 1332, Lat(ms): [ min: 0.8, avg: 1.5, 99%: 4.0, max: 13.7 ], Slowdowns: 0
2025/07/10 10:45:38 Loop: 1, Int: 1, Dur(s): 1.0, Mode: GET, Ops: 1220, MB/s: 11.91, IO/s: 1220, Lat(ms): [ min: 0.7, avg: 1.6, 99%: 7.4, max: 15.9 ], Slowdowns: 0
2025/07/10 10:45:39 Loop: 1, Int: TOTAL, Dur(s): 2.7, Mode: GET, Ops: 3485, MB/s: 12.77, IO/s: 1307, Lat(ms): [ min: 0.7, avg: 1.5, 99%: 5.1, max: 15.9 ], Slowdowns: 0
2025/07/10 10:45:39 Running Loop 1 Del Test
2025/07/10 10:45:40 Loop: 1, Int: 0, Dur(s): 1.0, Mode: DEL, Ops: 2805, MB/s: 27.39, IO/s: 2805, Lat(ms): [ min: 0.4, avg: 0.6, 99%: 1.4, max: 8.8 ], Slowdowns: 0
2025/07/10 10:45:40 Loop: 1, Int: TOTAL, Dur(s): 1.2, Mode: DEL, Ops: 3485, MB/s: 27.49, IO/s: 2815, Lat(ms): [ min: 0.4, avg: 0.6, 99%: 1.3, max: 8.8 ], Slowdowns: 0
```

### 测试自身开销

* 只是为了评估工具自身的开销

```text
# build/bin/blobstore/blobstore-bench -b dummy -d 3 -m pgd  -pr dummytest -s 10Ki -t 2 -l 2
2025/07/10 10:48:04 DataSize=10240 MD5=7d3112f86cdbcb45694735dd53928d94
2025/07/10 10:48:04 BlobStore Benchmark
2025/07/10 10:48:04 Parameters:
2025/07/10 10:48:04 backend=dummy
2025/07/10 10:48:04 database=
2025/07/10 10:48:04 conf=
2025/07/10 10:48:04 runName=dummytest
2025/07/10 10:48:04 poolName=
2025/07/10 10:48:04 objSize=10Ki
2025/07/10 10:48:04 maxObjCnt=-1
2025/07/10 10:48:04 durationSecs=3
2025/07/10 10:48:04 threads=2
2025/07/10 10:48:04 loops=2
2025/07/10 10:48:04 interval=1.000000
2025/07/10 10:48:04 dbDir=
2025/07/10 10:48:04 Running Loop 0 Put Test
2025/07/10 10:48:06 Loop: 0, Int: 0, Dur(s): 1.0, Mode: PUT, Ops: 4626891, MB/s: 45184.48, IO/s: 4626891, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.2 ], Slowdowns: 0
2025/07/10 10:48:06 Loop: 0, Int: 1, Dur(s): 1.0, Mode: PUT, Ops: 1309567, MB/s: 12788.74, IO/s: 1309567, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.0 ], Slowdowns: 0
2025/07/10 10:48:08 Loop: 0, Int: TOTAL, Dur(s): 3.0, Mode: PUT, Ops: 10025255, MB/s: 32141.80, IO/s: 3291320, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.8 ], Slowdowns: 0
2025/07/10 10:48:08 The subsequent '-m g' or '-m d' test should be specified with '-n 10025255'
2025/07/10 10:48:08 Running Loop 0 Get Test
2025/07/10 10:48:09 Loop: 0, Int: 0, Dur(s): 1.0, Mode: GET, Ops: 5868435, MB/s: 57308.94, IO/s: 5868435, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.1 ], Slowdowns: 0
2025/07/10 10:48:10 Loop: 0, Int: TOTAL, Dur(s): 1.8, Mode: GET, Ops: 10025255, MB/s: 55878.86, IO/s: 5721996, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.1 ], Slowdowns: 0
2025/07/10 10:48:10 Running Loop 0 Del Test
2025/07/10 10:48:12 Loop: 0, Int: 0, Dur(s): 1.0, Mode: DEL, Ops: 2955369, MB/s: 28861.03, IO/s: 2955369, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.1 ], Slowdowns: 0
2025/07/10 10:48:12 Loop: 0, Int: 1, Dur(s): 1.0, Mode: DEL, Ops: 2766585, MB/s: 27017.43, IO/s: 2766585, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 1.4 ], Slowdowns: 0
2025/07/10 10:48:14 Loop: 0, Int: TOTAL, Dur(s): 2.8, Mode: DEL, Ops: 10025255, MB/s: 34423.86, IO/s: 3525003, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 1.4 ], Slowdowns: 0
2025/07/10 10:48:14 Running Loop 1 Put Test
2025/07/10 10:48:15 Loop: 1, Int: 0, Dur(s): 1.0, Mode: PUT, Ops: 5314085, MB/s: 51895.36, IO/s: 5314085, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.2 ], Slowdowns: 0
2025/07/10 10:48:16 Loop: 1, Int: 1, Dur(s): 1.0, Mode: PUT, Ops: 5246882, MB/s: 51239.08, IO/s: 5246882, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.1 ], Slowdowns: 0
2025/07/10 10:48:17 Loop: 1, Int: TOTAL, Dur(s): 3.0, Mode: PUT, Ops: 15934614, MB/s: 51870.53, IO/s: 5311542, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.2 ], Slowdowns: 0
2025/07/10 10:48:17 The subsequent '-m g' or '-m d' test should be specified with '-n 15934614'
2025/07/10 10:48:17 Running Loop 1 Get Test
2025/07/10 10:48:18 Loop: 1, Int: 0, Dur(s): 1.0, Mode: GET, Ops: 5406327, MB/s: 52796.16, IO/s: 5406327, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.4 ], Slowdowns: 0
2025/07/10 10:48:19 Loop: 1, Int: 1, Dur(s): 1.0, Mode: GET, Ops: 4264340, MB/s: 41643.95, IO/s: 4264340, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.1 ], Slowdowns: 0
2025/07/10 10:48:21 Loop: 1, Int: TOTAL, Dur(s): 3.0, Mode: GET, Ops: 15303558, MB/s: 49816.30, IO/s: 5101189, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.4 ], Slowdowns: 0
2025/07/10 10:48:21 Running Loop 1 Del Test
2025/07/10 10:48:22 Loop: 1, Int: 0, Dur(s): 1.0, Mode: DEL, Ops: 5842697, MB/s: 57057.59, IO/s: 5842697, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.4 ], Slowdowns: 0
2025/07/10 10:48:23 Loop: 1, Int: 1, Dur(s): 1.0, Mode: DEL, Ops: 4858864, MB/s: 47449.84, IO/s: 4858864, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 1.0 ], Slowdowns: 0
2025/07/10 10:48:24 Loop: 1, Int: TOTAL, Dur(s): 3.0, Mode: DEL, Ops: 15723475, MB/s: 51183.42, IO/s: 5241183, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 1.0 ], Slowdowns: 0
```

## 其他

* `-m`：可以指定单个操作，也可以指定多个操作的组合，比如 `p`、`g`、`d`、`pg`、`pd`、`pggg`，但是一旦执行了 `d` 数据就会被删除
* `-n`：对于 `d/del` 操作是必须要要指定的，为了确定对象的最大编号，用于确定操作停止的边界
