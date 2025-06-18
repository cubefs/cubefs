# bench 工具使用

bench 工具用于评估 blobstore 的性能。支持 put、get、del 性能评估。

## 帮助

```text
root@cubefs:~/r.new# build/bin/bench -h

USAGE: build/bin/bench [OPTIONS]

OPTIONS:
  -b string
    Storage backend. 'blobstore', 'dummy'. (default "blobstore")
  -c string
    Conf file of blobstore
  -d int
    Maximum test duration in seconds <-1 for unlimited> (default 60)
  -e int
    Maximum number of errors allowed <-1 for unlimited> (default 3)
  -m string
    Run modes in order.  See NOTES for more info (default "pgd")
  -n int
    Maximum number of objects <-1 for unlimited> (default -1)
  -pr string
    Specifies the name of the test run, which also serves as a prefix for generated object names.
  -r string
    RocksDB direcotry. Only for blobstore backend
  -s string
    Size of objects in bytes with postfix K, M, and G (default "1M")
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
        "log_level": 4,
        "idc": "z0",
        "code_mode_put_quorums": {
                "11": 4
        },
        "cluster_config": {
                "clusters": [{"cluster_id": 1, "hosts": ["http://127.0.0.1:9998"]}]
        }
}
```

执行测试:

```text
root@cubefs:~/r.new# build/bin/bench -b blobstore -c blob.conf -r build/db/ -d 5 -m pgd -pr mybench -s 10K -t 2
2025/04/11 17:04:54 data MD5=TWEY31QJNqiaW+UuP6TjgA==
2025/04/11 17:04:54 BlobStore Benchmark
2025/04/11 17:04:54 Parameters:
2025/04/11 17:04:54 backend=blobstore
2025/04/11 17:04:54 conf=blob.conf
2025/04/11 17:04:54 runName=mybench
2025/04/11 17:04:54 objSize=10K
2025/04/11 17:04:54 maxObjCnt=-1
2025/04/11 17:04:54 durationSecs=5
2025/04/11 17:04:54 threads=2
2025/04/11 17:04:54 interval=1.000000
2025/04/11 17:04:54 dbDir=build/db/
2025/04/11 17:04:54 Running Loop 1 Put Test
2025/04/11 17:04:55 Loop: 1, Int: 0, Dur(s): 1.0, Mode: PUT, Ops: 460, MB/s: 4.49, IO/s: 460, Lat(ms): [ min: -1.1, avg: 3.8, 99%: 9.1, max: 319.0 ], Slowdowns: 0
2025/04/11 17:04:56 Loop: 1, Int: 1, Dur(s): 1.0, Mode: PUT, Ops: 499, MB/s: 4.87, IO/s: 499, Lat(ms): [ min: 0.0, avg: 3.5, 99%: 16.1, max: 31.1 ], Slowdowns: 0
2025/04/11 17:04:57 Loop: 1, Int: 2, Dur(s): 1.0, Mode: PUT, Ops: 497, MB/s: 4.85, IO/s: 497, Lat(ms): [ min: 1.9, avg: 3.5, 99%: 17.8, max: 39.9 ], Slowdowns: 0
2025/04/11 17:04:58 Loop: 1, Int: 3, Dur(s): 1.0, Mode: PUT, Ops: 599, MB/s: 5.85, IO/s: 599, Lat(ms): [ min: 0.6, avg: 2.8, 99%: 4.4, max: 6.4 ], Slowdowns: 0
2025/04/11 17:04:59 Loop: 1, Int: 4, Dur(s): 1.0, Mode: PUT, Ops: 602, MB/s: 5.88, IO/s: 602, Lat(ms): [ min: -1.0, avg: 2.8, 99%: 4.2, max: 7.8 ], Slowdowns: 0
2025/04/11 17:04:59 Loop: 1, Int: TOTAL, Dur(s): 5.0, Mode: PUT, Ops: 2659, MB/s: 5.19, IO/s: 532, Lat(ms): [ min: -1.1, avg: 3.2, 99%: 12.1, max: 319.0 ], Slowdowns: 0
2025/04/11 17:04:59 The subsequent '-m g' or '-m d' test should be specified with '-n 2659'
2025/04/11 17:04:59 Running Loop 1 Get Test
2025/04/11 17:05:00 Loop: 1, Int: 0, Dur(s): 1.0, Mode: GET, Ops: 1404, MB/s: 13.71, IO/s: 1404, Lat(ms): [ min: 0.8, avg: 1.4, 99%: 2.6, max: 5.7 ], Slowdowns: 0
2025/04/11 17:05:01 Loop: 1, Int: TOTAL, Dur(s): 1.9, Mode: GET, Ops: 2659, MB/s: 13.35, IO/s: 1367, Lat(ms): [ min: 0.7, avg: 1.5, 99%: 2.7, max: 6.0 ], Slowdowns: 0
2025/04/11 17:05:01 Running Loop 1 Del Test
2025/04/11 17:05:02 Loop: 1, Int: 0, Dur(s): 1.0, Mode: DEL, Ops: 1590, MB/s: 15.53, IO/s: 1590, Lat(ms): [ min: 0.5, avg: 1.1, 99%: 2.7, max: 186.2 ], Slowdowns: 0
2025/04/11 17:05:02 Loop: 1, Int: TOTAL, Dur(s): 1.4, Mode: DEL, Ops: 2659, MB/s: 18.77, IO/s: 1922, Lat(ms): [ min: 0.4, avg: 0.9, 99%: 2.3, max: 186.2 ], Slowdowns: 0
```

### 测试 bench 自身开销

* 只是为了评估 bench 工具自身的开销

```text
root@cubefs:~/r.4.14# build/bin/bench -b dummy -d 3 -m pgd  -pr dummytest -s 10K -t 2
2025/04/14 11:31:38 BlobStore Benchmark
2025/04/14 11:31:38 Parameters:
2025/04/14 11:31:38 backend=dummy
2025/04/14 11:31:38 conf=
2025/04/14 11:31:38 runName=dummytest
2025/04/14 11:31:38 objSize=10K
2025/04/14 11:31:38 maxObjCnt=-1
2025/04/14 11:31:38 durationSecs=3
2025/04/14 11:31:38 threads=2
2025/04/14 11:31:38 interval=1.000000
2025/04/14 11:31:38 dbDir=
2025/04/14 11:31:38 Running Loop 1 Put Test
2025/04/14 11:31:39 Loop: 1, Int: 0, Dur(s): 1.0, Mode: PUT, Ops: 4902175, MB/s: 47872.80, IO/s: 4902175, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.2 ], Slowdowns: 0
2025/04/14 11:31:40 Loop: 1, Int: 1, Dur(s): 1.0, Mode: PUT, Ops: 5054202, MB/s: 49357.44, IO/s: 5054202, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.1 ], Slowdowns: 0
2025/04/14 11:31:41 Loop: 1, Int: TOTAL, Dur(s): 3.0, Mode: PUT, Ops: 14831151, MB/s: 48278.54, IO/s: 4943722, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.3 ], Slowdowns: 0
2025/04/14 11:31:41 The subsequent '-m g' or '-m d' test should be specified with '-n 14831151'
2025/04/14 11:31:41 Running Loop 1 Get Test
2025/04/14 11:31:42 Loop: 1, Int: 0, Dur(s): 1.0, Mode: GET, Ops: 4916081, MB/s: 48008.60, IO/s: 4916081, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.5 ], Slowdowns: 0
2025/04/14 11:31:43 Loop: 1, Int: 1, Dur(s): 1.0, Mode: GET, Ops: 4977585, MB/s: 48609.23, IO/s: 4977585, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.3 ], Slowdowns: 0
2025/04/14 11:31:44 Loop: 1, Int: TOTAL, Dur(s): 3.0, Mode: GET, Ops: 14831151, MB/s: 48772.07, IO/s: 4994260, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.5 ], Slowdowns: 0
2025/04/14 11:31:44 Running Loop 1 Del Test
2025/04/14 11:31:45 Loop: 1, Int: 0, Dur(s): 1.0, Mode: DEL, Ops: 5243309, MB/s: 51204.19, IO/s: 5243309, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.4 ], Slowdowns: 0
2025/04/14 11:31:46 Loop: 1, Int: 1, Dur(s): 1.0, Mode: DEL, Ops: 5112014, MB/s: 49922.01, IO/s: 5112014, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.2 ], Slowdowns: 0
2025/04/14 11:31:47 Loop: 1, Int: TOTAL, Dur(s): 2.9, Mode: DEL, Ops: 14831151, MB/s: 50127.62, IO/s: 5133069, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.4 ], Slowdowns: 0
```

## 其他

* `-m`：可以指定单个操作，也可以指定多个操作的组合，比如 `p`、`g`、`d`、`pg`、`pd`、`pggg`，但是一旦执行了 `d` 数据就会被删除
* `-n`：对于 `d/del` 操作是必须要要指定的，为了确定对象的最大编号，用于确定操作停止的边界
