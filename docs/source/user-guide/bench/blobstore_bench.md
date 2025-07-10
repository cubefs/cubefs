# Using blobstore bench Tool

The blobstore-bench is used to evaluate the performance of blobstore. It supports performance assessments for put, get, and del operations.

## Usage

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

## Examples

### Testing Blobstore Performance

Prepare the configuration file blob.conf:

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

Run the test:

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
2025/07/12 21:28:47 Running Loop 0 Put Test
2025/07/12 21:28:48 Loop: 0, Int: 0, Dur(s): 1.0, Mode: PUT, Ops: 547, MB/s: 5.22, IO/s: 547, Lat(ms): [ min: 1.6, avg: 3.4, 99%: 5.6, max: 317.2 ], Slowdowns: 0
2025/07/12 21:28:49 Loop: 0, Int: 1, Dur(s): 1.0, Mode: PUT, Ops: 704, MB/s: 6.71, IO/s: 704, Lat(ms): [ min: 1.8, avg: 2.6, 99%: 6.3, max: 10.2 ], Slowdowns: 0
2025/07/12 21:28:50 Loop: 0, Int: 2, Dur(s): 1.0, Mode: PUT, Ops: 626, MB/s: 5.97, IO/s: 626, Lat(ms): [ min: 1.8, avg: 2.9, 99%: 11.9, max: 19.7 ], Slowdowns: 0
2025/07/12 21:28:51 Loop: 0, Int: 3, Dur(s): 1.0, Mode: PUT, Ops: 681, MB/s: 6.49, IO/s: 681, Lat(ms): [ min: 1.8, avg: 2.7, 99%: 5.4, max: 12.0 ], Slowdowns: 0
2025/07/12 21:28:52 Loop: 0, Int: 4, Dur(s): 1.0, Mode: PUT, Ops: 524, MB/s: 5.00, IO/s: 524, Lat(ms): [ min: 1.9, avg: 3.5, 99%: 8.1, max: 40.8 ], Slowdowns: 0
2025/07/12 21:28:52 Loop: 0, Int: TOTAL, Dur(s): 5.0, Mode: PUT, Ops: 3084, MB/s: 5.88, IO/s: 617, Lat(ms): [ min: 1.6, avg: 3.0, 99%: 7.8, max: 317.2 ], Slowdowns: 0
2025/07/12 21:28:52 The subsequent '-m g' or '-m d' test should be specified with '-n 3084'
2025/07/12 21:28:52 Running Loop 0 Get Test
2025/07/12 21:28:53 Loop: 0, Int: 0, Dur(s): 1.0, Mode: GET, Ops: 1355, MB/s: 12.92, IO/s: 1355, Lat(ms): [ min: 1.0, avg: 1.5, 99%: 2.9, max: 23.5 ], Slowdowns: 0
2025/07/12 21:28:54 Loop: 0, Int: 1, Dur(s): 1.0, Mode: GET, Ops: 1344, MB/s: 12.82, IO/s: 1344, Lat(ms): [ min: 0.6, avg: 1.5, 99%: 3.3, max: 22.6 ], Slowdowns: 0
2025/07/12 21:28:54 Loop: 0, Int: TOTAL, Dur(s): 2.3, Mode: GET, Ops: 3084, MB/s: 12.94, IO/s: 1357, Lat(ms): [ min: 0.6, avg: 1.5, 99%: 3.1, max: 23.5 ], Slowdowns: 0
2025/07/12 21:28:54 Running Loop 0 Del Test
2025/07/12 21:28:55 Loop: 0, Int: 0, Dur(s): 1.0, Mode: DEL, Ops: 2428, MB/s: 23.16, IO/s: 2428, Lat(ms): [ min: 0.4, avg: 0.7, 99%: 1.2, max: 118.8 ], Slowdowns: 0
2025/07/12 21:28:55 Loop: 0, Int: TOTAL, Dur(s): 1.3, Mode: DEL, Ops: 3084, MB/s: 22.98, IO/s: 2409, Lat(ms): [ min: 0.4, avg: 0.7, 99%: 1.2, max: 118.8 ], Slowdowns: 0
2025/07/12 21:28:55 Running Loop 1 Put Test
2025/07/12 21:28:56 Loop: 1, Int: 0, Dur(s): 1.0, Mode: PUT, Ops: 755, MB/s: 7.20, IO/s: 755, Lat(ms): [ min: 1.7, avg: 2.4, 99%: 4.3, max: 9.8 ], Slowdowns: 0
2025/07/12 21:28:57 Loop: 1, Int: 1, Dur(s): 1.0, Mode: PUT, Ops: 763, MB/s: 7.28, IO/s: 763, Lat(ms): [ min: 1.6, avg: 2.4, 99%: 4.1, max: 9.9 ], Slowdowns: 0
2025/07/12 21:28:58 Loop: 1, Int: 2, Dur(s): 1.0, Mode: PUT, Ops: 707, MB/s: 6.74, IO/s: 707, Lat(ms): [ min: 1.8, avg: 2.6, 99%: 4.4, max: 7.1 ], Slowdowns: 0
2025/07/12 21:28:59 Loop: 1, Int: 3, Dur(s): 1.0, Mode: PUT, Ops: 739, MB/s: 7.05, IO/s: 739, Lat(ms): [ min: 1.6, avg: 2.5, 99%: 4.2, max: 7.8 ], Slowdowns: 0
2025/07/12 21:29:00 Loop: 1, Int: 4, Dur(s): 1.0, Mode: PUT, Ops: 740, MB/s: 7.06, IO/s: 740, Lat(ms): [ min: 1.7, avg: 2.5, 99%: 4.7, max: 7.7 ], Slowdowns: 0
2025/07/12 21:29:00 Loop: 1, Int: TOTAL, Dur(s): 5.0, Mode: PUT, Ops: 3706, MB/s: 7.07, IO/s: 741, Lat(ms): [ min: 1.6, avg: 2.5, 99%: 4.4, max: 9.9 ], Slowdowns: 0
2025/07/12 21:29:00 The subsequent '-m g' or '-m d' test should be specified with '-n 3706'
2025/07/12 21:29:00 Running Loop 1 Get Test
2025/07/12 21:29:01 Loop: 1, Int: 0, Dur(s): 1.0, Mode: GET, Ops: 1272, MB/s: 12.13, IO/s: 1272, Lat(ms): [ min: 0.9, avg: 1.6, 99%: 4.2, max: 12.9 ], Slowdowns: 0
2025/07/12 21:29:02 Loop: 1, Int: 1, Dur(s): 1.0, Mode: GET, Ops: 1296, MB/s: 12.36, IO/s: 1296, Lat(ms): [ min: 0.8, avg: 1.5, 99%: 3.8, max: 15.0 ], Slowdowns: 0
2025/07/12 21:29:03 Loop: 1, Int: TOTAL, Dur(s): 2.9, Mode: GET, Ops: 3706, MB/s: 12.20, IO/s: 1279, Lat(ms): [ min: 0.8, avg: 1.6, 99%: 3.7, max: 15.0 ], Slowdowns: 0
2025/07/12 21:29:03 Running Loop 1 Del Test
2025/07/12 21:29:04 Loop: 1, Int: 0, Dur(s): 1.0, Mode: DEL, Ops: 2880, MB/s: 27.47, IO/s: 2880, Lat(ms): [ min: 0.4, avg: 0.6, 99%: 1.1, max: 1.7 ], Slowdowns: 0
2025/07/12 21:29:04 Loop: 1, Int: TOTAL, Dur(s): 1.3, Mode: DEL, Ops: 3706, MB/s: 27.38, IO/s: 2871, Lat(ms): [ min: 0.4, avg: 0.6, 99%: 1.2, max: 2.4 ], Slowdowns: 0
```

### Testing Overhead

* Just to evaluate the overhead of the tool itself

```text
# build/bin/blobstore/blobstore-bench -b dummy -d 3 -m pgd  -pr dummytest -s 10Ki -t 2 -l 2
2025/07/10 10:48:04 DataSize=10240 MD5=80a8c5663ee7ca537add8fd887c6ce93
2025/07/10 10:48:04 BlobStore Benchmark
2025/07/10 10:48:04 Parameters:
2025/07/10 10:48:04 backend=dummy
2025/07/10 10:48:04 database=
2025/07/12 21:29:35 conf=
2025/07/12 21:29:35 runName=dummytest
2025/07/12 21:29:35 poolName=
2025/07/12 21:29:35 objSize=10Ki
2025/07/12 21:29:35 maxObjCnt=-1
2025/07/12 21:29:35 durationSecs=3
2025/07/12 21:29:35 threads=2
2025/07/12 21:29:35 loops=2
2025/07/12 21:29:35 interval=1.000000
2025/07/12 21:29:35 dbDir=
2025/07/12 21:29:35 Running Loop 0 Put Test
2025/07/12 21:29:36 Loop: 0, Int: 0, Dur(s): 1.0, Mode: PUT, Ops: 4868550, MB/s: 46430.11, IO/s: 4868550, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.2 ], Slowdowns: 0
2025/07/12 21:29:37 Loop: 0, Int: 1, Dur(s): 1.0, Mode: PUT, Ops: 4275910, MB/s: 40778.26, IO/s: 4275910, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.2 ], Slowdowns: 0
2025/07/12 21:29:39 Loop: 0, Int: TOTAL, Dur(s): 3.0, Mode: PUT, Ops: 13223270, MB/s: 42060.02, IO/s: 4410313, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.2 ], Slowdowns: 0
2025/07/12 21:29:39 The subsequent '-m g' or '-m d' test should be specified with '-n 13223270'
2025/07/12 21:29:39 Running Loop 0 Get Test
2025/07/12 21:29:40 Loop: 0, Int: 0, Dur(s): 1.0, Mode: GET, Ops: 5984797, MB/s: 57075.47, IO/s: 5984797, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.3 ], Slowdowns: 0
2025/07/12 21:29:41 Loop: 0, Int: 1, Dur(s): 1.0, Mode: GET, Ops: 5263634, MB/s: 50197.93, IO/s: 5263634, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.1 ], Slowdowns: 0
2025/07/12 21:29:42 Loop: 0, Int: TOTAL, Dur(s): 2.4, Mode: GET, Ops: 13223270, MB/s: 53229.78, IO/s: 5581547, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.3 ], Slowdowns: 0
2025/07/12 21:29:42 Running Loop 0 Del Test
2025/07/12 21:29:43 Loop: 0, Int: 0, Dur(s): 1.0, Mode: DEL, Ops: 5578031, MB/s: 53196.25, IO/s: 5578031, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.1 ], Slowdowns: 0
2025/07/12 21:29:44 Loop: 0, Int: 1, Dur(s): 1.0, Mode: DEL, Ops: 5294927, MB/s: 50496.36, IO/s: 5294927, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.1 ], Slowdowns: 0
2025/07/12 21:29:44 Loop: 0, Int: TOTAL, Dur(s): 2.5, Mode: DEL, Ops: 13223270, MB/s: 51446.07, IO/s: 5394511, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.1 ], Slowdowns: 0
2025/07/12 21:29:44 Running Loop 1 Put Test
2025/07/12 21:29:45 Loop: 1, Int: 0, Dur(s): 1.0, Mode: PUT, Ops: 5397124, MB/s: 51470.99, IO/s: 5397124, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.2 ], Slowdowns: 0
2025/07/12 21:29:46 Loop: 1, Int: 1, Dur(s): 1.0, Mode: PUT, Ops: 4989291, MB/s: 47581.59, IO/s: 4989291, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.2 ], Slowdowns: 0
2025/07/12 21:29:48 Loop: 1, Int: TOTAL, Dur(s): 3.0, Mode: PUT, Ops: 15547268, MB/s: 49423.48, IO/s: 5182427, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.2 ], Slowdowns: 0
2025/07/12 21:29:48 The subsequent '-m g' or '-m d' test should be specified with '-n 15547268'
2025/07/12 21:29:48 Running Loop 1 Get Test
2025/07/12 21:29:49 Loop: 1, Int: 0, Dur(s): 1.0, Mode: GET, Ops: 5637404, MB/s: 53762.47, IO/s: 5637404, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.2 ], Slowdowns: 0
2025/07/12 21:29:50 Loop: 1, Int: 1, Dur(s): 1.0, Mode: GET, Ops: 5601770, MB/s: 53422.64, IO/s: 5601770, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.2 ], Slowdowns: 0
2025/07/12 21:29:51 Loop: 1, Int: TOTAL, Dur(s): 2.8, Mode: GET, Ops: 15547268, MB/s: 53826.12, IO/s: 5644078, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.2 ], Slowdowns: 0
2025/07/12 21:29:51 Running Loop 1 Del Test
2025/07/12 21:29:52 Loop: 1, Int: 0, Dur(s): 1.0, Mode: DEL, Ops: 5658764, MB/s: 53966.18, IO/s: 5658764, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.2 ], Slowdowns: 0
2025/07/12 21:29:53 Loop: 1, Int: 1, Dur(s): 1.0, Mode: DEL, Ops: 5424048, MB/s: 51727.75, IO/s: 5424048, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.2 ], Slowdowns: 0
2025/07/12 21:29:54 Loop: 1, Int: TOTAL, Dur(s): 2.8, Mode: DEL, Ops: 15547268, MB/s: 53546.62, IO/s: 5614770, Lat(ms): [ min: 0.0, avg: 0.0, 99%: 0.0, max: 0.2 ], Slowdowns: 0
```

## Others

* `-m`: You can specify a single operation or a combination of multiple operations, such as `p`, `g`, `d`, `pg`, `pd`, `pggg`. However, once `d` is executed, the data will be deleted.
* `-n`: This must be specified for the `d/del` operation to determine the maximum object number, which is used to define the stopping boundary for the operation.
