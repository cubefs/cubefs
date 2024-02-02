# IO Performance Evaluation

The results of IO performance testing using [fio](https://github.com/axboe/fio) are as follows:

::: tip Note
Multiple clients mount the same volume, and the process refers to the fio process.
:::

## Sequential Read

**Tool Settings**

``` bash
#!/bin/bash
fio -directory={} \
    -ioengine=psync \
    -rw=read \  # sequential read
    -bs=128k \  # block size
    -direct=1 \ # enable direct IO
    -group_reporting=1 \
    -fallocate=none \
    -time_based=1 \
    -runtime=120 \
    -name=test_file_c{} \
    -numjobs={} \
    -nrfiles=1 \
    -size=10G
```

**Bandwidth (MB/s)**

![Sequential Read Bandwidth (MB/s)](../pic/cfs-fio-sequential-read-bandwidth.png)

|           | 1 Process | 4 Processes | 16 Processes | 64 Processes |
|-----------|-----------|-------------|--------------|--------------|
| 1 Client  | 319.000   | 1145.000    | 3496.000     | 2747.000     |
| 2 Clients | 625.000   | 2237.000    | 6556.000     | 5300.000     |
| 4 Clients | 1326.000  | 4433.000    | 8979.000     | 9713.000     |
| 8 Clients | 2471.000  | 7963.000    | 11878.400    | 17510.400    |

**IOPS**

![Sequential Read IOPS](../pic/cfs-fio-sequential-read-iops.png)

|           | 1 Process | 4 Processes | 16 Processes | 64 Processes |
|-----------|-----------|-------------|--------------|--------------|
| 1 Client  | 2552      | 9158        | 27000        | 21000        |
| 2 Clients | 5003      | 17900       | 52400        | 42400        |
| 4 Clients | 10600     | 35500       | 71800        | 77700        |
| 8 Clients | 19800     | 63700       | 94700        | 140000       |

**Latency (Microsecond)**

![Sequential Read Latency (Microsecond)](../pic/cfs-fio-sequential-read-latency.png)

|           | 1 Process | 4 Processes | 16 Processes | 64 Processes |
|-----------|-----------|-------------|--------------|--------------|
| 1 Client  | 391.350   | 436.170     | 571.200      | 2910.960     |
| 2 Clients | 404.030   | 459.330     | 602.270      | 3011.920     |
| 4 Clients | 374.450   | 445.550     | 892.390      | 2948.990     |
| 8 Clients | 404.530   | 503.590     | 1353.910     | 4160.620     |

## Sequential Write

**Tool Settings**

``` bash
#!/bin/bash
fio -directory={} \
    -ioengine=psync \
    -rw=write \ # sequential write
    -bs=128k \  # block size
    -direct=1 \ # enable direct IO
    -group_reporting=1 \
    -fallocate=none \
    -name=test_file_c{} \
    -numjobs={} \
    -nrfiles=1 \
    -size=10G
```

**Bandwidth (MB/s)**

![Sequential Write Bandwidth (MB/s)](../pic/cfs-fio-sequential-write-bandwidth.png)

|           | 1 Process | 4 Processes | 16 Processes | 64 Processes |
|-----------|-----------|-------------|--------------|--------------|
| 1 Client  | 119.000   | 473.000     | 1618.000     | 2903.000     |
| 2 Clients | 203.000   | 886.000     | 2917.000     | 5465.000     |
| 4 Clients | 397.000   | 1691.000    | 4708.000     | 7256.000     |
| 8 Clients | 685.000   | 2648.000    | 6257.000     | 7166.000     |

**IOPS**

![Sequential Write IOPS](../pic/cfs-fio-sequential-write-iops.png)

|           | 1 Process | 4 Processes | 16 Processes | 64 Processes |
|-----------|-----------|-------------|--------------|--------------|
| 1 Client  | 948       | 3783        | 12900        | 23200        |
| 2 Clients | 1625      | 7087        | 23300        | 43700        |
| 4 Clients | 3179      | 13500       | 37700        | 58000        |
| 8 Clients | 5482      | 21200       | 50100        | 57300        |

**Latency (Microsecond)**

![Sequential Write Latency (Microsecond)](../pic/cfs-fio-sequential-write-latency.png)

|           | 1 Process | 4 Processes | 16 Processes | 64 Processes |
|-----------|-----------|-------------|--------------|--------------|
| 1 Client  | 1053.240  | 1051.450    | 1228.230     | 2745.800     |
| 2 Clients | 1229.270  | 1109.490    | 1359.350     | 2893.780     |
| 4 Clients | 1248.990  | 1164.050    | 1642.660     | 4115.970     |
| 8 Clients | 1316.560  | 1357.940    | 2378.950     | 8040.250     |

## Random Read

**Tool Settings**

``` bash
#!/bin/bash
fio -directory={} \
    -ioengine=psync \
    -rw=randread \ # random read
    -bs=4k \       # block size
    -direct=1 \    # enable direct IO
    -group_reporting=1 \
    -fallocate=none \
    -time_based=1 \
    -runtime=120 \
    -name=test_file_c{} \
    -numjobs={} \
    -nrfiles=1 \
    -size=10G
```

**Bandwidth (MB/s)**

![Random Read Bandwidth (MB/s)](../pic/cfs-fio-random-read-bandwidth.png)

|           | 1 Process | 4 Processes | 16 Processes | 64 Processes |
|-----------|-----------|-------------|--------------|--------------|
| 1 Client  | 15.500    | 76.300      | 307.000      | 496.000      |
| 2 Clients | 32.600    | 161.000     | 587.000      | 926.000      |
| 4 Clients | 74.400    | 340.000     | 1088.000     | 1775.000     |
| 8 Clients | 157.000   | 628.000     | 1723.000     | 2975.000     |

**IOPS**

![Random Read IOPS](../pic/cfs-fio-random-read-iops.png)

|           | 1 Process | 4 Processes | 16 Processes | 64 Processes |
|-----------|-----------|-------------|--------------|--------------|
| 1 Client  | 3979      | 19500       | 78700        | 127000       |
| 2 Clients | 8345      | 41300       | 150000       | 237000       |
| 4 Clients | 19000     | 86000       | 278000       | 454000       |
| 8 Clients | 40200     | 161000      | 441000       | 762000       |

**Latency (Microsecond)**

![Random Read Latency (Microsecond)](../pic/cfs-fio-random-read-latency.png)

|           | 1 Process | 4 Processes | 16 Processes | 64 Processes |
|-----------|-----------|-------------|--------------|--------------|
| 1 Client  | 250.720   | 203.960     | 202.480      | 502.940      |
| 2 Clients | 250.990   | 204.100     | 219.750      | 558.010      |
| 4 Clients | 211.240   | 180.720     | 226.840      | 551.470      |
| 8 Clients | 192.660   | 196.560     | 288.090      | 691.920      |

## Random Write

**Tool Settings**

``` bash
#!/bin/bash
fio -directory={} \
    -ioengine=psync \
    -rw=randwrite \ # random write
    -bs=4k \        # block size
    -direct=1 \     # enable direct IO
    -group_reporting=1 \
    -fallocate=none \
    -time_based=1 \
    -runtime=120 \
    -name=test_file_c{} \
    -numjobs={} \
    -nrfiles=1 \
    -size=10G
```

**Bandwidth (MB/s)**

![Random Write Bandwidth (MB/s)](../pic/cfs-fio-random-write-bandwidth.png)

|           | 1 Process | 4 Processes | 16 Processes | 64 Processes |
|-----------|-----------|-------------|--------------|--------------|
| 1 Client  | 7.743     | 43.500      | 164.000      | 429.000      |
| 2 Clients | 19.700    | 84.300      | 307.000      | 679.000      |
| 4 Clients | 41.900    | 167.000     | 480.000      | 877.000      |
| 8 Clients | 82.600    | 305.000     | 700.000      | 830.000      |

**IOPS**

![Random Write IOPS](../pic/cfs-fio-random-write-iops.png)

|           | 1 Process | 4 Processes | 16 Processes | 64 Processes |
|-----------|-----------|-------------|--------------|--------------|
| 1 Client  | 1982      | 11100       | 42100        | 110000       |
| 2 Clients | 5050      | 21600       | 78600        | 174000       |
| 4 Clients | 10700     | 42800       | 123000       | 225000       |
| 8 Clients | 1100      | 78100       | 179000       | 212000       |

**Latency (Microsecond)**

![Random Write Latency](../pic/cfs-fio-random-write-latency.png)

|           | 1 Process | 4 Processes | 16 Processes | 64 Processes |
|-----------|-----------|-------------|--------------|--------------|
| 1 Client  | 503.760   | 358.190     | 379.110      | 580.970      |
| 2 Clients | 400.150   | 374.010     | 412.900      | 751.020      |
| 4 Clients | 371.620   | 370.520     | 516.930      | 1139.920     |
| 8 Clients | 380.650   | 403.510     | 718.900      | 2409.250     |