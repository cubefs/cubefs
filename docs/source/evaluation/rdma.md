# Performance Test Comparison between RDMA and TCP

## Configuration

| Component   | Configuration      |
|------------|--------|
| CPU | Intel(R) Xeon(R) Gold 6330 CPU @ 2.00GHz  |
| Memory | 512GB  |
| Network | ConnectX-5  |

### fio Test Command Example
``` bash
fio --directory=/mnt/cubefs --direct=1 -iodepth 1 --thread --rw=read --ioengine=libaio --bs=4k --size=500M --numjobs=32 -group_reporting -name=testfile10
```

## direct write
Under the mode where direct = 1, RDMA generally has an advantage. Test results depend on the current network device status, so there can be some fluctuations. We consider an error margin of within 10% to be normal. The bandwidth units in the test results below are all in MB/s. The green parts indicate improvements of over 30%, while the blue parts indicate improvements between 20-30%.
|threads |  direct	 |  case  |  rdma  |  tcp  |  compare  |
|--------|--------|--------|--------|--------|--------|
|1  |  1  |  4k write  |  16.5  |  15.3  |  7.84  |
|1  |  1  |  128k write  |  202  |  140  |  44.29  |
|1  |  1  |  1M write  |  153  |  136  |  12.50  |
|1  |  1  |  4k random write  |  4.67  |  4.53  |  3.09  |
|1  |  1  |  128k random write  |  116  |  123  |  -5.69  |
|1  |  1  |  1M random write  |  156  |  153  |  1.96  |
|32  |  1  |  4k write  |  216  |  173  |  24.86  |
|32  |  1  |  4k random write  |  97.2  |  102  |  -4.71  |
|32  |  1  |  64k write  |  1548  |  1592  |  -2.76  |
|32  |  1  |  64k random write  |  1054  |  982  |  7.33  |
|32  |  1  |  128k write  |  2469  |  2449  |  0.82  |
|32  |  1  |  128k random write  |  1818  |  1939  |  -6.24  |
|32  |  1  |  1M write  |  2611  |  2432  |  7.36  |
|32  |  1  |  1M random write  |  2449  |  2004  |  22.21  |

Conclusion: In direct write mode, RDMA shows performance improvements in some scenarios. In other scenarios, it is roughly on par with TCP.


## direct read
In the case of small blocks and single threads, the direct read mode shows significant performance improvements. When the number of threads reaches 32, the bandwidth approaches the limit of the network card, so the results of the two methods are relatively close. Due to the influence of the current network conditions, the test results can fluctuate, making the comparison of random reads less obvious.

There are two comparison data points in the table that show a decline. This is due to the higher cost of creating new connections in RDMA and the impact of network performance fluctuations at the time. Overall, in read mode, the network advantages of RDMA can be demonstrated, resulting in certain performance improvements.
|threads | direct | case | rdma | tcp | compare |
|--------|--------|--------|--------|--------|--------|
|1 | 1 | 4k read | 35.8 | 29.5 | 21.36 |
|1 | 1 | 128k read | 519 | 395 | 31.39 |
|1 | 1 | 1M read | 581 | 396 | 46.72 |
|1 | 1 | 4k random read | 32.8 | 28.5 | 15.09 |
|1 | 1 | 128k random read | 504 | 406 | 24.14 |
|1 | 1 | 1M random read | 542 | 404 | 34.16 |
|32 | 1 | 4k read | 595 | 604 | -1.49 |
|32 | 1 | 4k random read | 526 | 591 | -11.00 |
|32 | 1 | 64k read | 4048 | 3108 | 30.24 | 
|32 | 1 | 64k random read | 3029 | 3099 | -2.26 |
|32 | 1 | 128k read | 3808 | 2815 | 35.28 |
|32 | 1 | 128k random read | 3421 | 3784 | -9.59 |
|32 | 1 | 1M read | 3394 | 2820 | 20.35 |
|32 | 1 | 1M random read | 3678 | 3697 | -0.51 |



## buffer write
Because the maximum queue size for sequential writes is 1024, and RDMA’s work queue (wq) is typically 32 with a maximum concurrency of 32, the write performance of RDMA is more than 30% lower than that of TCP. The wq number represents the maximum number of entries that can be sent simultaneously, and this number must be requested for each concurrent path, so it cannot be configured to be infinitely large.
|threads | direct | case | rdma | tcp | compare |
|--------|--------|--------|--------|--------|--------|
|1 | 0 | 4k write | 74.1 | 88.9 | -16.65 |
|1 | 0 | 4k random write | 16.2 | 14.3 | 13.29 |
|1 | 0 | 128k write | 618 | 841 | -26.52 |
|1 | 0 | 1M write | 567 | 1000 | -43.30 |
|1 | 0 | 1M random write | 144 | 127 | 13.39 |
|32 | 0 | 4k write | 675 | 956 | -29.39 |
|32 | 0 | 4k random write | 271 | 251 | 7.97 |
|32 | 0 | 128k write | 1444 | 3068 | -52.93 |
|32 | 0 | 1M write | 1289 | 3072 | -58.04 |
|32 | 0 | 1M random write | 1133 | 1148 | -1.31 |


Conclusion:
- In cache mode, sequential writes show a significant performance drop.
- Other random read and write operations are roughly on par. Therefore, the type of file operation affects the test results.
- If further optimization is needed, dynamic expansion and contraction of the buffer would be required, along with a buffer management interaction process between the client and the server. This is relatively complex and is not considered for optimization at this stage.


## buffer read
Sequential reads in RDMA mode show a significant performance improvement compared to TCP, as shown below.
|threads | direct | case | rdma | tcp | compare |
|--------|--------|--------|--------|--------|--------|
|1 | 0 | 4k read | 1039 | 889 | 14.44 |
|1 | 0 | 4k random read | 37.1 | 29.2 | 27.05 |
|1 | 0 | 128k read | 1387 | 915 | 51.58 |
|1 | 0 | 1M read | 1263 | 899 | 40.49 |
|1 | 0 | 1M random read | 435 | 332 | 31.02 |
|32 | 0 | 4k read | 2601 | 2592 | 0.35 |
|32 | 0 | 4k random read | 346 | 283 | 22.26 |
|32 | 0 | 128k read | 2971 | 2446 | 21.46 |
|32 | 0 | 1M read | 2827 | 2566 | 10.17 |
|32 | 0 | 1M random read | 2985 | 2362 | 26.38 |

Analysis:
Cache reads do not require writing three copies of data, master-slave synchronization, or updating metanode metadata. They only need to read a single copy of data from the datanode. In this scenario, the proportion of a single network connection is relatively high. Since RDMA can improve pure bandwidth by 100% compared to TCP, a performance improvement of over 20% in the test results shown in the table is normal.

Conclusion:
Sequential read operations generally show an improvement of over 20%.
