# Ahead Read 

In the sequential file reading scenario, the prefetch function can be enabled via the client-side parameter aheadReadEnable, which caches the subsequent data content of the file in the client's memory in advance.

![aheadread](./pic/aheadread.png)


The following table describes the meanings of various parameters in the configuration file:

| Parameter           | Type    | Meaning                                  | Required |
|--------------|-------|-------------------------------------|----|
| aheadReadEnable        | bool  | Whether to enable ahead read                              | Yes  |
| aheadReadTotalMemGB      | int64 | Memory occupied by ahead read (unit: GB; default: 10), if less than 10GB then occupies 1/3 of current available memory | No  |
| aheadReadBlockTimeOut      | int64 | Cache block miss recycling time (unit: s; default: 3s)                   | No  |
| aheadReadWindowCnt   | int64 | Size of cache sliding window (default: 8)         | No  |
| minReadAheadSize   | int64 | Minimum file size required to trigger the prefetch function (unit: byte; default: 10)         | No  |

After the prefetch function is enabled, for files whose size exceeds minReadAheadSize, the function will asynchronously load the content of aheadReadWindowCnt cached data blocks into the client memory, starting from the position immediately following the offset of the current read request. This effectively improves the performance of sequential reading. Currently, the size of each cached data block is 4 MB.

The total number of prefetched cache blocks for all files shares a single cache resource pool, whose size is specified by the parameter aheadReadTotalMemGB. If the cache resource pool is full, new data content cannot be cached any further, and the system must wait for the existing cached data blocks to be evicted.

::: tip Tips:
1. Currently, the prefetch function only supports the replica mode and does not support EC (Erasure Coding).
2. Enabling prefetch will occupy a certain amount of client memory. For scenarios with strict client memory constraints, you can adjust the value of aheadReadTotalMemGB, but this will lead to a certain degree of performance degradation.
3. The prefetch function has the highest read priority. If it is enabled alongside other read functions (e.g., distributed caching), data will be retrieved from the prefetch cache first.
::: 