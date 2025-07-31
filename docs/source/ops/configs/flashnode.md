# FlashNode Configuration
## Configuration Description

| Configuration Item | Type         | Description                                                  | Required | Default Value |
| ------------------ | ------------ | ------------------------------------------------------------ | -------- | ------------- |
| role               | string       | The role of the process, the value can only be flashnode     | Yes      |               |
| listen             | string       | Port number on which the TCP service listens                 | Yes      |               |
| prof               | string       | Golang pprof port number                                     | No       |               |
| logDir             | string       | Directory for storing log files                              | Yes      |               |
| logLevel           | string       | Log level                                                    | No       | error         |
| masterAddr         | string slice | Address of the master service                                | Yes      |               |
| disableTmpfs       | bool         | Use disk instead of tmpfs mount. The default value is false, indicating tmpfs is enabled by default. | No       | false         |
| memTotal           | int          | Memory size allocated for caching data when using memory mode | Yes      |               |
| cachePercent       | float        | Specifies the percentage of system memory used for caching when in memory mode, and the percentage of disk space used when in disk mode | No       | 1.0           |
| readRps            | int          | The RPS (Requests Per Second) value of FlashNode, used for rate limiting on the FlashNode. | Yes      |               |
| diskDataPath       | string slice | In disk mode, this field indicates the disk path and the corresponding cache capacity allocated on that disk. | Yes      |               |
| zoneName           | string       | FlashNodes can be organized and managed by zone, and the command-line interface (CLI) provides support for deleting nodes based on their zone. | Yes      |               |
| lruCapacity        | int          | Set the maximum number of entries (keys) that the LRU cache can hold | No       | 400000        |


## Configuration Example

``` json
{
    "role": "flashnode",
    "listen": "18510",
    "prof": "18511",
    "logDir": "./logs",
    "masterAddr": [
        "xxx",
        "xxx",
        "xxx"
    ],
    "memTotal": 0,
    "cachePercent": 0.8,
    "readRps": 100000,
    "disableTmpfs": true,
    "diskDataPath": [
      "/path/data1:0",
      "/path/data2:0"
      ],
    "zoneName":"default"
}
```