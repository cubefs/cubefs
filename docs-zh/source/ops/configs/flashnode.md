# FlashNode 配置
## 配置说明

| 配置项       | 类型         | 描述                                                         | 必需 | 默认值 |
| ------------ | ------------ | ------------------------------------------------------------ | ---- | ------ |
| role         | string       | 进程的角色，值只能是 flashnode                               | 是   |        |
| listen       | string       | tcp服务监听的端口号                                          | 是   |        |
| prof         | string       | golang pprof 端口号                                          |      |        |
| logDir       | string       | 日志文件存储目录                                             | 是   |        |
| logLevel     | string       | 日志级别                                                     | 否   | error  |
| masterAddr   | string slice | 格式: `HOST:PORT`，HOST: 资源管理节点IP（Master），PORT: 资源管理节点服务端口（Master） | 是   |        |
| disableTmpfs | bool         | 禁用tmpfs挂载，使用磁盘。缺省情况下，该值为false，使用tmpfs  | 否   | false  |
| memTotal     | int          | 使用内存时，flashnode可用于缓存数据的内存大小                | 是   |        |
| cachePercent | float        | 使用内存时，缓存容量占机器内存的百分比。使用磁盘时，缓存容量占磁盘容量的百分比。 | 否   | 1.0    |
| readRps      | int          | flashnode的rps值，用于flashnode限流                          | 是   | ``     |
| diskDataPath | string slice | 使用磁盘时，磁盘路径以及对应的配置磁盘容量                   | 是   |        |
| zoneName     | string       | 可以将flashNode都按zone进行管理cli可以用zone进行删除节点     | 是   |        |
| lruCapacity  | int          | 指定lru最多能存储的key的数量                                 | 否   | 400000 |

## 配置示例

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