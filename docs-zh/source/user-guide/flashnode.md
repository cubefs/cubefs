# 使用分布式缓存系统


## 编译构建

``` bash
$ git clone https://github.com/cubeFS/cubefs.git
$ cd cubefs
$ make server
```

## 启动flashnode

``` bash
./cfs-server -c flashnode.json
```

示例 `flashnode.json`:

``` json
{
    "role": "flashnode",
    "listen": "18510",
    "prof": "18511",
    "logDir": "./logs",
    "masterAddr": [
        "127.0.0.1:17010",
        "127.0.0.2:17010",
        "127.0.0.3:17010"
    ],
    "readRps": 100000,
    "disableTmpfs": true,
    "diskDataPath": [
      "/path/data1:0"
      ],
    "zoneName":"default"
}
```

更多详细配置请参考 [FlashNode Detailed Configuration](../ops/configs/flashnode.md).

## 分布式缓存配置

### 创建并启用flashGroup
通过cli工具的flashGroup create命令创建缓存组flashGroup，并分配到唯一识别该flashGroup的ID。

```bash
// fg的slot数量默认为32
./cfs-cli flashGroup create 
```

通过cli工具的flashGroup set命令启用flashGroup，参数为上一步被分配的ID。

```bash
// 创建后的flashGroup状态默认是inactive状态，需要设置成active状态
./cfs-cli flashGroup set 13 true
```

### flashGroup添加flashNode

通过cli工具的flashGroup nodeAdd命令往刚创建的flashGroup中添加缓存节点flashNode

```bash
// flashGroup添加flashNode，指定flashGroup的ID以及要添加的flashNode
./cfs-cli flashGroup nodeAdd 13 --zone-name=default --addr="*.*.*.*:18510"
```

通过cli工具的flashNode list命令，查看刚添加的flashNode是否正确。默认新添加的flashNode的active和enable状态都为true。

```bash
./cfs-cli flashnode list
```

### 开启卷的分布式缓存能力

通过cli工具的vol update命令开启目标卷的分布式缓存能力，默认新建卷以及集群升级前已存在的卷都没有不是缓存能力。

```bash
// 打开目标卷test（已创建）的remoteCacheEnable开关。
./cfs-cli vol update test --remoteCacheEnable true
```

通过cli工具的vol info命令查看目标卷是否打开分布式缓存能力

```bash
./cfs-cli vol info test
```
remoteCacheEnable为false表示未开启分布式缓存能力，true则表示已开启。


### 确认分布式缓存生效

client挂载已经开启分布式缓存能力的卷。

对卷根目录下的文件进行读请求测试。默认情况下，分布式缓存会缓存根目录下所有大小小于128GB的文件。

通过client端的mem_stat.log查看是否有flashNode的条目，该条目记录了客户端从flashNode读取的次数与平均延时。所以如果有对应条目，说明client端尝试flashNode读取对应的缓存数据。

# 使用分布式缓存独立服务

::: tip 提示
v3.5.3之后的版本支持分布式缓存独立服务
:::

## 编译构建

``` bash
$ git clone https://github.com/cubeFS/cubefs.git
$ cd cubefs
$ make server
```

## 启动flashGroupManager

``` bash
./cfs-server -c flashGroupManager.json
```

示例 `flashGroupManager.json`:

``` json
{
  "clusterName": "cfs_ocs_accesstest",
  "id": "1",
  "role": "flashgroupmanager",
  "ip": "127.0.0.1",
  "peers": "1:127.0.0.1:21010,4:127.0.0.2:21010,5:127.0.0.3:21010",
  "listen": "21010",
  "prof": "21020",
  "retainLogs": "20000",
  "walDir": "/var/logs/cfs/master/wal",
  "storeDir": "/var/cfs/master/store",
  "logLevel": "debug",
  "logDir": "./logs",
  "heartbeatPort": 5991,
  "replicaPort": 5992
}
```

## 分布式缓存配置

对于flashGroupManager,其分布式缓存资源的配置工具是cfs-remotecache-config。其使用方式和Master的分布式缓存资源配置工具cfs-cli完全一样。

比如查看创建并启用flashGroup，执行和cfs-cli一样的命令



### 构建配置工具

``` bash
$ git clone https://github.com/cubeFS/cubefs.git
$ cd cubefs
$ make rcconfig
```

```bash
// fg的slot数量默认为32
./cfs-remotecache-config  flashgroup create 
```

## 使用RemoteCacheClient SDK

RemoteCacheClient SDK提供了文件存储引擎以及对象存储引擎的存取服务。

对于文件存储引擎，RemoteCacheClient SDK已经集成在cfs-client中，只需要挂载卷开启分布式缓存能力，即可使用分布式缓存的读取能力。

对于对象存储引擎，可以参考CubeFS代码库中tool/remotecache-benchmark中的代码。通过初始化RemoteCacheClient执行Put和Get操作实现对象数据块的上传和下载。

### RemoteCacheClient配置项

RemoteCacheClient的配置项ClientConfig支持的参数如下:

| 参数                   | 类型    | 含义                                                                 | 必需 |
|------------------------|---------|----------------------------------------------------------------------|------|
| Masters                | string  | Master或者FlashGroupManager的地址（host:port）                       | 是   |
| BlockSize              | uint64  | 缓存数据的切片大小；文件存储建议1MB，对象存储按对象块大小            | 是   |
| NeedInitLog            | bool    | 是否开启日志                                                         | 否   |
| NeedInitStat           | bool    | 是否开启性能统计                                                     | 否   |
| LogLevelStr            | string  | 日志级别                                                             | 否   |
| LogDir                 | string  | 日志目录                                                             | 否   |
| ConnectTimeout         | int64   | 与 FlashNode 建立连接的超时（ms）                                     | 否   |
| FirstPacketTimeout     | int64   | 首包/首响应等待超时（ms）                                             | 否   |
| FromFuse               | bool    | 是否由文件存储客户端调用（兼容文件存储场景）                         | 是   |
| InitClientTime         | int     | 初始化 SDK 的最大超时（s）                                            | 否   |
| DisableBatch           | bool    | 是否禁止小文件合并上传（默认合并阈值≤16KB）                          | 否   |
| ActivateTime           | int64   | 批量上传小文件的最大时间间隔                                          | 否   |
| ConnWorkers            | int     | 批量上传小文件的最大连接数                                            | 否   |
| FlowLimit              | int64   | 客户端 Get/Put 流量上限（bytes/s，默认5GB/s）                         | 否   |
| DisableFlowLimitUpdate | bool    | 禁用 Master 下发的客户端 Get/Put 流量限制                             | 否   |
| WriteChunkSize         | int64   | Put 上传对象的单次块大小（bytes）                                     | 否   |
