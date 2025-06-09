# 集群模式

## 编译构建

使用如下命令同时构建 server，client 及相关的依赖：

``` bash
$ git clone https://github.com/cubeFS/cubefs.git
$ cd cubefs
$ make build
```

如果构建成功，将在 `build/bin` 目录中生成可执行文件 `cfs-server` 和 `cfs-client`。

## 集群部署

### 启动资源管理节点

``` bash
./cfs-server -c master.json
```

示例 `master.json` ：

::: tip 推荐
为了保证服务的高可用，Master 服务最少启动3个节点实例
:::

``` json
{
  "role": "master",
  "ip": "127.0.0.1", // 替换为主机 ip
  "listen": "17010",
  "prof":"17020",
  "id":"1", // 替换为对应 id
  "peers": "1:127.0.0.1:17010,2:127.0.0.2:17010,3:127.0.0.3:17010",
  "retainLogs":"20000",
  "logDir": "/cfs/master/log", // master 日志目录
  "logLevel":"info",
  "walDir":"/cfs/master/data/wal", // raft wal 日志目录
  "storeDir":"/cfs/master/data/store", // RocksDB 数据存储目录
  "consulAddr": "http://consul.prometheus-cfs.local",
  "clusterName":"cubefs01",
  "metaNodeReservedMem": "1073741824" // 元数据节点预留内存，1G
}
```

详细配置参数请参考 [Master详细配置](../ops/configs/master.md) 。

### 启动元数据节点

``` bash
./cfs-server -c metanode.json
```

示例 `meta.json` ：

::: tip 推荐
为了保证服务的高可用，MetaNode 服务最少启动3个节点实例
:::

``` json
{
    "role": "metanode",
    "listen": "17210",
    "prof": "17220",
    "logLevel": "info",
    "metadataDir": "/cfs/metanode/data/meta", // 元数据快照存储目录
    "logDir": "/cfs/metanode/log", // metanode 日志目录
    "raftDir": "/cfs/metanode/data/raft",
    "raftHeartbeatPort": "17230",
    "raftReplicaPort": "17240",
    "totalMem":  "8589934592", // 最大可用内存，需大于 metaNodeReservedMem 
    "consulAddr": "http://consul.prometheus-cfs.local",
    "exporterPort": 9501,
    "masterAddr": [
        "127.0.0.1:17010",
        "127.0.0.2:17010",
        "127.0.0.3:17010"
    ]
}
```

详细配置参数请参考 [MetaNode详细配置](../ops/configs/metanode.md)。

### 启动数据节点

::: tip 推荐
使用单独磁盘作为数据目录，配置多块磁盘能够达到更高的性能。
:::

- 准备数据目录
  - 查看机器磁盘信息，选择给 CubeFS 使用的磁盘
   ``` bash
   fdisk -l
   ```
  - 格式化磁盘，建议格式化为 XFS
   ``` bash
   mkfs.xfs -f /dev/sdx
   ```
  - 创建挂载目录
   ``` bash
   mkdir /data0
   ```
  - 挂载磁盘
   ``` bash
   mount /dev/sdx /data0
   ```

- 启动数据节点

 ``` bash
./cfs-server -c datanode.json
```

示例 `datanode.json` :

::: tip 推荐
为了保证服务的高可用，Datanode 服务最少启动3个节点实例
:::

``` json
{
  "role": "datanode",
  "listen": "17310",
  "prof": "17320",
  "logDir": "/cfs/datanode/log",
  "logLevel": "info",
  "raftHeartbeat": "17330",
  "raftReplica": "17340",
  "mediaType": 1, // disk type, 1 ssd, 2 hdd
  "raftDir":"/cfs/datanode/log",
  "consulAddr": "http://consul.prometheus-cfs.local",
  "exporterPort": 9502,
  "masterAddr": [
     "127.0.0.1:17010",
     "127.0.0.2:17010",
     "127.0.0.3:17010"
  ],
  "disks": [
     "/data0:10737418240", // 磁盘挂载路径：预留空间
     "/data1:10737418240"
 ]
}
```

详细配置参数请参考 [DataNode详细配置](../ops/configs/datanode.md)。

### 启动对象网关节点

::: tip 提示
可选章节，如果需要使用对象存储服务，则需要部署对象网关（ObjectNode）
:::

``` bash
./cfs-server -c objectnode.json
```

示例 `objectnode.json`， 内容如下

``` json
{
    "role": "objectnode",
    "domains": [
        "object.cfs.local"
    ],
    "listen": "17410",
    "masterAddr": [
       "127.0.0.1:17010",
       "127.0.0.2:17010",
       "127.0.0.3:17010"
    ],
    "logLevel": "info",
    "logDir": "/cfs/Logs/objectnode"
}
```

配置文件的详细信息请参考 [ObjectNode详细配置](../ops/configs/objectnode.md)

### 安装lcnode

::: tip Note
可选. 如果不适用数据迁移能力，可不部署
:::

``` bash
./cfs-server -c lifecycle.json
```

Example `lifecycle.json`, as follows:

``` json
{
    "role": "lcnode",
    "listen": "17510",
    "masterAddr": [
       "127.0.0.1:17010",
       "127.0.0.2:17010",
       "127.0.0.3:17010"
    ],
    "logLevel": "info",
    "logDir": "/cfs/Logs/lcnode"
}
```

更多详细配置请参考 [Lcnode Detailed Configuration](../ops/configs/lcnode.md).

### 安装flashnode

::: tip Note
可选. 如果不用缓存集群加速读取文件可以不部署
:::

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

### 启动纠删码子系统

::: tip 提示
可选章节，如果需要使用纠删码卷则需要部署
:::

部署参考[使用纠删码存储系统](../user-guide/blobstore.md)。

## 常见问题

### 内存空间不足

``` bash
err(readFromProcess: sub-process: [cmd.go 323] Fatal: failed to start the CubeFS metanode daemon err bad totalMem config,Recommended to be configured as 80 percent of physical machine memory
```

解决方法：根据实际物理内存调整 metanode.json 中的 totalMem 配置

### 端口被占用

``` bash
err(readFromProcess: sub-process: [cmd.go 311] cannot listen pprof 17320 err listen tcp :17320: bind: address already in use
```

解决方法：kill 占用该端口的服务，一般是之前启动失败的节点

### fuse 客户端问题

``` bash
# 挂载失败
err(readFromProcess: sub-process: [fuse.go 438] mount failed: fusermount: exec: "fusermount": executable file not found in $PATH)
```

首先检查 fuse 是否已安装

``` bash
$ rpm –qa|grep fuse
$ yum install fuse
```

如果 fuse 已安装，请根据 [fuse客户端问题](../faq/fuse.md) 排查相应问题
