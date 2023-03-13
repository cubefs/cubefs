# 分布式部署集群

## 编译构建

使用如下命令同时构建server，client及相关的依赖：

``` bash
$ git clone http://github.com/cubeFS/cubefs.git
$ cd cubefs
$ make build
```

如果构建成功，将在\`build/bin\`
目录中生成可执行文件\`cfs-server\`和\`cfs-client\`。

## 集群部署

### 启动资源管理节点

``` bash
./cfs-server -c master.json
```

示例 `master.json` ：注意：master服务最少应该启动3个节点实例

``` json
{
  "role": "master",
  "ip": "127.0.0.1",
  "listen": "17010",
  "prof":"17020",
  "id":"1",
  "peers": 1:127.0.0.1:17010,2:127.0.0.2:17010,3:127.0.0.3:17010",
  "retainLogs":"20000",
  "logDir": "/cfs/master/log",
  "logLevel":"info",
  "walDir":"/cfs/master/data/wal",
  "storeDir":"/cfs/master/data/store",
  "consulAddr": "http://consul.prometheus-cfs.local",
  "clusterName":"cubefs01",
  "metaNodeReservedMem": "1073741824"
}
```

详细配置参数请参考 [Master详细配置](../maintenance/configs/master.md) 。

### 启动元数据节点

``` bash
./cfs-server -c metanode.json
```

示例 `meta.json` ：注意：metanode服务最少应该启动3个节点实例

``` json
{
    "role": "metanode",
    "listen": "17210",
    "prof": "17220",
    "logLevel": "info",
    "metadataDir": "/cfs/metanode/data/meta",
    "logDir": "/cfs/metanode/log",
    "raftDir": "/cfs/metanode/data/raft",
    "raftHeartbeatPort": "17230",
    "raftReplicaPort": "17240",
    "totalMem":  "8589934592",
    "consulAddr": "http://consul.prometheus-cfs.local",
    "exporterPort": 9501,
    "masterAddr": [
        "127.0.0.1:17010",
        "127.0.0.2:17010",
        "127.0.0.3:17010"
    ]
}
```

详细配置参数请参考 [MetaNode详细配置](../maintenance/configs/metanode.md)。

### 启动数据节点

1. 准备数据目录

   **推荐** 使用单独磁盘作为数据目录，配置多块磁盘能够达到更高的性能。

   **磁盘准备**

   > 1.1 查看机器磁盘信息，选择给CubeFS使用的磁盘
   >
   > > ``` bash
    > > fdisk -l
    > > ```
   >
   > 1.2 格式化磁盘，建议格式化为XFS
   >
   > > ``` bash
    > > mkfs.xfs -f /dev/sdx
    > > ```
   >
   > 1.3 创建挂载目录
   >
   > > ``` bash
    > > mkdir /data0
    > > ```
   >
   > 1.4 挂载磁盘
   >
   > > ``` bash
    > > mount /dev/sdx /data0
    > > ```

2. 启动数据节点

   ``` bash
   ./cfs-server -c datanode.json
   ```

   示例 `datanode.json` :注意：datanode服务最少应该启动4个节点实例

   ``` json
   {
     "role": "datanode",
     "listen": "17310",
     "prof": "17320",
     "logDir": "/cfs/datanode/log",
     "logLevel": "info",
     "raftHeartbeat": "17330",
     "raftReplica": "17340",
     "raftDir":"/cfs/datanode/log",
     "consulAddr": "http://consul.prometheus-cfs.local",
     "exporterPort": 9502,
     "masterAddr": [
        "127.0.0.1:17010",
        "127.0.0.1:17010",
        "127.0.0.1:17010"
     ],
     "disks": [
        "/data0:10737418240",
        "/data1:10737418240"
    ]
   }
   ```

详细配置参数请参考 [DataNode详细配置](../maintenance/configs/datanode.md)。

### 启动对象存储节点

如果需要使用对象存储服务则需要部署ObjectNode服务

``` bash
./cfs-server -c objectnode.json
```

示例 `objectnode.json` 内容如下

``` json
{
    "role": "objectnode",
    "domains": [
        "object.cfs.local"
    ],
    "listen": 17410,
    "masterAddr": [
       "127.0.0.1:17010",
       "127.0.0.2:17010",
       "127.0.0.3:17010"
    ],
    "logLevel": "info",
    "logDir": "/cfs/Logs/objectnode"
}
```

配置文件的详细信息请参考 [ObjectNode详细配置](../maintenance/configs/objectnode.md)

### 启动纠删码子系统

部署参考[使用纠删码存储系统](../user-guide/blobstore.md)。