# 使用纠删码系统

::: tip 提示
快速体验请看[单机部署](../deploy/node.md)
:::

## 编译构建

``` bash
$ git clone https://github.com/cubefs/cubefs.git
$ cd cubefs/blobstore
$ source env.sh
$ sh build.sh
```

构建成功后，将在 `bin` 目录中生成以下可执行文件：

```text
├── bin
│   ├── access
│   ├── clustermgr
│   ├── proxy
│   ├── scheduler
│   ├── blobnode
│   └── blobstore-cli
```

## 集群部署

由于模块之间具有一定的关联性，需按照以下顺序进行部署，避免服务依赖导致部署失败。

### 基础环境

1.  **支持平台**

    > Linux

2.  **依赖组件**

    > [Kafka](https://kafka.apache.org/documentation/#basic_ops)
    >
    > [Consul](https://learn.hashicorp.com/tutorials/consul/get-started-install?in=consul/getting-started)

3.  **语言环境**

    > [Go](https://go.dev/) (1.16.x)

### 启动Clustermgr

::: tip 提示
部署Clustermgr至少需要三个节点，以保证服务可用性。
:::

启动节点示例如下，节点启动需要更改对应配置文件，并保证集群节点之间的关联配置是一致。

1.  启动（三节点集群）

```bash
# 示例，推荐采用进程托管启停服务，下同
nohup ./clustermgr -f clustermgr.conf
nohup ./clustermgr -f clustermgr1.conf
nohup ./clustermgr -f clustermgr2.conf
```

2. 三个节点的集群配置，示例节点一：`clustermgr.conf`

```json
{
     "bind_addr":":9998",
     "cluster_id":1,
     "idc":["z0"],
     "chunk_size": 16777216,
     "log": {
         "level": "info",
         "filename": "./run/logs/clustermgr.log"
      },
     "auth": {
         "enable_auth": false,
         "secret": "testsecret"
     },
     "region": "test-region",
     "db_path":"./run/db0",
     "code_mode_policies": [ 
         {"mode_name":"EC3P3","min_size":0,"max_size":50331648,"size_ratio":1,"enable":true}
     ],
     "raft_config": {
         "server_config": {
             "nodeId": 1,
             "listen_port": 10110,
             "raft_wal_dir": "./run/raftwal0"
         },
         "raft_node_config":{
             "node_protocol": "http://",
             "members": [
                     {"id":1, "host":"127.0.0.1:10110", "learner": false, "node_host":"127.0.0.1:9998"},
                     {"id":2, "host":"127.0.0.1:10111", "learner": false, "node_host":"127.0.0.1:9999"},
                     {"id":3, "host":"127.0.0.1:10112", "learner": false, "node_host":"127.0.0.1:10000"}]
         }
     },
     "disk_mgr_config": {
         "refresh_interval_s": 10,
         "rack_aware":false,
         "host_aware":false
     }
}
```

### 启动Proxy

1. `proxy` 依赖kafka组件，需要提前创建blob_delete_topic、shard_repair_topic、shard_repair_priority_topic对应主题

::: tip 提示
kafka也可以用其他主题名，需要保证Proxy与Scheduler两个服务模块的kafka一致。
:::

```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic blob_delete shard_repair shard_repair_priority
```

2. 启动服务。保证可用性，每个机房 ``idc`` 至少需要部署一个proxy节点
```bash
nohup ./proxy -f proxy.conf &
```

3. 示例 `proxy.conf`:

```json
{
   "bind_addr": ":9600",
   "host": "http://127.0.0.1:9600",
   "idc": "z0",
   "cluster_id": 1,
   "clustermgr": {
     "hosts": [
       "http://127.0.0.1:9998",
       "http://127.0.0.1:9999",
       "http://127.0.0.1:10000"
       ]
   },
   "auth": {
       "enable_auth": false,
       "secret": "test"
   },
   "mq": {
     "blob_delete_topic": "blob_delete",
     "shard_repair_topic": "shard_repair",
     "shard_repair_priority_topic": "shard_repair_prior",
     "msg_sender": {
       "broker_list": ["127.0.0.1:9092"]
     }
   },
   "log": {
     "level": "info",
     "filename": "./run/logs/proxy.log"
   }
}
```

### 启动Scheduler

1. 启动服务

```bash
nohup ./scheduler -f scheduler.conf &
```

2. 示例 `scheduler.conf`: 注意Scheduler模块单节点部署

```json
{
   "bind_addr": ":9800",
   "cluster_id": 1,
   "services": { 
     "leader": 1,
     "node_id": 1,
     "members": {"1": "127.0.0.1:9800"}
   },
   "service_register": {
     "host": "http://127.0.0.1:9800",
     "idc": "z0"
   },
   "clustermgr": { 
     "hosts": ["http://127.0.0.1:9998", "http://127.0.0.1:9999", "http://127.0.0.1:10000"]
   },
   "kafka": {
     "broker_list": ["127.0.0.1:9092"]
   },
   "blob_delete": {
     "delete_log": {
       "dir": "./run/delete_log"
     }
   },
   "shard_repair": {
     "orphan_shard_log": {
       "dir": "./run/orphan_shard_log"
     }
   },
   "log": {
     "level": "info",
     "filename": "./run/logs/scheduler.log"
   },
   "task_log": {
     "dir": "./run/task_log"
   }
}
```

### 启动BlobNode

1. 在编译好的 `blobnode` 二进制目录下\**创建相关目录*\*

```bash
# 该目录对应配置文件的路径
mkdir -p ./run/disks/disk{1..8} # 每个目录需要挂载磁盘，保证数据收集准确性
```

2. 启动服务

```bash
nohup ./blobnode -f blobnode.conf
```

3.  示例 `blobnode.conf`:

```json
{
   "bind_addr": ":8899",
   "cluster_id": 1,
   "idc": "z0",
   "rack": "testrack",
   "host": "http://127.0.0.1:8899",
   "dropped_bid_record": {
     "dir": "./run/logs/blobnode_dropped"
   },
   "disks": [
     {
       "path": "./run/disks/disk1",
       "auto_format": true,
       "max_chunks": 1024 
     },
     {
       "path": "./run/disks/disk2",
       "auto_format": true,
       "max_chunks": 1024
     },
     {
       "path": "./run/disks/disk3",
       "auto_format": true,
       "max_chunks": 1024
     },
     {
       "path": "./run/disks/disk4",
       "auto_format": true,
       "max_chunks": 1024
     },
     {
       "path": "./run/disks/disk5",
       "auto_format": true,
       "max_chunks": 1024
     },
     {
       "path": "./run/disks/disk6",
       "auto_format": true,
       "max_chunks": 1024
     },
     {
       "path": "./run/disks/disk7",
       "auto_format": true,
       "max_chunks": 1024
     },
     {
       "path": "./run/disks/disk8",
       "auto_format": true,
       "max_chunks": 1024
     }
   ],
   "clustermgr": {
     "hosts": [
       "http://127.0.0.1:9998",
       "http://127.0.0.1:9999",
       "http://127.0.0.1:10000"
     ]
   },
   "disk_config":{
     "disk_reserved_space_B":1
   },
   "log": {
     "level": "info",
     "filename": "./run/logs/blobnode.log"
   }
}
```

### 启动Access

::: tip 提示
Access模块为无状态服务节点，可以部署多个节点
:::

1. 启动服务。

```bash
nohup ./access -f access.conf
```

2. 示例 `access.conf`:

```json
{
     "bind_addr": ":9500",
     "log": {
         "level": "info",
         "filename": "./run/logs/access.log"
      },
     "stream": {
         "idc": "z0",
         "cluster_config": {
             "region": "test-region",
             "clusters":[
                 {"cluster_id":1,"hosts":["http://127.0.0.1:9998","http://127.0.0.1:9999","http://127.0.0.1:10000"]}]
         }
     }
}
```

### 配置说明

- [通用配置说明](../maintenance/configs/blobstore/base.md)
- [RPC配置说明](../maintenance/configs/blobstore/rpc.md)
- [Clustermgr配置说明](../maintenance/configs/blobstore/cm.md)
- [Access配置说明](../maintenance/configs/blobstore/access.md)
- [BlobNode配置说明](../maintenance/configs/blobstore/blobnode.md)
- [Proxy配置说明](../maintenance/configs/blobstore/proxy.md)
- [Scheduler配置说明](../maintenance/configs/blobstore/scheduler.md)

## 部署提示

1. 对于Clustermgr和BlobNode部署失败后，重新部署需清理残留数据，避免注册盘失败或者数据显示错误，命令如下：

```bash
# blobnode示例
rm -f -r ./run/disks/disk*/.*
rm -f -r ./run/disks/disk*/*

# clustermgr示例
rm -f -r /tmp/raftdb0
rm -f -r /tmp/volumedb0
rm -f -r /tmp/clustermgr
rm -f -r /tmp/normaldb0
rm -f -r /tmp/normalwal0
```
2. clustermgr增加`learner`节点

::: tip 提示
learner节点一般用于数据备份，故障恢复
:::

- 在新的节点启用Clustermgr服务，将新服务中的配置中加上当前节点的成员信息；
- 调用[成员添加接口](../maintenance/admin-api/blobstore/cm.md)将刚启动的learner 节点加到集群中；
  ```bash
  curl -X POST --header 'Content-Type: application/json' -d '{"peer_id": 4, "host": "127.0.0.1:10113","node_host": "127.0.0.1:10001", "member_type": 1}' "http://127.0.0.1:9998/member/add" 
  ```
- 添加成功后，数据会自动同步

参考配置如下 `Clustermgr-learner.conf`:
```json
{
     "bind_addr":":10001",
     "cluster_id":1,
     "idc":["z0"],
     "chunk_size": 16777216,
     "log": {
         "level": "info",
         "filename": "./run/logs/clustermgr3.log"
      },
     "auth": {
         "enable_auth": false,
         "secret": "testsecret"
     },
     "region": "test-region",
     "db_path":"./run/db3",
     "code_mode_policies": [ 
         {"mode_name":"EC3P3","min_size":0,"max_size":50331648,"size_ratio":1,"enable":true}
     ],
     "raft_config": {
         "server_config": {
             "nodeId": 4,
             "listen_port": 10113,
             "raft_wal_dir": "./run/raftwal3"
         },
         "raft_node_config":{
             "node_protocol": "http://",
             "members": [
                     {"id":1, "host":"127.0.0.1:10110", "learner": false, "node_host":"127.0.0.1:9998"},
                     {"id":2, "host":"127.0.0.1:10111", "learner": false, "node_host":"127.0.0.1:9999"},
                     {"id":3, "host":"127.0.0.1:10112", "learner": false, "node_host":"127.0.0.1:10000"},
                     {"id":4, "host":"127.0.0.1:10113", "learner": true, "node_host": "127.0.0.1:10001"}]
         }
     },
     "disk_mgr_config": {
         "refresh_interval_s": 10,
         "rack_aware":false,
         "host_aware":false
     }
}
```

2. 所有模块部署成功后，上传验证需要延缓一段时间，等待创建卷成功。

## 上传测试

> 可参考[CLI部署验证](../deploy/verify.md)

## 修改Master配置支持纠删码

修改Master配置文件中的`ebsAddr`配置项（[更多配置参考](../maintenance/configs/master.md)），配置为Access节点注册的Consul地址。

## 创建纠删码卷

参考[创建卷](./volume.md)

## 附录

1.  编码策略: 常用策略表

| 类别       | 描述                                                                              |
|-----------|-----------------------------------------------------------------------------------|
| EC12P4    | {N: 12, M: 04, L: 0, AZCount: 1, PutQuorum: 15, GetQuorum: 0, MinShardSize: 2048} |
| EC3P3     | {N: 3,  M: 3,  L: 0, AZCount: 1, PutQuorum: 5, GetQuorum: 0, MinShardSize: 2048}  |
| EC16P20L2 | {N: 16, M: 20, L: 2, AZCount: 2, PutQuorum: 34, GetQuorum: 0, MinShardSize: 2048} |
| EC6P10L2  | {N: 6,  M: 10, L: 2, AZCount: 2, PutQuorum: 14, GetQuorum: 0, MinShardSize: 2048} |
| EC12P9    | {N: 12, M: 9,  L: 0, AZCount: 3, PutQuorum: 20, GetQuorum: 0, MinShardSize: 2048} |
| EC15P12   | {N: 15, M: 12, L: 0, AZCount: 3, PutQuorum: 24, GetQuorum: 0, MinShardSize: 2048} |
| EC6P6     | {N: 6,  M: 6,  L: 0, AZCount: 3, PutQuorum: 11, GetQuorum: 0, MinShardSize: 2048} |

其中
- N: 数据块数量，M: 校验块数量， L: 本地校验块数量，AZCount: AZ数量
- PutQuorum: `(N + M) / AZCount + N \<= PutQuorum \<= M + N`
- MinShardSize: 最小shard大小,将数据连续填充到`0-N`分片中，如果数据大小小于`MinShardSize*N`，则与零字节对齐，详见[代码](https://github.com/cubefs/cubefs/blob/release-3.2.0/blobstore/common/codemode/codemode.go)
。
