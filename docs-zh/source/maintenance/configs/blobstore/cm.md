# Clustermgr 配置

`Clustermgr`是集群管理模块，主要负责磁盘注册、逻辑卷的生成和分配、集群资源(如磁盘、节点、存储空间单元)的管理等。

Clustermgr的配置是基于[公有配置](./base.md)，以下配置说明主要针对于Clustermgr的私有配置。

## 配置说明
### 关键配置

| 配置项                | 说明                                                                           | 必需  |
|:-------------------|:-----------------------------------------------------------------------------|:----|
| 公有配置               | 如服务端口、运行日志以及审计日志等，参考[基础服务配置](./base.md)章节                                    | 是   |
| chunk_size         | blobnode中每一个chunk的大小，即创建的文件的大小                                               | 是   |
| cluster_id         | 集群编号                                                                         | 是   |
| idc                | 所在机房编号                                                                       | 是   |
| region             | 区域名                                                                          | 是   |
| db_path            | 元数据DB存储路径，生产环境中， 建议存放的数据盘为SSD                                                | 是   |
| code_mode_policies | 编码模式，配置参考编码模式详细配置，具体编码信息可参考[如何使用纠删码存储系统](../../../user-guide/blobstore.md)附录 | 是   |
| raft_config        | 详细参考Raft详细配置                                                                 | 是   |
| disk_mgr_config    | 详细参考磁盘管理配置                                                                   | 是   |
| host_aware         | 主机感知                                                                         | 是   |
| rack_aware         | 机架感知                                                                         | 是   |
| volume_mgr_config  | 卷管理模块                                                                        | 否   |


### 全部配置
```json
{
  "cluster_id": "集群编号",
  "idc": "所在机房编号",
  "region": "区域名，一个区域下可以有多个cluster，这个可以配合access写入时的多个区域多cluster的选择",
  "unavailable_idc": "不可用idc编号",
  "readonly": "是否只读",
  "db_path": "元数据DB存储默认路径，以下的db没有额外配置，对应的DB数据将在此目录自动创建目录",
  "normal_db_path": "通用DB路径",
  "normal_db_option": "通用DB选项配置，主要针对于rocksdb的调参",
  "kv_db_path": "kv DB存储的路径",
  "kv_db_option": "kv DB选项配置，主要针对于rocksdb的调参",
  "code_mode_policies": [
    {
      "mode_name": "编码名称",
      "min_size": "可写入最小对象的大小",
      "max_size": "可写入最大对象的大小",
      "size_ratio": "该模式占用的比例",
      "enable": "是否启用"
    }],
  "raft_config": {
    "raft_db_path": "raft的DB路径",
    "snapshot_patch_num": "一个db的快照分成多个片，一个db的快照数据很大，需要分成多个片来发送",
    "raft_db_option": "主要针对于rocksdb的调参",
    "server_config": {
      "nodeId": "raft节点ID",
      "listen_port": "raft专用端口",
      "raft_wal_dir": "wal日志路径",
      "raft_wal_sync": "是否立即刷盘",
      "tick_interval": "心跳间隔",
      "heartbeat_tick": "心跳时钟，默认为1",
      "election_tick": "选举时钟，建议设置为 5*heartbeat_tick，默认为5",
      "max_snapshots": "快照最大并发数，默认为10",
      "snapshot_timeout":"快照超时时间",
      "propose_timeout": "提报消息超时时间"
    },
    
    "raft_node_config": {
      "flush_num_interval": "出发flush的日志数",
      "FlushTimeIntervalS": "flush周期间隔",
      "truncate_num_interval": "leader 保留最大的日志条目，服务启动初始加载日志条目的数目，又可以理解为leader和follower的日志条目的差额，超过这个值日志同步需要走快照同步，因而这个值一般保留10万以上",
      "node_protocol": "raft节点间同步协议，一般为 http:// 协议",
      "members": [{
        "id":"节点ID", 
        "host":"raft主机地址，ip:Raft端口", 
        "learner": "是否参与主节点选举的节点", 
        "node_host":"服务主机地址，ip:服务端口"
      }],
      "apply_flush": "是否flush"
    }
  },
  "volume_mgr_config": {
    "blob_node_config": "参见rpc配置",
    "volume_db_path": "卷数据DB路径",
    "volume_db_option": "选项配置",
    "retain_time_s": "续租时间，配合proxy的续租时间",
    "retain_threshold": "卷可续租的健康度，卷的健康段须大于这个值才能被续租",
    "flush_interval_s": "flush时间间隔",
    "check_expired_volume_interval_s": "检查卷是否过期的时间间隔",
    "volume_slice_map_num": "cm的卷管理用的concurrentMap，用于提高卷读写的性能,这个值决定将所有卷分到所少个map里面管理",
    "apply_concurrency": "应用wal日志并发",
    "min_allocable_volume_count": "最小可分配的卷数",
    "allocatable_disk_load_threshold": "卷可分配的对应磁盘的负载"
  },
  "disk_mgr_config": {
    "refresh_interval_s": "磁盘刷新时间间隔,用于刷新当前cluster的磁盘状态",
    "host_aware": "主机感知，分配卷时是否可以在同一机器，在生产环境必须配上主机隔离",
    "heartbeat_expire_interval_s": "心跳过期间隔时间，针对于BlobNode上报的心跳时间", 
    "rack_aware": "机架感知，分配卷时是否可以在同一机架，机架隔离根据存储环境的条件进行配置",
    "flush_interval_s": "刷新时间间隔",
    "apply_concurrency": "应用并发",
    "blob_node_config": "",
    "ensure_index": "用来建立磁盘索引"
  },
  
  "cluster_report_interval_s": "上报consul的间隔",
  "consul_agent_addr": "consul地址",
  "heartbeat_notify_interval_s": "心跳通知间隔，用来定时处理BlobNode上报的磁盘信息，这个时间许小于BlobNode上报的时间间隔，避免磁盘心跳超时过期",
  "max_heartbeat_notify_num": "最大心跳通知数目",
  "chunk_size": "BlobNode中每一个chunk的大小，即创建的文件的大小  "
}
```


### 示例配置
```json
{
    "bind_addr":":9998",
    "cluster_id":1,
    "idc":["z0"],
    "chunk_size": 16777216,
    "log": {
        "level": "info",
        "filename": "./run/logs/clustermgr1.log"
    },
    "auth": {
        "enable_auth": false,
        "secret": "testsecret"
    },
    "region": "test-region",
    "db_path": "./run/db1",
    "code_mode_policies": [
        {"mode_name":"EC3P3","min_size":0,"max_size":5368709120,"size_ratio":1,"enable":true}
    ],
    "raft_config": {
        "snapshot_patch_num": 64,
        "server_config": {
            "nodeId": 1,
            "listen_port": 10110,
            "raft_wal_dir": "./run/raftwal1"
        },
        "raft_node_config":{
            "flush_num_interval": 10000,
            "flush_time_interval_s": 10,
            "truncate_num_interval": 10,
            "node_protocol": "http://",
            "members": [
                {"id":1, "host":"127.0.0.1:10110", "learner": false, "node_host":"127.0.0.1:9998"},
                {"id":2, "host":"127.0.0.1:10111", "learner": false, "node_host":"127.0.0.1:9999"},
                {"id":3, "host":"127.0.0.1:10112", "learner": false, "node_host":"127.0.0.1:10000"}
            ]
        }
    },
    "disk_mgr_config": {
        "refresh_interval_s": 10,
        "rack_aware":false,
        "host_aware":false
    }
}
```