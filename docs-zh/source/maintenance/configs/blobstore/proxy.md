# Proxy 配置

> `proxy` 是代理模块，主要负责消息转发、卷的分配与续租代理、缓存等，主要目的在于缓解clustermgr的服务压力。proxy的配置是基于[公有配置](./base.md)，以下配置说明主要针对于proxy的私有配置。

## 配置说明

### 关键配置

| 配置项                    | 说明                                                      | 是否必须 |
|:-----------------------|:--------------------------------------------------------|:-----|
| 公有配置                   | 如服务端口、运行日志以及审计日志等，参考[基础服务配置](./base.md)章节               | 是    |
| host                   | 当前host信息，用于上报clustermgr作服务发现使用，例如 http://服务ip:bind_port | 是    |
| cluster_id             | 集群编号                                                    | 是    |
| idc                    | 所在机房编号                                                  | 是    |
| retain_interval_s      | 续租间隔周期，配合cm卷过期时间设定                                      | 是    |
| init_volume_num        | 初始启动向clustermgr申请卷的数量，根据集群大小设定                          | 是    |
| default_alloc_vols_num | 每次向clustermgr申请卷的个数，根据集群大小设定                            | 是    |
| mq                     | kafka生产者配置                                              | 是    |

### 全部配置

```json
{
  "heartbeat_interval_s": "向 Clustermgr 心跳的间隔周期， 心跳时间为heartbeatTicks * tickInterval",
  "heartbeat_ticks": "配合heartbeat_interval_s使用",
  "expires_ticks": "",
  "clustermgr": {
    "hosts": "clustermgr的主机列表，[ `http://ip:port`,`http://ip1:port`]",
    "rpc": "参见rpc LbClient配置介绍"
  },
  "bid_alloc_nums": "每次access 向proxy申请的最大bid个数",
  "host": "当前host信息，用于上报clustermgr作服务发现使用，例如 http://服务ip:bind_port",
  "cluster_id": "集群编号",
  "idc": "所在机房编号",
  "retain_interval_s": "续租间隔周期，配合cm卷过期时间设定",
  "init_volume_num": "初始启动向clustermgr申请卷的数量，根据集群大小设定",
  "default_alloc_vols_num": "每次向clustermgr申请卷的个数，access的分配请求可以触发",
  "metric_report_interval_s": "proxy上报运行状态给普罗米修斯的时间周期",
  "mq": {
    "blob_delete_topic": "删除消息主题名",
    "shard_repair_topic": "修复消息主题名",
    "shard_repair_priority_topic": "高优修复的消息会投递至该主题，一般是某个bid在多个chunk有缺失的情况", 
    "msg_sender": {
      "kafka": "参见kafka生产者使用配置介绍"
    }
  }
}
```

### 示例配置

```json
{
  "bind_addr": ":9600",
  "host": "http://127.0.0.1:9600",
  "idc": "z0",
  "cluster_id": 1,
  "default_alloc_vols_num" : 2,
  "init_volume_num": 4,
  "clustermgr": {
    "hosts": [
      "http://127.0.0.1:9998",
      "http://127.0.0.1:9999",
      "http://127.0.0.1:10000"
    ]
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