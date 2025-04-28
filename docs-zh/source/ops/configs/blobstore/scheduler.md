# Scheduler 配置

`Scheduler` 后台任务管理模块，主要负责均衡、磁盘修复、节点下线、删除、修补等后台任务。

Scheduler的配置是基于[公有配置](./base.md)，以下配置说明主要针对于Scheduler的私有配置。

## 配置说明

| 配置项                            | 说明                                        | 必需                                                        |
|:-------------------------------|:------------------------------------------|:----------------------------------------------------------|
| 公共配置项                          | 如服务端口、运行日志以及审计日志等，参考[基础服务配置](./base.md)章节 | 是                                                         |
| cluster_id                     | 集群id，集群内id统一                              | 是                                                         |
| services                       | scheduler所有节点列表                           | 是，参考示例                                                    |
| service_register               | 服务注册信息                                    | 是，参考示例                                                    |
| clustermgr                     | Clustermgr客户端初始化配置                        | 是，需要配置clustermgr服务地址                                      |
| proxy                          | Proxy客户端初始化配置                             | 否，参考rpc配置示例                                               |
| blobnode                       | BlobNode客户端初始化配置                          | 否，参考rpc配置示例                                               |
| kafka                          | kafka相关配置                                 | 是                                                         |
| balance                        | 均衡任务参数配置                                  | 否                                                         |
| disk_drop                      | 磁盘下线任务参数配置                                | 否                                                         |
| disk_repair                    | 磁盘修复任务参数配置                                | 否                                                         |
| volume_inspect                 | 卷巡检任务参数配置（这个卷指纠删码子系统中的卷）                  | 否                                                         |
| shard_repair                   | 修补任务参数配置                                  | 是，需要配置孤本数据日志存放目录                                          |
| blob_delete                    | 删除任务参数配置                                  | 是，需要配置删除日志存放目录                                            |
| topology_update_interval_min   | 配置集群拓扑更新时间间隔                              | 否，默认1分钟                                                   |
| volume_cache_update_interval_s | 卷缓存更新频率，避免短时间内频繁更新卷                       | 否，默认10s                                                   |
| free_chunk_counter_buckets     | 统计freechunk指标的bucket访问                    | 否，默认\[1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000\] |
| task_log                       | 记录已完成后台任务信息，用于备份                          | 是，需要配置dir，chunkbits默认29                                   |

## 配置示例
### services示例

* leader，主节点id
* node_id，本节点id
* members，所有scheduler节点
```json
{
  "leader": 1,
  "node_id": 1,
  "members": {
    "1": "127.0.0.1:9800",
    "2": "127.0.0.2:9800"
  }
}
```

### service_register示例

* host，本节点服务地址
* idc，本节点所在idc信息
```json
{
  "host": "http://127.0.0.1:9800",
  "idc": "z0"
}
```

### clustermgr示例

* hosts，clustermgr服务列表
* 其他一些rpc参数，参考通用模块说明
```json
{
  "hosts": [
    "http://127.0.0.1:9998",
    "http://127.0.0.2:9998",
    "http://127.0.0.3:9998"
  ]
}
```

### kafka示例

::: tip 提示
v3.3.0版本开始支持消费组，之前版本请参考对应版本配置文件
:::

* broker_list，kafka节点列表
* fail_msg_sender_timeout_ms，消息消费失败后重新投递至失败主题的超时时间，默认为1000ms
* version，kafka的版本号，默认为2.1.0
* topics，主题信息
  * shard_repair，修补主题，包含两个：普通修补主题与高优修补主题，默认为`shard_repair`与`shard_repair_prior`
  * shard_repair_failed，修补失败主题，默认为`shard_repair_failed`
  * blob_delete，删除主题，默认`blob_delete`
  * blob_delete_failed，删除失败主题，默认`blob_delete_failed`
```json
{
  "broker_list": ["127.0.0.1:9095","127.0.0.1:9095","127.0.0.1:9095"],
  "fail_msg_sender_timeout_ms": 1000,
  "version": "0.10.2.0",
  "topics": {
    "shard_repair": [
      "shard_repair",
      "shard_repair_prior"
    ],
    "shard_repair_failed": "shard_repair_failed",
    "blob_delete": "blob_delete",
    "blob_delete_failed": "blob_delete_failed"
  }
}
```

### balance示例

* disk_concurrency，允许同时执行均衡的最大磁盘数，默认1（release-3.2.2版本之前该值为balance_disk_cnt_limit，默认100）
* max_disk_free_chunk_cnt，均衡时会判断本idc内是否存在freechunk大于等于该值的磁盘，如果不存在则不会发起均衡，默认1024
* min_disk_free_chunk_cnt，均衡freechunk数小于该值的磁盘，默认20
* prepare_queue_retry_delay_s，准备队列重试时间间隔，当准备队列中的任务执行失败后的重试时间间隔，默认10
* finish_queue_retry_delay_s，完成队列重试时间间隔，默认10
* cancel_punish_duration_s，任务取消之后重试时间间隔，默认20
* work_queue_size，执行中任务队列大小，默认20
* collect_task_interval_s，收集任务时间间隔，默认5
* check_task_interval_s，任务校验时间间隔，默认5
```json
{
    "disk_concurrency": 700,    
    "max_disk_free_chunk_cnt": 500,    
    "min_disk_free_chunk_cnt": 105,      
    "prepare_queue_retry_delay_s": 60,    
    "finish_queue_retry_delay_s": 60,    
    "cancel_punish_duration_s": 60,    
    "work_queue_size": 600,    
    "collect_task_interval_s": 10,    
    "check_task_interval_s": 1    
}
```
### disk_drop示例

::: tip 提示
v3.3.0版本开始支持并发下线磁盘。
:::

* prepare_queue_retry_delay_s，准备队列重试时间间隔，当准备队列中的任务执行失败后的重试时间间隔，默认10
* finish_queue_retry_delay_s，完成队列重试时间间隔，默认10
* cancel_punish_duration_s，任务取消之后重试时间间隔，默认20
* work_queue_size，执行中任务队列大小，默认20
* collect_task_interval_s，收集任务时间间隔，默认5
* check_task_interval_s，任务校验时间间隔，默认5
* disk_concurrency，并发下线磁盘数，默认为1
* task_limit_per_disk, 每块盘可并行的任务数，默认是1
* total_task_limit， 同时并行下线的总任务数，默认值是 disk_concurrency * task_limit_per_disk
```json
{     
    "prepare_queue_retry_delay_s": 60,    
    "finish_queue_retry_delay_s": 60,    
    "cancel_punish_duration_s": 60,    
    "work_queue_size": 600,    
    "collect_task_interval_s": 10,    
    "check_task_interval_s": 1,
    "task_limit_per_disk": 1,
    "total_task_limit": 10,
    "disk_concurrency": 1
}
```

### disk_repair示例

::: tip 提示
v3.3.0版本开始支持并发修复磁盘。
:::

* prepare_queue_retry_delay_s，准备队列重试时间间隔，当准备队列中的任务执行失败后的重试时间间隔，默认10
* finish_queue_retry_delay_s，完成队列重试时间间隔，默认10
* cancel_punish_duration_s，任务取消之后重试时间间隔，默认20
* work_queue_size，执行中任务队列大小，默认20
* collect_task_interval_s，收集任务时间间隔，默认5
* check_task_interval_s，任务校验时间间隔，默认5
* disk_concurrency，并发修盘数，默认为1
```json
{     
    "prepare_queue_retry_delay_s": 60,    
    "finish_queue_retry_delay_s": 60,    
    "cancel_punish_duration_s": 60,    
    "work_queue_size": 600,    
    "collect_task_interval_s": 10,    
    "check_task_interval_s": 1,
    "disk_concurrency": 1
}
```

### volume_inspect示例

* inspect_interval_s，巡检时间间隔，默认1s
* inspect_batch，批量巡检卷大小，默认1000
* list_vol_step，请求clustermgr列举卷大小，可控制请求clustermgr的qps，默认100
* list_vol_interval_ms，请求clustermgr列举卷的时间间隔，默认10ms
* timeout_ms，检查一批巡检任务是否完成的时间间隔，默认10000ms
```json
{
    "inspect_interval_s": 100,     
    "inspect_batch": 10,    
    "list_vol_step": 20,    
    "list_vol_interval_ms": 10,    
    "timeout_ms": 10000   
}
```
### shard_repair示例

* task_pool_size，修补任务的并发度，默认10
* message_punish_threshold，惩罚阈值，如果对应消费失败次数超过该值，则会惩罚一段时间，避免短时间内大量重试，默认3次
* message_punish_time_m，惩罚时间，默认10分钟
* orphan_shard_log，记录修补失败的孤本信息，dir需要配置，chunkbits为日志文件轮转大小，默认29（2^29字节）
```json
{
  "task_pool_size": 10,
  "message_punish_threshold": 3,
  "message_punish_time_m": 10,
  "orphan_shard_log": {
    "dir": "/home/service/scheduler/_package/orphan_shard_log",
    "chunkbits": 29
  }
} 
```

### blob_delete示例

::: tip 提示
v3.3.0版本开始支持配置数据删除时间段。
:::

* task_pool_size，正常队列删除任务的并发度，默认10
* fail_task_pool_size， 失败队列删除任务的并发度，默认10
* delete_rate_per_second， bid粒度删除速率，默认100
* safe_delay_time_h，删除保护期，默认72h，如果配置负数则表示直接删除
* message_punish_threshold，惩罚阈值，如果对应消费失败次数超过该值，则会惩罚一段时间，避免短时间内大量重试，默认3次
* message_punish_time_m，惩罚时间，默认10分钟
* message_slow_down_time_s，删除太快时候的减速时间，默认3秒
* delete_log，删除日志保留目录，需要配置，chunkbits默认为29
* delete_hour_range，支持配置删除时间段，24小时制，比如以下配置表示凌晨1点到3点中间时间段才会发起删除请求，如果不配置默认全天删除
* max_batch_size, 批量消费kafka消息的大小，默认10; 如果batch大小已满或已经达到时间间隔，则消费在此期间累积的Kafka消息
* batch_interval_s, 消费kafka消息的最大间隔，默认2秒
```json
{
  "task_pool_size": 400,
  "fail_task_pool_size": 100,
  "delete_rate_per_second": 100,
  "message_punish_threshold": 3,
  "message_punish_time_m": 10,
  "safe_delay_time_h": 12,
  "delete_hour_range": {
    "from": 1,
    "to": 3
  },
  "max_batch_size": 10,
  "batch_interval_s": 2,
  "delete_log": {
    "dir": "/home/service/scheduler/_package/delete_log",
    "chunkbits": 29
  }
} 
```
