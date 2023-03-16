# Scheduler 配置

## 配置说明

| 配置项                            | 说明                                        | 是否必须                                                      |
|:-------------------------------|:------------------------------------------|:----------------------------------------------------------|
| 公共配置项                          | 如服务端口、运行日志以及审计日志等，参考[基础服务配置](./base.md)章节 | 是                                                         |
| cluster_id                     | 集群id，集群内id统一                              | 是                                                         |
| services                       | scheduler所有节点列表                           | 是，参考示例                                                    |
| service_register               | 服务注册信息                                    | 是，参考示例                                                    |
| clustermgr                     | clustermgr客户端初始化配置                        | 是，需要配置clustermgr服务地址                                      |
| proxy                          | proxy客户端初始化配置                             | 否，参考rpc配置示例                                               |
| blobnode                       | blobnode客户端初始化配置                          | 否，参考rpc配置示例                                               |
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

* broker_list，kafka节点列表
* fail_msg_sender_timeout_ms，消息消费失败后重新投递至失败主题的超时时间，默认为1000ms
* shard_repair，修补消息主题及消费分区指定（分区未指定默认消费所有分区），其中包含三类主题：
    * normal，普通主题，默认为shard_repair
    * failed，失败主题（正常消息消费失败后会将消息重新投递至该主题），默认为shard_repair_prior
    * priority，优先消费主题，主要存放一些需要高优先级消费的修补消息，默认为shard_repair_failed
* blob_delete，删除消息主题及消费分区指定，其中包含两类主题
    * normal，普通主题，默认为blob_delete
    * failed，失败主题（正常消息消费失败后会将消息重新投递至该主题），默认为blob_delete_failed
```json
{
  "broker_list": ["127.0.0.1:9095","127.0.0.1:9095","127.0.0.1:9095"],
  "fail_msg_sender_timeout_ms": 1000,
  "shard_repair": {
    "normal": {
      "topic": "shard_repair",
      "partitions": [0,1]
    },
    "failed": {
      "topic": "shard_repair_failed",
      "partitions": [0,1]
    },
    "priority": {
      "topic": "shard_repair_prior",
      "partitions": [0,1]
    }
  },
  "blob_delete": {
    "normal": {
      "topic": "blob_delete",
      "partitions": [0,1]
    },
    "failed": {
      "topic": "blob_delete_failed",
      "partitions": [0,1]
    }
  }
}
```

### balance示例

* balance_disk_cnt_limit，允许同时执行均衡的最大磁盘数，默认100
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
    "balance_disk_cnt_limit": 700,    
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
* prepare_queue_retry_delay_s，准备队列重试时间间隔，当准备队列中的任务执行失败后的重试时间间隔，默认10
* finish_queue_retry_delay_s，完成队列重试时间间隔，默认10
* cancel_punish_duration_s，任务取消之后重试时间间隔，默认20
* work_queue_size，执行中任务队列大小，默认20
* collect_task_interval_s，收集任务时间间隔，默认5
* check_task_interval_s，任务校验时间间隔，默认5
```json
{     
    "prepare_queue_retry_delay_s": 60,    
    "finish_queue_retry_delay_s": 60,    
    "cancel_punish_duration_s": 60,    
    "work_queue_size": 600,    
    "collect_task_interval_s": 10,    
    "check_task_interval_s": 1    
}
```

### disk_repair示例

* prepare_queue_retry_delay_s，准备队列重试时间间隔，当准备队列中的任务执行失败后的重试时间间隔，默认10
* finish_queue_retry_delay_s，完成队列重试时间间隔，默认10
* cancel_punish_duration_s，任务取消之后重试时间间隔，默认20
* work_queue_size，执行中任务队列大小，默认20
* collect_task_interval_s，收集任务时间间隔，默认5
* check_task_interval_s，任务校验时间间隔，默认5
```json
{     
    "prepare_queue_retry_delay_s": 60,    
    "finish_queue_retry_delay_s": 60,    
    "cancel_punish_duration_s": 60,    
    "work_queue_size": 600,    
    "collect_task_interval_s": 10,    
    "check_task_interval_s": 1    
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
* normal_handle_batch_cnt，批量消费普通消息大小，默认100
* fail_handle_batch_cnt，批量消费失败消息大小，默认100
* fail_msg_consume_interval_ms，失败消息消费时间间隔，默认10000ms
* orphan_shard_log，记录修补失败的孤本信息，dir需要配置，chunkbits为日志文件轮转大小，默认29（2^29字节）
```json
{
  "task_pool_size": 10,
  "normal_handle_batch_cnt": 500,
  "fail_handle_batch_cnt": 100,
  "fail_msg_consume_interval_ms": 10000,
  "orphan_shard_log": {
    "dir": "/home/service/scheduler/_package/orphan_shard_log",
    "chunkbits": 29
  }
} 
```

### blob_delete示例

* task_pool_size，修补任务的并发度，默认10
* normal_handle_batch_cnt，批量消费普通消息大小，默认100
* fail_handle_batch_cnt，批量消费失败消息大小，默认100
* fail_msg_consume_interval_ms，失败消息消费时间间隔，默认10000ms
* safe_delay_time_h，删除保护器，默认72h，如果配置负数则表示直接删除
* delete_log，删除日志保留目录，需要配置，chunkbits默认为29
```json
{
  "task_pool_size": 400,
  "normal_handle_batch_cnt": 1000,
  "fail_handle_batch_cnt": 1000,
  "fail_msg_consume_interval_ms": 6000,
  "safe_delay_time_h": 12,
  "delete_log": {
    "dir": "/home/service/scheduler/_package/delete_log",
    "chunkbits": 29
  }
} 
```