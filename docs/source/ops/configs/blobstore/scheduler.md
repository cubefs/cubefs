# Scheduler Configuration

`Scheduler` is a background task management module, mainly responsible for balancing, disk repair, node offline, deletion, repair and other background tasks.

The configuration of the Scheduler is based on the [public configuration](./base.md), and the following configuration instructions mainly focus on the private configuration of the Scheduler.

## Configuration Instructions

| Configuration Item             | Description                                                                                                         | Required                                                               |
|:-------------------------------|:--------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------|
| Public configuration           | Such as service port, running logs, audit logs, etc., refer to the [Basic Service Configuration](./base.md) section | Yes                                                                    |
| cluster_id                     | Cluster ID, unified ID within the cluster                                                                           | Yes                                                                    |
| services                       | List of all nodes of the Scheduler                                                                                  | Yes, refer to the example                                              |
| service_register               | Service registration information                                                                                    | Yes, refer to the example                                              |
| clustermgr                     | Clustermgr client initialization configuration                                                                      | Yes, clustermgr service address needs to be configured                 |
| proxy                          | Proxy client initialization configuration                                                                           | No, refer to the rpc configuration example                             |
| blobnode                       | BlobNode client initialization configuration                                                                        | No, refer to the rpc configuration example                             |
| kafka                          | Kafka related configuration                                                                                         | Yes                                                                    |
| balance                        | Load balancing task parameter configuration                                                                         | No                                                                     |
| disk_drop                      | Disk offline task parameter configuration                                                                           | No                                                                     |
| disk_repair                    | Disk repair task parameter configuration                                                                            | No                                                                     |
| volume_inspect                 | Volume inspection task parameter configuration (this volume refers to the volume in the erasure code subsystem)     | No                                                                     |
| shard_repair                   | Repair task parameter configuration                                                                                 | Yes, the directory for storing orphan data logs needs to be configured |
| blob_delete                    | Deletion task parameter configuration                                                                               | Yes, the directory for storing deletion logs needs to be configured    |
| topology_update_interval_min   | Configure the time interval for updating the cluster topology                                                       | No, default is 1 minute                                                |
| volume_cache_update_interval_s | Volume cache update frequency to avoid frequent updates of volumes in a short period of time                        | No, default is 10s                                                     |
| free_chunk_counter_buckets     | Bucket access for freechunk indicators                                                                              | No, default is \[1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000\]   |
| task_log                       | Record information of completed background tasks for backup                                                         | Yes, directory needs to be configured, chunkbits default is 29         |

## Configuration Example

### services

* leader, ID of the main node
* node_id, ID of the current node
* members, all Scheduler nodes
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

### service_register

* host, service address of the current node
* idc, IDC information of the current node
```json
{
  "host": "http://127.0.0.1:9800",
  "idc": "z0"
}
```

## clustermgr

* hosts, clustermgr service list
* Other RPC parameters, refer to the general module description
```json
{
  "hosts": [
    "http://127.0.0.1:9998",
    "http://127.0.0.2:9998",
    "http://127.0.0.3:9998"
  ]
}
```

### kafka

::: tip Note
Starting from v3.3.0, consumer groups are supported. For previous versions, please refer to the corresponding configuration file.
:::

* broker_list, Kafka node list
* fail_msg_sender_timeout_ms, timeout for resending messages to the failed topic after message consumption fails, default is 1000ms
* version, kafka version, default is 2.1.0
* topics，consume topics
  * shard_repair, normal topic, default are `shard_repair` and `shard_repair_prior`
  * shard_repair_failed, failed topic, default is `shard_repair_failed`
  * blob_delete, normal topic, default is `blob_delete`
  * blob_delete_failed, failed topic, default is `blob_delete_failed`

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

### balance

* disk_concurrency, the maximum number of disks allowed to be balanced simultaneously, default is 1 (before v3.3.0, this value was balance_disk_cnt_limit, default is 100)
* max_disk_free_chunk_cnt, when balancing, it will be judged whether there are disks with freechunk greater than or equal to this value in the current IDC. If not, no balance will be initiated. The default is 1024.
* min_disk_free_chunk_cnt, disks with freechunk less than this value will be balanced, default is 20
* prepare_queue_retry_delay_s, retry interval for the preparation queue when a task in the preparation queue fails to execute, default is 10
* finish_queue_retry_delay_s, retry interval for the completion queue, default is 10
* cancel_punish_duration_s, retry interval after task cancellation, default is 20
* work_queue_size, size of the queue for executing tasks, default is 20
* collect_task_interval_s, time interval for collecting tasks, default is 5
* check_task_interval_s, time interval for task verification, default is 5
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
### disk_drop

::: tip Note
Starting from version v3.3.0, concurrent disk offline is supported.
:::

* prepare_queue_retry_delay_s, retry interval for the preparation queue when a task in the preparation queue fails to execute, default is 10
* finish_queue_retry_delay_s, retry interval for the completion queue, default is 10
* cancel_punish_duration_s, retry interval after task cancellation, default is 20
* work_queue_size, size of the queue for executing tasks, default is 20
* collect_task_interval_s, time interval for collecting tasks, default is 5
* check_task_interval_s, time interval for task verification, default is 5
* disk_concurrency, the number of disks to be offline concurrently, default is 1
* task_limit_per_disk, task limit per disk，default 1
* total_task_limit, total task limit, default disk_concurrency * task_limit_per_disk
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

### disk_repair

::: tip Note
Starting from version v3.3.0, concurrent disk repair is supported.
:::

* prepare_queue_retry_delay_s, retry interval for the preparation queue when a task in the preparation queue fails to execute, default is 10
* finish_queue_retry_delay_s, retry interval for the completion queue, default is 10
* cancel_punish_duration_s, retry interval after task cancellation, default is 20
* work_queue_size, size of the queue for executing tasks, default is 20
* collect_task_interval_s, time interval for collecting tasks, default is 5
* check_task_interval_s, time interval for task verification, default is 5
* disk_concurrency, the number of disks to be repaired concurrently, default is 1
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

### volume_inspect

* inspect_interval_s, inspection time interval, default is 1s
* inspect_batch, batch inspection volume size, default is 1000
* list_vol_step, the size of requesting clustermgr to list volumes, which can control the QPS of requesting clustermgr, default is 100
* list_vol_interval_ms, time interval for requesting clustermgr to list volumes, default is 10ms
* timeout_ms, time interval for checking whether a batch of inspection tasks is completed, default is 10000ms
```json
{
    "inspect_interval_s": 100,     
    "inspect_batch": 10,    
    "list_vol_step": 20,    
    "list_vol_interval_ms": 10,    
    "timeout_ms": 10000   
}
```
### shard_repair

* task_pool_size, concurrency of repair tasks, default is 10
* message_punish_threshold, Punishment threshold, if the corresponding number of failed attempts to consume a message exceeds this value, a punishment will be imposed for a period of time to avoid excessive retries within a short period. The default value is 3.
* message_punish_time_m, punishment time, default 10 minutes
* orphan_shard_log, record information of orphan data repair failures, directory needs to be configured, chunkbits is the log file rotation size, default is 29 (2^29 bytes)
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

### blob_delete

::: tip Note
Starting from version v3.3.0, it is supported to configure the data deletion time period.
:::

* task_pool_size, concurrency of deletion tasks for kafka normal queue, default is 10
* fail_task_pool_size, concurrency of deletion tasks for kafka fail queue, default is 10
* delete_rate_per_second, delete rate limit in bid/s,
* safe_delay_time_h, deletion protection period, default is 72h. If a negative value is configured, the data will be deleted directly.
* message_punish_threshold, Punishment threshold, if the corresponding number of failed attempts to consume a message exceeds this value, a punishment will be imposed for a period of time to avoid excessive retries within a short period. The default value is 3.
* message_punish_time_m, punishment time, default 10 minutes
* message_slow_down_time_s, slow down when it overload, default 3 second
* delete_log, directory for storing deletion logs, needs to be configured, chunkbits default is 29
* delete_hour_range, supports configuring the deletion time period in 24-hour format. For example, the following configuration indicates that deletion requests will only be initiated during the time period between 1:00 a.m. and 3:00 a.m. If not configured, deletion will be performed all day.
* max_batch_size, batch consumption size of kafka messages, default is 10. If the batch is full or the time interval is reached, consume the Kafka messages accumulated during this period
* batch_interval_s, time interval for consuming kafka messages, default is 2s
```json
{
  "task_pool_size": 400,
  "fail_task_pool_size": 100,
  "delete_rate_per_second": 100,
  "message_punish_threshold": 3,
  "message_punish_time_m": 10,
  "message_slow_down_time_s": 3,
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
