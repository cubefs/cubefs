# Monitoring Metrics Items

CubeFS integrates Prometheus as the monitoring metric collection module, which can be combined with specific situations to enable monitoring metrics or configure monitoring panels.

## Process-Related Metrics

Support reporting go gc stats and mem stats, as follows

```bash
# GC-related
go_gc_duration_seconds_sum
go_gc_duration_seconds
...
# Memory allocation
go_memstats_alloc_bytes
go_memstats_heap_idle_bytes
...
# Process-related
process_resident_memory_bytes
process_start_time_seconds
process_open_fds
...
```

## Master

The monitoring metrics reported by the `Master` module mainly include the health status, usage, and statistical data of nodes in the cluster.

| Metric Name                                            | Description                                                                  |
|--------------------------------------------------------|------------------------------------------------------------------------------|
| cfs_master_dataNodes_count                             | Number of data nodes in the cluster                                          |
| cfs_master_dataNodes_inactive                          | Number of abnormal data nodes in the cluster                                 |
| cfs_master_dataNodes_increased_GB                      | Change in disk space usage in the cluster within 2 minutes                   |
| cfs_master_dataNodes_total_GB                          | Total disk space size in the cluster                                         |
| cfs_master_dataNodes_used_GB                           | Disk space size used in the cluster                                          |
| cfs_master_disk_error{addr="xx",path="xx"}             | Bad disk monitoring in the cluster, including abnormal node IP and disk path |
| cfs_master_metaNodes_count                             | Total number of metadata nodes in the cluster                                |
| cfs_master_metaNodes_inactive                          | Number of abnormal metadata nodes in the cluster                             |
| cfs_master_metaNodes_increased_GB                      | Change in memory usage of metadata within 2 minutes in the cluster           |
| cfs_master_metaNodes_total_GB                          | Total memory size of metadata in the cluster                                 |
| cfs_master_metaNodes_used_GB                           | Memory size used by metadata in the cluster                                  |
| cfs_master_vol_count                                   | Number of volumes in the cluster                                             |
| cfs_master_vol_meta_count{type="dentry",volName="vol"} | Details of the specified volume, type: dentry, inode, dp, mp                 |
| cfs_master_vol_total_GB{volName="xx"}                  | Capacity of the specified volume                                             |
| cfs_master_vol_usage_ratio{volName="xx"}               | Usage rate of the specified volume                                           |
| cfs_master_vol_used_GB{volName="xx"}                   | Used capacity of the specified volume                                        |

## MetaNode

The monitoring metrics of the `MetaNode` can be used to monitor the qps and latency data of various metadata operations of each volume, such as lookup, createInode, createDentry, etc.

| Metric Name                  | Description                                                                                                                                        |
|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| cfs_metanode_$op_count       | Total number of requests for the corresponding operation of the meta node, which can be used to calculate the request qps                          |
| cfs_metanode_$op_hist_bucket | Hist data of the corresponding operation request of the meta node, which can be used to calculate the 95 value of the latency                      |
| cfs_metanode_$op_hist_count  | Total number of corresponding requests for the meta node, same as cfs_metanode_$op_count                                                           |
| cfs_metanode_$op_hist_sum    | Total time consumption of the corresponding operation request of the meta node, which can be used to calculate the average latency with hist_count |

## DataNode

The monitoring metrics of the `DataNode` can be used to monitor the qps and latency data of various data operations of each volume, as well as bandwidth, such as read, write, etc.

| Metric Name                              | Description                                                                                                                                        |
|------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| cfs_dataNode_$op_count                   | Total number of requests for the corresponding operation of the data node, which can be used to calculate the request qps                          |
| cfs_dataNode_$op_hist_bucket             | Hist data of the corresponding operation request of the data node, which can be used to calculate the 95 value of the latency                      |
| cfs_dataNode_$op_hist_count              | Total number of corresponding requests for the data node, same as cfs_datanode_$op_count                                                           |
| cfs_dataNode_$op_hist_sum                | Total time consumption of the corresponding operation request of the data node, which can be used to calculate the average latency with hist_count |
| cfs_dataNode_dataPartitionIOBytes        | Total amount of data read and written by the data node, which can be used to calculate the bandwidth data of the specified disk and volume         |
| cfs_dataNode_dataPartitionIO_count       | Total number of IOs of the data node, which can be used to calculate the disk IO qps data                                                          |
| cfs_dataNode_dataPartitionIO_hist_bucket | Histogram data of the IO operation of the data node, which can be used to calculate the 95 value of the IO                                         |
| cfs_dataNode_dataPartitionIO_hist_count  | Total number of IO operations of the data node, same as above                                                                                      |
| cfs_dataNode_dataPartitionIO_hist_sum    | Total delay of the IO operation of the data node, which can be used to calculate the average delay with hist_count                                 |

## ObjectNode

The monitoring metrics of the `ObjectNode` are mainly used to monitor the request volume and time consumption of various S3 operations, such as copyObject, putObject, etc.

| Metric Name                    | Description                                                                                                                                          |
|--------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| cfs_objectnode_$op_count       | Total number of requests for the corresponding operation of the object node, which can be used to calculate the qps                                  |
| cfs_objectnode_$op_hist_count  | Same as above                                                                                                                                        |
| cfs_objectnode_$op_hist_sum    | Total time consumption of the corresponding operation request of the object node, which can be used to calculate the average latency with hist_count |
| cfs_objectnode_$op_hist_bucket | Histogram data of the corresponding request of the object node, which can be used to calculate the 95 value and 99 value of the request latency      |

## FuseClient

The monitoring metrics of the `FuseClient` module are mainly used to monitor the request volume, time consumption, cache hit rate, etc. of the interaction with the data module or metadata module, such as fileread, filewrite, etc. The details are as follows:

| Metric Name                    | Description                                                                                                                                     |
|--------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| cfs_fuseclient_$dp_count       | Total number of requests for the corresponding operation of the client, which can be used to calculate the qps                                  |
| cfs_fuseclient_$dp_hist_count  | Same as above                                                                                                                                   |
| cfs_fuseclient_$dp_hist_sum    | Total time consumption of the corresponding operation request of the client, which can be used to calculate the average latency with hist_count |
| cfs_fuseclient_$dp_hist_bucket | Histogram data of the corresponding request of the client, which can be used to calculate the 95 value of the request latency                   |

## Blobstore

### Common Metrics Items

| Label   | Description                                           |
|---------|-------------------------------------------------------|
| api     | Request API name, can configure path depth            |
| code    | Response status code                                  |
| host    | Host name, automatically obtain the current host name |
| idc     | Configuration file configuration item                 |
| method  | Request type                                          |
| service | Service name                                          |
| tag     | Custom tag                                            |
| team    | Custom team name                                      |

You can modify the service audit log configuration item to enable related metrics

| Configuration Item     | Description                                                             | Required                                                  |
|:-----------------------|:------------------------------------------------------------------------|:----------------------------------------------------------|
| idc                    | IDC name                                                                | No, it is recommended to fill in if the metric is enabled |
| service                | Module name                                                             | No, it is recommended to fill in if the metric is enabled |
| tag                    | Custom tag, such as configuring clusterid                               | No, it is recommended to fill in if the metric is enabled |
| enable_http_method     | Whether to enable status code statistics                                | No, default is off                                        |
| enable_req_length_cnt  | Whether to enable request length statistics                             | No, default is off                                        |
| enable_resp_length_cnt | Whether to enable response length statistics                            | No, default is off                                        |
| enable_resp_duration   | Whether to enable request/response interval time consumption statistics | No, default is off                                        |
| max_api_level          | Maximum API path depth                                                  | No                                                        |

```json
{
  "auditlog": {
    "metric_config": {
      "idc": "z0",
      "service": "SCHEDULER",
      "tag": "100",
      "team": "cubefs",
      "enable_http_method": true,
      "enable_req_length_cnt": true,
      "enable_resp_length_cnt": true,
      "enable_resp_duration": true,
      "max_api_level": 3
    }
  }
}
```

**service_response_code**

Request status code metrics, record request API, status code, module name and other information, generally used to calculate request error rate, request volume, etc.

```bash
# TYPE service_response_code counter
service_response_code{api="scheduler.inspect.acquire",code="200",host="xxx",idc="z0",method="GET",service="SCHEDULER",tag="100",team=""} 8.766433e+06
```

**service_request_length**

Request body length statistics metrics, generally used to calculate request bandwidth, traffic.

```bash
# TYPE service_request_length counter
service_request_length{api="clustermgr.chunk.report",code="200",host="xxx",idc="z0",method="POST",service="CLUSTERMGR",tag="",team=""} 27631
```

**service_response_length**

Request response body length statistics metrics, generally used to calculate download bandwidth, traffic.

```bash
# TYPE service_response_length counter
service_response_length{api="clustermgr.chunk.report",code="200",host="xxxx",idc="z0",method="POST",service="CLUSTERMGR",tag="",team=""} 6
```

**service_response_duration_ms**

Request response time statistics metrics, which can be used to calculate the 95 latency of the request or response.

```bash
# TYPE service_response_duration_ms histogram
service_response_duration_ms_bucket{api="clustermgr.config.get",code="200",host="xxx",idc="z0",method="GET",reqlength="",resplength="",service="CLUSTERMGR",tag="",team="",le="1"} 22
```

### Access

**blobstore_access_cache_hit_rate**

Cache hit rate, status includes four types: none (the hit value is nil), hit (hit), miss (not hit), expired (expired)

| Label   | Description                                         |
|---------|-----------------------------------------------------|
| cluster | Cluster ID                                          |
| service | Monitoring component, such as memcache, proxy, etc. |
| status  | Status, none, hit, miss, expired                    |

```bash
# TYPE blobstore_access_cache_hit_rate counter
blobstore_access_cache_hit_rate{cluster="100",service="memcache",status="hit"} 3.4829103e+13
blobstore_access_cache_hit_rate{cluster="100",service="proxy",status="hit"} 2.4991594e+07
```

**blobstore_access_unhealth**

Failure degradation metrics statistics

| Label   | Description                                      |
|---------|--------------------------------------------------|
| cluster | Cluster ID                                       |
| action  | allocate, punish, repair.msg, delete.msg         |
| host    | Failed node                                      |
| reason  | Failure reason                                   |
| module  | Degradation dimension, diskwith, volume, service |

```bash
# TYPE blobstore_access_unhealth counter
blobstore_access_unhealth{action="punish",cluster="100",host="xxx",module="diskwith",reason="Timeout"} 7763
```

**blobstore_access_download**

Download failure statistics metrics

| Label   | Description                                      |
|---------|--------------------------------------------------|
| cluster | Cluster ID                                       |
| way     | Download method, EC read or direct read (Direct) |

```bash
# TYPE blobstore_access_download counter
blobstore_access_download{cluster="100",way="Direct"} 37
blobstore_access_download{cluster="100",way="EC"} 3016
```

### Clustermgr

**blobstore_clusterMgr_chunk_stat_info**

Chunk status metrics, statistics of the total number of chunks and available chunks

| Label     | Description                   |
|-----------|-------------------------------|
| cluster   | Cluster ID                    |
| idc       | IDC                           |
| region    | Region information            |
| is_leader | Whether it is the master node |
| item      | TotalChunk, TotalFreeChunk    |

```bash
# TYPE blobstore_clusterMgr_chunk_stat_info gauge
blobstore_clusterMgr_chunk_stat_info{cluster="100",idc="z0",is_leader="false",item="TotalChunk",region="cn-south-2"} 55619
```

**blobstore_clusterMgr_disk_stat_info**

Disk status metrics

| Label     | Description                                                                         |
|-----------|-------------------------------------------------------------------------------------|
| cluster   | Cluster ID                                                                          |
| idc       | IDC                                                                                 |
| region    | Region information                                                                  |
| is_leader | Whether it is the master node                                                       |
| item      | Available, Broken, Dropped, Dropping, Expired, Readonly, Repaired, Repairing, Total |

```bash
# TYPE blobstore_clusterMgr_disk_stat_info gauge
blobstore_clusterMgr_disk_stat_info{cluster="100",idc="z0",is_leader="false",item="Available",region="cn-south-2"} 107
```

**blobstore_clusterMgr_raft_stat**

Raft status metrics

| Label     | Description                                 |
|-----------|---------------------------------------------|
| cluster   | Cluster ID                                  |
| idc       | IDC                                         |
| region    | Region information                          |
| is_leader | Whether it is the master node               |
| item      | applied_index, committed_index, peers, term |

```bash
# TYPE blobstore_clusterMgr_raft_stat gauge
blobstore_clusterMgr_raft_stat{cluster="100",is_leader="false",item="applied_index",region="cn-south-2"} 2.97061597e+08
```

**blobstore_clusterMgr_space_stat_info**

Cluster space metrics

| Label     | Description                                                               |
|-----------|---------------------------------------------------------------------------|
| cluster   | Cluster ID                                                                |
| idc       | IDC                                                                       |
| region    | Region information                                                        |
| is_leader | Whether it is the master node                                             |
| item      | FreeSpace, TotalBlobNode, TotalDisk, TotalSpace, UsedSpace, WritableSpace |

```bash
# TYPE blobstore_clusterMgr_space_stat_info gauge
blobstore_clusterMgr_space_stat_info{cluster="100",is_leader="false",item="FreeSpace",region="cn-south-2"} 1.76973072064512e+15
```

**blobstore_clusterMgr_vol_status_vol_count**

Volume status metrics

| Label     | Description                                       |
|-----------|---------------------------------------------------|
| cluster   | Cluster ID                                        |
| idc       | IDC                                               |
| region    | Region information                                |
| is_leader | Whether it is the master node                     |
| status    | active, allocatable, idle, lock, total, unlocking |

```bash
# TYPE blobstore_clusterMgr_vol_status_vol_count gauge
blobstore_clusterMgr_vol_status_vol_count{cluster="100",is_leader="false",region="cn-south-2",status="active"} 316
```

### BlobNode

**blobstore_blobnode_disk_stat**

Disk status metrics

| Label      | Description                           |
|------------|---------------------------------------|
| cluster_id | Cluster ID                            |
| idc        | IDC                                   |
| disk_id    | Disk ID                               |
| host       | Local service address                 |
| rack       | Rack information                      |
| item       | free, reserved, total_disk_size, used |

```bash
# TYPE blobstore_blobnode_disk_stat gauge
blobstore_blobnode_disk_stat{cluster_id="100",disk_id="243",host="xxx",idc="z2",item="free",rack="testrack"} 6.47616868352e+12
```

### Scheduler

**scheduler_task_shard_cnt**

Task shard count

| Label      | Description                                                                      |
|------------|----------------------------------------------------------------------------------|
| cluster_id | Cluster ID                                                                       |
| kind       | success, failed                                                                  |
| task_type  | Task type, delete, shard_repair, balance, disk_drop, disk_repair, manual_migrate |

```bash
# TYPE scheduler_task_shard_cnt counter
scheduler_task_shard_cnt{cluster_id="100",kind="failed",task_type="delete"} 7.4912551e+07
```

**scheduler_task_reclaim**

Task redistribution metrics

| Label      | Description                                                |
|------------|------------------------------------------------------------|
| cluster_id | Cluster ID                                                 |
| kind       | success, failed                                            |
| task_type  | Task type, balance, disk_drop, disk_repair, manual_migrate |

```bash
# TYPE scheduler_task_reclaim counter
scheduler_task_reclaim{cluster_id="100",kind="success",task_type="balance"} 0
```

**scheduler_task_data_size**

Task data migration metrics, unit: byte

| Label      | Description                                                |
|------------|------------------------------------------------------------|
| cluster_id | Cluster ID                                                 |
| kind       | success, failed                                            |
| task_type  | Task type, balance, disk_drop, disk_repair, manual_migrate |

```bash
# TYPE scheduler_task_data_size counter
scheduler_task_data_size{cluster_id="100",kind="success",task_type="balance"} 0
```

**scheduler_task_cnt**

Task count metrics

| Label       | Description                                                |
|-------------|------------------------------------------------------------|
| cluster_id  | Cluster ID                                                 |
| kind        | success, failed                                            |
| task_type   | Task type, balance, disk_drop, disk_repair, manual_migrate |
| task_status | finishing, preparing, worker_doing                         |

```bash
# TYPE scheduler_task_cnt gauge
scheduler_task_cnt{cluster_id="100",kind="success",task_status="finishing",task_type="balance"} 0
```

**scheduler_task_cancel**

Task cancellation metrics

| Label      | Description                                                |
|------------|------------------------------------------------------------|
| cluster_id | Cluster ID                                                 |
| kind       | success, failed                                            |
| task_type  | Task type, balance, disk_drop, disk_repair, manual_migrate |

```bash
# TYPE scheduler_task_cancel counter
scheduler_task_cancel{cluster_id="100",kind="success",task_type="balance"} 0
```

**scheduler_free_chunk_cnt_range**

Cluster idle chunk statistics

| Label      | Description |
|------------|-------------|
| cluster_id | Cluster ID  |
| idc        | IDC         |
| rack       | Rack        |

```bash
# TYPE scheduler_free_chunk_cnt_range histogram
scheduler_free_chunk_cnt_range_bucket{cluster_id="100",idc="z0",rack="testrack",le="5"} 0
```

**kafka_topic_partition_consume_lag**

Kafka consumer latency

| Label       | Description  |
|-------------|--------------|
| cluster_id  | Cluster ID   |
| module_name | Service name |
| partition   | Partition    |
| topic       | Topic        |

```bash
# TYPE kafka_topic_partition_consume_lag gauge
kafka_topic_partition_consume_lag{cluster_id="100",module_name="SCHEDULER",partition="0",topic="dg_blob_delete"} 1.488541e+06
```

**kafka_topic_partition_offset**

| Label       | Description             |
|-------------|-------------------------|
| cluster_id  | Cluster ID              |
| module_name | Service name            |
| partition   | Partition               |
| topic       | Topic                   |
| type        | consume, newest, oldest |

```bash
# TYPE kafka_topic_partition_offset gauge
kafka_topic_partition_offset{cluster_id="100",module_name="SCHEDULER",partition="0",topic="dg_blob_delete",type="consume"} 5.37820629e+08
```

### Proxy

**blobstore_proxy_volume_status**

Volume status metrics

| Label    | Description                  |
|----------|------------------------------|
| cluster  | Cluster ID                   |
| idc      | IDC                          |
| codemode | Volume mode                  |
| type     | total_free_size, volume_nums |

```bash
# TYPE blobstore_proxy_volume_status gauge
blobstore_proxy_volume_status{cluster="100",codemode="EC15P12",idc="z0",service="PROXY",type="total_free_size"} 9.01538397118e+11
```

**blobstore_proxy_cache**

Volume and disk cache status metrics

| Label   | Description                              |
|---------|------------------------------------------|
| cluster | Cluster ID                               |
| service | Service name, disk, volume               |
| name    | Cache layer, memcache, diskv, clustermgr |
| action  | Cache value, hit, miss, expired, error   |

```bash
# TYPE blobstore_proxy_cache counter
blobstore_proxy_cache{action="hit",cluster="100",name="clustermgr",service="disk"} 6345
blobstore_proxy_cache{action="hit",cluster="100",name="memcache",service="volume"} 2.3056289e+07
blobstore_proxy_cache{action="miss",cluster="100",name="diskv",service="volume"} 230595

```
