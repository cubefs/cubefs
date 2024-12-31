# BlobNode Configuration

`BlobNode` is a single-machine storage engine module, mainly responsible for organizing data to disk, reading data from disk, deleting data from disk, and executing background tasks.

BlobNode configuration is based on the [public configuration](./base.md), and the following configuration instructions mainly apply to private configuration for BlobNode.

## Configuration Instructions

### Key Configuration

| Configuration Item   | Description                                                                                                       | Required |
|:---------------------|:------------------------------------------------------------------------------------------------------------------|:---------|
| Public Configuration | Such as server ports, running logs, and audit logs, refer to the [Basic Service Configuration](./base.md) section | Yes      |
| disks                | List of disk paths to register                                                                                    | Yes      |
| disable_sync         | Whether to disable disk sync. A value of true means that sync is disabled, which can improve write performance.   | No       |
| rack                 | Rack number. This field is required when clustermgr opens rack isolation.                                         | No       |
| host                 | Blobnode service address for this machine, which needs to be registered with clustermgr                           | Yes      |
| must_mount_point     | Verify whether the registered path is a mount point. It is recommended to enable this in production environments. | No       |
| data_qos             | Data QoS hierarchical flow control. It is recommended to enable this in production environments.                  | No       |
| meta_config          | Metadata-related configuration, including the cache size of RocksDB.                                              | No       |
| clustermgr           | Clustermgr service address information, etc.                                                                      | Yes      |

### Complete Configuration

```json
{
  "cluster_id": "cluster ID",
  "idc": "IDC",
  "rack": "rack",
  "host": "service address IP:port",
  "dropped_bid_record": {
    "dir": "directory for dropped bid records",
    "chunkbits": "n: file size, 2^n bytes"
  },
  "disks": [
    {
      "auto_format": "whether to automatically create directories",
      "disable_sync": "whether to disable disk sync",
      "path": "data storage directory",
      "max_chunks": "maximum number of chunks per disk"
    },
    {
      "auto_format": "same as above",
      "disable_sync": "same as above",
      "path": "same as above: each disk needs to be configured and registered with clustermgr",
      "max_chunks": "same as above"
    }
  ],
  "disk_config": {
    "disk_reserved_space_B": "reserved space per disk. The available space will be reduced by this value. Default is 60GB.",
    "compact_reserved_space_B": "reserved space for compression per disk. Default is 20GB.",
    "chunk_protection_M": "protection period for released chunks. If the last modification time is within this period, release is not allowed.",
    "chunk_compact_interval_S": "interval for the compression task",
    "chunk_clean_interval_S": "interval for the chunk cleaning task",
    "chunk_gc_create_time_protection_M": "protection period for the creation time of chunks during cleaning",
    "chunk_gc_modify_time_protection_M": "protection period for the modification time of chunks during cleaning",
    "disk_usage_interval_S": "interval for updating disk space usage",
    "disk_clean_trash_interval_S": "interval for cleaning disk garbage data",
    "disk_trash_protection_M": "protection period for disk garbage data",
    "allow_clean_trash": "whether to allow cleaning garbage",
    "disable_modify_in_compacting": "whether to disallow chunk modification during compression",
    "compact_min_size_threshold": "minimum chunk size for compression",
    "compact_trigger_threshold": "chunk size at which compression is triggered",
    "compact_empty_rate_threshold": "hole rate at which chunks can be compressed",
    "need_compact_check": "whether to check the consistency of data before and after compression",
    "allow_force_compact": "whether to allow forced compression through the interface, bypassing compression conditions",
    "compact_batch_size": "number of bids per batch for compression",
    "metric_report_interval_S": "interval for metric reporting",
    "set_default_switch": "whether to set switch.suggest you set it to true,will set: need_compact_check,allow_force_compact,allow_clean_trash",
    "must_mount_point": "whether the data storage directory must be a mount point",
    "write_thread_cnt": "limit the number of write threads，default 4",
    "read_thread_cnt": "limit the number of read threads，default 4",
    "write_queue_depth": "write queue depth, default 32",
    "read_queue_depth": "read queue depth, default 64",
    "data_qos": {
      "read_mbps": "per disk normal read IO bandwidth",
      "write_mbps": "per disk normal write IO bandwidth",
      "background_mbps": "per disk background IO bandwidth"
    }
  },
  "meta_config": {
    "meta_root_prefix": "unified meta data storage directory configuration. Can be configured to an SSD disk to improve metadata read/write speed. Not configured by default.",
    "support_inline": "whether to enable inline writing of small files to metadata storage RocksDB",
    "tinyfile_threshold_B": "threshold for small files, can be set to less than or equal to 128k",
    "sync": "whether to enable disk sync",
    "cache_size": "lru cache size",
    "batch_process_count": "number of batch processing requests for metadata, including deletion and writing",
    "write_pri_ratio": "ratio of write requests for metadata batch processing. The specific number is batch_process_count*write_pri_ratio"
  },
  "clustermgr": {
    "hosts": "clustermgr service address"
  },
  "blobnode": {
    "client_timeout_ms": "timeout for blobnode client used in background tasks"
  },
  "scheduler": {
    "host_sync_interval_ms": "backend node synchronization time for scheduler client used in background tasks"
  },
  "chunk_protection_period_S": "protection period for expired epoch chunks based on creation time",
  "put_qps_limit_per_disk": "concurrency control for single-disk writes",
  "get_qps_limit_per_disk": "concurrency control for single-disk reads",
  "get_qps_limit_per_key": "concurrency control for reads of a single shard",
  "delete_qps_limit_per_disk": "concurrency control for single-disk deletions",
  "shard_repair_concurrency": "concurrency control for background task shard repair",
  "flock_filename": "process file lock path"
}
```

### Example Configuration

```json
{
  "bind_addr": ":8889",
  "log": {
    "level": 2
  },
  "cluster_id": 10001,
  "idc": "bjht",
  "rack": "HT02-B11-F4-402-0406",
  "host": "http://127.0.0.1:8889",
  "dropped_bid_record": {
    "dir": "/home/service/ebs-blobnode/_package/dropped_bids/",
    "chunkbits": 29
  },
  "disks": [
    {"auto_format": true,"disable_sync": true,"path": "/home/service/var/data1"},
    {"auto_format": true,"disable_sync": true,"path": "/home/service/var/data2"}
  ],
  "disk_config": {
    "set_default_switch": true,
    "must_mount_point": true,
    "data_qos": {
      "read_mbps": 100,
      "write_mbps": 60,
      "background_mbps": 20
    }
  },
  "meta_config": {
    "sync": false
  },
  "clustermgr": {
    "hosts": [
      "http://10.39.30.78:9998",
      "http://10.39.32.224:9998",
      "http://10.39.32.234:9998"
    ],
    "transport_config": {
      "max_conns_per_host": 4,
      "auth": {
        "enable_auth": false,
        "secret": "b2e5e2ed-6fca-47ce-bfbc-5e8f0650603b"
      }
    }
  },
  "blobnode": {
    "client_timeout_ms": 5000
  },
  "scheduler": {
    "host_sync_interval_ms": 3600000
  },
  "chunk_protection_period_S": 600,
  "delete_qps_limit_per_disk": 64,
  "shard_repair_concurrency": 100,
  "flock_filename": "/home/service/ebs-blobnode/_package/run/blobnode.0.flock",
  "auditlog": {
    "logdir": "/home/service/ebs-blobnode/_package/run/auditlog/ebs-blobnode",
    "chunkbits": 29,
    "log_file_suffix": ".log",
    "backup": 10,
    "filters": [
      {"should": {"match": {"path": ["list", "metrics", "/shard/get/"]}}}
    ],
    "metric_config": {
      "idc": "bjht",
      "team": "ocs",
      "set_default_switch": true
    }
  }
}
```
