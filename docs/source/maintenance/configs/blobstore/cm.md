# Clustermgr Configuration

`Clustermgr` is a cluster management module, mainly responsible for disk registration, generation and allocation of logical volumes, and management of cluster resources (such as disks, nodes, and storage space units).

Clustermgr configuration is based on the [public configuration](./base.md), and the following configuration instructions mainly apply to private configuration for Clustermgr.

## Configuration Instructions

### Key Configuration

| Configuration Item   | Description                                                                                                                                                                                                      | Required |
|:---------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:---------|
| Public Configuration | Such as server ports, running logs, and audit logs, refer to the [Basic Service Configuration](./base.md) section                                                                                                | Yes      |
| chunk_size           | The size of each chunk in blobnode, that is, the size of the created file                                                                                                                                        | Yes      |
| cluster_id           | Cluster ID                                                                                                                                                                                                       | Yes      |
| idc                  | IDC number                                                                                                                                                                                                       | Yes      |
| region               | Region name                                                                                                                                                                                                      | Yes      |
| db_path              | Metadata DB storage path. In production environments, it is recommended to store data on an SSD disk.                                                                                                            | Yes      |
| code_mode_policies   | Encoding mode configuration, refer to the encoding mode detailed configuration, specific encoding information can refer to [How to Use Erasure Coding Storage System](../../../user-guide/blobstore.md) appendix | Yes      |
| raft_config          | Detailed Raft configuration                                                                                                                                                                                      | Yes      |
| disk_mgr_config      | Detailed disk management configuration                                                                                                                                                                           | Yes      |
| host_aware           | Host awareness                                                                                                                                                                                                   | Yes      |
| rack_aware           | Rack awareness                                                                                                                                                                                                   | Yes      |
| volume_mgr_config    | Volume management module                                                                                                                                                                                         | No       |

### Complete Configuration

```json
{
  "cluster_id": "Cluster ID",
  "idc": "IDC ID",
  "region": "Region name. There can be multiple clusters under one region. This can be used in conjunction with access to select multiple regions and clusters when writing.",
  "unavailable_idc": "Unavailable IDC ID",
  "readonly": "Whether it is read-only",
  "db_path": "Default path for storing metadata DB. The corresponding DB data will be automatically created in this directory if there is no additional configuration for the DB.",
  "normal_db_path": "Path for common DB",
  "normal_db_option": "Common DB option configuration, mainly for tuning rocksdb",
  "kv_db_path": "Path for storing kv DB",
  "kv_db_option": "KV DB option configuration, mainly for tuning rocksdb",
  "code_mode_policies": [
    {
      "mode_name": "Encoding name",
      "min_size": "Minimum size of writable object",
      "max_size": "Maximum size of writable object",
      "size_ratio": "The proportion occupied by this mode",
      "enable": "Whether it is enabled"
    }],
  "raft_config": {
    "raft_db_path": "Path for raft DB",
    "snapshot_patch_num": "A snapshot of a DB is divided into multiple patches. The snapshot data of a DB is very large and needs to be divided into multiple patches to be sent.",
    "raft_db_option": "Mainly for tuning rocksdb",
    "server_config": {
      "nodeId": "Raft node ID",
      "listen_port": "Raft dedicated port",
      "raft_wal_dir": "WAL log path",
      "raft_wal_sync": "Whether to flush to disk immediately",
      "tick_interval": "Heartbeat interval",
      "heartbeat_tick": "Heartbeat clock, default is 1",
      "election_tick": "Election clock, it is recommended to set it to 5*heartbeat_tick, default is 5",
      "max_snapshots": "Maximum concurrency of snapshots, default is 10",
      "snapshot_timeout":"Snapshot timeout",
      "propose_timeout": "Timeout for proposing messages"
    },

    "raft_node_config": {
      "flush_num_interval": "Number of logs that trigger flush",
      "FlushTimeIntervalS": "Flush cycle interval",
      "truncate_num_interval": "The leader retains the maximum number of log entries. The number of log entries loaded when the service starts can also be understood as the difference between the log entries of the leader and the follower. If it exceeds this value, log synchronization needs to go through snapshot synchronization, so this value is generally kept above 100,000.",
      "node_protocol": "Raft node synchronization protocol, generally http:// protocol",
      "members": [{
        "id":"Node ID",
        "host":"Raft host address, ip:Raft port",
        "learner": "Whether the node participates in the election of the main node",
        "node_host":"Service host address, ip:Service port"
      }],
      "apply_flush": "Whether to flush"
    }
  },
  "volume_mgr_config": {
    "blob_node_config": "See rpc configuration",
    "volume_db_path": "Volume data DB path",
    "volume_db_option": "Option configuration",
    "retain_time_s": "Renewal time, used in conjunction with the renewal time of the proxy",
    "retain_threshold": "The health of the volume that can be renewed. The health segment of the volume must be greater than this value to be renewed",
    "flush_interval_s": "Flush time interval",
    "check_expired_volume_interval_s": "Time interval for checking whether the volume has expired",
    "volume_slice_map_num": "ConcurrentMap used for volume management in cm, used to improve the performance of volume read and write. This value determines how many maps all volumes are divided into for management",
    "apply_concurrency": "Concurrency of applying wal logs",
    "min_allocable_volume_count": "Minimum number of allocatable volumes",
    "allocatable_disk_load_threshold": "Load of the corresponding disk that the volume can be allocated to"
  },
  "disk_mgr_config": {
    "refresh_interval_s": "Interval for refreshing disk status of the current cluster",
    "host_aware": "Host awareness. Whether to allocate volumes on the same machine when allocating volumes. Host isolation must be configured in production environment",
    "heartbeat_expire_interval_s": "Interval for heartbeat expiration, for the heartbeat time reported by BlobNode",
    "rack_aware": "Rack awareness. Whether to allocate volumes on the same rack when allocating volumes. Rack isolation is configured based on the storage environment conditions",
    "flush_interval_s": "Flush time interval",
    "apply_concurrency": "Concurrency of application",
    "blob_node_config": "",
    "ensure_index": "Used to establish disk index"
  },

  "cluster_report_interval_s": "Interval for reporting to consul",
  "consul_agent_addr": "Consul address",
  "heartbeat_notify_interval_s": "Interval for heartbeat notification, used to process the disk information reported by BlobNode regularly. This time should be smaller than the time interval reported by BlobNode to avoid disk heartbeat timeout expiration",
  "max_heartbeat_notify_num": "Maximum number of heartbeat notifications",
  "chunk_size": "Size of each chunk in BlobNode, that is, the size of the created file"
}
```


### Example Configuration

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