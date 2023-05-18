# Proxy Configuration

`Proxy` is the proxy module, mainly responsible for message forwarding, volume allocation and renewal proxy, caching, etc., mainly to alleviate the service pressure of clustermgr.

The configuration of the proxy is based on the [public configuration](./base.md), and the following configuration instructions mainly focus on the private configuration of the proxy.

## Configuration Instructions

### Key Configuration

::: tip Note
Starting from version v3.3.0, Proxy node supports caching of volume and disk information.
:::

| Configuration Item     | Description                                                                                                                | Required |
|:-----------------------|:---------------------------------------------------------------------------------------------------------------------------|:---------|
| Public configuration   | Such as service port, running logs, audit logs, etc., refer to the [Basic Service Configuration](./base.md) section        | Yes      |
| host                   | Current host information, used for reporting to clustermgr for service discovery, for example, http://service_ip:bind_port | Yes      |
| cluster_id             | Cluster ID                                                                                                                 | Yes      |
| idc                    | IDC ID                                                                                                                     | Yes      |
| retain_interval_s      | Renewal interval cycle, used in conjunction with the volume expiration time set by cm                                      | Yes      |
| init_volume_num        | The number of volumes requested from clustermgr when starting up, set according to the size of the cluster                 | Yes      |
| default_alloc_vols_num | The number of volumes requested from clustermgr each time, set according to the size of the cluster                        | Yes      |
| mq                     | Kafka producer configuration                                                                                               | Yes      |
| diskv_base_path        | Persistent path for caching volume and disk information                                                                    | Yes      |

### All Configuration

```json
{
  "heartbeat_interval_s": "Interval for sending heartbeat to Clustermgr. The heartbeat time is heartbeatTicks * tickInterval",
  "heartbeat_ticks": "Used in conjunction with heartbeat_interval_s",
  "expires_ticks": "",
  "diskv_base_path": "Local persistent path for caching information",
  "volume_capacity": "Capacity of memory volume information, default is 1M",
  "volume_expiration_seconds": "Expiration time of memory volume information, default is 0, which means no expiration",
  "disk_capacity": "Capacity of memory disk information, default is 1M",
  "disk_expiration_seconds": "Expiration time of memory disk information, default is 0, which means no expiration",
  "clustermgr": {
    "hosts": "List of clustermgr hosts, [`http://ip:port`,`http://ip1:port`]",
    "rpc": "Refer to the rpc LbClient configuration introduction"
  },
  "bid_alloc_nums": "Maximum number of bids that access can request from proxy each time",
  "host": "Current host information, used for reporting to clustermgr for service discovery, for example, http://service_ip:bind_port",
  "cluster_id": "Cluster ID",
  "idc": "IDC ID",
  "retain_interval_s": "Renewal interval cycle, used in conjunction with the volume expiration time set by cm",
  "init_volume_num": "The number of volumes requested from clustermgr when starting up, set according to the size of the cluster",
  "default_alloc_vols_num": "The number of volumes requested from clustermgr each time, access allocation requests can trigger",
  "metric_report_interval_s": "Time interval for proxy to report running status to Prometheus",
  "mq": {
    "blob_delete_topic": "Topic name for delete messages",
    "shard_repair_topic": "Topic name for repair messages",
    "shard_repair_priority_topic": "Messages with high-priority repair will be delivered to this topic, usually when a bid has missing chunks in multiple chunks",
    "msg_sender": {
      "kafka": "Refer to the Kafka producer usage configuration introduction"
    }
  }
}
```

### Example Configuration

```json
{
  "bind_addr": ":9600",
  "host": "http://127.0.0.1:9600",
  "idc": "z0",
  "cluster_id": 1,
  "default_alloc_vols_num" : 2,
  "init_volume_num": 4,
  "diskv_base_path": "./run/cache",
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
