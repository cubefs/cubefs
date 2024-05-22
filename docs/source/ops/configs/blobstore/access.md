# Access Configuration

`Access` is the access module, mainly responsible for data upload, download, deletion, etc.

Access configuration is based on the [public configuration](./base.md), and the following configuration instructions mainly apply to private configuration for Access.

## Configuration Instructions

### First-Level Configuration

| Configuration Item         | Description                                                     | Required                                                             |
|:---------------------------|:----------------------------------------------------------------|:---------------------------------------------------------------------|
| Public Configuration Items | Refer to the [Basic Service Configuration](./base.md) section   | Yes                                                                  |
| service_register           | [Service registration information](#service_register)           | Yes, can be used for service discovery in Access after configuration |
| limit                      | [Rate limiting configuration](#limit)                | No, single-machine rate limiting configuration                       |
| stream                     | Main Access configuration item                                  | Yes, refer to the following second-level configuration options       |

### Second-Level Stream Configuration

| Configuration Item        | Description                                              | Required                                                                                                    |
|:--------------------------|:---------------------------------------------------------|:------------------------------------------------------------------------------------------------------------|
| idc                       | IDC for the service                                      | Yes                                                                                                         |
| max_blob_size             | File segment blob size                                   | No, default is 4MB                                                                                          |
| mem_pool_size_classes     | Memory control for file read/write                       | No                                                                                                          |
| encoder_concurrency       | EC encoding/decoding concurrency                         | No, default is 1000                                                                                         |
| encoder_enableverify      | Whether to enable EC encoding/decoding verification      | No, default is enabled                                                                                      |
| min_read_shards_x         | Number of shards to download concurrently for EC reading | No, default is 1. The larger the number, the higher the fault tolerance, but also the higher the bandwidth. |
| shard_crc_disabled        | Whether to verify the data CRC of the blobnode           | No, default is enabled                                                                                      |
| disk_punish_interval_s    | Interval for temporarily marking a bad disk              | No, default is 60s                                                                                          |
| service_punish_interval_s | Interval for temporarily marking a bad service           | No, default is 60s                                                                                          |
| blobnode_config           | Blobnode RPC configuration                               | Refer to the RPC configuration section [rpc](./rpc.md)                                                      |
| proxy_config              | Proxy RPC configuration                                  | Refer to the RPC configuration section [rpc](./rpc.md)                                                      |
| cluster_config            | Main cluster configuration                               | Yes, refer to the following third-level configuration options                                               |

### Third-Level Cluster Configuration

| Configuration Item       | Description                                    | Required                                                                               |
|:-------------------------|:-----------------------------------------------|:---------------------------------------------------------------------------------------|
| region                   | Region information                             | Yes, do not change after configuration                                                 |
| region_magic             | CRC field used for encoding file location      | Yes, do not change after configuration. If changed, all locations will be invalidated. |
| consul_agent_addr        | Consul address for cluster information         | Yes                                                                                    |
| cluster_reload_secs      | Interval for synchronizing cluster information | No, default is 3s                                                                      |
| service_reload_secs      | Interval for synchronizing service information | No, default is 3s                                                                      |
| clustermgr_client_config | Clustermgr RPC configuration                   | Refer to the RPC configuration example [rpc](./rpc.md)                                 |

## Configuration Example

### service_register

::: tip Note
Support for `health_port` began with version v3.2.1.
:::

* consul_addr: Consul address for Access service registration
* service_ip: Access service bind IP
* node: Hostname
* health_port: Health check port range for Consul
```json
{
    "consul_addr": "127.0.0.1:8500",
    "service_ip": "127.0.0.1",
    "node": "access-node1",
    "health_port": [9700, 9799]
}
```

### limit

* reader_mbps: Single-machine download bandwidth (MB/s)
* writer_mbps: Single-machine upload bandwidth (MB/s)
* name_rps: RPS limit for each interface

```json
{
    "name_rps": {
        "alloc": 0,
        "put": 100,
        "putat": 0,
        "get": 0,
        "delete": 0,
        "sign": 0
    },
    "reader_mbps": 1000,
    "writer_mbps": 200
}
```

### mem_pool_size_classes

* key: Memory allocation ladder
* value: Limit on the number of items, 0 means no limit (Access currently does not enable quantity limits)
```json
{
    "2048": 0,
    "65536": 0,
    "524288": 0,
    "2097152": 10240,
    "8389632": 4096,
    "16777216": 1024,
    "33554432": 512,
    "67108864": 64
}
```

### Complete Example

```json
{
    "max_procs": 0,
    "shutdown_timeout_s": 30,
    "log": {
        "level": "info",
        "filename": "./run/logs/access.log",
        "maxsize": 1024,
        "maxage": 7,
        "maxbackups": 7
    },
    "bind_addr": ":9500",
    "service_register": {
        "consul_addr": "127.0.0.1:8500",
        "service_ip": "127.0.0.1",
        "node": "access-node1",
        "health_port": [9700, 9799]
    },
    "limit": {
        "name_rps": {
           "put": 100
        },
        "reader_mbps": 1000,
        "writer_mbps": 200
    },
    "stream": {
        "idc": "idc",
        "max_blob_size": 4194304,
        "mem_pool_size_classes": {
            "2048": 0,
            "65536": 0,
            "524288": 0,
            "2097152": 10240,
            "8389632": 4096,
            "16777216": 1024,
            "33554432": 512,
            "67108864": 64
        },
        "encoder_concurrency": 1000,
        "encoder_enableverify": true,
        "min_read_shards_x": 1,
        "shard_crc_disabled": false,
        "cluster_config": {
            "region": "region",
            "region_magic": "region",
            "cluster_reload_secs": 3,
            "service_reload_secs": 3,
            "clustermgr_client_config": {
                "client_timeout_ms": 3000,
                "transport_config": {
                    "auth": {
                        "enable_auth": true,
                        "secret": "secret key"
                    },
                    "dial_timeout_ms": 2000
                }
            },
            "consul_agent_addr": "127.0.0.1:8500"
        },
        "disk_punish_interval_s": 60,
        "service_punish_interval_s": 60,
        "blobnode_config": {
            "client_timeout_ms": 10000
        },
        "proxy_config": {
            "client_timeout_ms": 5000
        }
    }
}
```
