# Access 配置

> `access` 是接入模块，主要负责数据上传、下载、删除等。access的配置是基于[公有配置](./base.md)，以下配置说明主要针对于access的私有配置。

## 配置说明

### 一级配置

| 配置项              | 说明                            | 是否必须                |
|:-----------------|:------------------------------|:--------------------|
| 公共配置项            | 参考[基础服务配置](./base.md)章节       | 是                   |
| service_register | [服务注册信息](#service_register示例) | 是，配置后可用于access的服务发现 |
| limit            | [限速配置](#limit示例)              | 否，单机限速配置            |
| stream           | access 主要配置项                  | 是，参考下列二级配置选项        |

### 二级stream配置

| 配置项                       | 说明                 | 是否必须                     |
|:--------------------------|:-------------------|:-------------------------|
| idc                       | 服务的IDC             | 是                        |
| max_blob_size             | 文件分段Blob大小         | 否，默认为4MB                 |
| mem_pool_size_classes     | 文件读写内存控制           | 否                        |
| encoder_concurrency       | EC编解码并发数           | 否，默认1000                 |
| encoder_enableverify      | EC编解码是否启用验证        | 否，默认开启                   |
| min_read_shards_x         | EC读取并发多下载几个shards  | 否，默认1，越大容错率越高，但带宽也越高     |
| shard_crc_disabled        | 是否验证blobnode的数据crc | 否，默认开启验证                 |
| disk_punish_interval_s    | 临时标记坏盘间隔时间         | 否，默认60s                  |
| service_punish_interval_s | 临时标记坏服务间隔时间        | 否，默认60s                  |
| blobnode_config           | blobnode rpc 配置    | 参考rpc配置章节[rpc](./rpc.md) |
| proxy_config              | proxy rpc 配置       | 参考rpc配置章节[rpc](./rpc.md) |
| cluster_config            | cluster 主要配置       | 是，参考下列三级配置选项             |

### 三级cluster配置

| 配置项                      | 说明                   | 是否必须                        |
|:-------------------------|:---------------------|:----------------------------|
| region                   | region 信息            | 是，配置后不要变更                   |
| region_magic             | 用于编码文件Location的crc字段 | 是，配置后不要变更，发生变更后Location全部失效 |
| consul_agent_addr        | 集群信息的consul地址        | 是                           |
| cluster_reload_secs      | 集群信息同步间隔             | 否，默认3s                      |
| service_reload_secs      | 服务信息同步间隔             | 否，默认3s                      |
| clustermgr_client_config | clustermgr rpc 配置    | 参考rpc配置示例[rpc](./rpc.md)    |


## 配置示例

### service_register示例

* consul_addr，access 服务注册的consul地址
* service_ip，access 服务bind ip
* node，主机名
```json
{
    "consul_addr": "127.0.0.1:8500",
    "service_ip": "127.0.0.1",
    "node": "access-node1"
}
```

### limit示例

* reader_mbps，单机下载带宽（MB/s）
* writer_mbps, 单机上传带宽（MB/s）
* name_rps, 各接口的rps限制数
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

### mem_pool_size_classes示例

* key, 内存分配阶梯
* value, 限制多少数量，0表示不限制,（当前access不启用数量限制）
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

### 完整示例

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
        "node": "access-node1"
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
