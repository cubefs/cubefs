# Access 配置

`Access` 是接入模块，主要负责数据上传、下载、删除等。

access的配置是基于[公有配置](./base.md)，以下配置说明主要针对于access的私有配置。

## 配置说明

### 一级配置

| 配置项              | 说明                            | 必需                  |
|:-----------------|:------------------------------|:--------------------|
| 公共配置项            | 参考[基础服务配置](./base.md)章节       | 是                   |
| service_register | [服务注册信息](#service_register示例) | 是，配置后可用于access的服务发现 |
| limit            | [限速配置](#limit示例)              | 否，单机限速配置            |
| stream           | access 主要配置项                  | 是，参考下列二级配置选项        |

### 二级stream配置

| 配置项                       | 说明                 | 必需                       |
|:--------------------------|:-------------------|:-------------------------|
| idc                       | 服务的IDC             | 是                        |
| max_blob_size             | 文件分段Blob大小         | 否，默认为4MB                 |
| mem_pool_size_classes     | 文件读写内存控制           | 否                        |
| code_mode_put_quorums     | 配置修改 put quorums | 否 |
| code_mode_get_ordered     | codemode 下载时保持 shards 顺序 | 否 |
| encoder_concurrency       | EC编解码并发数           | 否，默认1000                 |
| encoder_enableverify      | EC编解码是否启用验证        | 否，默认开启                   |
| min_read_shards_x         | EC读取并发多下载几个shards  | 否，默认1，越大容错率越高，但带宽也越高     |
| read_data_only_timeout_ms | 触发直读的超时时间  | 否，默认3000ms，直读超时后再EC修复读     |
| shard_crc_write_disable   | 是否验证write blobnode的数据crc | 否，默认开启验证                 |
| shard_crc_read_enable     | 是否验证read blobnode的数据crc | 否，默认不开启验证                 |
| disk_punish_interval_s    | 临时标记坏盘间隔时间              | 否，默认60s                  |
| volume_punish_interval_s  | 临时标记坏卷间隔时间              | 否，默认60s                  |
| service_punish_interval_s | 临时标记坏服务间隔时间            | 否，默认60s                  |
|shardnode_punish_interval_s| 临时标记坏 shardnode 服务间隔时间 | 否，默认60s                  |
| alloc_retry_times         | 申请卷重试次数         | 否, 默认 3 |
| alloc_retry_interval_ms   | 申请卷重试间隔时间     | 否, 默认 100ms |
| shardnode_retry_times     | shardnode 接口重试次数 | 否, 默认 3 |
|shardnode_retry_interval_ms| Shardnode 重试间隔时间 | 否, 默认 200ms |
| blobnode_config           | blobnode rpc 配置    | 参考rpc配置章节[rpc](./rpc.md) |
| proxy_config              | proxy rpc 配置       | 参考rpc配置章节[rpc](./rpc.md) |
| shardnode_config          | Shardnode rpc 配置   | 参考 shardnode 配置章节[rpc2](./rpc2.md) |
| cluster_config            | cluster 主要配置       | 是，参考下列三级配置选项             |

### 三级cluster配置

| 配置项                      | 说明                   | 必需                          |
|:-------------------------|:---------------------|:----------------------------|
| region                   | region 信息            | 是，配置后不要变更                   |
| region_magic             | 用于编码文件Location的crc字段 | 是，配置后不要变更，发生变更后Location全部失效 |
| consul_agent_addr        | 集群信息的consul地址        | 否                           |
| consul_token             | 集群信息的consul token      | 否                           |
| consul_token_file        | 集群信息的consul token file | 否                           |
| cluster_reload_secs      | 集群信息同步间隔             | 否，默认3s                      |
| service_reload_secs      | 服务信息同步间隔             | 否，默认3s                      |
| shard_reload_secs        | shardnode信息同步间隔        | 否，默认3s                      |
| load_disk_interval_s     | 坏盘信息同步间隔             | 否，默认300s                    |
| volume_memcache_size     | 卷缓存数大小                 | 否，默认 1048576                |
| volume_memcache_punish_size   | punish 卷缓存数大小     | 否，默认 1024                   |
| volume_memcache_expiration_ms | 卷缓存过期时间          | 否，默认 120000                 |
| clustermgr_client_config | clustermgr rpc 配置    | 参考rpc配置示例[rpc](./rpc.md)    |


## 配置示例

### service_register示例

::: tip 提示
v3.2.1版本开始支持`health_port`
:::

* consul_addr，access 服务注册的consul地址
* service_ip，access 服务bind ip
* node，主机名
* health_port，consul的健康检查端口范围
```json
{
    "consul_addr": "127.0.0.1:8500",
    "service_ip": "127.0.0.1",
    "node": "access-node1",
    "health_port": [9700, 9799]
}
```

### limit示例

* reader_mbps，单机上传带宽（MB/s）
* writer_mbps, 单机下载带宽（MB/s）
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
    "reader_mbps": 100,
    "writer_mbps": 1000
}
```

### mem_pool_size_classes示例

* key, 内存分配阶梯
* value, 限制多少数量，-1表示不限制,（当前access不启用数量限制）
```json
{
    "2048": -1,
    "65536": -1,
    "524288": -1,
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
        "maxbackups": 7,
        "compress": true
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
        "reader_mbps": 100,
        "writer_mbps": 1000
    },
    "stream": {
        "idc": "idc",
        "max_blob_size": 4194304,
        "mem_pool_size_classes": {
            "2048": -1,
            "65536": -1,
            "524288": -1,
            "2097152": 10240,
            "8389632": 4096,
            "16777216": 1024,
            "33554432": 512,
            "67108864": 64
        },
        "code_mode_put_quorums": {
            "1": 24,
            "2": 11,
            "3": 34,
            "4": 14
        },
        "code_mode_get_ordered": {
            "12": true,
            "15": true
        },
        "alloc_retry_times": 3,
        "alloc_retry_interval_ms": 100,
        "encoder_concurrency": 1000,
        "encoder_enableverify": true,
        "min_read_shards_x": 1,
        "read_data_only_timeout_ms": 3000,
        "shard_crc_write_disable": false,
        "shard_crc_read_enable": false,
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
