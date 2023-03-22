# 配额管理
::: warning 注意
配额管理为release-3.2.1版本新增feature
:::

## 目录文件数目配额
限制单个目录下子文件数或者目录数，避免文件数过多导致父目录所在的meta partition内存占用过大，引发MetaNode节点内存使用不均衡，易导致MetaNode出现OOM。

- 每个目录的默认孩子数quota为2千万，可以配置，最小值为1百万，无上限。当单个目录下创建的孩子超过quota，则创建失败。 
- 配置的quota值对整个集群生效，持久化在master。 
- 完整支持需要Client，Metanode，Master均升级到3.2.1版本。

### 设置配额
```bash
curl -v "http://192.168.0.11:17010/setClusterInfo?dirQuota=20000000"
```

::: tip 提示
`192.168.0.11`为Master的ip地址，下同
:::

| 参数   | 类型     | 描述  |
|------|--------|-----|
| dirQuota | uint32 | 配额值 |

### 获取配额信息

```bash
curl -v "http://192.168.0.11:17010/admin/getIp"
```
响应如下：

```json
{
    "code": 0,
    "data": {
        "Cluster": "test",
        "DataNodeAutoRepairLimitRate": 0,
        "DataNodeDeleteLimitRate": 0,
        "DirChildrenNumLimit": 20000000,
        "EbsAddr": "",
        "Ip": "192.168.0.1",
        "MetaNodeDeleteBatchCount": 0,
        "MetaNodeDeleteWorkerSleepMs": 0,
        "ServicePath": ""
    },
    "msg": "success"
}
```

`DirChildrenNumLimit`字段为当前集群的目录配额值