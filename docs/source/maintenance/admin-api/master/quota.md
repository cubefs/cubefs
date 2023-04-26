# Quota Management

::: warning Note
Quota management is a new feature added in v3.2.1.
:::

## Directory and File Quota

Limits the number of child files or directories under a single directory to avoid excessive memory usage in the metadata partition where the parent directory is located, which can cause uneven memory usage on the metadata node and easily lead to OOM on the metadata node.

- The default quota for the number of children in each directory is 20 million, which can be configured with a minimum value of 1 million and no upper limit. If the number of children created under a single directory exceeds the quota, the creation fails.
- The configured quota value takes effect on the entire cluster and is persisted on the master.
- Full support requires upgrading the client, metadata node, and master to version 3.2.1.

### Set Quota

```bash
curl -v "http://192.168.0.11:17010/setClusterInfo?dirQuota=20000000"
```

::: tip Note
`192.168.0.11` is the IP address of the master, and the same applies below.
:::

| Parameter | Type   | Description |
|-----------|--------|-------------|
| dirQuota  | uint32 | Quota value |

### Get Quota Information

```bash
curl -v "http://192.168.0.11:17010/admin/getIp"
```

The response is as follows:

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

The `DirChildrenNumLimit` field is the directory quota value for the current cluster.