# Directory File Count and Quota Management

::: warning Note
Quota management is a new feature added in v3.2.1.
:::

## Limit on the Number of Files in a Single Directory

Limits the number of files or directories within a single directory to avoid the occurrence of excessively large directories, which could exhaust the resources of MP nodes.

- The default limit for the number of children in each directory is 20 million, which can be configured with a minimum value of 1 million and no upper limit. If the number of children created under a single directory exceeds the limit, the creation fails.
- The configured limit value takes effect on the entire cluster and is persisted on the master.
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