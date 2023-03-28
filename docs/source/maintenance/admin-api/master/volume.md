# Volume Management

## Create

``` bash
curl -v "http://10.196.59.198:17010/admin/createVol?name=test&capacity=100&owner=cfs&mpCount=3"
```

Create a volume for the user and allocate a set of data shards and metadata shards. By default, when creating a new volume, 10 data shards and 3 metadata shards are allocated.

CubeFS uses the **Owner** parameter as the user ID.
- When creating a volume, if there is no user with the same name as the Owner of the volume in the cluster, a user with the ID Owner will be created automatically.
- If a user with the ID Owner already exists in the cluster, the ownership of the volume will be automatically assigned to that user.

For more information, please refer to: [User Guide](./user.md)

Parameter List

| Parameter        | Type   | Description                                                                                                                                                             | Required | Default Value                                                                                          |
|------------------|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|--------------------------------------------------------------------------------------------------------|
| name             | string | Volume name                                                                                                                                                             | Yes      | None                                                                                                   |
| volType          | int    | Volume type: 0: replica volume, 1: erasure-coded volume                                                                                                                 | No       | 0                                                                                                      |
| capacity         | int    | Volume quota, in GB                                                                                                                                                     | Yes      | None                                                                                                   |
| owner            | string | Volume owner, also the user ID                                                                                                                                          | Yes      | None                                                                                                   |
| mpCount          | int    | Number of initialized metadata shards                                                                                                                                   | No       | 3                                                                                                      |
| replicaNum       | int    | Number of replicas                                                                                                                                                      | No       | 3 for replica volume (supports 1, 3), 1 for erasure-coded volume (supports 1-16)                       |
| size             | int    | Data shard size, in GB                                                                                                                                                  | No       | 120                                                                                                    |
| enablePosixAcl   | bool   | Whether to configure POSIX permission restrictions                                                                                                                      | No       | false                                                                                                  |
| followerRead     | bool   | Whether to allow reading data from followers, true by default for erasure-coded volume                                                                                  | No       | false                                                                                                  |
| crossZone        | bool   | Whether to cross regions. If set to true, the zoneName parameter cannot be set                                                                                          | No       | false                                                                                                  |
| normalZonesFirst | bool   | Whether to prioritize writing to normal domains                                                                                                                         | No       | false                                                                                                  |
| zoneName         | string | Specify the region                                                                                                                                                      | No       | default if crossZone is set to false                                                                   |
| cacheRuleKey     | string | Used for erasure-coded volume                                                                                                                                           | No       | Only data matching this field will be written to the cache if it is not empty                          |
| ebsBlkSize       | int    | Size of each block, in bytes                                                                                                                                            | No       | Default 8M                                                                                             |
| cacheCap         | int    | Size of the erasure-coded volume cache, in GB                                                                                                                           | No       | Required if the cache is enabled for the erasure-coded volume                                          |
| cacheAction      | int    | The scenario for writing the erasure-coded volume cache: 0 - do not write to the cache, 1 - read data and write back to the cache, 2 - read and write data to the cache | No       | 0                                                                                                      |
| cacheThreshold   | int    | The minimum size of data to be written to the cache, in bytes                                                                                                           | No       | Default 10M                                                                                            |
| cacheTTL         | int    | The erasure-coded volume cache eviction time, in days                                                                                                                   | No       | Default 30                                                                                             |
| cacheHighWater   | int    | The threshold for erasure-coded volume cache eviction, the upper limit of the content to be evicted, when it reaches this value, the eviction is triggered              | No       | Default 80, i.e., when the content of dp reaches 96G (120G * 80/100), the dp starts to evict data      |
| cacheLowWater    | int    | The lower limit of the capacity to be evicted when it reaches this value, the dp will no longer evict data                                                              | No       | Default 60, i.e., when the content of dp reaches 72G (120G * 60/100), the dp will no longer evict data |
| cacheLRUInterval | int    | The detection cycle for low-capacity eviction, in minutes                                                                                                               | No       | Default 5 minutes                                                                                      |

## Delete

``` bash
curl -v "http://10.196.59.198:17010/vol/delete?name=test&authKey=md5(owner)"
```

First, mark the volume as logically deleted (set status to 1), then delete all data shards and metadata shards through periodic tasks, and finally delete them from persistent storage.

::: warning Note
The erasure-coded volume can only be deleted when the usage size is 0.
:::

When deleting a volume, all permission information related to the volume will be deleted from all user information.

Parameter List

| Parameter | Type   | Description                                                                            | 
|-----------|--------|----------------------------------------------------------------------------------------|
| name      | string | Volume name                                                                            |
| authKey   | string | Calculate the 32-bit MD5 value of the owner field of vol as authentication information |

## Query Volume Details

``` bash
curl -v "http://10.196.59.198:17010/admin/getVol?name=test" | python -m json.tool
```

Displays basic information about the volume, including the name of the volume, information about all data shards and metadata shards, etc.

Parameter List

| Parameter | Type   | Description |
|-----------|--------|-------------|
| name      | string | Volume name |

Response Example

``` json
{
    "Authenticate": false,
     "CacheAction": 0,
     "CacheCapacity": 0,
     "CacheHighWater": 80,
     "CacheLowWater": 60,
     "CacheLruInterval": 5,
     "CacheRule": "",
     "CacheThreshold": 10485760,
     "CacheTtl": 30,
     "Capacity": 10,
     "CreateTime": "2022-03-31 16:08:31",
     "CrossZone": false,
     "DefaultPriority": false,
     "DefaultZonePrior": false,
     "DentryCount": 0,
     "Description": "",
     "DomainOn": false,
     "DpCnt": 0,
     "DpReplicaNum": 16,
     "DpSelectorName": "",
     "DpSelectorParm": "",
     "FollowerRead": true,
     "ID": 706,
     "InodeCount": 1,
     "MaxMetaPartitionID": 2319,
     "MpCnt": 3,
     "MpReplicaNum": 3,
     "Name": "abc",
     "NeedToLowerReplica": false,
     "ObjBlockSize": 8388608,
     "Owner": "cfs",
     "PreloadCapacity": 0,
     "RwDpCnt": 0,
     "Status": 0,
     "VolType": 1,
     "ZoneName": "default"
}
```

Query Volume Data Shard Details

``` bash
curl -v "http://192.168.0.12:17010/client/partitions?name=ltptest" | python -m json.tool
```

Displays information about all data shards of the volume.

Parameter List

| Parameter | Type   | Description |
|-----------|--------|-------------|
| name      | string | Volume name |

Response Example

``` json
{
    "Epoch": 0,
    "Hosts": [
        "192.168.0.34:17310",
        "192.168.0.33:17310",
        "192.168.0.32:17310"
    ],
    "IsRecover": false,
    "LeaderAddr": "192.168.0.33:17310",
    "PartitionID": 4,
    "ReplicaNum": 3,
    "Status": 2
}
```

Query Volume Metadata Shard Details

``` bash
curl -v "http://192.168.0.12:17010/client/metaPartitions?name=ltptest" | python -m json.tool
```

Displays information about all metadata shards of the volume.

Parameter List

| Parameter | Type   | Description |
|-----------|--------|-------------|
| name      | string | Volume name |

Response Example

``` json
{
    "DentryCount": 1,
    "End": 16777216,
    "InodeCount": 1,
    "IsRecover": false,
    "LeaderAddr": "192.168.0.23:17210",
    "MaxInodeID": 3,
    "Members": [
        "192.168.0.22:17210",
        "192.168.0.23:17210",
        "192.168.0.24:17210"
    ],
    "PartitionID": 1,
    "Start": 0,
    "Status": 2
}
```

## Statistics

``` bash
curl -v http://10.196.59.198:17010/client/volStat?name=test
```

Displays the total space size, used space size, and whether read-write token control is enabled for the volume.

Parameter List

| Parameter | Type   | Description                                                                                                                           |
|-----------|--------|---------------------------------------------------------------------------------------------------------------------------------------|
| name      | string | Volume name                                                                                                                           |
| version   | int    | Volume version, 0: replica volume, 1: erasure-coded volume, default 0 for replica volume, required for accessing erasure-coded volume |

Response Example

``` json
{
    "CacheTotalSize": 0,
    "CacheUsedRatio": "",
    "CacheUsedSize": 0,
    "EnableToken": false,
    "InodeCount": 1,
    "Name": "abc-test",
    "TotalSize": 10737418240,
    "UsedRatio": "0.00",
    "UsedSize": 0
}
```

## Update

``` bash
curl -v "http://10.196.59.198:17010/vol/update?name=test&capacity=100&authKey=md5(owner)"
```

Increases the quota of the volume and adjusts other related parameters.

Parameter List

| Parameter        | Type   | Description                                                                                                                      | Required |
|------------------|--------|----------------------------------------------------------------------------------------------------------------------------------|----------|
| name             | string | Volume name                                                                                                                      | Yes      |
| description      | string | Volume description information                                                                                                   | No       |
| authKey          | string | Calculate the 32-bit MD5 value of the owner field of vol as authentication information                                           | Yes      |
| capacity         | int    | Update the datanode capacity of the volume, in GB. The replica volume cannot be less than the used capacity                      | No       |
| zoneName         | string | The region where the volume is located after the update. If not set, it will be updated to the default region                    | Yes      |
| followerRead     | bool   | Whether to allow reading data from followers                                                                                     | No       |
| enablePosixAcl   | bool   | Whether to configure POSIX permission restrictions                                                                               | No       |
| emptyCacheRule   | string | Whether to empty the cacheRule                                                                                                   | No       |
| cacheRuleKey     | string | Cache rule, used for erasure-coded volume. Only data that meets the corresponding rule will be cached                            | No       |
| ebsBlkSize       | int    | The size of each block of the erasure-coded volume                                                                               | No       |
| cacheCap         | int    | The capacity of the cache when the erasure-coded volume uses the secondary cache                                                 | No       |
| cacheAction      | int    | For erasure-coded volume, 0: do not write to the cache, 1: read data and write to the cache, 2: read and write data to the cache | No       |
| cacheThreshold   | int    | The size limit of the cached file. Only files smaller than this value will be written to the cache                               | No       |
| cacheTTL         | int    | Cache expiration time, in days                                                                                                   | No       |
| cacheHighWater   | int    | Eviction high water mark                                                                                                         | No       |
| cacheLowWater    | int    | Cache eviction low water mark                                                                                                    | No       |
| cacheLRUInterval | int    | Cache detection cycle, in minutes                                                                                                | No       |

## Get Volume List

``` bash
curl -v "http://10.196.59.198:17010/vol/list?keywords=test"
```

Gets a list of all volumes, filtered by keyword.

Parameter List

| Parameter | Type   | Description                                                        | Required |
|-----------|--------|--------------------------------------------------------------------|----------|
| keywords  | string | Get the information of the volume whose name contains this keyword | No       |

Response Example

``` json
[
   {
       "Name": "test1",
       "Owner": "cfs",
       "CreateTime": 0,
       "Status": 0,
       "TotalSize": 155515112832780000,
       "UsedSize": 155515112832780000
   },
   {
       "Name": "test2",
       "Owner": "cfs",
       "CreateTime": 0,
       "Status": 0,
       "TotalSize": 155515112832780000,
       "UsedSize": 155515112832780000
   }
]
```

## Expand

``` bash
curl -v "http://10.196.59.198:17010/vol/expand?name=test&capacity=100&authKey=md5(owner) "
```

Expands the specified volume to the specified capacity.

Parameter List

| Parameter | Type   | Description                                                                            | Required |
|-----------|--------|----------------------------------------------------------------------------------------|----------|
| name      | string | Volume name                                                                            | Yes      |
| authKey   | string | Calculate the 32-bit MD5 value of the owner field of vol as authentication information | Yes      |
| capacity  | int    | The quota of the volume after expansion, in GB                                         | Yes      |

## Shrink

``` bash
curl -v "http://10.196.59.198:17010/vol/shrink?name=test&capacity=100&authKey=md5(owner) "
```

Reduces the specified volume to the specified capacity.

Parameter List

| Parameter | Type   | Description                                                                            | Required |
|-----------|--------|----------------------------------------------------------------------------------------|----------|
| name      | string | Volume name                                                                            | Yes      |
| authKey   | string | Calculate the 32-bit MD5 value of the owner field of vol as authentication information | Yes      |
| capacity  | int    | The quota of the volume after compression, in GB                                       | Yes      |

## Two Replicas

### Main Issues

Two replicas can support modifications and writes normally (using other dps and their ranges).

1. Supports setting 2 replicas for a 3-replica volume that has been created, and takes effect when creating a new dp, but not for old dps.
2. If one of the replicas of a two-replica volume crashes and there is no leader, use the `raftForceDel` parameter to delete the abnormal replica.

### Handling of Exceptional Scenarios

For example, there is a dp with two replicas, A and B.

**Exceptional Scenarios for Two-Replica Migration**

The migration target is C. The process we implement is to add replica C first, then delete the source A, and during the migration process, B crashes.

**Solution**:

If B crashes and raft is unavailable, delete B first, wait for the migration to complete, delete A, and then add a replica.

**Normal Operating Process with One Replica Crashing, such as B**

Without a leader, according to the raft rules, B cannot be deleted with two replicas because it needs to be committed first and then applied, but the condition for committing is that the majority is alive.

**Solution**:

Force delete B.

::: danger Warning
Raft supports the new interface `del`, and the replica does not use the raft log commit directly (back up the dp data first).
:::

```bash
curl "http://127.0.0.1:17010/dataReplica/delete?raftForceDel=true&addr=127.0.0.1:17310&id=47128
```

- `addr` is the address of replica B.
- `id` is the partition ID (`dpid`).
- `raftForceDel` is used to force delete the raft replica.
- The DataNode will check the number of replicas (both the volume and dp must have 2 replicas to prevent improper use) and the `force` field.

### Command Line

1. Creating a Two-Replica Volume

> ``` bash
> curl -v "http://192.168.0.11:17010/admin/createVol?name=2replica&capacity=100&owner=cfs&mpCount=3&replicaNum=2&followerRead=true"
> ```

2. Reducing a Three-Replica Volume to Two Replicas

- Existing data is read-only (batch scripts are recommended)

``` bash
curl -v "http://192.168.0.13:17010/admin/setDpRdOnly?id=**&rdOnly=true
```

- Update the number of replicas for the volume. After the update, the 3-replica partition will be asynchronously reduced to 2 replicas.

``` bash
curl -v "http://192.168.0.13:17010/vol/update?name=ltptest&replicaNum=2&followerRead=true&authKey=0e20229116d5a9a4a9e876806b514a85"
```

- Force delete (used when there is no leader, note: make sure that the replica to be deleted is no longer available)

``` bash
curl "10.86.180.77:17010/dataReplica/delete?raftForceDel=true&addr=10.33.64.33:17310&id=47128"  
```

## Flow Control

### Main Issues

- Considering the storage components that do not differentiate volume, volume flow control is done on the client side.
- In a distributed scenario, central control of client traffic is required, with the master acting as the center to ensure IOPS and reduce operational pressure by not adding additional flow control servers.
- The client uses a power function to control traffic growth, which can grow rapidly in scenarios where resources are abundant.
- Ensure smooth overall volume flow control.
- The master can balance client traffic and adaptively adjust it based on client requests.

### Configuration Items

There are no configuration items, and settings are done through URL commands.

### QOS Flow Control Parameters and Interfaces

- Enable QOS when creating a volume:

``` bash
curl -v "http://192.168.0.11:17010/admin/createVol?name=volName&capacity=100&owner=cfs&qosEnable=true&flowWKey=10000"

# Enable QOS, set write traffic to 10000MB
```

- Get volume traffic information:

``` bash
curl  "http://192.168.0.11:17010/qos/getStatus?name=ltptest"
```

- Get client data:

``` bash
curl  "http://192.168.0.11:17010/qos/getClientsInfo?name=ltptest"
```

- Update server parameters, enable/disable flow control, and adjust read/write traffic values:

``` bash
curl  "http://192.168.0.11:17010/qos/update?name=ltptest&qosEnable=true&flowWKey=100000"|jq
```

Fields involved include:
- `FlowWKey = "flowWKey"` // Write (volume)
- `FlowRKey = "flowRKey"` // Read (volume)

### Explanation of Some System Parameters

1. Default Unit

Currently, whether on the client or datanode side, traffic is measured in MB.

2. Minimum Traffic and IO Parameters

These are settings that apply to datanodes and volumes. If a value is set, the following requirements must be met, otherwise an error will occur:
- `MinFlowLimit = 100 \* util.MB`
- `MinIoLimit = 100`

3. If no traffic value is set but flow control is enabled, default values (in bytes) will be used:
   - `defaultIopsRLimit uint64 = 1 \<\< 16`
   - `defaultIopsWLimit uint64 = 1 \<\< 16`
   - `defaultFlowWLimit uint64 = 1 \<\< 35`
   - `defaultFlowRLimit uint64 = 1 \<\< 35`

### Communication Between Client and Master

1. If the client does not receive flow control from the master for a long time, a warning will be logged.
2. If communication between the client and master fails, the original flow control will be maintained, and a warning will be logged.
3. If traffic is 0 for a long time, the client will not actively request flow control and will not report to the master to reduce communication requests. The master will clean up client information that has not been reported for a long time.

### Cold Volumes

1. Level 1 cache reads do not count as traffic.
2. Cache writes are not included in write traffic control.
3. Everything else counts as traffic.