# Metadata Shard Management

## Create

``` bash
curl -v "http://10.196.59.198:17010/metaPartition/create?name=test&start=10000"
```

Manually splits the metadata shard. If the maximum metadata shard inode range of the volume is `[begin, end)`:

- If **start** is greater than **begin** and less than **end**, the inode range of the original maximum metadata shard becomes `[begin, start]`, and the range of the newly created metadata shard is `[start+1,+inf)`.
- If **start** is less than **begin**, max is the maximum inode number on the current shard, and the inode range becomes `[begin, max+16777216]`, and the range of the newly created metadata shard is `[max+16777217,+inf)`.
- If **start** is greater than **end**, max is the maximum inode number on the current shard, and the inode range becomes `[begin, start]`, and the range of the newly created metadata shard is `[start+1, +inf)`.

::: warning Note
A large **start** value will cause a large inode on a single shard, occupying a large amount of memory. When there are too many inodes on the last shard, automatic splitting of the metadata partition will also be triggered.
:::

Parameter List

| Parameter | Type   | Description                                  |
|-----------|--------|----------------------------------------------|
| name      | string | Volume name                                  |
| start     | uint64 | Split the metadata shard based on this value |

## Query

``` bash
curl -v "http://10.196.59.198:17010/metaPartition/get?id=1" | python -m json.tool
```

Displays detailed information about the metadata shard, including the shard ID, the starting range of the shard, etc.

Parameter List

| Parameter | Type   | Description       |
|-----------|--------|-------------------|
| id        | uint64 | Metadata shard ID |

Response Example

``` json
{
    "PartitionID": 1,
    "Start": 0,
    "End": 9223372036854776000,
    "MaxNodeID": 1,
    "VolName": "test",
    "Replicas": {},
    "ReplicaNum": 3,
    "Status": 2,
    "IsRecover": true,
    "Hosts": {},
    "Peers": {},
    "Zones": {},
    "MissNodes": {},
    "LoadResponse": {}
}
```

## Decommission Replica

``` bash
curl -v "http://10.196.59.198:17010/metaPartition/decommission?id=13&addr=10.196.59.202:17210"
```

Removes a replica of the metadata shard and creates a new replica.

Parameter List

| Parameter | Type   | Description                          |
|-----------|--------|--------------------------------------|
| id        | uint64 | Metadata partition ID                |
| addr      | string | Address of the replica to be removed |

## Compare Replica

``` bash
curl -v "http://10.196.59.198:17010/metaPartition/load?id=1"
```

Sends a task to compare the replica to each replica, and then checks whether the CRC of each replica is consistent.

Parameter List

| Parameter | Type   | Description           |
|-----------|--------|-----------------------|
| id        | uint64 | Metadata partition ID |