# Metadata Shard Management

## Create

``` bash
curl -v "http://192.168.0.1:17010/metaPartition/create?count=10&name=test"
```

Batch create meta partitions.

Parameter List

| Parameter | Type   | Description                                  |
|-----------|--------|----------------------------------------------|
| name      | string | Volume name                                  |
| count     | uint64 | Number of newly added mp |

## Query

``` bash
curl -v "http://192.168.0.1:17010/metaPartition/get?id=1" | python -m json.tool
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
curl -v "http://192.168.0.1:17010/metaPartition/decommission?id=13&addr=10.196.59.202:17210"
```

Removes a replica of the metadata shard and creates a new replica.

Parameter List

| Parameter | Type   | Description                          |
|-----------|--------|--------------------------------------|
| id        | uint64 | Metadata partition ID                |
| addr      | string | Address of the replica to be removed |

## Compare Replica

``` bash
curl -v "http://192.168.0.1:17010/metaPartition/load?id=1"
```

Sends a task to compare the replica to each replica, and then checks whether the CRC of each replica is consistent.

Parameter List

| Parameter | Type   | Description           |
|-----------|--------|-----------------------|
| id        | uint64 | Metadata partition ID |