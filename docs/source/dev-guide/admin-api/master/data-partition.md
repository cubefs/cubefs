# Data Shard Management

## Create

``` bash
curl -v "http://10.196.59.198:17010/dataPartition/create?count=400&name=test"
```

Creates a specified number of data shards.

Parameter List

| Parameter | Type   | Description                     |
|-----------|--------|---------------------------------|
| count     | int    | Number of data shards to create |
| name      | string | Volume name                     |

## Query

``` bash
curl -v "http://10.196.59.198:17010/dataPartition/get?id=100"  | python -m json.tool
```

Displays detailed information about the data shard, including the number of replicas, volume information, etc.

Parameter List

| Parameter | Type   | Description   |
|-----------|--------|---------------|
| id        | uint64 | Data shard ID |

Response Example

``` json
{
    "PartitionID": 100,
    "LastLoadedTime": 1544082851,
    "ReplicaNum": 3,
    "Status": 2,
    "Replicas": {},
    "Hosts": {},
    "Peers": {},
    "Zones": {},
    "MissingNodes": {},
    "VolName": "test",
    "VolID": 2,
    "FileInCoreMap": {},
    "FilesWithMissingReplica": {}
}
```

## Decommission Replica

``` bash
curl -v "http://10.196.59.198:17010/dataPartition/decommission?id=13&addr=10.196.59.201:17310"
```

Removes a replica of the data shard and creates a new replica.

Parameter List

| Parameter | Type   | Description                          |
|-----------|--------|--------------------------------------|
| id        | uint64 | Data shard ID                        |
| addr      | string | Address of the replica to be removed |

## Compare Replica Files

``` bash
curl -v "http://10.196.59.198:17010/dataPartition/load?id=1"
```

Sends a task to compare the replica files for each replica of the data shard, and then asynchronously checks whether the file CRC on each replica is consistent.

Parameter List

| Parameter | Type   | Description   |
|-----------|--------|---------------|
| id        | uint64 | Data shard ID |
