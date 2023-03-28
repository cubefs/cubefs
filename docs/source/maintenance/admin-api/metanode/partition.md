# Metadata Shard Management

## Obtaining Information for All Shards on the Current Device

``` bash
curl -v  http://10.196.59.202:17210/getPartitions
```

## Obtaining Current Status Information for a Specified Shard ID

``` bash
curl -v http://10.196.59.202:17210/getPartitionById?pid=100
```

Obtains current status information for a specified shard ID, including the raft leader address for the current shard group, the raft group members, and the inode allocation cursor.

Request Parameters:

| Parameter | Type    | Description       |
|-----------|---------|-------------------|
| pid       | Integer | Metadata shard ID |