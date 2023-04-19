# Node or Disk Failure

## DataNode

### Node Failure Handling
When a machine failure of one of the datanodes in a three-replica volume causes it to crash, resulting in all data partitions on the node being in an unavailable state, timely migration and offline processing is required to prevent other nodes from crashing and causing some data partitions to be unavailable due to the lack of two replicas at the same time.

#### Offline Node
```bash
curl -v "http://192.168.0.11:17010/dataNode/decommission?addr=192.168.0.33:17310"
```

::: tip Note
All offline commands are sent to the master node. The following examples are based on a single-machine Docker environment, with the master at 192.168.0.11.
:::

| Parameter | Type   | Description       |
|-----------|--------|-------------------|
| addr      | string | Data node address |

Offline a data node from the cluster. All data shards on the data node will be asynchronously migrated to other available data nodes in the cluster.

#### Setting Migration Rate
When there are a large number of extent files to be migrated, the other nodes where the replicas are located will bring a large amount of read IO, and the migrated node will have a large amount of write IO. If the load is high, the migration rate can be limited:

```shell
curl -v "http://192.168.0.11:17010/admin/setNodeInfo?autoRepairRate=2000"
```

| Parameter           | Type   | Description                                                                                 |
|---------------------|--------|---------------------------------------------------------------------------------------------|
| batchCount          | uint64 | Metanode deletion batch size.                                                               |
| markDeleteRate      | uint64 | Datanode batch deletion rate limit setting. 0 means no rate limit setting.                  |
| autoRepairRate      | uint64 | The number of extents repaired simultaneously on the datanode.                              |
| deleteWorkerSleepMs | uint64 | Deletion interval time.                                                                     |
| loadFactor          | uint64 | Cluster overselling ratio. Default is 0, no limit.                                          |
| maxDpCntLimit       | uint64 | The maximum number of data partitions on each node, default is 3000, 0 means default value. |

#### Check Offline Migration Status
If you need to determine whether the offline migration is complete, you can check the status of the current data partitions in the cluster.

```bash
curl -v "http://192.168.0.11:17010/dataPartition/diagnose"
```

If the `BadDataPartitionIDs` field is empty, it means that the offline process is complete.

```json
{
    "code": 0,
    "data": {
        "BadDataPartitionIDs": [],
        "BadReplicaDataPartitionIDs": [],
        "CorruptDataPartitionIDs": [],
        "InactiveDataNodes": [],
        "LackReplicaDataPartitionIDs": []
    },
    "msg": "success"
}
```

### Disk Failure Handling
When a disk failure occurs on a datanode, the specific disk can be migrated offline according to the disk.

```bash
curl -v http://192.168.0.11:17010/disk/decommission?addr=192.168.0.30:17310&disk=/path/to/disk/dir"
```

| Parameter | Type   | Description                                                                                         |
|-----------|--------|-----------------------------------------------------------------------------------------------------|
| addr      | string | Node address of the disk to be taken offline                                                        |
| disk      | string | Failed disk                                                                                         |
| count     | int    | Number of disks taken offline each time. The default is 0, which means all disks are taken offline. |

Similarly, the migration rate can be set based on the load of the datanode.

## MetaNode
### Node Failure Handling
When a node is unavailable (due to machine failure or network issues), the node needs to be taken offline to prevent most replicas of the meta partition from being unavailable, resulting in the entire partition being unavailable.

#### Offline Node
Offline a metadata node from the cluster. All metadata shards on the node will be asynchronously migrated to other available metadata nodes in the cluster.

```bash
curl -v "http://10.196.59.198:17010/metaNode/decommission?addr=10.196.59.202:17210"
```

| Parameter | Type   | Description           |
|-----------|--------|-----------------------|
| addr      | string | Metadata node address |

To avoid writing new data to the node being taken offline, the node status can be set first.

```bash
curl -v "http://192.168.0.11:17010/admin/setNodeRdOnly?addr=192.168.0.40:17210&nodeType=1&rdOnly=true"
```

| Parameter | Type   | Description                 |
|-----------|--------|-----------------------------|
| addr      | string | Metanode node address       |
| nodeType  | string | 1: metanode, 2: datanode    |
| rdOnly    | bool   | true or false               |

#### Check Offline Migration Status
If you need to determine whether the offline migration is complete, you can check the status of the current metadata partitions in the cluster.

```bash
curl -v "http://192.168.0.11:17010/metaPartition/diagnose"
```

If the `BadMetaPartitionIDs` field is empty, it means that the offline process is complete.

```json
{
    "code": 0,
    "data": {
        "BadMetaPartitionIDs": [],
        "CorruptMetaPartitionIDs": [],
        "InactiveMetaNodes": [],
        "LackReplicaMetaPartitionIDs": []
    },
    "msg": "success"
}
```

## Erasure Coding Subsystem

Access and Proxy are stateless nodes and do not involve data migration. Here, we only introduce the failure handling of Clustermgr and BlobNode.

### Clustermgr

#### Node Failure
The cluster is available, and the number of nodes that have crashed does not exceed the majority of the cluster.
- Enable the clustermgr service on a new node and add the member information of the current node to the configuration of the new service;
- Call the [member removal interface](admin-api/blobstore/cm.md) to remove the crashed node;
- Call the [member addition interface](admin-api/blobstore/cm.md) to add the newly started Clustermgr node to the cluster;
- Wait for data to be automatically synchronized.

### BlobNode

#### Disk Failure

Call the clusterMgr interface to set the bad disk and go through the bad disk repair process. For details, refer to [Disk Management](admin-api/blobstore/cm.md).

```bash
curl -X POST --header 'Content-Type: application/json' -d '{"disk_id":2,"status":2}' "http://127.0.0.1:9998/disk/set"
```

#### Node Failure
1. If the machine is available, restart the blobnode service and observe whether it can be automatically restored;
2. If the machine is unavailable or the service cannot be automatically restored, manually take the machine offline.

```bash
# List the first XX disks of offline nodes (the returned data includes the disk ids that were offline or repaired before)
curl "http://127.0.0.1:9998/disk/list?host=http://Offline_node_IP:8899&count=XX"
# Manually take the disk offline and go through the bad disk repair process.
```


