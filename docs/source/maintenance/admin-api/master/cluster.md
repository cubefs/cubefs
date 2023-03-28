# Cluster Management

## Overview

Use the API provided by the resource management node for cluster management. The IP and port address in the curl command are the ip and listen options in the resource management node configuration file.

``` bash
curl -v "http://10.196.59.198:17010/admin/getCluster" | python -m json.tool
```

Displays basic information about the cluster, such as which data nodes and metadata nodes are included in the cluster, and volumes.

Response Example

``` json
{
 "code":0,
 "data":{
     "Applied":886268,
     "BadMetaPartitionIDs":[

     ],
     "BadPartitionIDs":[

     ],
     "DataNodeStatInfo":{

     },
     "DataNodes":[

     ],
     "DisableAutoAlloc":false,
     "LeaderAddr":"127.0.0.1:17010",
     "MaxDataPartitionID":735,
     "MaxMetaNodeID":57,
     "MaxMetaPartitionID":59,
     "MetaNodeStatInfo":{

     },
     "MetaNodeThreshold":0.75,
     "MetaNodes":[

     ],
     "Name":"cluster",
     "VolStatInfo":[

     ]
 },
 "msg":"success"
}
```

## Freeze Cluster

``` bash
curl -v "http://10.196.59.198:17010/cluster/freeze?enable=true"
```

If the freeze cluster function is enabled, volumes will no longer automatically create data shards, and you cannot manually create shards.

Parameter List

| Parameter | Type | Description                           |
|-----------|------|---------------------------------------|
| enable    | bool | If set to true, the cluster is frozen |

## Get Cluster Space Information

``` bash
curl -v "http://10.196.59.198:17010/cluster/stat"
```

Displays the space information of the cluster by region.

Response Example

``` json
{
    "DataNodeStatInfo": {
        "TotalGB": 1,
        "UsedGB": 0,
        "IncreasedGB": -2,
        "UsedRatio": "0.0"
    },
    "MetaNodeStatInfo": {
        "TotalGB": 1,
        "UsedGB": 0,
        "IncreasedGB": -8,
        "UsedRatio": "0.0"
    },
    "ZoneStatInfo": {
        "zone1": {
            "DataNodeStat": {
                "TotalGB": 1,
                "UsedGB": 0,
                "AvailGB": 0,
                "UsedRatio": 0,
                "TotalNodes": 0,
                "WritableNodes": 0
            },
            "MetaNodeStat": {
                "TotalGB": 1,
                "UsedGB": 0,
                "AvailGB": 0,
                "UsedRatio": 0,
                "TotalNodes": 0,
                "WritableNodes": 0
            }
        }
    }
}
```

## Get Cluster Topology Information

``` bash
curl -v "http://10.196.59.198:17010/topo/get"
```

Displays the topology information of the cluster by region.

Response Example

``` json
[
    {
        "Name": "zone1",
        "Status": "available",
        "NodeSet": {
            "700": {
                "DataNodeLen": 0,
                "MetaNodeLen": 0,
                "MetaNodes": [],
                "DataNodes": []
            }
        }
    },
    {
        "Name": "zone2",
        "Status": "available",
        "NodeSet": {
            "800": {
                "DataNodeLen": 0,
                "MetaNodeLen": 0,
                "MetaNodes": [],
                "DataNodes": []
            }
        }
    }
]
```

## Update Zone Status

``` bash
curl -v "http://10.196.59.198:17010/zone/update?name=zone1&enable=false"
```

Updates the status of the zone to available or unavailable.

Parameter List

| Parameter | Type   | Description |
| --------- | ------ | ----------- |
| name      | string | Zone name   |
| enable    | bool   | true means available, false means unavailable |

## Get All Zone Information

``` bash
curl -v "http://10.196.59.198:17010/zone/list"
```

Gets the names and availability status of all zones.

Response Example

``` json
[
    {
        "Name": "zone1",
        "Status": "available",
        "NodeSet": {}
    },
    {
        "Name": "zone2",
        "Status": "available",
        "NodeSet": {}
    }
]
```

## Get Cluster Information

``` bash
curl -v "http://192.168.0.11:17010/admin/getNodeInfo"
```

Gets the cluster information.

Response Example

``` json
{
    "code": 0,
    "data": {
        "autoRepairRate": "0",
        "batchCount": "0",
        "deleteWorkerSleepMs": "0",
        "loadFactor": "0",
        "maxDpCntLimit":"0",
        "markDeleteRate": "0"
    },
    "msg": "success"
}
```

## Set Cluster Information

``` bash
curl -v "http://192.168.0.11:17010/admin/setNodeInfo?batchCount=100&markDeleteRate=100"
```

Sets the cluster information.

Parameter List

| Parameter           | Type   | Description                                                             |
|---------------------|--------|-------------------------------------------------------------------------|
| batchCount          | uint64 | Metanode delete batch size                                              |
| markDeleteRate      | uint64 | Datanode batch delete rate limit setting. 0 means no rate limit setting |
| autoRepairRate      | uint64 | Number of extents repaired on a datanode at the same time               |
| deleteWorkerSleepMs | uint64 | Deletion interval                                                       |
| loadFactor          | uint64 | Cluster overselling ratio, default 0, no limit                          |
| maxDpCntLimit       | uint64 | Maximum number of DPs on each node, default 3000, 0 means default value |
