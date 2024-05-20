# Metadata Node Management

## Add

``` bash
curl -v "http://192.168.0.11:17010/metaNode/add?addr=192.168.0.33:17310"
```

Adds a new metadata node in the corresponding region.

Parameter List

| Parameter | Type   | Description                                              |
|-----------|--------|----------------------------------------------------------|
| addr      | string | Address for interaction between metadata node and master |

## Query

``` bash
curl -v "http://10.196.59.198:17010/metaNode/get?addr=10.196.59.202:17210"  | python -m json.tool
```

Displays detailed information about the metadata node, including the address, total memory size, used memory size, etc.

Parameter List

| Parameter | Type   | Description                                              |
|-----------|--------|----------------------------------------------------------|
| addr      | string | Address for interaction between metadata node and master |

Response Example

``` json
{
    "ID": 3,
    "Addr": "10.196.59.202:17210",
    "IsActive": true,
    "Zone": "zone1",
    "MaxMemAvailWeight": 66556215048,
    "TotalWeight": 67132641280,
    "UsedWeight": 576426232,
    "Ratio": 0.008586377967698518,
    "SelectCount": 0,
    "Carry": 0.6645600532184904,
    "Threshold": 0.75,
    "ReportTime": "2018-12-05T17:26:28.29309577+08:00",
    "MetaPartitionCount": 1,
    "NodeSetID": 2,
    "PersistenceMetaPartitions": {}
}
```

## Decommission Node

``` bash
curl -v "http://10.196.59.198:17010/metaNode/decommission?addr=10.196.59.202:17210" 
```

::: tip Recommendation
To avoid writing new data to the node being decommissioned, you can first set the node status to offline. All metadata shards on the node will be asynchronously migrated to other available metadata nodes in the cluster. There are two modes: normal mode and strict mode.
:::

Parameter List

| Parameter | Type   | Description                                              |
|-----------|--------|----------------------------------------------------------|
| addr      | string | Address for interaction between metadata node and master |

## Set Threshold

``` bash
curl -v "http://10.196.59.198:17010/threshold/set?threshold=0.75"
```

If the memory usage rate of a metadata node reaches this threshold, all metadata shards on the node will be set to read-only.

Parameter List

| Parameter | Type    | Description                                                             |
|-----------|---------|-------------------------------------------------------------------------|
| threshold | float64 | Maximum ratio of memory that a metadata node can use on its own machine |

## Migrate

``` bash
curl -v "http://10.196.59.198:17010/metaNode/migrate?srcAddr=src&targetAddr=dst&count=3"
```

Migrates a specified number of metadata shards from the source metadata node to the target metadata node.

Parameter List

| Parameter  | Type   | Description                                                                  |
|------------|--------|------------------------------------------------------------------------------|
| srcAddr    | string | Address of the source metadata node                                          |
| targetAddr | string | Address of the target metadata node                                          |
| count      | int    | Number of metadata shards to be migrated. Optional. The default value is 15. |