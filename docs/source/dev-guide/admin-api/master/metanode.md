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

## Balance

When some metanode machines in the cluster have high memory load while others have low load, you can achieve memory load balancing by migrating MPs. This way, the network connections that each metanode needs to handle will also be more balanced.

### Auto-generate Migration Plan

A migration plan can be generated when there are MN nodes with memory usage reaching 75% and idle nodes with usage below 30%.

#### Create Migration Plan

``` bash
curl -v "http://10.52.140.165:17010/metaNode/createBalanceTask"  | python -m json.tool
```

#### Show Migration Plan

``` bash
curl -v "http://10.52.140.165:17010/metaNode/getBalanceTask"  | python -m json.tool
```

#### Run Migration Plan

``` bash
curl -v "http://10.52.140.165:17010/metaNode/runBalanceTask"  | python -m json.tool
```

#### Stop Migration Plan

``` bash
curl -v "http://10.52.140.165:17010/metaNode/stopBalanceTask"  | python -m json.tool
```

#### Delete Migration Plan

``` bash
curl -v "http://10.52.140.165:17010/metaNode/deleteBalanceTask"  | python -m json.tool
```

### cfs-cli Tool Commands

The corresponding cfs-cli tool commands for these operations are:

``` bash
./cfs-cli mp-balance create
./cfs-cli mp-balance show
./cfs-cli mp-balance run
./cfs-cli mp-balance stop
./cfs-cli mp-balance delete
```

### Important Notes

::: tip Note
We only save one migration plan in a cluster. New migration plans cannot be created until the existing one is deleted. For operational convenience, multiple migration plans are not created simultaneously.

Completed migration plans are only retained for three days, after which they are automatically deleted by the background. If a task status is in error, it will remain stopped, waiting for developers to investigate. This means that even with autoMetaPartitionMigrate enabled, balance tasks only run once every 3 days.

You can change the high water mark, low water mark, and auto-migration settings in the cluster through /admin/setConfig. Only one value can be modified at a time. Since this is rarely used, it's not designed to modify multiple values simultaneously to reduce code changes.

When problems occur, you can correct errors by specifying the MP to migrate using the command /metaNode/migratePartition.
:::

### Specify Detailed Migration Information

``` bash
curl -v "http://10.52.140.165:17010/metaNode/migratePartition?srcAddr=10.52.140.165:17210&targetAddr=10.52.140.102:17210&id=13"
```

### Query Migration Results

``` bash
curl -v "http://10.52.140.165:17010/metaNode/migrateResult?targetAddr=10.52.140.165:17210&id=13"  | python -m json.tool
```