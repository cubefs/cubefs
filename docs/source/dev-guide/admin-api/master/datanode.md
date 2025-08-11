# DataNode Management

## Add

``` bash
curl -v "http://192.168.0.11:17010/dataNode/add?addr=192.168.0.33:17310&zoneName=default"
```

Adds a new data node in the corresponding region.

Parameter List

| Parameter | Type   | Description                                                  |
|-----------|--------|--------------------------------------------------------------|
| addr      | string | Address for interaction between data node and master         |
| zoneName  | string | Specifies the region. If empty, the default value is default |

## Query

``` bash
curl -v "http://10.196.59.198:17010/dataNode/get?addr=10.196.59.201:17310"  | python -m json.tool
```

Displays details about the data node, including the address of the data node, total capacity, used space, etc.

Parameter List

| Parameter | Type   | Description                                          |
|-----------|--------|------------------------------------------------------|
| addr      | string | Address for interaction between data node and master |

Response Example

``` json
{
    "TotalWeight": 39666212700160,
    "UsedWeight": 2438143586304,
    "AvailableSpace": 37228069113856,
    "ID": 2,
    "Zone": "zone1",
    "Addr": "10.196.59.201:17310",
    "ReportTime": "2018-12-06T10:56:38.881784447+08:00",
    "IsActive": true
    "UsageRatio": 0.06146650815226848,
    "SelectTimes": 5,
    "Carry": 1.0655859145960367,
    "DataPartitionReports": {},
    "DataPartitionCount": 21,
    "NodeSetID": 3,
    "PersistenceDataPartitions": {},
    "BadDisks": {}
}
```

## Decommission Node

``` bash
curl -v "http://192.168.0.11:17010/dataNode/decommission?addr=192.168.0.33:17310&weight=2"
```

Removes a data node from the cluster. All data shards on the data node will be asynchronously migrated to other available data nodes in the cluster.

Parameter List

| Parameter | Type   | Description                                          |
|-----------|--------|------------------------------------------------------|
| addr      | string | Address for interaction between data node and master |
| weight    | int    | Decommission weight, default is 2                    |

## Get Disk

``` bash
curl -v "http://192.168.0.11:17320/disks"
```

Gets disk information, including disk path, space usage, disk status, etc.

## Get Node Partition

``` bash
curl -v "http://192.168.0.11:17320/partitions"
```

## Disk Decommission

```bash
curl -v "http://192.168.0.11:17010/disk/decommission?addr=192.168.0.12:17310&disk=/home/service/var/data1&weight=2"
```

Parameter List

| Parameter | Type   | Description                                      |
|-----------|--------|--------------------------------------------------|
| addr      | string | The node address of the disk to be taken offline |
| disk  | string | Faulty disk address                        |
| count | int    | Number of dp to take offline each time, default is 0, which represents all dp offline. |
| diskDisable | bool    | Whether to disable the disk, i.e., prohibit creating new data partitions on the offline disks. Default is true |
| decommissionType | int    | Offline type, default is 0, which represents manual offline; 1 indicates automatic offline |
| weight    | int    | Decommission weight, default is 2                |

## Migration

```bash
curl -v "http://10.196.59.198:17010/dataNode/migrate?srcAddr=src&targetAddr=dst&count=3"
```

Parameter List

| Parameter  | Type   | Description                                                    |
|------------|--------|----------------------------------------------------------------|
| srcAddr    | string | The source data node address to be migrated out                |
| targetAddr | string | The destination data node address for migration                |
| count      | int    | The number of data partitions to migrate, optional, default 50 |


## Set Disk Decommission Control Speed

``` bash
curl -v "http://192.168.0.11:17320/admin/updateDecommissionLimit?decommissionLimit=10"
```

Parameter List

| Parameter         | Type | Description                                   |
|-------------------|------|-----------------------------------------------|
| decommissionLimit | int  | Number of concurrent DPs to be decommissioned |

::: tip Note
New interface in v3.2.1
:::

## Query Disk Decommission Control Speed

``` bash
curl -v "http://192.168.0.11:17320/admin/queryDecommissionLimit"
```

::: tip Note
New interface in v3.2.1
:::

## Query Disk Decommission Progress

``` bash
curl -v "http://192.168.0.11:17320/disk/queryDecommissionProgress?addr=192.168.0.12:17310&disk=/home/service/var/data1"
```

Parameter List

| Parameter | Type   | Description                                                        |
|-----------|--------|--------------------------------------------------------------------|
| addr      | string | Address of the node where the disk to be decommissioned is located |
| disk      | string | Address of the disk to be decommissioned                           |

::: tip Note
New interface in v3.2.1
:::

## Query Node Decommission Progress

``` bash
curl -v "http://192.168.0.11:17010/dataNode/queryDecommissionProgress?addr=192.168.0.33:17310"
```

Parameter List

| Parameter | Type   | Description                                          |
|-----------|--------|------------------------------------------------------|
| addr      | string | Address for interaction between data node and master |

::: tip Note
New interface in v3.2.1
:::

## Cancel Disk Decommission

``` bash
curl -v "http://192.168.0.11:17320/disk/pauseDecommission?addr=192.168.0.12:17310&disk=/home/service/var/data1"
```

Parameter List

| Parameter | Type   | Description                                                        |
|-----------|--------|--------------------------------------------------------------------|
| addr      | string | Address of the node where the disk to be decommissioned is located |
| disk      | string | Address of the disk to be decommissioned                           |

::: tip Note
New interface in v3.4.0
:::

## Cancel Node Decommission

``` bash
curl -v "http://192.168.0.11:17010/dataNode/cancelDecommission?addr=192.168.0.33:17310"
```

Parameter List

| Parameter | Type   | Description                                          |
|-----------|--------|------------------------------------------------------|
| addr      | string | Address for data node |

::: tip Note
New interface in v3.2.1
:::

## Query all offline disks

``` bash
curl -v "http://192.168.0.11:17010/disk/queryAllDecommissionDisk"
```

::: tip Note
New interface in v3.4.0
:::