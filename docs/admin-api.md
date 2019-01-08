# API

## Cluster

### Overview

```bash
curl -v "http://127.0.0.1/admin/getCluster" | python -m json.tool
```

display the base information of the cluster, such as the detail of metaNode,dataNode,vol and so on.

response

``` json
{
    "Name": "containerfs",
    "LeaderAddr": "127.0.0.1:80",
    "CompactStatus": false,
    "DisableAutoAlloc": false,
    "Applied": 225,
    "MaxDataPartitionID": 100,
    "MaxMetaNodeID": 3,
    "MaxMetaPartitionID": 1,
    "DataNodeStat": {},
    "MetaNodeStat": {},
    "VolStat": [...],
    "MetaNodes": [...],
    "DataNodes": [...]
}
```

### Freeze

```bash
curl -v "http://127.0.0.1/cluster/freeze?enable=true"
```

if cluster is freezed,the vol never allocates dataPartitions automaticlly

|parameter | type | desc|
|---|---|---|
|enable|bool|if enable is true,the cluster is freezed

## MetaNode

### GET

```bash
curl -v "http://127.0.0.1/metaNode/get?addr=127.0.0.1:9021"  | python -m json.tool
```

show the base information of the metaNode,such as addr,total memory,used memory and so on.

|parameter | type | desc|
|---|---|---|
|addr|string| the addr which communicate with master

response

``` json
{
    "ID": 3,
    "Addr": "127.0.0.1:9021",
    "IsActive": true,
    "Sender": {
        "TaskMap": {}
    },
    "Rack": "",
    "MaxMemAvailWeight": 66556215048,
    "TotalWeight": 67132641280,
    "UsedWeight": 576426232,
    "Ratio": 0.008586377967698518,
    "SelectCount": 0,
    "Carry": 0.6645600532184904,
    "Threshold": 0.75,
    "ReportTime": "2018-12-05T17:26:28.29309577+08:00",
    "MetaPartitionCount": 1
}
```

### Offline

```bash
curl -v "http://127.0.0.1/metaNode/offline?addr=127.0.0.1:9021"
```

remove the metaNode from cluster, meta partitions which locate the metaNode will be migrate other available metaNode asynchronous

|parameter | type | desc|
|---|---|---|
|addr|string| the addr which communicate with master

### Threshold

```bash
curl -v "http://127.0.0.1/threshold/set?threshold=0.75"
```

the used memory percent arrives the threshold,the status of the meta partitions which locate the metaNode will be read only

|parameter | type | desc|
|---|---|---|
|threshold|float64| the max percent of memory which metaNode can use

## DataNode API

### GET
```bash
curl -v "http://127.0.0.1/dataNode/get?addr=127.0.0.1:5000"  | python -m json.tool
```

show the base information of the dataNode,such as addr,disk total size,disk used size and so on.

|parameter | type | desc|
|---|---|---|
|addr|string| the addr which communicate with master

response

``` json
{
    "MaxDiskAvailWeight": 3708923232256,
    "CreatedVolWeights": 2705829396480,
    "RemainWeightsForCreateVol": 36960383303680,
    "TotalWeight": 39666212700160,
    "UsedWeight": 2438143586304,
    "Available": 37228069113856,
    "Rack": "huitian_rack1",
    "Addr": "10.196.30.231:6000",
    "ReportTime": "2018-12-06T10:56:38.881784447+08:00",
    "Ratio": 0.06146650815226848,
    "SelectCount": 5,
    "Carry": 1.0655859145960367,
    "Sender": {
        "TaskMap": {}
    },
    "DataPartitionCount": 21
}
```

### Offline

```bash
curl -v "http://127.0.0.1/dataNode/offline?addr=127.0.0.1:5000"
```

remove the dataNode from cluster, data partitions which locate the dataNode will be migrate other available dataNode asynchronous

|parameter | type | desc|
|---|---|---|
|addr|string| the addr which communicate with master

## Vol API

### Create

```bash
curl -v "http://127.0.0.1/admin/createVol?name=test&replicas=3&type=extent&randomWrite=true&capacity=100"
```bash

allocate a set of data partition and a meta partition to the user

|parameter | type | desc|
|---|---|---|
|name|string|
|replicas|int|the number replica of data partition and meta partition
|type|string|the type of data partition,now only support extent type
|randomWrite|bool| true is the file in the data partition can be modified
|capacity|int| the quota of vol,unit is GB

### Delete

```bash
curl -v "http://127.0.0.1/vol/delete?name=test"
```

Mark the vol status to MarkDelete first, then delete data partition and meta partition asynchronous,finally delete meta data from persist store

|parameter | type | desc|
|---|---|---|
|name|string|

### Get

```bash
curl -v "http://127.0.0.1/client/vol?name=test" | python -m json.tool
```

show the base information of the vol,such as name,the detail of data partitions and meta partitions and so on.

|parameter | type | desc|
|---|---|---|
|name|string|

response

``` json
{
    "Name": "test",
    "VolType": "extent",
    "MetaPartitions": [...],
    "DataPartitions": [...]
}
```

### Stat

```bash
curl -v http://127.0.0.1/client/volStat?name=test
```

show vol stat information

|parameter | type | desc|
|---|---|---|
|name|string|

response

``` json
{
    "Name": "test",
    "TotalSize": 322122547200000000,
    "UsedSize": 15551511283278
}
```

### Update

```bash
curl -v "http://127.0.0.1/vol/update?name=test&capacity=100"
```
add the vol quota

|parameter | type | desc|
|---|---|---|
|name|string|
|capacity|int| the quota of vol,unit is GB

## MetaPartition API

### Create

```bash
curl -v "http://127.0.0.1/metaPartition/create?name=test&start=10000"
```

split meta parition manully,if max meta partition of the vol which range is [0,end),end larger than start parameter,old meta paritioin range is[0,start], new meta partition is [start+1,end)

|parameter | type | desc|
|---|---|---|
|name|string| the name of vol
|start|uint64| the start value of meta partition which will be create

### Get

```bash
curl -v "http://127.0.0.1/client/metaPartition?name=test&id=1" | python -m json.tool
```

show base information of meta partition,such as id,start,end and so on.

|parameter | type | desc|
|---|---|---|
|name|string| the name of vol
|id|uint64| the id of meta partition

response

``` json
{
    "PartitionID": 1,
    "Start": 0,
    "End": 9223372036854776000,
    "MaxNodeID": 1,
    "Replicas": [...],
    "ReplicaNum": 3,
    "Status": 2,
    "PersistenceHosts": [...],
    "Peers": [...],
    "MissNodes": {}
}
```

### Offline

```bash
curl -v "http://127.0.0.1/metaPartition/offline?name=test&id=13&addr=127.0.0.1:9021"
```

remove the replica of meta parition,and create new replica asynchronous

|parameter | type | desc|
|---|---|---|
|name|string| the name of vol
|id|uint64| the id of meta partition
|addr|string|the addr of replica which will be offfline


## DataPartition API

### Create

```bash
curl -v "http://127.0.0.1/dataPartition/create?count=40&name=test&type=extent"
```

create a set of data partition

|parameter | type | desc|
|---|---|---|
|count|int| the  num of dataPartitions will be create
|name|string| the name of vol
|type|string|the type of data partition,now only support extent type

### Get

```bash
curl -v "http://127.0.0.1/dataPartition/get?id=100"  | python -m json.tool
```

|parameter | type | desc|
|---|---|---|
|id|uint64| the  id of data partition

response

``` json
{
    "PartitionID": 100,
    "LastLoadTime": 1544082851,
    "ReplicaNum": 3,
    "Status": 2,
    "Replicas": [...],
    "PartitionType": "extent",
    "PersistenceHosts": [...],
    "Peers": [...],
    "MissNodes": {},
    "VolName": "test",
    "RandomWrite": true,
    "FileInCoreMap": {}
}
```

### Load

```bash
curl -v "http://127.0.0.1/dataPartition/load?name=test&id=1"
```

send load task to the dataNode which data parition locate on,then check the crc of each file in the data parttion asynchronous

|parameter | type | desc|
|---|---|---|
|name|string| the name of vol
|id|uint64| the  id of data partition


## Master manage API

### Add

```bash
curl -v "http://127.0.0.1/raftNode/add?addr=127.0.0.1:80&id=3"
```

add new master  node to master raft group

|parameter | type | desc|
|---|---|---|
|addr|string| the addr of master server, format is ip:port
|id|uint64| the node id of master server

### Remove

```bash
curl -v "http://127.0.0.1/raftNode/remove?addr=127.0.0.1:80&id=3"
```

remove the master node from master raft group

|parameter | type | desc|
|---|---|---|
|addr|string| the addr of master server, format is ip:port
|id|uint64| the node id of master server