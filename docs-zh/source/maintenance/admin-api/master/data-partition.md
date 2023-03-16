# 数据分片管理命令

## 创建

``` bash
curl -v "http://10.196.59.198:17010/dataPartition/create?count=400&name=test"
```

创建指定数量的数据分片。

参数列表

| 参数    | 类型     | 描述        |
|-------|--------|-----------|
| count | int    | 创建多少个数据分片 |
| name  | string | 卷的名字      |

## 查询

``` bash
curl -v "http://10.196.59.198:17010/dataPartition/get?id=100"  | python -m json.tool
```

展示数据分片的详细信息，包括副本数量、卷信息等。

参数列表

| 参数  | 类型     | 描述      |
|-----|--------|---------|
| id  | uint64 | 数据分片的ID |

响应示例

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

## 下线副本

``` bash
curl -v "http://10.196.59.198:17010/dataPartition/decommission?id=13&addr=10.196.59.201:17310"
```

移除数据分片的某个副本，并且创建一个新的副本。

参数列表

| 参数   | 类型     | 描述        |
|------|--------|-----------|
| id   | uint64 | 数据分片的ID   |
| addr | string | 要下线的副本的地址 |

## 比对副本文件

``` bash
curl -v "http://10.196.59.198:17010/dataPartition/load?id=1"
```

给数据分片的每个副本都发送比对副本文件的任务，然后异步的检查每个副本上的文件crc是否一致。

参数列表

| 参数  | 类型     | 描述      |
|-----|--------|---------|
| id  | uint64 | 数据分片的ID |
