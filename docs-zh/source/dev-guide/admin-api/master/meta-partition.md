# 元数据分片管理

## 创建

``` bash
curl -v "http://192.168.0.1:17010/metaPartition/create?count=10&name=test"
```

批量创建元数据分区

参数列表

| 参数    | 类型     | 描述          |
|-------|--------|-------------|
| name  | string | 卷的名字        |
| count | uint64 | 新增的mp数目 |

## 查询

``` bash
curl -v "http://192.168.0.1:17010/metaPartition/get?id=1" | python -m json.tool
```

展示元数据分片的详细信息，包括分片ID，分片的起始范围等等。

参数列表

| 参数  | 类型     | 描述      |
|-----|--------|---------|
| id  | uint64 | 元数据分片ID |

响应示例

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

## 下线副本

``` bash
curl -v "http://192.168.0.1:17010/metaPartition/decommission?id=13&addr=10.196.59.202:17210"
```

下线元数据分片的某个副本，并且创建一个新的副本。

参数列表

| 参数   | 类型     | 描述       |
|------|--------|----------|
| id   | uint64 | 元数据分片ID  |
| addr | string | 要下线副本的地址 |

## 比对副本

``` bash
curl -v "http://192.168.0.1:17010/metaPartition/load?id=1"
```

发送比对副本任务到各个副本，然后检查各个副本的Crc是否一致。

参数列表

| 参数  | 类型     | 描述      |
|-----|--------|---------|
| id  | uint64 | 元数据分片ID |
