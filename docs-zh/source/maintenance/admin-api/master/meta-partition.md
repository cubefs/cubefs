# 元数据分片管理

## 创建

``` bash
curl -v "http://10.196.59.198:17010/metaPartition/create?name=test&start=10000"
```

手动切分元数据分片，如果卷的最大的元数据分片 **inode** 的范围是
`[begin, end)`:

若 **start** 大于 **begin** 小于 **end**
参数，原来最大的元数据分片的inode范围变为 `[begin, start]`
，新创建的元数据分片的范围是 `[start+1,+inf)` ;

若 **start** 小于 **begin**,
max为当前分片上最大的inode编号，则inode范围变为 `[begin,  max+16777216]`
，新创建的元数据分片的范围是 `[max+16777217,+inf)` ;

若 **start** 大于 **end**,
max为当前分片上最大的inode编号，则inode范围变为 `[begin,  start]`
，新创建的元数据分片的范围是 `[start+1, +inf)` ;

*注意：start过大会导致单个分片上inode过大，占用较大内存，
当最后一个分片上inode过多时，也会触发mp自动分裂的*

参数列表

| 参数    | 类型     | 描述          |
|-------|--------|-------------|
| name  | string | 卷的名字        |
| start | uint64 | 根据此值切分元数据分片 |

## 查询

``` bash
curl -v "http://10.196.59.198:17010/metaPartition/get?id=1" | python -m json.tool
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
curl -v "http://10.196.59.198:17010/metaPartition/decommission?id=13&addr=10.196.59.202:17210"
```

下线元数据分片的某个副本，并且创建一个新的副本。

参数列表

| 参数   | 类型     | 描述       |
|------|--------|----------|
| id   | uint64 | 元数据分片ID  |
| addr | string | 要下线副本的地址 |

## 比对副本

``` bash
curl -v "http://10.196.59.198:17010/metaPartition/load?id=1"
```

发送比对副本任务到各个副本，然后检查各个副本的Crc是否一致。

参数列表

| 参数  | 类型     | 描述      |
|-----|--------|---------|
| id  | uint64 | 元数据分片ID |
