# 元数据节点管理

## 新增

``` bash
curl -v "http://192.168.0.11:17010/metaNode/add?addr=192.168.0.33:17310"
```

在对应区域上添加新的元数据节点

参数列表

| 参数   | 类型     | 描述                |
|------|--------|-------------------|
| addr | string | 元数据节点和 master 的交互地址 |

## 查询

``` bash
curl -v "http://10.196.59.198:17010/metaNode/get?addr=10.196.59.202:17210"  | python -m json.tool
```

展示元数据节点的详细信息，包括地址、总的内存大小、已使用内存大小等等。

参数列表

| 参数   | 类型     | 描述                |
|------|--------|-------------------|
| addr | string | 元数据节点和 master 的交互地址 |

响应示例

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

## 下线节点

``` bash
curl -v "http://10.196.59.198:17010/metaNode/decommission?addr=10.196.59.202:17210" 
```

::: tip 推荐
为了避免下线 node 时其被写入新数据，可以先进行设置节点状态操作从集群中下线某个元数据节点,该节点上的所有元数据分片都会被异步的迁移到集群中其它可用的元数据节点，分为普通模式和严格模式。
:::

参数列表

| 参数   | 类型     | 描述                |
|------|--------|-------------------|
| addr | string | 元数据节点和 master 的交互地址 |

## 设置阈值

``` bash
curl -v "http://10.196.59.198:17010/threshold/set?threshold=0.75"
```

如果某元数据节点内存使用率达到这个阈值，则该节点上所有的元数据分片都会被设置为只读。

参数列表

| 参数        | 类型      | 描述                |
|-----------|---------|-------------------|
| threshold | float64 | 元数据节点能使用本机内存的最大比率 |

## 迁移

``` bash
curl -v "http://10.196.59.198:17010/metaNode/migrate?srcAddr=src&targetAddr=dst&count=3"
```

从源元数据节点迁移指定个数元数据分区至目标元数据节点。

参数列表

| 参数         | 类型     | 描述                   |
|------------|--------|----------------------|
| srcAddr    | string | 迁出元数据节点地址            |
| targetAddr | string | 迁入元数据节点地址            |
| count      | int    | 迁移元数据分区的个数，非必填，默认15个 |

## 均衡

当集群里面部分metanode机器的内存负载高，部分低下，就可以通过迁移mp来实现内存负载的均衡。这样每台metanode需要处理的网络链接也会比较均衡。

### 自动生成迁移计划

有mn节点内存使用率达到75%，并且有空闲节点使用率低于30%，就可以生成。

#### 创建迁移计划

``` bash
curl -v "http://10.52.140.165:17010/metaNode/createBalanceTask"  | python -m json.tool
```

#### 显示迁移计划

``` bash
curl -v "http://10.52.140.165:17010/metaNode/getBalanceTask"  | python -m json.tool
```

#### 运行迁移计划

``` bash
curl -v "http://10.52.140.165:17010/metaNode/runBalanceTask"  | python -m json.tool
```

#### 中止迁移计划

``` bash
curl -v "http://10.52.140.165:17010/metaNode/stopBalanceTask"  | python -m json.tool
```

#### 删除迁移计划

``` bash
curl -v "http://10.52.140.165:17010/metaNode/deleteBalanceTask"  | python -m json.tool
```

### cfs-cli 工具命令

这部分命令对应的cfs-cli工具如下：

``` bash
./cfs-cli mp-balance create
./cfs-cli mp-balance show
./cfs-cli mp-balance run
./cfs-cli mp-balance stop
./cfs-cli mp-balance delete
```

### 注意事项

::: tip 注意
我们在一个集群里面只保存一份迁移计划。现有的迁移计划没有删除之前，不能创建新的。为了方便运维使用，没有同时创建多份迁移计划。

完成状态的迁移计划只会保留三天时间，后台就会主动去删除。如果任务状态出错，就一直停止，等待研发人员检查。也就是说哪怕是开启自动迁移autoMetaPartitionMigrate，也是3天才跑一下均衡任务。

可以通过/admin/setConfig更改集群里面的内存使用率的高水位，低水位，自动迁移。每次只能单独修改一个值。这个很少用到，就没有做成可以同时修改的方式，减少代码改动量。

出现问题时，可以通过指定迁移的mp来更正错误，命令是/metaNode/migratePartition。
:::

### 指定详细迁移信息

``` bash
curl -v "http://10.52.140.165:17010/metaNode/migratePartition?srcAddr=10.52.140.165:17210&targetAddr=10.52.140.102:17210&id=13"
```

### 查询迁移结果

``` bash
curl -v "http://10.52.140.165:17010/metaNode/migrateResult?targetAddr=10.52.140.165:17210&id=13"  | python -m json.tool
```