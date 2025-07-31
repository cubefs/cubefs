# 数据节点管理

## 新增

``` bash
curl -v "http://192.168.0.11:17010/dataNode/add?addr=192.168.0.33:17310&zoneName=default"
```

在对应区域上添加新的数据节点

参数列表

| 参数     | 类型   | 描述                               |
|----------|--------|----------------------------------|
| addr     | string | 数据节点和master的交互地址         |
| zoneName | string | 指定区域，如果为空则默认值为default |

## 查询

``` bash
curl -v "http://10.196.59.198:17010/dataNode/get?addr=10.196.59.201:17310"  | python -m json.tool
```

显示数据节点的详情，包括数据节点的地址、总的容量、已使用空间等等。

参数列表

| 参数 | 类型   | 描述                       |
|------|--------|--------------------------|
| addr | string | 数据节点和master的交互地址 |

响应示例

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

## 下线节点

``` bash
curl -v "http://192.168.0.11:17010/dataNode/decommission?addr=192.168.0.33:17310&weight=2"
```

从集群中下线某个数据节点,
该数据节点上的所有数据分片都会被异步的迁移到集群中其它可用的数据节点

参数列表

| 参数 | 类型   | 描述                       |
|------|--------|--------------------------|
| addr | string | 数据节点和master的交互地址 |
| weight | int | 下线权重，默认是2 |

## 获取磁盘信息

``` bash
curl -v "http://192.168.0.11:17320/disks"
```

获取磁盘信息，包括磁盘路径，空间使用情况，磁盘状态等。

## 获取节点分区信息

``` bash
curl -v "http://192.168.0.11:17320/partitions"
```

获取磁盘信息，包括分区ID，分区大小和状态等。

## 磁盘下线

``` bash
curl -v "http://192.168.0.11:17010/disk/decommission?addr=192.168.0.12:17310&disk=/home/service/var/data1&weight=2"
```

参数列表

| 参数  | 类型   | 描述                            |
|-------|--------|-------------------------------|
| addr  | string | 要下线的磁盘的节点地址          |
| disk  | string | 故障磁盘                        |
| count | int    | 每次下线个数，默认0，代表全部下线 |
| diskDisable | bool    | 是否禁用磁盘，即禁止在下线的磁盘上新建dp，默认为true |
| decommissionType | int    | 下线类型，默认0,代表手动下线；1表示自动下线 |
| weight | int    | 下线权重，默认是2 |

## 迁移

``` bash
curl -v "http://10.196.59.198:17010/dataNode/migrate?srcAddr=src&targetAddr=dst&count=3"
```

从源数据节点迁移指定个数数据分区至目标数据节点。

参数列表

| 参数       | 类型   | 描述                               |
|------------|--------|----------------------------------|
| srcAddr    | string | 迁出数据节点地址                   |
| targetAddr | string | 迁入数据节点地址                   |
| count      | int    | 迁移数据分区的个数，非必填，默认50个 |

## 设置磁盘下线控制速度

``` bash
curl -v "http://192.168.0.11:17320/admin/updateDecommissionLimit?decommissionLimit=10"
```

参数列表

| 参数              | 类型 | 描述             |
|-------------------|------|----------------|
| decommissionLimit | int  | 并发下线的dp个数 |

::: tip 提示
v3.2.1新增接口
:::

## 查询磁盘下线控制速度

``` bash
curl -v "http://192.168.0.11:17320/admin/queryDecommissionLimit"
```

::: tip 提示
v3.2.1新增接口
:::

## 查询磁盘下线进度

``` bash
curl -v "http://192.168.0.11:17320/disk/queryDecommissionProgress?addr=192.168.0.12:17310&disk=/home/service/var/data1"
```

参数列表

| 参数 | 类型   | 描述               |
|------|--------|------------------|
| addr | string | 下线磁盘的节点地址 |
| disk | string | 下线磁盘地址       |

::: tip 提示
v3.2.1新增接口
:::

## 查询节点下线进度

``` bash
curl -v "http://192.168.0.11:17010/dataNode/queryDecommissionProgress?addr=192.168.0.33:17310"
```

参数列表

| 参数 | 类型   | 描述                       |
|------|--------|--------------------------|
| addr | string | 数据节点和master的交互地址 |

::: tip 提示
v3.2.1新增接口
:::

## 取消磁盘下线

``` bash
curl -v "http://192.168.0.11:17320/disk/pauseDecommission?addr=192.168.0.12:17310&disk=/home/service/var/data1"
```

参数列表

| 参数 | 类型   | 描述               |
|------|--------|------------------|
| addr | string | 下线磁盘的节点地址 |
| disk | string | 下线磁盘地址       |

::: tip 提示
v3.4.0新增接口
:::

## 取消节点下线

``` bash
curl -v "http://192.168.0.11:17010/dataNode/cancelDecommission?addr=192.168.0.33:17310"
```

参数列表

| 参数 | 类型   | 描述                       |
|------|--------|--------------------------|
| addr | string | 数据节点地址 |

::: tip 提示
v3.2.1新增接口
:::

## 查询所有下线磁盘

``` bash
curl -v "http://192.168.0.11:17010/disk/queryAllDecommissionDisk"
```

::: tip 提示
v3.4.0新增接口
:::