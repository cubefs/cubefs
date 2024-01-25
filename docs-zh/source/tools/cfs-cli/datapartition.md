# 数据分片管理

## 获取指定dp信息

获取指定data partition的信息

```bash
cfs-cli datapartition info [Partition ID]
```

## 下线dp分片

将目标节点上的指定data partition分片下线，并自动转移至其他可用节点

```bash
cfs-cli datapartition decommission [Address] [Partition ID]
```

## 新增dp分片

在目标节点新增一个data partition分片

```bash
cfs-cli datapartition add-replica [Address] [Partition ID]
```

## 删除dp分片

删除目标节点上的data partition分片

```bash
cfs-cli datapartition del-replica [Address] [Partition ID]
```

## 故障dp查找

查找多半分片不可用和分片缺失的data partition

```bash
cfs-cli datapartition check
```

## 标记副本丢失

```bash
cfs-cli datapartition set-discard [DATA PARTITION ID] [DISCARD]
```