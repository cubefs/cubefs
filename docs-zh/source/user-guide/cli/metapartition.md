# 元数据分片管理

## 获取指定mp信息

获取指定 meta partition 的信息

```bash
cfs-cli metapartition info [Partition ID]
```

## 下线mp分片

将目标节点上的指定 meta partition 分片下线，并自动转移至其他可用节点

```bash
cfs-cli metapartition decommission [Address] [Partition ID]
```

## 新增mp分片

在目标节点新增一个 meta partition 分片

```bash
cfs-cli metapartition add-replica [Address] [Partition ID]
```

## 删除mp分片

删除目标节点上的 meta partition 分片

```bash
cfs-cli metapartition del-replica [Address] [Partition ID]
```

## 故障mp查找

查找多半分片不可用和分片缺失的 meta partition

```bash
cfs-cli metapartition check
```
