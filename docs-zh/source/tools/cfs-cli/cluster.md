# 集群管理

## 获取集群信息

包括集群名称、地址、卷数量、节点数量及使用率等

```bash
cfs-cli cluster info
```

## 获取集群状态

按区域获取元数据和数据节点的使用量、状态等

```bash
cfs-cli cluster stat
```

## 冻结/解冻集群

设置为 `true` 冻结后，当partition写满，集群不会自动分配新的partition

```bash
cfs-cli cluster freeze [true/false]
```

## 设置内存阈值

设置集群中每个MetaNode的内存阈值。当内存使用率超过该阈值时，上面的meta partition将会被设为只读。[float]应当是一个介于0和1之间的小数.

```bash
cfs-cli cluster threshold [float]
```

## 设置集群参数

```bash
cfs-cli cluster set [flags]
```
```bash
Flags:
      --autoRepairRate string        DataNode auto repair rate
      --batchCount string            MetaNode delete batch count
      --deleteWorkerSleepMs string   MetaNode delete worker sleep time with millisecond. if 0 for no sleep
  -h, --help                         help for set
      --loadFactor string            Load Factor
      --markDeleteRate string        DataNode batch mark delete limit rate. if 0 for no infinity limit
      --maxDpCntLimit string         Maximum number of dp on each datanode, default 3000, 0 represents setting to default
```

