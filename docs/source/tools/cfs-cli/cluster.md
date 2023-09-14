# Cluster Management

## Show Cluster Information

Get cluster information, including cluster name, address, number of volumes, number of nodes, and usage rate, etc.

```bash
cfs-cli cluster info
```

## Show Cluster Usage

Get usage, status, etc. of metaNodes and dataNodes by region.

```bash
cfs-cli cluster stat
```

## Freeze/Unfreeze Cluster

Freeze the cluster. After setting it to `true`, when the partition is full, the cluster will not automatically allocate new partitions.

```bash
cfs-cli cluster freeze [true/false]
```

## Set Memory Threshold

Set the memory threshold for each MetaNode in the cluster. If the memory usage reaches this threshold, all the metaPartition will be readOnly. [float] should be a float number between 0 and 1.

```bash
cfs-cli cluster threshold [float]
```

## Cluster Configuration Setup

Set the configurations of the cluster.

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

