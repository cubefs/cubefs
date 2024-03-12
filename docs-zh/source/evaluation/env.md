# 环境准备

## 集群信息

**版本: v3.3.1**

| 节点类型   | 节点数 | CPU | CPU 频率                                 | 内存   | 内存频率      | 存储           | 磁盘接口类型 | 网络    | 备注       |
|--------|--------|-----|------------------------------------------|--------|---------------|----------------|--------------|---------|----------|
| 管理节点   | 3      | 8   | Intel(R) Xeon(R) Gold 6130 CPU @ 2.10GHz | 32GiB  | DDR4 2666 MHz | 197GiB     HDD | SCSI         | 10 Gb/s | docker容器 |
| 元数据节点 | 5      | 80  | Intel(R) Xeon(R) Gold 6230 CPU @ 2.10GHz | 377GiB | DDR4 2933MHz  | 4 x 3.7TiB SSD | NVMe         | 50 Gb/s | 混合部署   |
| 数据节点   | 5      | 80  | Intel(R) Xeon(R) Gold 6230 CPU @ 2.10GHz | 377GiB | DDR4 2933MHz  | 4 x 3.7TiB SSD | NVMe         | 50 Gb/s | 混合部署   |


## 卷设置

**未设置配额，DataNode未设置流控限速**

| 参数                 | 默认值 | 推荐值         | 说明                          |
|----------------------|--------|----------------|-----------------------------|
| FollowerRead         | False  | False          | 是否开启FollowerRead          |
| Capacity             | 10 GB  | 300 000 000 GB | 容量                          |
| Data Replica Number  | 3      | 500            | 数据副本数                    |
| Meta Replica Number  | 3      | 10             | 元数据副本数                  |
| Data Partition Size  | 120 GB | 120 GB         | 只是理论值上限 并不预分配空间 |
| Data Partition Count | 10     | 1500           | 数据分区数                    |
| Meta Partition Count | 3      | 10             | 元数据分区数                  |
| Cross Zone           | False  | False          | 是否跨zone                    |

设置方法:

```bash
./cfs-cli volume create test-vol {owner} --capacity=300000000 --mp-count=10
Create a new volume:
  Name                     : test-vol
  Owner                    : {owner}
  capacity                 : 300000000 G
  deleteLockTime           : 0 h
  crossZone                : false
  DefaultPriority          : false
  description              : 
  mpCount                  : 10
  replicaNum               : 
  size                     : 120 G
  volType                  : 0
  followerRead             : false
  readOnlyWhenFull         : false
  zoneName                 : 
  cacheRuleKey             : 
  ebsBlkSize               : 8388608 byte
  cacheCapacity            : 0 G
  cacheAction              : 0
  cacheThreshold           : 10485760 byte
  cacheTTL                 : 30 day
  cacheHighWater           : 80
  cacheLowWater            : 60
  cacheLRUInterval         : 5 min
  TransactionMask          : 
  TransactionTimeout       : 1 min
  TxConflictRetryNum       : 0
  TxConflictRetryInterval  : 0 ms

Confirm (yes/no)[yes]: yes
Create volume success.
```

扩容 dp 至 500 个：

```bash
curl -v "http://10.196.59.198:17010/dataPartition/create?count=32&name=test-vol"
```

