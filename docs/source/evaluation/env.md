# Environment Preparation

## Cluster Information

**Version: v3.3.1**

| Node Type       | Number of Nodes | CPU                                           | Memory              | Storage             | Network | Remarks          |
|-----------------|-----------------|-----------------------------------------------|---------------------|---------------------|---------|------------------|
| Management Node | 3               | 8    Intel(R) Xeon(R) Gold 6130 CPU @ 2.10GHz | 32GiB DDR4 2666 MHz | 197GiB     HDD SCSI | 10 Gb/s | Docker container |
| Metadata Node   | 5               | 80   Intel(R) Xeon(R) Gold 6230 CPU @ 2.10GHz | 377GiB DDR4 2933MHz | 4 x 3.7TiB SSD NVMe | 50 Gb/s | Mixed deployment |
| Data Node       | 5               | 80   Intel(R) Xeon(R) Gold 6230 CPU @ 2.10GHz | 377GiB DDR4 2933MHz | 4 x 3.7TiB SSD NVMe | 50 Gb/s | Mixed deployment |

## Volume Settings

**No quota is set, and no flow control rate limit is set on DataNode.**

| Parameter            | Default Value | Recommended Value | Description                                        |
|----------------------|---------------|-------------------|----------------------------------------------------|
| FollowerRead         | False         | False             | Whether to enable FollowerRead                     |
| Capacity             | 10 GB         | 300,000,000 GB    | Capacity                                           |
| Data Replica Number  | 3             | 500               | Number of data replicas                            |
| Meta Replica Number  | 3             | 3                 | Number of metadata replicas                        |
| Data Partition Size  | 120 GB        | 120 GB            | Theoretical upper limit, no space is pre-allocated |
| Data Partition Count | 10            | 1500              | Number of data partitions                          |
| Meta Partition Count | 3             | 10                | Number of metadata partitions                      |
| Cross Zone           | False         | False             | Whether to cross zones                             |

Setting method:

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

Expand dp to 500：

```bash
curl -v "http://10.196.59.198:17010/dataPartition/create?count=32&name=test-vol"
```