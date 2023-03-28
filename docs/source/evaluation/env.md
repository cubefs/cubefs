# Environment Preparation

## Cluster Information

| Node Type       | Number of Nodes | CPU | Memory | Storage      | Network | Remarks          |
|-----------------|-----------------|-----|--------|--------------|---------|------------------|
| Management Node | 3               | 32  | 32GB   | 120GB SSD    | 10 Gb/s |                  |
| Metadata Node   | 10              | 32  | 32GB   | 16 x 1TB SSD | 10 Gb/s | Mixed deployment |
| Data Node       | 10              | 32  | 32GB   | 16 x 1TB SSD | 10 Gb/s | Mixed deployment |

## Volume Settings

| Parameter            | Default Value | Recommended Value | Description                                        |
|----------------------|---------------|-------------------|----------------------------------------------------|
| FollowerRead         | True          | True              | Whether to enable FollowerRead                     |
| Capacity             | 10 GB         | 300,000,000 GB    | Capacity                                           |
| Data Replica Number  | 3             | 3                 | Number of data replicas                            |
| Meta Replica Number  | 3             | 3                 | Number of metadata replicas                        |
| Data Partition Size  | 120 GB        | 120 GB            | Theoretical upper limit, no space is pre-allocated |
| Data Partition Count | 10            | 1500              | Number of data partitions                          |
| Meta Partition Count | 3             | 10                | Number of metadata partitions                      |
| Cross Zone           | False         | False             | Whether to cross zones                             |

Setting method:

```bash
 $ cfs-cli volume create test-vol {owner} --capacity=300000000 --mp-count=10
 Create a new volume:
   Name                : test-vol
   Owner               : ltptest
   Dara partition size : 120 GB
   Meta partition count: 10
   Capacity            : 300000000 GB
   Replicas            : 3
   Allow follower read : Enabled

 Confirm (yes/no)[yes]: yes
 Create volume success.

 $ cfs-cli volume add-dp test-vol 1490
```


## Client Configuration

```bash
# View the current IOPS:
$ http://[ClientIP]:[ProfPort]/rate/get
# Set the IOPS. The default value of -1 means that the IOPS is not limited.
$ http://[ClientIP]:[ProfPort]/rate/set?write=800&read=800
```