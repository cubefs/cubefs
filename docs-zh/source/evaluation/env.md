# 环境准备

## 集群信息

| 节点类型  | 节点数 | CPU | 内存   | 存储           | 网络      | 备注   |
|-------|-----|-----|------|--------------|---------|------|
| 管理节点  | 3   | 32  | 32GB | 120GB SSD    | 10 Gb/s |      |
| 元数据节点 | 10  | 32  | 32GB | 16 x 1TB SSD | 10 Gb/s | 混合部署 |
| 数据节点  | 10  | 32  | 32GB | 16 x 1TB SSD | 10 Gb/s | 混合部署 |


## 卷设置

| 参数                   | 默认值    | 推荐值            | 说明               |
|----------------------|--------|----------------|------------------|
| FollowerRead         | True   | True           | 是否开启FollowerRead |
| Capacity             | 10 GB  | 300 000 000 GB | 容量               |
| Data Replica Number  | 3      | 3              | 数据副本数            |
| Meta Replica Number  | 3      | 3              | 元数据副本数           |
| Data Partition Size  | 120 GB | 120 GB         | 只是理论值上限 并不预分配空间  |
| Data Partition Count | 10     | 1500           | 数据分区数            |
| Meta Partition Count | 3      | 10             | 元数据分区数           |
| Cross Zone           | False  | False          | 是否跨zone          |

设置方法:

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


## client配置

```bash
# 查看当前iops：
$ http://[ClientIP]:[ProfPort]/rate/get
# 设置iops，默认值-1代表不限制iops
$ http://[ClientIP]:[ProfPort]/rate/set?write=800&read=800
```

