# 故障域管理

设置集群分区可以防止单个分区故障导致整个集群不可用。每台节点启动的时候在配置文件中设置 `zoneName` 将自动加入该分区。

## 查看分区信息

``` bash
$ cfs-cli zone list
ZONE        STATUS    
default     available
```

## 修改分区 

如果不小心错误设置了 volume 分区，可以通过以下命令来改变分区

``` bash
$ cfs-cli volume update {volume name}  --zone-name={zone name}
```

## 默认分区

集群中大部分参数都是有默认值的，节点默认分配至 `default` 分区。需要注意的是，一个分区内必须同时有足够的 datanode 和 metanode，否则在该分区创建 volume 时，可能会导致数据分片(dp)初始化失败或元数据分片(mp)初始化失败。

## NodeSet 含义

每个 `zone` 会有若干 `nodeset`，每个 nodeset 的默认容量为 18 个节点。

因为 CubeFS 实现了 `multi-raft`，每个节点启动了一个 raft server 进程, 每个 raft server 管理该节点上的 m 个 raft 实例，如果这些 raft 实例的其他复制组成员分布在 n 个节点上，raft 实例之间会发送 raft 心跳。这样，心跳会在 n 个节点之间传递。随着集群规模的扩大，n 也会相应增大。

通过 nodeset 限制，心跳在 nodeset 内部相对独立，从而避免了集群维度的心跳风暴。我们使用了 multi-raft 和 nodeset 机制共同来防止 raft 心跳风暴问题的产生。

![nodeset](../pic/nodeset.png)

## dp/mp 在 NodeSet 分布

dp/mp 在 ns 中均匀分布，每创建一个 dp/mp，都会从上一个 dp/mp 所在的 ns 开始轮询，查找可用的 ns 进行创建。

## NodeSet 数量规划

对于 3 副本的 dp/mp，只有当一个 ns 中存在至少 3 个可用节点时，dp/mp 才会选择该 ns。