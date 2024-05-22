# 故障域管理

设置集群分区可以防止单个分区故障而引发整个集群不可用。每台节点启动的时候设置 cell 将自动加入该分区。

## 查看分区信息

``` bash
$ cfs-cli zone list
```

## 修改分区 

不小心错误设置了volume分区，希望改变分区

``` bash
$ cfs-cli volume update {volume name}  --zone-name={zone name}
```

## 默认分区

集群中大部分参数都是有默认值的，默认分区名字为 default。需要注意的是一个分区内必须同时有足够的 dn 和 mn，否则在该分区创建 volume，要么数据分片初始化失败，要么元数据分片初始化失败。

## NodeSet 含义

每个 zone 会有若干 nodeset，每个 nodeset 的默认容量为 18 个节点。

因为 CubeFS 实现了 multi-raft，每个 node 启动了一个 raft server 进程, 每个 raft server 管理该节点上的 m 个 raft 实例，如果这些 raft 实例的其他复制组成员分布在 n 个 node 上，raft 实例之间会发送 raft 心跳，那么心跳会在 n 个节点之间传递，随着集群规模的扩大，n 也会变得比较大。

通过 nodeset 限制，心跳在 nodeset 内部相对独立，避免了集群维度的心跳风暴,我们是使用了 multi raft 和 nodeset 机制一起来避免产生 raft 心跳风暴问题。

![nodeset](./pic/nodeset.png)

## dp/mp 在 NodeSet 分布

dp/mp 在 ns 中均匀分布，每创建一个 dp/mp，都会从上一个 dp/mp 所在的 ns 开始轮询，查找可用的 ns 进行创建。

## NodeSet 数量规划

对于 3 副本的 dp/mp，只有当一个 ns 中存在至少 3 个可用节点时，dp/mp 才会选择该 ns。`count(ns)>= 18*n + 3`