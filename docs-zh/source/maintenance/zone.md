# 分区管理

设置集群分区可以防止单个分区故障而引发整个集群不可用。每台节点启动的时候设置
cell 将自动加入该分区。

1 .  查看分区信息

``` bash
$ cfs-cli zone list
```

2 .  不小心错误设置了volume分区，希望改变分区

``` bash
$ cfs-cli volume update {volume name}  --zone-name={zone name}
```

3 .  MetaNde和DataNode没有设置分区会怎么样？

集群中大部分参数都是有默认值的，默认分区名字为default。需要注意的是一个分区内必须同时有足够的dn和mn，否则在该分区创建volume，要么数据分片初始化失败，要么元数据分片初始化失败。

4 .  NodeSet的意义？

每个zone会有若干nodeset，每个nodeset的默认容量为18个节点。因为CubeFS实现了multi-raft，每个node启动了一个raft
server进程, 每个raft
server管理该节点上的m个raft实例，如果这些raft实例的其他复制组成员分布在n个node上，raft实例之间会发送raft心跳，那么心跳会在n个节点之间传递，随着集群规模的扩大，n也会变得比较大。而通过nodeset限制，心跳在nodeset内部相对独立，避免了集群维度的心跳风暴,我们是使用了multi
raft和nodeset机制一起来避免产生raft心跳风暴问题。

![nodeset](../pic/nodeset.png)

5 .  一个volume的dp/mp在NodeSet中如何分布？

dp/mp在ns中均匀分布，每创建一个dp/mp，都会从上一个dp/mp所在的ns开始轮询，查找可用的ns进行创建。

6 .  NodeSet数量如何规划？

对于3副本的dp/mp，只有当一个ns中存在至少3个可用节点时，dp/mp才会选择该ns。count(ns)>= 18\*n + 3