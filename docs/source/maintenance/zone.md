# Zone Management

Setting cluster zones can prevent the entire cluster from becoming unavailable due to a single partition failure. When each node starts, the cell set will automatically join the partition.

## View Zone Information

``` bash
$ cfs-cli zone list
```

## Modify Zone

If you accidentally set the volume partition incorrectly and want to change the partition:

``` bash
$ cfs-cli volume update {volume name}  --zone-name={zone name}
```

## Default Zone

Most parameters in the cluster have default values, and the default zone name is "default". It should be noted that there must be enough datanodes and metanodes in a partition at the same time, otherwise, when creating a volume in the partition, either the data partition initialization will fail or the metadata partition initialization will fail.

## Meaning of NodeSet

Each zone will have several nodesets, and the default capacity of each nodeset is 18 nodes.

Because CubeFS has implemented multi-raft, each node has started a raft server process, and each raft server manages m raft instances on the node. If the other replicas of these raft instances are distributed on n nodes and the raft instances send raft heartbeats between them, the heartbeats will be transmitted between n nodes. As the cluster scales up, n will become relatively large.

Through the nodeset restriction, the heartbeats are relatively independent within the nodeset, avoiding the heartbeat storm problem at the cluster level. We use the multi-raft and nodeset mechanisms together to avoid the problem of raft heartbeat storms.

![nodeset](../pic/nodeset.png)

## Distribution of dp/mp in NodeSet

The dp/mp is evenly distributed in the ns. Each time a dp/mp is created, it will start polling from the ns where the previous dp/mp was located to find an available ns for creation.

## Planning the Number of NodeSets

For dp/mp with 3 replicas, dp/mp will only select the ns when there are at least 3 available nodes in the ns. `count(ns)>= 18\*n + 3`