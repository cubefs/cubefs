Operations Guides
==================

1. Log information：metaPartition(xx) changeLeader to (xx)：

    Change leader. It is a normal behavior.

2. Log information：inode count is not equal, vol[xxx], mpID[xx]：

    The number of inodes is inconsistent. When writing data, as long as two of the three replicas are successful, it will be considered successful. So there will be inconsistencies in the three replicas. Check the logs for specific reasons.

3. Log information：clusterID[xxx] addr[xxx]_op[xx] has no response until time out：

    During the [op] operation, the connection to the node at this address timed out. Solution: Check the network connectivity; see if the service process is still there.

4. Log information：checkFileCrcTaskErr clusterID[xxx] partitionID:xxx File:xxx badCrc On xxx:

    The file crc check failed. Check the DataNode logs for specific reasons.

5. Can the master change IP?

    Yes. But in addition to adjusting the configuration of the Master, you also need to adjust the configuration of all functional nodes (MetaNode, DataNode, ObjectNode) and Client in the entire cluster, so that these components can communicate with the Master after the adjustment of the IP.

6. One of the three masters is broken. Can the remaining two restart to provide services normally?

    Yes. Since the Master uses the RAFT algorithm, services can be provided normally when the number of remaining nodes exceeds 50% of the total number of nodes.

7. How to deal with physical machine failure of DataNode?

    Refer to the DataNode API :doc:`/admin-api/master/datanode` to decommission the specified DataNode. The offline operation will remove the specified DataNode from the resource pool of the cluster, and ChubaoFS will automatically use the available resources in the cluster to complete the data replicas loss caused by the offline DataNode.

8. How to deal with physical machine failure of MetaNode?

    Refer to the MetaNode API :doc:`/admin-api/master/metanode` to decommission the specified MetaNode. The offline operation will remove the specified MetaNode from the resource pool of the cluster, and ChubaoFS will automatically use the available resources in the cluster to complete the meta replicas loss caused by the offline MetaNode.

