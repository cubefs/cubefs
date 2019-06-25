Resource Manager (Master)
=========================

The resource manager  manages the file system by  processing  different types of tasks, such as creating/deleting/updating/loading partitions and keeping track of the resource status (such as the memory/disk utilization). The resource manager is also responsible for creating new volumes and adding new meta/data nodes to the ChubaoFS cluster. It has multiple replicas, among which the consistency is maintained by a consensus algorithm such as Raft, and persisted to a key value store such as RocksDB for backup and recovery.

Utilization-Based Distribution/Placement
-----------------------------------------

The resource manager is a utilization-based distribution strategy to places the file metadata and contents across different meta and data nodes.
Because each  node can have multiple  partitions, and  the  partitions in a volume do not need to reside on the same node, this distribution can be controlled at a finer  granularity, resulting a more efficient resource management.
Specifically, the distribution of file metadata and contents works follows:

First, when mounting a volume, the client  asks the resource manager for a set of available meta and data partitions. These partitions are usually the ones  on the nodes with the lowest memory/disk utilizations. Later on, when writing a file,  the client can only choose the meta and data partitions  in a random fashion  from the ones allocated by the resource manager.

Second,  when the resource manager  finds that all the partitions in a volume is about to be full,  it  automatically adds a set of new  partitions to this volume.  These partitions are usually the ones on the nodes with the lowest memory/disk utilizations.  Note that, when a  partition is full, or a threshold (i.e.,  the number of files on a meta partition or the number of extents on a data partition) is reached, no new data can be stored on this partition, although it can still be modified or deleted.


Replica Placement
-----------------------

When choosing partitions for the replicas, the resource manager ensures that  two replicas of the same partition never reside on the same node.

Meta Partition Splitting
-------------------------

There is a special requirement when splitting a meta partition.
In particular,  if  a meta partition is about to reach its  upper  limit of the number of stored inodes and  dentries,  a splitting task needs to be performed with the requirement to ensure that the inode ids stored at the newly created partition are unique from the ones stored at the original partition.

To meet this requirement, when splitting a meta partition, the resource manager cuts off the inode range of the meta partition in advance at a upper bound *end*, a value greater than highest inode id used so far (denoted as *maxInodeID*), and sends a split request to the meta node  to (1) update the inode range from *1* to *end*  for the original meta partition, and (2) create a new meta partition with the inode range from *end + 1* to *infinity* for this volume.
As a result, the inode range for these two meta partitions becomes *[1, end]* and *[end + 1, infinity]*, respectively. If there is another file needs to be created, then its inode id will be chosen as *maxInodeID + 1* in the original meta partition, or *end + 1* in the newly created meta partition.
The *maxInodeID* of each meta partition can be obtained by the periodical communication between the resource manager and the the meta nodes.



Exception Handling
-----------------------

When a request to a meta/data partition times out (e.g., due to  network outage), the remaining replicas of this partition are marked as read-only.
When a meta/data partition is no longer available (e.g., due to hardware failures), all the data on this partition will eventually be migrated to a new available partition manually. This unavailability is identified by the multiple failures reported by the  node when operating the files.


