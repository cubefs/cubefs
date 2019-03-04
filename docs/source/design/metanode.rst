Metadata Subsystem
===================

The  metadata operations could make up as much as half of typical file system workloads.
this can be important as there could be  heavy accesses to the  metadata of files by tens of thousands of clients simultaneously. A single node  that stores the file metadata could easily  become the performance bottleneck. As a result, we employ a distributed metadata subsystem to manage the file metadata. In this way, the metadata requests from different clients can be forwarded to different  nodes, which improves the scalability of the entire system.


Internal Structure
-------------------

The metadata subsystem can be considered as an in-memory datastore of  the file metadata. It can have thousands of meta nodes, each of which can have a set of meta partitions.
Each meta partition on a meta node stores the file metadata in memory by maintaining a set of *inodes* and  a set of *dentries*.


Generally speaking, An inode is an object that represents the underlying file (or  directory), and can be identified by an unsigned 64-bit integer called the *inode id*. A dentry is an object  that represents the directory hierarchy and can be identified by  a string name  and the id of the parent   inode.
For example, if we have two directories *foo* and *bar*, where *foo* is the parent directory of *bar*, then there are two inodes: one for *foo* called *i1*, and the other for *bar* called *i2*, and one dentry to represent the hierarchy of these two directories where *i2* is the current inode  and *i1* is the parent inode.


A meta partition can only store the inodes and dentries of the files from the same volume. We employ two b-trees called *inodeTree*  and *dentryTree*  for fast lookup of   inodes  and dentries in the memory. The  *inodeTree* is indexed by the inode id, and the *dentryTree*  is indexed by the dentry name and the parent inode id.   We also maintain a range of  the inode ids (denoted as *start* and *end*) stored on a meta partition for splitting (see :doc:`master`).


Replication with Strong Consistency
------------------------------------

The replication during file write is performed in terms of meta partitions.
The strong consistency among the replicas of each meta partition is ensured by a  revision of the  Raft consensus protocol  called the  MultiRaft, which has the advantage of reduced  heartbeat network traffic comparing to the original version.


Failure Recovery
-----------------

The in-memory meta partitions  are  persisted  to the local disk by snapshots and  logs for backup and recovery. Some techniques such as log compaction are used to reduce the log files sizes and shorten the recovery time.

It worths noting that, a  failure  that happens during a metadata operation could result an *orphan* inode with which has no dentry to be associated. The memory and disk space occupied by this inode can be hard to free.  To minimize the chance of this case to happen, the client always issues a retry after a failure until the request succeeds or the maximum retry limit is reached.



