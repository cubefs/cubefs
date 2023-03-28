# Metadata Subsystem

The Metadata subsystem is a memory-centric distributed data structure consisting of one or more metadata partitions.

The metadata subsystem data uses Multi-Raft to fully utilize server resources and ensure high availability and strong consistency of data. It can easily migrate resources and achieve horizontal scaling through splitting.

## Internal Design of Metadata

Each metadata can contain hundreds or thousands of metadata partitions, and each partition consists of an `InodeTree (BTree)` and a `DentryTree (BTree)`.

Each `inode` represents a file or directory in the file system, and each `dentry` represents a directory entry, which is composed of `parentId` and `name`.

- In the `DentryTree`, storage and retrieval are performed using an index composed of `ParentId` and `name`.
- In the `InodeTree`, indexing is performed using the `inode id`.
- MultiRaft protocol is used to ensure high availability and data consistency replication, and each node set contains a large number of partition groups, each of which corresponds to a `raft group`.
- Each partition group belongs to a certain volume, and the partition group is a metadata range (`inode id` range `[100-20000)` ) of a certain volume.
- The Metadata subsystem achieves dynamic scaling through splitting.
- When the performance of a partition group (including the following indicators: memory) is close to a certain value, the Resource Manager service will estimate an end point and notify the device of this group of nodes to only serve data before this point. At the same time, a new group of nodes will be selected and dynamically added to the current business system. The starting point of the new node group is exactly the end point position of the previous node group.

## Replication

The replication of metadata updates is based on metadata partitions. Replication strong consistency is achieved through MultiRaft, an improved version of Raft. MultiRaft reduces the burden of heartbeat communication.

## Fault Recovery

Memory metadata partitions are persisted to disk through snapshots for backup and recovery purposes. Log compression technology is used to reduce the size of log files and recovery time.

It is worth mentioning that metadata operations may cause orphan inodes, which means that there are only inodes but no corresponding dentry. To reduce the occurrence of this situation:

- First, the metadata node ensures high availability through Raft, and can quickly recover after a single point of failure.
- Secondly, the client ensures retries within a certain period of time.