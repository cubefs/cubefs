Overview
========

ContainerFS is a distributed fle system that is designed to natively support containers running at JD’s Kubernetes platform. 

High Level Architecture
-----------------------

CFS consists of a metadata subsystem, a data subsystem, and a resource manager (master), and can be accessed by different clients (as a set of application processes) hosted on the containers through different fle system instances called volumes.

Features
--------

Scalable Meta Management
^^^^^^^^^^^^^^^^^^^^^^^^

The metadata operations could make up as much as half of typical file system workloads. On our platform, this becomes even more important as there could be heavy accesses to the metadata of files by tens of thousands of clients simultaneously. A single node that stores the file metadata could easily become the performance bottleneck. As a result, we employ a distributed metadata subsystem to manage the file metadata. In this way, the metadata requests from different clients can be forwarded to different nodes, which improves the scalability of the entire system.
The metadata subsystem can be considered as an in-memory 
datastore of the file metadata. It can have thousands of meta nodes, each of which can have hundreds of billions of meta partitions. Each meta partition on a meta node stores the file metadata in memory by maintaining a set of inodes and a set of dentries.
We employ two b-trees called inodeTree and dentryTree for 
fast lookup of inodes and dentries in the memory. The inodeTree is indexed by the inode id, and the dentryTree is indexed by the dentry name and the parent inode id. We also maintain a range of the inode ids (denoted as start and end) stored on a meta partition for splitting.

Multi-tenancy
^^^^^^^^^^^^^

Strong Consistency
^^^^^^^^^^^^^^^^^^

POSIX-compliance
^^^^^^^^^^^^^^^^

