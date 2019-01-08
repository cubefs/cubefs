# Data Subsystem

The data subsystem is design to meet the multi-tenancy requirement of supporting the large and small files with the sequential and random accesses, and adopts two different replication protocols to ensure the strong consistency among the replicas with some tradeoffs on the performance and code usability.

## Features

**Large File Storage**

For large files, the contents are stored as a sequence of one or multiple extents, which can be distributed
across different data partitions on different data nodes. Writing a new file to the extent store always causes the data to be written at the zero-offset of a new extent, which eliminates the need for the offset within the extent. The last extent of a file does not need to fill up its size limit by padding (i.e., the extent does not have holes), and never stores the data from other files.

**Small File Storage**

The contents of multiple small files are aggregated and stored in a single extent, and the physical offset
of each file content in the extent is recorded in the corresponding meta node. Deleting a file content (free the disk space occupied by this file) is achieved by the punch hole interface (fallocate()) provided by the underlying file system. The advantage of this design is to eliminate the need of implementing a garbage collection mechanism and therefore avoid to employ a mapping from logical offset to physical offset in an extent.

![data-subsystem.png](../assert/data-subsystem.png)

**Strong Consistency**

The replication is performed in terms of partitions during file writes. Depending on the file write pattern,
CFS adopts different replication strategies. <br>

- When a file is sequentially written into CFS, a primary-backup replication protocol is used to ensure the strong consistency with optimized IO throughput.<br>

    ![workflow-sequentail.png](../assert/workflow-sequential-write.png)

- When overwriting an existing file portion during random writes, we employ a MultiRaft-based replication protocol, which is similar to the one used in the metadata subsystem, to ensure the strong consistency.<br>

    ![workflow-overwriting.png](../assert/workflow-overwriting.png)

**Failure Recovery**

Because of the existence of two different replication protocols, when a failure on a replica is discovered,
we first start the recovery process in the primary-backup-based replication by checking the length of each extent and making all extents aligned. Once this processed is finished, we then start the recovery process in our MultiRaft-based replication.

## HTTP APIs

| API         | Method | Params                            | Desc                                |
| :---------- | :----- | :-------------------------------- | :---------------------------------- |
| /disks      | GET    | None                              | Get disk list and informations.     |
| /partitions | GET    | None                              | Get parttion list and infomartions. |
| /partition  | GET    | partitionId[int]                  | Get detail of specified partition.  |
| /extent     | GET    | partitionId[int]<br>extentId[int] | Get extent informations.            |
| /stats      | GET    | None                              | Get status of the datanode.         |

**Notes:**
>Cause of major components of CFS developed by Golang, the pprof APIs will be enabled automatically when the prof port have been config (specified by `prof` properties in configuratio file). So that you can use pprof tool or send pprof http request to check status of server runtime.