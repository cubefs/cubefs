# Roadmap of 2024

### Release scheduled
|Feature|Type|Version|Status|Branch|Scheduled Release Date|Details|
|:----|:----|:----|:----|:----|:----|:----------------|
|Automatic migration|Stability|Release-3.4.0|QA Testing|develop-v3.4.0|JUNE|Automatic disk migration reduces the atomicity problem of metadata information during the migration process and improves the level of operational automation.|
|Snapshot|Feature|Release-3.4.0|QA Testing|develop-v3.4.0|JUNE|Provides an experimental version of the volume snapshot feature, supporting second-level snapshots|
|Hybrid Cloud automatic<br>data hierarchy<br>|cost<br>optimization<br>|Release-3.5.0|QA Testing|develop-hybridcloudlifecycle|JULY|Hybrid cloud projects support a unified namespace, provide the ability to use multiple storage systems in a mixed manner, and provide external S3 and HDFS capabilities. Support life cycle driven data flow between different media, storage types, and on and off the cloud, reducing costs and increasing efficiency. The first issue will be released soon.|
|Distributed Cache|Feature|Release-3.6.0|SELF-Testing/Unit<br>Test|flash_cache|AUG|Further optimize the distributed multi-level cache architecture to support cross-computer room and cross-cloud read and write acceleration capabilities to support AI training acceleration needs.|
|Metanode<br>persist with rocksdb|cost<br>optimization<br>|Release-3.6.0|SELF-Testing/Unit<br>Test|metanode_rocksdb_dev|AUG|The cost of massive metadata is relatively high and can satisfy most scenarios.It is possible to reduce metadata storage costs by over 70%.|
|RDMA|Performance|Release-3.6.0|SELF-Testing/Unit<br>Test|cubefs-rdma|AUG|Writing acceleration, making full use of hardware capabilities.It is possible to increase read and write throughput by over 30%.|
|Kernel FileSystem Client<br>And GPU Direct Storage|Performance|Release-3.7.0|SELF-Testing/Unit<br>Test<br>|cubefs-kernel-rdma|OCT|Provides a kernel client and supports GDS (GPU Direct Storage) and RDMA technology to reduce IO latency and CPU overhead.|
|Call Chain|Feature|Release-3.7.0|SELF-Testing/Unit<br>Test|blobstore-tracelog|OCT|Improve issue tracking capability|
|Disk CRC enhancement|Stability|Release-3.7.0|SELF-Testing/Unit<br>Test|blobstore-tracelog|OCT|Disk CRC enhancement to improve CRC checking capabilities such as master-slave synchronization and random writing.|


### In preparation, scheduled as needed

**Architecture refactoring**

- The prototype design for the reconstruction of the storage engine is being carried out with the aim of providing an append-only file system, achieving low latency and high throughput for data reading and writing.
- Merge the main branch with the branch code from JD

**Improved stability and reliability**

- System module operation monitoring and alarms are strengthened to enhance observability.

**Performance improvements**

- Optimize the reading and writing capabilities of existing systems based on TCP links.
- Optimize client local cache (level one cache) performance

**Feature**
- The erasure coding subsystem removes Kafka component dependencies and provides SDK for direct client access, shortening the data transmission path.
- Provides event notification features, S3api QoS, objnode audit log function, cross-region replication, QPS and bandwidth metering and billing capabilities;


