# Roadmap of 2025

### Release scheduled
|Feature|Type|Version|Status|Development branch|Scheduled Release Date|Details|
|:----|:----|:----|:----|:----|:----|:----------------|
|Hybrid Cloud automatic<br>data hierarchy<br>|Feature|Release-3.5.0|Preparing for Release|develop-v3.5.0-hybridcloud-lifecycle|FEBRUARY|Hybrid cloud projects support a unified namespace, provide the ability to use multiple storage systems in a mixed manner, and provide external S3 and HDFS capabilities. Support life cycle driven data flow between different media, storage types, and on and off the cloud, reducing costs and increasing efficiency. The first issue will be released soon.|
|Distributed Cache|Feature|Release-3.5.1|System Testing|develop-v3.5.0-flash_cache|APRIL|Further optimize the distributed multi-level cache architecture to support cross-computer room and cross-cloud read and write acceleration capabilities to support AI training acceleration needs.|
|Distributed Cache && Stability|Enhancement|Release-3.5.2|Not Started||JULY|Enhance Distributed Cache service && Enhance system stability|
|Metanode<br>persist with rocksdb|Enhancement|Release-3.6.0|HOLD|develop-v3.5.0-metanode_rocksdb|OCT|The cost of massive metadata is relatively high and can satisfy most scenarios.It is possible to reduce metadata storage costs by over 70%.|

### In preparation, scheduled as needed

**Performance improvements**
- Optimize the reading and writing capabilities of existing systems based on TCP links.

**Feature**
- CubeFS can run in public cloud services, providing cache acceleration and file system semantics on top of public cloud storage such as S3.