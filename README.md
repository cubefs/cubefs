# ContainerFS

## Overview

ContainerFS is a container-native distributed filesystem as a unified platform for unstructured data storage. 

* unlimited small files and large files.

* append-only or random writes

* scale-out metadata management

* strong consistency

* multi-tenancy: millions of filesystem volumes

* POSIX-compatible

Containerfs has been built and deployed in production since 2013.

## Concepts

volume

inode

directory

blob

extent

## Architecture

CFS consists of several components:

* the cluster master. single raft replication, managing volumes, metanodes, datanodes, meta-partitions and data-partitions

* metanode. multi-raft replication, a meta partition (inode range) per replication state machine

* datanode. de-clustering of data partitions, two storage engines - Blob Store (BS) and Extent Store (ES), optimized for small and large files respectively. 

* client interfaces: FUSE, Java SDK, Go SDK, Linux kernel

### replication

master: single-raft

metadata partition: multi-raft

data partition: chained append-only replication for append operations; multi-raft for extent updates

## Usage

* mounted by containers for decoupling storage from compute

integrated with Kubernetes to run various workloads from application microservices to complex databases e.g., HBase, MyRocks, ElasticSearch, and ChuBao DB

* Image Store

based on the namespace-less key-file interface, nginx integration

* Object Store

integration with minio

## Deployment

rack

set

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
For detail see [LICENSE](LICENSE) and [NOTICE](NOTICE).
