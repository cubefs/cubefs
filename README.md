# ContainerFS

## Overview

ContainerFS is a container-native distributed filesystem as a unified platform for unstructured data storage. 

* both large-scale small files and large files.

* key-file data model, i.e., namespaceless, or hierachical namespaces

* append-only or random writes

* scale-out metadata management

* strong consistency

* multi-tenancy: millions of filesystem volumes

Containerfs has been built and deployed in production since 2013.

## Architecture

CFS consists of several components:

* the cluster master. single raft replication, managing volumes, metanodes, datanodes, meta-partitions and data-partitions

* metanode. multi-raft replication, a meta partition (inode range) per replication state machine

* datanode. de-clustering of data partitions, two storage engines for BLOBs and extents, replicated via efficient consistent replication - chained replication for sequential write and raft for random write

* client interfaces: FUSE, Java SDK, Go SDK.

## Usage

* mounted by containers for decoupling storage from compute

integrated with Kubernetes to run various workloads from application microservices to complex databases e.g., HBase, MyRocks, ElasticSearch, and ChuBao DB

* Image Store

based on the namespace-less key-file interface, nginx integration

* Object Store

integration with minio

## Contact


