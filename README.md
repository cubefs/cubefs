# ChuBao FS

## Overview

ChuBao FS is a distributed filesystem as a unified platform for unstructured data storage. 

* both large-scale small files and large files.

* key-file data model, i.e., namespaceless, or multiple hierachical namespaces

* scale-out metadata management

* strong consistency

* append-only or random writes

ChuBao FS has been built and deployed in production since 2013.

## Architecture

CBFS consists of several components:

* the cluster master. single raft replication

* metanode. multi-raft replication, a meta partition (inode range) per raft

* datanode. de-clustering of data partitions, two storage engines for BLOBs and extents, replicated via an efficient consistent replication protocol

* client interfaces. FUSE, Java SDK, Go SDK.

## Usage

* Image Store

based on the namespace-less key-file interface, nginx integration

* Object Store

integration with minio

* Datacenter filesystem

run complex data workloads like databases on top of it, e.g., HBase, MyRocks, ElasticSearch, and ChuBao DB. 

* Container Persistent Volumes

integration with Kubernetes via CSI


## Contact

chubao.io

