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

* datanode. de-clustering of data partitions, two storage engines - Blob Store (BS) and Extent Store (ES).

BS is optimized for storing small files.

ES is used for appendable continous blocks - extents, which is optimized for large files.

Both BS and ES are replicated with strong consistency: atomic block appending; raft for extent update. 

* client interfaces: FUSE, Java SDK, Go SDK, NFS, Linux kernel

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


