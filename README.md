# CFS

## Overview

CFS is a container-native distributed filesystem.

* scale-out metadata management

* strong consistency

* multi-tenancy: lots of filesystem volumes

* POSIX-compatible


## Architecture

### Concepts

volume

inode

directory

extent

meta partition

data partition

### Components

CFS consists of several components:

* the cluster master. single raft replication, managing volumes, metanodes, datanodes, meta-partitions and data-partitions

* metanode. multi-raft replication, a meta partition (inode range) per replication state machine

* datanode. de-clustering of data partitions, sequential append-only replication + multi-raft

* FUSE client

### Replication Consistency

master: single-raft

metadata partition: multi-raft

data partition: primary-backup replication for extent appending; multi-raft for extent update and truncation

failure recovery for data partitions: firstly, execute the recovery process of the sequential replication; and then the raft recovery process.


## Usage

CFS is mounted by containers for decoupling storage from compute.

And it is integrated with Kubernetes to run various workloads from statelsss microserverices for logging, database servers for data persistence (e.g., MySQL), and machine learning systems for data training (e.g. TensorFlow).


## Deployment

a single datacenter of multiple racks, each rack as an availability zone; or a region of multiple datacenters, each datacenter as an AZ.

a number of datanodes/metanodes (say M) are deployed as a 'set' for optimizing hearbeats into O(M^2)

## Document
https://cfs.readthedocs.io/en/latest/

https://cfs.readthedocs.io/cn/latest/

## License


Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
For detail see [LICENSE](LICENSE) and [NOTICE](NOTICE).
