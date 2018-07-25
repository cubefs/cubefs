# ChubaoFS

## Overview

ChubaoFS is a unified distributed filesystem for small files and large files. 

* key-file data model, i.e., namespaceless, or hierachical namespace

* unlimited scalability of both metadata and data

* strong consistency

* append-only or random writes


ChubaoFS has been built and deployed in production since 2013. 

## Architecture

metadata partition: inode range

data partition: two storage engines: tiny file chunks, extents; one replication protocol

It consists of several components:

* the cluster master. single raft replication

* metanode. multi-raft replication, a meta partition (inode range) per raft

* datanode. de-clustering of data partitions, replicated via a simple but efficient consistent replication protocol


## Interfaces

- Go SDK
- Java SDK
- FUSE

## Usage

* Image Store

based on the namespace-less key-file interface, nginx integration

* Object Store

integration with minio

* Datacenter filesystem

run complex data workloads like databases on top of it

* Container Persistent Volumes

integration with Kubernetes via CSI


