# MetaNode

## Concepts

metaNode is the manager of metaPartition and replication by raft. metaNode is 
base on [multiRaft](http://github.com/tiglabs/raft), so each metaNode manager
 many partitions.  Each partition is an inode range, and composed of two 
 in-memory btree: inode btree and dentry btree.  

## How to install

```bash
 go build -o baudfs
 baudfs -c meta.json
```

## Configuration

* configure

```json
{
    "role": "metanode",
    "listen": "9021",
    "prof": "9092",
    "logLevel": "debug",
    "metaDir": "/var/baudfs/metanode_meta",
    "logDir": "/var/log",
    "raftDir": "/var/baudfs/metanode_raft",
    "raftHeartbeatPort": "9093",
    "raftReplicatePort": "9094",
    "masterAddrs": [
         "192.168.1.10:80",
        "192.168.1.200:80"
    ]
}
```

* configure description

| key | desc |  
|---|---|  
| role | metanode |  
| listen | metaNode listen port |  
| prof | profile and http api port |  
| logLevel | log level |  
| metaDir| meta file store dir |  
| logDir | log file dir |  
| raftDir | raft WAL file store dir |  
| raftHeartbeatPort | raft heartbeat port |  
| raftReplicatePort | raft replication port |  
| masterAddrs | master server ip:port|  
 
 
 

## Manage HTTP API

| URL | Param | Example | Desc |
|:-----:|:-----:|:------:|:-----:|
|/getAllPartitions| NULL | http://127.0.0.1:9092/getAllPartitions| get all metaPartition |
|/getInodeInfo| id=100 | http://127.0.0.1:9092/getInodeInfo?id=100 | get the meta-info of the 100th partition|
|/getInodeRange| id=100 | http://127.0.0.1:9092/getInodeRange?id=100 | get all inode info of the 100th partition(maybe very big).|
|/getExtents| pid=100&ino=203 | http://127.0.0.1:9092/getExtents?pid=100&ino=203 | get the extents(data meta) of the specified partition and inode id |
