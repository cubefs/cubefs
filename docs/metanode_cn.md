# MetaNode

## 概要

>metaNode是管理metaPartition的节点； 
使用RAFT来保证数据的多副本，每个metaPartiton对应的副本数量，可以通过在master上进行配置管理。
>每个metaPartition包含两个btree在内存中：1. inode树用于存储元数据信息（例如：id， size， accesstime， 
extents（data元信息[dataMode]）等); 2. dentry树用于存储目录和文件关系。

## 编译及启动命令

> ```bash
> go build -o baudfs
> baudfs -c meta.json
>```

## 配置及说明

>* 配置文件

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

>* 配置文件说明

| key | description |
|-------|--------|
| role | 角色，baudFS目前有三种角色（master， metanode， datanode），指定不同的角色，启动不同服务 |
| listen | metaNode通信端口 |
| prof | profile监听端口，便于调试和通过HTTP API接口获取数据 |
| logLevel | 日志记录级别： debug/warn/error/info等|
| metaDir| partition的meta文件存储目录 |
| logDir | 日志文件存储目录 |
| raftDir | raft WAL文件存储目录 |
| raftHeartbeatPort | raft之间心跳通信端口 |
| raftReplicatePort | raft之间数据同步端口 |
| masterAddrs | master服务的IP地址和端口 |

## 管理端HTTP API

> HTTP API监听在127.0.0.1， 端口为配置文件中配置的prof所监听的端口：

| URL | 参数 | 样例 | 说明 |
|:-----:|:-----:|:------:|:-----:|
|/getAllPartitions| NULL | http://127.0.0.1:9092/getAllPartitions|获取本metaNode上的所有metaPartition信息|
|/getInodeInfo| id=100 | http://127.0.0.1:9092/getInodeInfo?id=100 |获取指定metaPartition为100的meta信息（包含inode分配的起始结束范围及Raft Leader信息等|
|/getInodeRange| id=100 | http://127.0.0.1:9092/getInodeRange?id=100 |获取metaPartition id为100的Inode btree里全部存储的信息，显示的是json格式 |
|/getExtents| pid=100&ino=203 | http://127.0.0.1:9092/getExtents?pid=100&ino=203 |获取partititon=100，且inode id为203的所有数据存储的元信息 |