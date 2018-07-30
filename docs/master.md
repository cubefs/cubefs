# Master

   The cluster metadata contains dataNodes,metaNodes,vols,dataPartitions and metaPartitions,they are managed by master server. The master server caches the metadata in mem,persist to rocksDB,and ensure consistence by raft protocol.
  The master server manages dataPartition id to dataNode server mapping,metaPartition id to metaNode server mapping.

## Features

- Multi-tenant, Resource Isolation
- dataNodes,metaNodes shared,vol owns dataPartition and metaPartition exclusive
- Asynchronous communication with dataNode and metaNode

## Prerequisite

RocksDB version: 5.9.2

BaudFS use [dep](https://github.com/golang/dep) for dependency management.

All necessary third part dependencies have been define in `Gopkg.toml` under the project root.

To install dependencies make sure [dep](https://github.com/golang/dep) have been installed and execute `dep` with `ensure` command at the `$GOPATH/github.com/tiglibs/baudfs`.

```shell
$ dep ensure
```
## Build

```shell
$ sh build.sh
```
## Configuration

 BaudFS using **JSON** as for configuration file format. Generally,the master cluster contains three nodes.

**Example:**
  ```json
   {
    "role": "master",
    "ip": "127.0.0.1",
     "port": "8080",
    "prof":"10088",
    "id":"1",
    "peers": "1:127.0.0.1:8080,1:127.0.0.1:8081,1:127.0.0.1:8082",
    "logDir": "/export/master",
    "logLevel":"DEBUG",
    "retainLogs":"2000",
    "walDir":"/export/raft",
    "storeDir":"/export/rocks",
    "clusterName":"baudfs"
}
```

## Start
```sh
$ nohup ./master -c config.json > nohup.out &
```
# API
## Cluster
- http://127.0.0.1/admin/getCluster

## Vol API

### Parameter specification
  - **name**: the name of vol
  - **replicas**: the replica num
  - **type**: store engine type

### Create

 http://127.0.0.1/admin/createVol?name=baudfs&replicas=3&type=extent

### Get
 http://127.0.0.1/client/vol?name=baudfs
### Stat
 http://127.0.0.1/client/volStat?name=baudfs

## MetaPartition API

### Parameter specification
  - **name**: the name of vol
  - **id**: the id of metaPartition
  - **addr**: the addr of metaNode, format is ip:port

### Get
 http://127.0.0.1/client/metaPartition?name=baudfs&id=1
### Offline one replica
 http://127.0.0.1/metaPartition/offline?name=baudfs&id=13&addr=ip:port

## DataPartition API

### Parameter specification
  - **name**: the name of vol
  - **id**: the id of dataPartition
  - **addr**: the addr of dataNode, format is ip:port
  - **count**： the total num of dataPartitions in the vol
  - **type**: store engine type

### Create
- http://127.0.0.1/dataPartition/create?count=40&name=baudfs&type=extent
### Get
- http://127.0.0.1/dataPartition/get?id=100
### Load
- http://127.0.0.1/dataPartition/load?name=baudfs&id=1
### Offline one replica
- http://127.0.0.1/dataPartition/offline?name=baudfs&id=13&addr=ip:port
### Get all dataPartitions of a vol
- http://127.0.0.1/client/dataPartitions?name=baudfs

## MetaNode API

### Parameter specification
  - **addr**: the addr of metaNode, format is ip:port

### Examples

- http://127.0.0.1/metaNode/get?addr=ip:port
- http://127.0.0.1/metaNode/add?addr=ip:port
- http://127.0.0.1/metaNode/offline?addr=ip:port

## DataNode API

### Parameter specification
  - **addr**: the addr of dataNode, format is ip:port

### Examples
- http://127.0.0.1/dataNode/get?addr=ip:port
- http://127.0.0.1/dataNode/add?addr=ip:port
- http://127.0.0.1/dataNode/offline?addr=ip:port

## Master manage API

### Parameter specification
  - **addr**: the addr of master server, format is ip:port
  - **id**: the node id of master server

### Add
- http://127.0.0.1/raftNode/add?addr=ip:port&id=3

### Remove

- http://127.0.0.1/raftNode/remove?addr=ip:port&id=3