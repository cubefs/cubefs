# Master

  The cluster metadata contains dataNodes,metaNodes,vols,dataPartitions and metaPartitions,they are managed by master server. The master server caches the metadata in mem,persist to GoLevelDB,and ensure consistence by raft protocol.
  The master server manages dataPartition id to dataNode server mapping,metaPartition id to metaNode server mapping.

## Features

- Multi-tenant, Resource Isolation
- dataNodes,metaNodes shared,vol owns dataPartition and metaPartition exclusive
- Asynchronous communication with dataNode and metaNode

## Build

```shell
$ go build
```
### Configuration

CFS using **JSON** as for configuration file format.

**Properties:**

| Key           | Type     | Description                                      | Required |
| :---------    | :------- | :----------------------------------------------- | :------: |
| role          | string   | Role of process and must be set to "master".     | Yes      |
| ip            | string   | host ip                                          | Yes      |
| port          | string   | Http port which api service listen on.           | Yes      |
| prof          | string   | golang pprof port                                | Yes      |
| id            | string   | identy different master node                     | Yes      |
| peers         | string   | the member information of raft group             | Yes      |
| logDir        | string   | Path for log file storage.                       | Yes      |
| logLevel      | string   | Level operation for logging. Default is "error". | No       |
| retainLogs    | string   | the number of raft logs will be retain.          | Yes      |
| walDir        | string   | Path for raft log file storage.                  | Yes      |
| storeDir      | string   | Path for goleveldb file storage.                 | Yes      |
| clusterName   | string   | The cluster identifier                           | Yes      |
| exporterPort  | int      | The prometheus exporter port.                    | No      |
| consulAddr    | string   | The consul register addr for prometheus exporter.| No      |

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
    "exporterPort": 9510,
    "consulAddr": "http://127.0.0.1:8500",
    "clusterName":"containerfs"
}
```

## Start
```sh
$ nohup ./master -c config.json > nohup.out &
```