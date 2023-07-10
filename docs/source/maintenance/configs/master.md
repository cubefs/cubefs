# Master Configuration
## Configuration Description

CubeFS uses **JSON** as the format of the configuration file.

| Configuration Item                  | Type   | Description                                                                                                                                | Required | Default Value |
|-------------------------------------|--------|--------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------|
| role                                | string | The role of the process, the value can only be master                                                                                      | Yes      |               |
| ip                                  | string | Host IP address                                                                                                                            | Yes      |               |
| listen                              | string | Port number on which the HTTP service listens                                                                                              | Yes      |               |
| prof                                | string | Golang pprof port number                                                                                                                   | Yes      |               |
| id                                  | string | Distinguish different master nodes                                                                                                         | Yes      |               |
| peers                               | string | Raft replication group member information                                                                                                  | Yes      |               |
| logDir                              | string | Directory for storing log files                                                                                                            | Yes      |               |
| logLevel                            | string | Log level                                                                                                                                  | No       | error         |
| retainLogs                          | string | How many Raft logs to keep.                                                                                                                | Yes      |               |
| walDir                              | string | Directory for storing Raft WAL logs.                                                                                                       | Yes      |               |
| storeDir                            | string | Directory for storing RocksDB data. This directory must exist. If the directory does not exist, the service cannot be started.             | Yes      |               |
| clusterName                         | string | Cluster name                                                                                                                               | Yes      |               |
| ebsAddr                             | string | Address of the erasure coding subsystem. This must be configured when using the erasure coding subsystem.                                  | No       |               |
| exporterPort                        | int    | Port for Prometheus to obtain monitoring data                                                                                              | No       |               |
| consulAddr                          | string | Consul registration address, used by Prometheus exporter                                                                                   | No       |               |
| metaNodeReservedMem                 | string | Reserved memory size for metadata nodes, in bytes                                                                                          | No       | 1073741824    |
| heartbeatPort                       | string | Raft heartbeat communication port                                                                                                          | No       | 5901          |
| replicaPort                         | string | Raft data transmission port                                                                                                                | No       | 5902          |
| nodeSetCap                          | string | Capacity of NodeSet                                                                                                                        | No       | 18            |
| missingDataPartitionInterval        | string | If no heartbeat is received during this time period, the replica is considered lost, in seconds                                            | No       | 24h           |
| dataPartitionTimeOutSec             | string | If no heartbeat is received during this time period, the replica is considered not alive, in seconds                                       | No       | 10min         |
| numberOfDataPartitionsToLoad        | string | Maximum number of data partitions to check at a time                                                                                       | No       | 40            |
| secondsToFreeDataPartitionAfterLoad | string | After how many seconds to start releasing the memory occupied by the loaded data partition task                                            | No       | 300           |
| tickInterval                        | string | Timer interval for checking heartbeat and election timeout, in milliseconds                                                                | No       | 500           |
| electionTick                        | string | How many times the timer is reset before the election times out                                                                            | No       | 5             |
| bindIp                              | bool   | Whether to listen for connections only on the host IP                                                                                      | No       | false         |
| faultDomain                         | bool   | Whether to enable fault domain                                                                                                             | No       | false         |
| faultDomainBuildAsPossible          | bool   | Whether to still try to build a nodeSetGroup as much as possible if the number of available fault domains is less than the expected number | No       | false         |
| faultDomainGrpBatchCnt              | string | Number of available fault domains                                                                                                          | No       | 3             |
| dpNoLeaderReportIntervalSec         | string | How often to report when data partitions has no leader, unit: s                                                                            | No       | 60            |
| mpNoLeaderReportIntervalSec         | string | How often to report when meta partitions has no leader, unit: s                                                                            | No       | 60            |
| maxQuotaNumPerVol                   | string | Maximum quota number per volume                                                                                                            | No       | 100           |

## Configuration Example

``` json
{
 "role": "master",
 "id":"1",
 "ip": "127.0.0.1",
 "listen": "17010",
 "prof":"17020",
 "peers": "1:127.0.0.1:17010,2:127.0.0.2:17010,3:127.0.0.3:17010",
 "retainLogs":"20000",
 "logDir": "/cfs/master/log",
 "logLevel":"info",
 "walDir":"/cfs/master/data/wal",
 "storeDir":"/cfs/master/data/store",
 "exporterPort": 9500,
 "consulAddr": "http://consul.prometheus-cfs.local",
 "clusterName":"cubefs01",
 "metaNodeReservedMem": "1073741824"
}
```