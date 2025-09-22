# MetaNode Configuration

## Configuration Description

| Configuration Item  | Type         | Description                                                                                                                                                | Required |
|:---------------------|:--------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------|:----------|
| role                | string       | Process role: *MetaNode*                                                                                                                                   | Yes      |
| listen              | string       | Port for listening and accepting requests                                                                                                                  | Yes      |
| prof                | string       | Debugging and administrator API interface                                                                                                                  | Yes      |
| logLevel            | string       | Log level, default: *error*                                                                                                                                | No       |
| metadataDir         | string       | Directory for storing metadata snapshots                                                                                                                   | Yes      |
| logDir              | string       | Directory for storing logs                                                                                                                                 | Yes      |
| raftDir             | string       | Directory for storing Raft WAL logs                                                                                                                        | Yes      |
| raftHeartbeatPort   | string       | Raft heartbeat communication port                                                                                                                          | Yes      |
| raftReplicaPort     | string       | Raft data transmission port                                                                                                                                | Yes      |
| consulAddr          | string       | Prometheus registration interface                                                                                                                          | No       |
| exporterPort        | string       | Port for Prometheus to obtain monitoring data                                                                                                              | No       |
| masterAddr          | string slice | Address of the master service                                                                                                                              | Yes      |
| totalMem            | string       | Maximum available memory. This value must be higher than the value of metaNodeReservedMem in the master configuration, in bytes                            | Yes      |
| memRatio            | string       | The ratio of maximum available memory to the total memory of the host. If this option is filled in, the calculated value will override the `totalMem` item | No       |
| localIP             | string       | IP address of the local machine. If this option is not specified, the IP address used for communication with the master is used                            | No       |
| advertisedAddr      | string       | Advertised address used when registering the node to the master. If not configured, localIP is used                                                        | No       |
| bindIp              | bool         | Whether to listen for connections only on the localIP, default is `false`                                                                                  | No       |
| zoneName            | string       | Specify the zone. By default, it is assigned to the `default` zone                                                                                         | No       |
| deleteBatchCount    | int64        | Number of inode nodes to be deleted in batches at one time, default is `500`                                                                               | No       |
| tickInterval        | float64      | Interval for Raft to check heartbeats and election timeouts, unit is milliseconds, default is `300`                                                        | No       |
| raftRecvBufSize     | int          | Size of the Raft receive buffer, unit: bytes, default is `2048`                                                                                            | No       |
| nameResolveInterval | int          | Interval for Raft node address resolution, unit: minutes, the value should be between [1-60], default is `1`                                               | No       |

## Configuration Example

``` json
{
     "role": "metanode",
     "listen": "17210",
     "prof": "17220",
     "logLevel": "debug",
     "localIP":"127.0.0.1",
     "metadataDir": "/cfs/metanode/data/meta",
     "logDir": "/cfs/metanode/log",
     "raftDir": "/cfs/metanode/data/raft",
     "raftHeartbeatPort": "17230",
     "raftReplicaPort": "17240",
     "consulAddr": "http://consul.prometheus-cfs.local",
     "exporterPort": 9501,
     "totalMem":  "8589934592",
     "masterAddr": [
         "127.0.0.1:17010",
         "127.0.0.2:17010",
         "127.0.0.3:17010"
     ]
 }
```

## Notes

-   The configuration options `listen`, `raftHeartbeatPort`, and `raftReplicaPort` cannot be modified after the program is first configured and started.
-   The relevant configuration information is recorded in the `constcfg` file under the `metadataDir` directory. If you need to force modification, you need to manually delete the file.
-   The above three configuration options are related to the registration information of the `MetaNode` in the `Master`. If modified, the `Master` will not be able to locate the `MetaNode` information before the modification.