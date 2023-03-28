# DataNode Configuration
## Configuration Description

| Keyword       | Parameter Type | Description                                                                                                                     | Required |
|---------------|----------------|---------------------------------------------------------------------------------------------------------------------------------|----------|
| role          | string         | Role must be configured as "datanode"                                                                                           | Yes      |
| listen        | string         | Port on which the data node starts TCP listening as a server                                                                    | Yes      |
| localIP       | string         | IP address selected by the data node as a server                                                                                | No       |
| prof          | string         | Port used by the data node to provide HTTP interface                                                                            | Yes      |
| logDir        | string         | Path to store debugging logs                                                                                                    | Yes      |
| logLevel      | string         | Debug log level. Default is error                                                                                               | No       |
| raftHeartbeat | string         | Port used by RAFT to send heartbeat messages between nodes                                                                      | Yes      |
| raftReplica   | string         | Port used by RAFT to send log messages                                                                                          | Yes      |
| raftDir       | string         | Path to store RAFT debugging logs. Default is in the binary file startup path                                                   | No       |
| consulAddr    | string         | Address of the monitoring system                                                                                                | No       |
| exporterPort  | string         | Port of the monitoring system                                                                                                   | No       |
| masterAddr    | string slice   | Address of the cluster manager                                                                                                  | Yes      |
| localIP       | string         | IP address of the local machine. If this option is not specified, the IP address used for communication with the master is used | No       |
| zoneName      | string         | Specify the zone. By default, it is assigned to the `default` zone                                                              | No       |
| disks         | string slice   | Format: `disk mount path:reserved space`, reserved space configuration range `[20G,50G]`                                        | Yes      |

## Configuration Example

``` json
{
     "role": "datanode",
     "listen": "17310",
     "prof": "17320",
     "logDir": "/cfs/datanode/log",
     "logLevel": "info",
     "raftHeartbeat": "17330",
     "raftReplica": "17340",
     "raftDir": "/cfs/datanode/log",
     "consulAddr": "http://consul.prometheus-cfs.local",
     "exporterPort": 9502,
     "masterAddr": [
         "10.196.59.198:17010",
         "10.196.59.199:17010",
         "10.196.59.200:17010"
     ],
     "disks": [
         "/data0:10737418240",
         "/data1:10737418240"
     ]
}
```

## Notes

-   The configuration options listen, raftHeartbeat, and raftReplica cannot be modified after the program is first configured and started.
-   The relevant configuration information is recorded in the constcfg file under the raftDir directory. If you need to force modification, you need to manually delete the file.
-   The above three configuration options are related to the registration information of the datanode in the master. If modified, the master will not be able to locate the datanode information before the modification.