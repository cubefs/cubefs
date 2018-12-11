# How To Start DataNode

Start a DataNode process by execute the server binary of CFS you built with `-c` argument and specify configuration file.

```shell
$ PATH_OF_SERVER_BINARY -c PATH_OF_CONFIGURATION
```  

### Configuration

CFS using **JSON** as for configuration file format. 

**Properties:**

| Key           | Type     | Description                                      | Required |
| :---------    | :------- | :----------------------------------------------- | :------: |
| role          | string   | Role of process and must be set to "datanode".   | Yes      |
| port          | string   | Port of TCP network to be listen.                | Yes      |
| prof          | string   | Port of HTTP based prof and api service.         | Yes      |
| logDir        | string   | Path for log file storage.                       | Yes      |
| logLevel      | string   | Level operation for logging. Default is "error". | No       |
| raftHeartbeat | string   | Port of raft heartbeat TCP network to be listen. | Yes      |
| raftReplicate | string   | Port of raft replicate TCP network to be listen. | Yes      |
| masterAddr    | []string | Addresses of master server.                      | Yes      |
| rack          | string   | Identity of rack.                                | No       |
| disks         | []string | Format: "PATH:MAX_ERRS:REST_SIZE".               | Yes      |

**Example:**

```json
{
    "role": "datanode",
    "port": "6000",
    "prof": "6001",
    "logDir": "/var/logs",
    "logLevel": "debug",
    "raftHeartbeat": "9095",
    "raftReplicate": "9096",    
    "masterAddr": [
        "10.196.30.200:80",
        "10.196.31.141:80",
        "10.196.31.173:80"
    ],
    "rack": "main",
    "disks": [
        "/data0:1:20000",
        "/data1:1:20000",
        "/data2:1:20000",
        "/data3:1:20000",
        "/data4:1:20000"
    ]
}
```

**Notes:**
>Cause of major components of CFS developed by Golang, the pprof APIs will be enabled automatically when the prof port have been config (specified by `prof` properties in configuratio file). So that you can use pprof tool or send pprof http request to check status of server runtime.