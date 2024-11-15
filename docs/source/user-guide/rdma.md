# Use RDMA

The server’s datanode and metanode network services support both RDMA and TCP listening. The TCP service is always enabled, while the RDMA service can be enabled or disabled through configuration options. Currently, the client (FUSE client) can only choose to use either RDMA or TCP.

### Client configuration
The client configuration file should include the following RDMA-related options:
- enableDataRdma: Data communication between the client and datanode uses RDMA.
- enableMetaRdma: Data communication between the client and metanode uses RDMA.
- rdmaDpPort: The RDMA port number that the datanode listens on.
- rdmaMpPort: The RDMA port number that the metanode listens on.
- rdmaMemBlockNum: The number of memory blocks in the memory pool used by buddy.
- rdmaMemBlockSize: The size of each memory block in the memory pool used by buddy.
- rdmaMemPoolLevel: The maximum number of blocks that buddy can manage is (2^{\text{rdmaMemPoolLevel}}).
- rdmaConnDataSize: The size of the send and receive memory allocated for each connection.
- wqDepth: The number of WQs (Work Queues) set in an RDMA connection, which affects the pre-allocation of RDMA cache and the number of  concurrent connections supported.
- minCqeNum: The maximum number of completion events set for the completion queue corresponding to a thread.
- rdmaLogLevel: The log level for the underlying RDMA.
- rdmaLogFile: The log file for the underlying RDMA.
- workerNum: The number of worker threads.
Example of client configuration
```json
{
  "masterAddr": "10.177.80.10:17010,10.177.80.11:17010,10.177.182.171:17010",
  "mountPoint": "/mnt/cubefs",
  "volName": "test",
  "owner": "test",
  "enableDataRdma": "true",
  "enableMetaRdma": "true",
  "rdmaDpPort": "17360",
  "rdmaMpPort": "17260",
  "rdmaMemBlockNum": "20971520",
  "rdmaMemBlockSize": "1025",
  "rdmaMemPoolLevel": "25",
  "rdmaConnDataSize": "4194304",
  "wqDepth": "32",
  "minCqeNum": "65536",
  "rdmaLogLevel": "error",
  "rdmaLogFile": "/home/service/client/logs/rdma.log",
  "workerNum": "4",
  "logDir": "/home/service/client/logs",
  "logLevel": "warn"
}
```

The main configuration parameters are for enabling/disabling RDMA, the RDMA port numbers on the server side, and the log level and location. Other parameters can use the default settings.

### Datanode configuration
Add RDMA-related options to the datanode configuration file.
- enableRdma: Whether to enable the RDMA listening service. true means both RDMA and TCP services are enabled.
- rdmaPort: The listening RDMA port.
- rdmaIP: The listening RDMA address, using the local address: 127.0.0.1.
- rdmaMemBlockNum: The number of memory blocks in the buddy memory pool.
- rdmaMemBlockSize: The size of each memory block in the buddy memory pool.
- rdmaMemPoolLevel: The maximum number of blocks that buddy can manage is 2^rdmaMemPoolLevel.
- rdmaConnDataSize: The size of the send and receive memory allocated for each connection.
- wqDepth: The number of WQs set in an RDMA link, which affects the pre-allocation of the RDMA cache and the number of concurrent connections supported.
- minCqeNum: The maximum number of completion events in the completion queue for a thread.
- rdmaLogLevel: The log level for the underlying RDMA.
- rdmaLogFile: The log file for the underlying RDMA.
- workerNum: The number of worker threads.
Example of datanode configuration:
```json
{
  "role": "datanode",
  "listen": "17310",
  "enableRdma": "true",
  "rdmaPort": "17360",
  "rdmaIP": "127.0.0.1",
  "rdmaMemBlockNum": "20971520",
  "rdmaMemBlockSize": "1025",
  "rdmaMemPoolLevel": "25",
  "rdmaConnDataSize": "4194304",
  "wqDepth": "32",
  "minCqeNum": "65536",
  "workerNum": "4",
  "rdmaLogLevel": "error",
  "rdmaLogFile": "/home/service/cfs-data/logs/rdma.log",
  "logDir": "./logs",
  "warnLogDir": "./logs",
  "logLevel": "warn",
  "disks":["/home/service/var/data:10737418240"],
  "masterAddr": ["master.wanyol.com:17010"]
}
```

### Metanode configuration
The metanode rdma configure option is the same as datanode.
Example of metanode configuration:
```json
{
  "role": "metanode",
  "listen": "17210",
  "prof": "17220",
  "enableRdma": "true",
  "rdmaPort": "17260",
  "rdmaIP": "127.0.0.1",
  "rdmaMemBlockNum": "20971520",
  "rdmaMemBlockSize": "1025",
  "rdmaMemPoolLevel": "25",
  "rdmaConnDataSize": "4194304",
  "wqDepth": "32",
  "minCqeNum": "65536",
  "workerNum": "4",
  "rdmaLogLevel": "error",
  "rdmaLogFile": "/home/service/app/cfs-meta/logs/rdma.log",
  "raftHeartbeatPort": "17230",
  "raftReplicaPort": "17240",
  "exporterPort": 17250,
  "logLevel": "warn",
  "logDir": "./logs",
  "warnLogDir": "./logs",
  "memRatio": "70",
  "totalMem": "92122547200",
  "metadataDir": "/home/service/var/data/cfs/metanode/meta",
  "raftDir": "/home/service/var/data/cfs/metanode/raft",
  "consulAddr":"10.177.92.51:8500",
  "consulMeta":"dataset=custom;category=custom;app=cfs;appid=cfs-master;metric_path=/metrics;role=metanode;cluster=cfs_kerneltest1",
  "masterAddr": ["10.177.80.10:17010", "10.177.80.11:17010", "10.177.182.171:17010"]
}
```

### Pre-request for RDMA
- The network card device supports RDMA. The network card used for testing is: Mellanox Technologies MT27800 Family [ConnectX-5].
- Install the network card driver: MLNX_OFED_LINUX-5.6-2.0.9.0. It is recommended that users install the MLNX_OFED_LINUX software.
- The default configuration settings for RDMA buffers are relatively large, typically reaching 4G. It is recommended that the host memory be 256G or more.

::: tip Note
1. If the client has RDMA enabled, some network packets may still be of the TCP type. This is because the data volume of these packets is very small, and using RDMA does not provide an advantage.
2. Each time the network mode of the client is switched, the client needs to be restarted.
3. Clients using RDMA mode pre-allocate a large amount of memory, so they start up much slower than those using TCP mode.
4. If enableRdma = true and the host machine does not have an RDMA-capable network card, this will cause the program to fail to start. This is because during the initialization of the cache, the program will attempt to find an RDMA device.

:::
