# Cluster Mode

## Get Software

Use the following command to build server, client, and related dependencies at the same time:

``` bash
$ git clone https://github.com/cubeFS/cubefs.git
$ cd cubefs
$ make build
```

If the build is successful, the executable files `cfs-server` and `cfs-client` will be generated in the `build/bin` directory.

## Cluster Deployment

### Install Master

``` bash
./cfs-server -c master.json
```

Example `master.json`:

::: tip Recommended
To ensure high availability of the service, at least 3 instances of the Master service should be started.
:::

``` json
{
  "role": "master",
  "ip": "127.0.0.1",  // Replace with host ip
  "listen": "17010",
  "prof":"17020",
  "id":"1", // Replace with the corresponding id
  "peers": "1:127.0.0.1:17010,2:127.0.0.2:17010,3:127.0.0.3:17010",
  "retainLogs":"20000",
  "logDir": "/cfs/master/log", // Master log directory
  "logLevel":"info",
  "walDir":"/cfs/master/data/wal", // Raft wal log directory
  "storeDir":"/cfs/master/data/store", // RocksDB data storage directory
  "consulAddr": "http://consul.prometheus-cfs.local",
  "clusterName":"cubefs01",
  "metaNodeReservedMem": "1073741824" // Metadata node reserved memory, 1G
}
```

Configuration parameters can be found in [Master Detailed Configuration](../ops/configs/master.md).

### Install MetaNode

``` bash
./cfs-server -c metanode.json
```

Example `meta.json`:

::: tip Recommended
To ensure high availability of the service, at least 3 instances of the MetaNode service should be started.
:::

``` json
{
    "role": "metanode",
    "listen": "17210",
    "prof": "17220",
    "logLevel": "info",
    "metadataDir": "/cfs/metanode/data/meta", // Metadata snapshot storage directory
    "logDir": "/cfs/metanode/log", // Metanode log directory
    "raftDir": "/cfs/metanode/data/raft",
    "raftHeartbeatPort": "17230",
    "raftReplicaPort": "17240",
    "totalMem":  "8589934592", // Maximum available memory, which must be larger than metaNodeReservedMem
    "consulAddr": "http://consul.prometheus-cfs.local",
    "exporterPort": 9501,
    "masterAddr": [
        "127.0.0.1:17010",
        "127.0.0.2:17010",
        "127.0.0.3:17010"
    ]
}
```

For detailed configuration parameters, please refer to [MetaNode Detailed Configuration](../ops/configs/metanode.md).

### Install DataNode

::: tip Recommended
Using a separate disk as the data directory and configuring multiple disks can achieve higher performance.
:::

- Prepare the data directory
  - View the machine's disk information and select the disk to be used by CubeFS
   ``` bash
   fdisk -l
   ```
  - Format the disk, it is recommended to format it as XFS
   ``` bash
   mkfs.xfs -f /dev/sdx
   ```
  - Create a mount directory
   ``` bash
   mkdir /data0
   ```
  - Mount the disk
   ``` bash
   mount /dev/sdx /data0
   ```

- Start the Data Node

 ``` bash
./cfs-server -c datanode.json
```

Example `datanode.json`:

::: tip Recommended
To ensure high availability of the service, at least 3 instances of the DataNode service should be started.
:::

``` json
{
  "role": "datanode",
  "listen": "17310",
  "prof": "17320",
  "logDir": "/cfs/datanode/log",
  "logLevel": "info",
  "raftHeartbeat": "17330",
  "raftReplica": "17340",
  "raftDir":"/cfs/datanode/log",
  "consulAddr": "http://consul.prometheus-cfs.local",
  "exporterPort": 9502,
  "mediaType": 1, // datanode disk type, 1 ssd, 2 hdd
  "masterAddr": [
     "127.0.0.1:17010",
     "127.0.0.2:17010",
     "127.0.0.3:17010"
  ],
  "disks": [
     "/data0:10737418240", // Disk mount path: Reserved space
     "/data1:10737418240"
 ]
}
```

For detailed configuration parameters, please refer to [DataNode Detailed Configuration](../ops/configs/datanode.md).

### Install Object Gateway

::: tip Note
Optional section. If you need to use the object storage service, you need to deploy the object gateway (ObjectNode).
:::

``` bash
./cfs-server -c objectnode.json
```

Example `objectnode.json`, as follows:

``` json
{
    "role": "objectnode",
    "domains": [
        "object.cfs.local"
    ],
    "listen": "17410",
    "masterAddr": [
       "127.0.0.1:17010",
       "127.0.0.2:17010",
       "127.0.0.3:17010"
    ],
    "logLevel": "info",
    "logDir": "/cfs/Logs/objectnode"
}
```

For detailed configuration parameters, please refer to [ObjectNode Detailed Configuration](../ops/configs/objectnode.md).

### Install Lcnode

::: tip Note
Optional section. If you need to use the data migration, you need to deploy the lifecycle.
:::

``` bash
./cfs-server -c lifecycle.json
```

Example `lifecycle.json`, as follows:

``` json
{
    "role": "lcnode",
    "listen": "17510",
    "masterAddr": [
       "127.0.0.1:17010",
       "127.0.0.2:17010",
       "127.0.0.3:17010"
    ],
    "logLevel": "info",
    "logDir": "/cfs/Logs/lcnode"
}
```

For detailed configuration parameters, please refer to [Lcnode Detailed Configuration](../ops/configs/lcnode.md).

### Install FlashNode

::: tip Note
This component is optional and can be omitted if file reads do not require caching or cluster-based acceleration
:::

``` bash
./cfs-server -c flashnode.json
```

Example `flashnode.json`, as follows:

``` json
{
    "role": "flashnode",
    "listen": "18510",
    "prof": "18511",
    "logDir": "./logs",
    "masterAddr": [
        "127.0.0.1:17010",
        "127.0.0.2:17010",
        "127.0.0.3:17010"
    ],
    "readRps": 100000,
    "disableTmpfs": true,
    "diskDataPath": [
      "/path/data1:0"
      ],
    "zoneName":"default"
}
```

For detailed configuration parameters, please refer to [FlashNode Detailed Configuration](../ops/configs/flashnode.md).

### Install Erasure Coding Subsystem

::: tip Note
Optional section. If you need to use the erasure coding volume, you need to deploy it.
:::

Please refer to [Using the Erasure Coding Storage System](../user-guide/blobstore.md) for deployment.

## FAQ

### Insufficient memory space

``` bash
err(readFromProcess: sub-process: [cmd.go 323] Fatal: failed to start the CubeFS metanode daemon err bad totalMem config,Recommended to be configured as 80 percent of physical machine memory
```

Solution: Adjust the totalMem configuration in metanode.json according to the actual physical memory

### Port is occupied

``` bash
err(readFromProcess: sub-process: [cmd.go 311] cannot listen pprof 17320 err listen tcp :17320: bind: address already in use
```

Solution: Kill the service occupying the port, usually the node that failed to start before

### fuse client issues

``` bash
# Mount failed
err(readFromProcess: sub-process: [fuse.go 438] mount failed: fusermount: exec: "fusermount": executable file not found in $PATH)
```

First check if the fuse is installed

``` bash
$ rpm –qa|grep fuse
$ yum install fuse
```

If the fuse is already installed，please refer to [fuse client issues](../faq/fuse.md) to troubleshoot the corresponding issues