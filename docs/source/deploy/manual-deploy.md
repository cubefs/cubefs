# Distributed Mode

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
  "ip": "127.0.0.1",
  "listen": "17010",
  "prof":"17020",
  "id":"1",
  "peers": 1:127.0.0.1:17010,2:127.0.0.2:17010,3:127.0.0.3:17010",
  "retainLogs":"20000",
  "logDir": "/cfs/master/log",
  "logLevel":"info",
  "walDir":"/cfs/master/data/wal",
  "storeDir":"/cfs/master/data/store",
  "consulAddr": "http://consul.prometheus-cfs.local",
  "clusterName":"cubefs01",
  "metaNodeReservedMem": "1073741824"
}
```

Configuration parameters can be found in [Master Detailed Configuration](../maintenance/configs/master.md).

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
    "metadataDir": "/cfs/metanode/data/meta",
    "logDir": "/cfs/metanode/log",
    "raftDir": "/cfs/metanode/data/raft",
    "raftHeartbeatPort": "17230",
    "raftReplicaPort": "17240",
    "totalMem":  "8589934592",
    "consulAddr": "http://consul.prometheus-cfs.local",
    "exporterPort": 9501,
    "masterAddr": [
        "127.0.0.1:17010",
        "127.0.0.2:17010",
        "127.0.0.3:17010"
    ]
}
```

For detailed configuration parameters, please refer to [MetaNode Detailed Configuration](../maintenance/configs/metanode.md).

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
  "masterAddr": [
     "127.0.0.1:17010",
     "127.0.0.1:17010",
     "127.0.0.1:17010"
  ],
  "disks": [
     "/data0:10737418240",
     "/data1:10737418240"
 ]
}
```

For detailed configuration parameters, please refer to [DataNode Detailed Configuration](../maintenance/configs/datanode.md).

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
    "listen": 17410,
    "masterAddr": [
       "127.0.0.1:17010",
       "127.0.0.2:17010",
       "127.0.0.3:17010"
    ],
    "logLevel": "info",
    "logDir": "/cfs/Logs/objectnode"
}
```

For detailed configuration parameters, please refer to [ObjectNode Detailed Configuration](../maintenance/configs/objectnode.md).

### Install Erasure Coding Subsystem

::: tip Note
Optional section. If you need to use the erasure coding volume, you need to deploy it.
:::

Please refer to [Using the Erasure Coding Storage System](../user-guide/blobstore.md) for deployment.