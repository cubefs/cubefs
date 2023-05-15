# Using Erasure Coding System

::: tip Note
For a quick experience, please refer to [Single-Node Deployment](../deploy/node.md).
:::

## Compilation and Building

``` bash
$ git clone https://github.com/cubefs/cubefs.git
$ cd cubefs/blobstore
$ source env.sh
$ sh build.sh
```

After successful building, the following executable files will be generated in the `bin` directory:

```text
├── bin
│   ├── access
│   ├── clustermgr
│   ├── proxy
│   ├── scheduler
│   ├── blobnode
│   └── blobstore-cli
```

## Cluster Deployment

Due to the interdependence of the modules, the deployment should be carried out in the following order to avoid deployment failure caused by service dependencies.

### Basic Environment

1.  **Supported Platforms**

    > Linux

2.  **Dependent Components**

    > [Kafka](https://kafka.apache.org/documentation/#basic_ops)
    >
    > [Consul](https://learn.hashicorp.com/tutorials/consul/get-started-install?in=consul/getting-started)

3.  **Language Environment**

    > [Go](https://go.dev/) (1.16.x)

### Install Clustermgr

::: tip Note
Deploying Clustermgr requires at least three nodes to ensure service availability.
:::

The node startup example is as follows. The node startup requires changing the corresponding configuration file and ensuring that the associated configuration between the cluster nodes is consistent.

1.  Startup (three-node cluster)

```bash
# Example, it is recommended to use process management to start and stop services, the same below
nohup ./clustermgr -f clustermgr.conf
nohup ./clustermgr -f clustermgr1.conf
nohup ./clustermgr -f clustermgr2.conf
```

2.  Cluster configuration of the three nodes, example node 1: `clustermgr.conf`

```json
{
     "bind_addr":":9998",
     "cluster_id":1,
     "idc":["z0"],
     "chunk_size": 16777216,
     "log": {
         "level": "info",
         "filename": "./run/logs/clustermgr.log"
      },
     "auth": {
         "enable_auth": false,
         "secret": "testsecret"
     },
     "region": "test-region",
     "db_path":"./run/db0",
     "code_mode_policies": [ 
         {"mode_name":"EC3P3","min_size":0,"max_size":50331648,"size_ratio":1,"enable":true}
     ],
     "raft_config": {
         "server_config": {
             "nodeId": 1,
             "listen_port": 10110,
             "raft_wal_dir": "./run/raftwal0"
         },
         "raft_node_config":{
             "node_protocol": "http://",
             "members": [
                     {"id":1, "host":"127.0.0.1:10110", "learner": false, "node_host":"127.0.0.1:9998"},
                     {"id":2, "host":"127.0.0.1:10111", "learner": false, "node_host":"127.0.0.1:9999"},
                     {"id":3, "host":"127.0.0.1:10112", "learner": false, "node_host":"127.0.0.1:10000"}]
         }
     },
     "disk_mgr_config": {
         "refresh_interval_s": 10,
         "rack_aware":false,
         "host_aware":false
     }
}
```

### Install Proxy

1. `proxy` depends on the Kafka component and requires the creation of corresponding topics for `blob_delete_topic`, `shard_repair_topic`, and `shard_repair_priority_topic` in advance.

::: tip Note
Kafka can also use other topic names, but it is necessary to ensure that the Kafka of the Proxy and Scheduler service modules are consistent.
:::

```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic blob_delete shard_repair shard_repair_priority
```

2. Start the service. To ensure availability, at least one proxy node needs to be deployed in each IDC.

```bash
nohup ./proxy -f proxy.conf &
```

3. Example `proxy.conf`:

```json
{
   "bind_addr": ":9600",
   "host": "http://127.0.0.1:9600",
   "idc": "z0",
   "cluster_id": 1,
   "clustermgr": {
     "hosts": [
       "http://127.0.0.1:9998",
       "http://127.0.0.1:9999",
       "http://127.0.0.1:10000"
       ]
   },
   "auth": {
       "enable_auth": false,
       "secret": "test"
   },
   "mq": {
     "blob_delete_topic": "blob_delete",
     "shard_repair_topic": "shard_repair",
     "shard_repair_priority_topic": "shard_repair_prior",
     "msg_sender": {
       "broker_list": ["127.0.0.1:9092"]
     }
   },
   "log": {
     "level": "info",
     "filename": "./run/logs/proxy.log"
   }
}
```

### Install Scheduler

1. Start the service.

```bash
nohup ./scheduler -f scheduler.conf &
```

2. Example `scheduler.conf`: Note that the Scheduler module is deployed on a single node.

```json
{
   "bind_addr": ":9800",
   "cluster_id": 1,
   "services": { 
     "leader": 1,
     "node_id": 1,
     "members": {"1": "127.0.0.1:9800"}
   },
   "service_register": {
     "host": "http://127.0.0.1:9800",
     "idc": "z0"
   },
   "clustermgr": { 
     "hosts": ["http://127.0.0.1:9998", "http://127.0.0.1:9999", "http://127.0.0.1:10000"]
   },
   "kafka": {
     "broker_list": ["127.0.0.1:9092"]
   },
   "blob_delete": {
     "delete_log": {
       "dir": "./run/delete_log"
     }
   },
   "shard_repair": {
     "orphan_shard_log": {
       "dir": "./run/orphan_shard_log"
     }
   },
   "log": {
     "level": "info",
     "filename": "./run/logs/scheduler.log"
   },
   "task_log": {
     "dir": "./run/task_log"
   }
}
```

### Install BlobNode

1. Create the relevant directories in the compiled `blobnode` binary directory.

```bash
# This directory corresponds to the path in the configuration file
mkdir -p ./run/disks/disk{1..8} # Each directory needs to mount a disk to ensure data collection accuracy
```

2. Start the service.

```bash
nohup ./blobnode -f blobnode.conf
```

3. Example `blobnode.conf`:

```json
{
   "bind_addr": ":8899",
   "cluster_id": 1,
   "idc": "z0",
   "rack": "testrack",
   "host": "http://127.0.0.1:8899",
   "dropped_bid_record": {
     "dir": "./run/logs/blobnode_dropped"
   },
   "disks": [
     {
       "path": "./run/disks/disk1",
       "auto_format": true,
       "max_chunks": 1024 
     },
     {
       "path": "./run/disks/disk2",
       "auto_format": true,
       "max_chunks": 1024
     },
     {
       "path": "./run/disks/disk3",
       "auto_format": true,
       "max_chunks": 1024
     },
     {
       "path": "./run/disks/disk4",
       "auto_format": true,
       "max_chunks": 1024
     },
     {
       "path": "./run/disks/disk5",
       "auto_format": true,
       "max_chunks": 1024
     },
     {
       "path": "./run/disks/disk6",
       "auto_format": true,
       "max_chunks": 1024
     },
     {
       "path": "./run/disks/disk7",
       "auto_format": true,
       "max_chunks": 1024
     },
     {
       "path": "./run/disks/disk8",
       "auto_format": true,
       "max_chunks": 1024
     }
   ],
   "clustermgr": {
     "hosts": [
       "http://127.0.0.1:9998",
       "http://127.0.0.1:9999",
       "http://127.0.0.1:10000"
     ]
   },
   "disk_config":{
     "disk_reserved_space_B":1
   },
   "log": {
     "level": "info",
     "filename": "./run/logs/blobnode.log"
   }
}
```

### Install Access

::: tip Note
The Access module is a stateless service node and can be deployed on multiple nodes.
:::

1. Start the service.

```bash
nohup ./access -f access.conf
```

2. Example `access.conf`:

```json
{
     "bind_addr": ":9500",
     "log": {
         "level": "info",
         "filename": "./run/logs/access.log"
      },
     "stream": {
         "idc": "z0",
         "cluster_config": {
             "region": "test-region",
             "clusters":[
                 {"cluster_id":1,"hosts":["http://127.0.0.1:9998","http://127.0.0.1:9999","http://127.0.0.1:10000"]}]
         }
     }
}
```

### Configuration Instructions

- [General Configuration Instructions](../maintenance/configs/blobstore/base.md)
- [RPC Configuration Instructions](../maintenance/configs/blobstore/rpc.md)
- [Clustermgr Configuration Instructions](../maintenance/configs/blobstore/cm.md)
- [Access Configuration Instructions](../maintenance/configs/blobstore/access.md)
- [BlobNode Configuration Instructions](../maintenance/configs/blobstore/blobnode.md)
- [Proxy Configuration Instructions](../maintenance/configs/blobstore/proxy.md)
- [Scheduler Configuration Instructions](../maintenance/configs/blobstore/scheduler.md)

## Deployment Tips

1. After the deployment of Clustermgr and BlobNode fails, residual data needs to be cleaned up before redeployment to avoid registration disk failure or data display errors. The command is as follows:

```bash
# Example for blobnode
rm -f -r ./run/disks/disk*/.*
rm -f -r ./run/disks/disk*/*

# Example for clustermgr
rm -f -r /tmp/raftdb0
rm -f -r /tmp/volumedb0
rm -f -r /tmp/clustermgr
rm -f -r /tmp/normaldb0
rm -f -r /tmp/normalwal0
```

2. Clustermgr adds `learner` nodes.

::: tip Note
Learner nodes are generally used for data backup and fault recovery.
:::

- Enable the Clustermgr service on the new node and add the member information of the current node to the configuration of the new service.
- Call the [member addition interface](../maintenance/admin-api/blobstore/cm.md) to add the newly started learner node to the cluster.
  ```bash
  curl -X POST --header 'Content-Type: application/json' -d '{"peer_id": 4, "host": "127.0.0.1:10113","node_host": "127.0.0.1:10001", "member_type": 1}' "http://127.0.0.1:9998/member/add" 
  ```
- After the addition is successful, the data will be automatically synchronized.

The reference configuration is as follows: `clustermgr-learner.conf`:

```json
{
     "bind_addr":":10001",
     "cluster_id":1,
     "idc":["z0"],
     "chunk_size": 16777216,
     "log": {
         "level": "info",
         "filename": "./run/logs/clustermgr3.log"
      },
     "auth": {
         "enable_auth": false,
         "secret": "testsecret"
     },
     "region": "test-region",
     "db_path":"./run/db3",
     "code_mode_policies": [ 
         {"mode_name":"EC3P3","min_size":0,"max_size":50331648,"size_ratio":1,"enable":true}
     ],
     "raft_config": {
         "server_config": {
             "nodeId": 4,
             "listen_port": 10113,
             "raft_wal_dir": "./run/raftwal3"
         },
         "raft_node_config":{
             "node_protocol": "http://",
             "members": [
                     {"id":1, "host":"127.0.0.1:10110", "learner": false, "node_host":"127.0.0.1:9998"},
                     {"id":2, "host":"127.0.0.1:10111", "learner": false, "node_host":"127.0.0.1:9999"},
                     {"id":3, "host":"127.0.0.1:10112", "learner": false, "node_host":"127.0.0.1:10000"},
                     {"id":4, "host":"127.0.0.1:10113", "learner": true, "node_host": "127.0.0.1:10001"}]
         }
     },
     "disk_mgr_config": {
         "refresh_interval_s": 10,
         "rack_aware":false,
         "host_aware":false
     }
}
```

2. After all modules are deployed successfully, the upload verification needs to be delayed for a period of time to wait for the successful creation of the volume.

## Upload Testing

> Refer to [CLI Deployment Verification](../deploy/verify.md) for details.

## Modifying Master Configuration

Modify the `ebsAddr` configuration item in the Master configuration file ([more configuration references](../maintenance/configs/master.md)) to the Consul address registered by the Access node.

## Creating Erasure-coded Volume

Refer to [Creating a Volume](./volume.md).

## Appendix

1. Encoding strategy: commonly used strategy table

| Category  | Description                                                                       |
|-----------|-----------------------------------------------------------------------------------|
| EC12P4    | {N: 12, M: 04, L: 0, AZCount: 1, PutQuorum: 15, GetQuorum: 0, MinShardSize: 2048} |
| EC3P3     | {N: 3,  M: 3,  L: 0, AZCount: 1, PutQuorum: 5, GetQuorum: 0, MinShardSize: 2048}  |
| EC16P20L2 | {N: 16, M: 20, L: 2, AZCount: 2, PutQuorum: 34, GetQuorum: 0, MinShardSize: 2048} |
| EC6P10L2  | {N: 6,  M: 10, L: 2, AZCount: 2, PutQuorum: 14, GetQuorum: 0, MinShardSize: 2048} |
| EC12P9    | {N: 12, M: 9,  L: 0, AZCount: 3, PutQuorum: 20, GetQuorum: 0, MinShardSize: 2048} |
| EC15P12   | {N: 15, M: 12, L: 0, AZCount: 3, PutQuorum: 24, GetQuorum: 0, MinShardSize: 2048} |
| EC6P6     | {N: 6,  M: 6,  L: 0, AZCount: 3, PutQuorum: 11, GetQuorum: 0, MinShardSize: 2048} |

Where
- N: number of data blocks, M: number of check blocks, L: number of local check blocks, AZCount: number of AZs
- PutQuorum: `(N + M) / AZCount + N \<= PutQuorum \<= M + N`
- MinShardSize: minimum shard size, the data is continuously filled into the `0-N` shards. If the data size is less than `MinShardSize*N`, it is aligned with zero bytes. See [code](https://github.com/cubefs/cubefs/blob/release-3.2.0/blobstore/common/codemode/codemode.go) for details.