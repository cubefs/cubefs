# Yum Deployment

You can use the yum tool to quickly deploy and start the CubeFS cluster in CentOS 7+ operating system.

## Get the Corresponding Version

The RPM dependencies of this tool can be installed with the following command:

::: tip Note
The cluster is managed through Ansible, please make sure that Ansible has been deployed.
:::

``` bash
$ yum install https://ocs-cn-north1.heytapcs.com/cubefs/rpm/3.2.1/cfs-install-3.2.1-el7.x86_64.rpm
$ cd /cfs/install
$ tree -L 3
 .
 ├── install_cfs.yml
 ├── install.sh
 ├── iplist
 ├── src
 └── template
     ├── client.json.j2
     ├── create_vol.sh.j2
     ├── datanode.json.j2
     ├── grafana
     │   ├── grafana.ini
     │   ├── init.sh
     │   └── provisioning
     ├── master.json.j2
     ├── metanode.json.j2
     └── objectnode.json.j2
```

## Configuration Instructions

You can modify the parameters of the CubeFS cluster in the **iplist** file according to the actual environment.

- `master`, `datanode`, `metanode`, `objectnode`, `monitor`, `client` contain the IP addresses of each module member.
- The `cfs:vars` module defines the SSH login information of all nodes, and the login name and password of all nodes in the cluster need to be unified in advance.

### Master Config Module

Defines the startup parameters of each Master node.

| Parameter                  | Type   | Description                                                                                                                                                      | Required |
|----------------------------|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| master_clusterName         | string | Cluster name                                                                                                                                                     | Yes      |
| master_listen              | string | Port number for http service listening                                                                                                                           | Yes      |
| master_prof                | string | Port number for golang pprof                                                                                                                                     | Yes      |
| master_logDir              | string | Directory for storing log files                                                                                                                                  | Yes      |
| master_logLevel            | string | Log level, default is *info*                                                                                                                                     | No       |
| master_retainLogs          | string | How many raft logs to keep                                                                                                                                       | Yes      |
| master_walDir              | string | Directory for storing raft wal logs                                                                                                                              | Yes      |
| master_storeDir            | string | Directory for storing RocksDB data. This directory must exist. If the directory does not exist, the service cannot be started.                                   | Yes      |
| master_exporterPort        | int    | Port for prometheus to obtain monitoring data                                                                                                                    | No       |
| master_metaNodeReservedMem | string | Reserved memory size for metadata nodes. If the remaining memory is less than this value, the MetaNode becomes read-only. Unit: bytes, default value: 1073741824 | No       |

> For more configuration information, please refer to [Master Configuration Instructions](../maintenance/configs/master.md).

## Datanode Config Module

Defines the startup parameters of each DataNode.

| Parameter              | Type         | Description                                                                                                                                                                                                                 | Required |
|------------------------|--------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| datanode_listen        | string       | Port for DataNode to start TCP listening as a server                                                                                                                                                                        | Yes      |
| datanode_prof          | string       | Port used by DataNode to provide HTTP interface                                                                                                                                                                             | Yes      |
| datanode_logDir        | string       | Path to store logs                                                                                                                                                                                                          | Yes      |
| datanode_logLevel      | string       | Log level. The default is *info*.                                                                                                                                                                                           | No       |
| datanode_raftHeartbeat | string       | Port used by RAFT to send heartbeat messages between nodes                                                                                                                                                                  | Yes      |
| datanode_raftReplica   | string       | Port used by RAFT to send log messages                                                                                                                                                                                      | Yes      |
| datanode_raftDir       | string       | Path to store RAFT debugging logs. The default is the binary file startup path.                                                                                                                                             | No       |
| datanode_exporterPort  | string       | Port for the monitoring system to collect data                                                                                                                                                                              | No       |
| datanode_disks         | string array | Format: *PATH:RETAIN*, PATH: disk mount path, RETAIN: the minimum reserved space under this path, and the disk is considered full if the remaining space is less than this value. Unit: bytes. (Recommended value: 20G~50G) | Yes      |

> For more configuration information, please refer to [DataNode Configuration Instructions](../maintenance/configs/datanode.md).

### Metanode Config Module

Defines the startup parameters of the MetaNode.

| Parameter                  | Type   | Description                                                                                                                             | Required |
|----------------------------|--------|-----------------------------------------------------------------------------------------------------------------------------------------|----------|
| metanode_listen            | string | Port for listening and accepting requests                                                                                               | Yes      |
| metanode_prof              | string | Debugging and administrator API interface                                                                                               | Yes      |
| metanode_logLevel          | string | Log level. The default is *info*.                                                                                                       | No       |
| metanode_metadataDir       | string | Directory for storing metadata snapshots                                                                                                | Yes      |
| metanode_logDir            | string | Directory for storing logs                                                                                                              | Yes      |
| metanode_raftDir           | string | Directory for storing raft wal logs                                                                                                     | Yes      |
| metanode_raftHeartbeatPort | string | Port for raft heartbeat communication                                                                                                   | Yes      |
| metanode_raftReplicaPort   | string | Port for raft data transmission                                                                                                         | Yes      |
| metanode_exporterPort      | string | Port for prometheus to obtain monitoring data                                                                                           | No       |
| metanode_totalMem          | string | Maximum available memory. This value needs to be higher than the value of metaNodeReservedMem in the master configuration. Unit: bytes. | Yes      |

> For more configuration information, please refer to [MetaNode Configuration Instructions](../maintenance/configs/metanode.md).

### ObjectNode Config Module

Defines the startup parameters of the ObjectNode.

| Parameter               | Type         | Description                                                                                                    | Required |
|-------------------------|--------------|----------------------------------------------------------------------------------------------------------------|----------|
| objectnode_listen       | string       | IP address and port number for http service listening                                                          | Yes      |
| objectnode_domains      | string array | Configure domain names for S3-compatible interfaces to support DNS-style access to resources. Format: `DOMAIN` | No       |
| objectnode_logDir       | string       | Path to store logs                                                                                             | Yes      |
| objectnode_logLevel     | string       | Log level. The default is `error`.                                                                             | No       |
| objectnode_exporterPort | string       | Port for prometheus to obtain monitoring data                                                                  | No       |
| objectnode_enableHTTPS  | string       | Whether to support the HTTPS protocol                                                                          | Yes      |

> For more configuration information, please refer to [ObjectNode Configuration Instructions](../maintenance/configs/objectnode.md).

### Client Config Module

Defines the startup parameters of the FUSE client.

| Parameter           | Type   | Description                                                                    | Required |
|---------------------|--------|--------------------------------------------------------------------------------|----------|
| client_mountPoint   | string | Mount point                                                                    | Yes      |
| client_volName      | string | Volume name                                                                    | No       |
| client_owner        | string | Volume owner                                                                   | Yes      |
| client_SizeGB       | string | If the volume does not exist, a volume of this size will be created. Unit: GB. | No       |
| client_logDir       | string | Path to store logs                                                             | Yes      |
| client_logLevel     | string | Log level: *debug*, *info*, *warn*, *error*, default is *info*.                | No       |
| client_exporterPort | string | Port for prometheus to obtain monitoring data                                  | Yes      |
| client_profPort     | string | Port for golang pprof debugging                                                | No       |

> For more configuration information, please refer to [Client Configuration Instructions](../maintenance/configs/client.md).

``` yaml
[master]
10.196.59.198
10.196.59.199
10.196.59.200
[datanode]
...
[cfs:vars]
ansible_ssh_port=22
ansible_ssh_user=root
ansible_ssh_pass="password"
...
#master config
...
#datanode config
...
datanode_disks =  '"/data0:10737418240","/data1:10737418240"'
...
#metanode config
...
metanode_totalMem = "28589934592"
...
#objectnode config
...
```

::: tip Note
CubeFS supports mixed deployment. If mixed deployment is adopted, pay attention to modifying the port configuration of each module to avoid port conflicts.
:::

## Start the Cluster

Use the **install.sh** script to start the CubeFS cluster, and make sure to start the Master first.

``` bash
$ bash install.sh -h
Usage: install.sh -r | --role [datanode | metanode | master | objectnode | client | all | createvol ]
$ bash install.sh -r master
$ bash install.sh -r metanode
$ bash install.sh -r datanode
$ bash install.sh -r objectnode

$ bash install.sh -r createvol
$ bash install.sh -r client
```

After all roles are started, you can log in to the node where the **client** role is located to verify whether the mount point **/cfs/mountpoint** has been mounted to the CubeFS file system.