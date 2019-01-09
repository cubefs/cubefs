#MetaNode
metaNode is the manager of metaPartition and replication by raft. metaNode is
base on multiRaft, so each metaNode manager
 many partitions.  Each partition is an inode range, and composed of two
 in-memory btree: inode btree and dentry btree.
 
## Configuration
CFS using JSON as for configuration file format.

 
###Properties:
 
| key | Type | Description | Required |
|:---:|:----:|:-----------:|:--------:|
| role|string|Role of process and must be set to "metanode". | Yes |
|listen| string |Listen and accept port of the server. | Yes|
|prof|string |pprof port| Yes |
|logLevel|string|Level operation for logging. Default is "error".| No |
|metaDir|string|metaNode store snapshot directory | Yes |
|logDir|string|log directory| Yes|
|raftDir|string|raft wal directory| Yes|
|raftHeartbeatPort|string|raft heartbeat port| Yes |
|raftReplicatePort|string|raft replicate port|Yes|
|consulAddr|string|Addresses of monitor system|No|
|exporterPort|string|Port for monitor system|No|
|masterAddrs|[]string|Addresses of master server|Yes



### Example:
```json
{
     "role": "metanode",
     "listen": "9021",
     "prof": "9092",
     "logLevel": "debug",
     "metaDir": "/export/cfs/metanode_meta",
     "logDir": "/export/Logs/cfs/metanode",
     "raftDir": "/export/cfs/metanode_raft",
     "raftHeartbeatPort": "9093",
     "raftReplicatePort": "9094",
     "consulAddr": "http://cbconsul-cfs01.cbmonitor.svc.ht7.n.jd.local",
      "exporterPort": 9511,
     "masterAddrs": [
         "192.168.31.173:80",
         "192.168.31.141:80",
         "192.168.30.200:80"
     ]
 }
```