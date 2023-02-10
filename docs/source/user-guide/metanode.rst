Meta Subsystem
====================

Metanode is the manager of meta partitions and replicated by MultiRaft. Each metanode manages various of partitions.  Each partition covers an inode range, and maintains two in-memory btrees: inode btree and dentry btree.

At lease 3 meta nodes are required in respect to high availability.

.. csv-table:: Properties
   :header: "Key", "Type", "Description", "Mandatory"
 
   "role", "string", "Role of process and must be set to *metanode*", "Yes"
   "listen", "string", "Listen and accept port of the server", "Yes"
   "prof", "string", "Pprof port", "Yes"
   "localIP", "string", "IP of network to be choose", "No. If not specified, the ip address used to communicate with the master is used."
   "logLevel", "string", "Level operation for logging. Default is *error*", "No"
   "metadataDir", "string", "MetaNode store snapshot directory", "Yes"
   "logDir", "string", "Log directory", "Yes",
   "raftDir", "string", "Raft wal directory", "Yes",
   "raftHeartbeatPort", "string", "Raft heartbeat port", "Yes"
   "raftReplicaPort", "string", "Raft replicate port", "Yes"
   "consulAddr", "string", "Addresses of monitor system", "No" 
   "exporterPort", "string", "Port for monitor system", "No" 
   "masterAddr", "string", "Addresses of master server", "Yes"
   "zoneName", "string", "Specified zone. ``default`` by default.", "No"
   "totalMem","string", "Max memory metadata used. The value needs to be higher than the value of *metaNodeReservedMem* in the master configuration. Unit: byte", "Yes"
   "deleteBatchCount","int64","when deleting inodes, how many are deleted at a time ,500 by default","No"



Example:

.. code-block:: json

   {
        "role": "metanode",
        "listen": "17210",
        "prof": "17220",
        "logLevel": "debug",
        "metadataDir": "/cfs/metanode/data/meta",
        "logDir": "/cfs/metanode/log",
        "raftDir": "/cfs/metanode/data/raft",
        "raftHeartbeatPort": "17230",
        "raftReplicaPort": "17240",
        "consulAddr": "http://consul.prometheus-cfs.local",
        "exporterPort": 9501,
        "totalMem":  "8589934592",
        "masterAddr": [
            "10.196.59.198:17010",
            "10.196.59.199:17010",
            "10.196.59.200:17010"
        ]
    }


Notice
-------------

  * `listen`, `raftHeartbeatPort`, `raftReplicaPort` can't be modified after boot startup first time;
  * Above config would be stored under directory `raftDir` in `constcfg` file. If need modified forcely,you must delete this file manually;
  * These configuration items associated with master's metanode infomation . If they have been modified, master would't be found old metanode;
