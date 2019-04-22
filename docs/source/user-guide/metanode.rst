Meta Subsystem
====================

Metanode is the manager of meta partitions and replicated by MultiRaft. Each metanode manages various of partitions.  Each partition covers an inode range, and maintains two in-memory btrees: inode btree and dentry btree.

.. csv-table:: Properties
   :header: "Key", "Type", "Description", "Mandatory"
 
   "role", "string", "Role of process and must be set to *metanode*", "Yes"
   "listen", "string", "Listen and accept port of the server", "Yes"
   "prof", "string", "pprof port", "Yes"
   "logLevel", "string", "Level operation for logging. Default is *error*", "No"
   "metadataDir", "string", metaNode store snapshot directory", "Yes"
   "logDir", "string", "log directory", "Yes", 
   "raftDir", "string", "raft wal directory",  "Yes", 
   "raftHeartbeatPort", "string", "raft heartbeat port", "Yes" 
   "raftReplicaPort", "string", "raft replicate port", "Yes"
   "consulAddr", "string", "Addresses of monitor system", "No" 
   "exporterPort", "string", "Port for monitor system", "No" 
   "masterAddrs", "string", "Addresses of master server", "Yes"
   "warnLogDir","string","Warn message directory","No"




Example:

.. code-block:: json

   {
        "role": "metanode",
        "listen": "9021",
        "prof": "9092",
        "logLevel": "debug",
        "metadataDir": "/export/cfs/metanode_meta",
        "logDir": "/export/Logs/cfs/metanode",
        "raftDir": "/export/cfs/metanode_raft",
        "raftHeartbeatPort": "9093",
        "raftReplicaPort": "9094",
        "warnLogDir":"/export/home/tomcat/UMP-Monitor/logs/",
        "consulAddr": "http://consul.prometheus-cfs.local",
         "exporterPort": 9511,
        "masterAddrs": [
            "192.168.31.173:80",
            "192.168.31.141:80",
            "192.168.30.200:80"
        ]
    }
