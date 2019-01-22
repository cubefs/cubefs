Metanode
========

Metanode is the manager of meta partitions and replicated by MultiRaft. Each metanode manages various of partitions.  Each partition covers an inode range, and maintains two in-memory btrees: inode btree and dentry btree.

.. csv-table:: Properties
   :header: "Key", "Type", "Description", "Required"
 
   "role", "string", "Role of process and must be set to *metanode*", "Yes"
   "listen", "string", "Listen and accept port of the server", "Yes"
   "prof", "string", "pprof port", "Yes"
   "logLevel", "string", "Level operation for logging. Default is *error*", "No"
   "metaDir", "string", metaNode store snapshot directory", "Yes" 
   "logDir", "string", "log directory", "Yes", 
   "raftDir", "string", "raft wal directory",  "Yes", 
   "raftHeartbeatPort", "string", "raft heartbeat port", "Yes" 
   "raftReplicatePort", "string", "raft replicate port", "Yes" 
   "consulAddr", "string", "Addresses of monitor system", "No" 
   "exporterPort", "string", "Port for monitor system", "No" 
   "masterAddrs", "string", "Addresses of master server", "Yes"



Example:

.. code-block:: json

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
