Meta Subsystem
====================

Metanode is the manager of meta partitions and replicated by MultiRaft. Each metanode manages various of partitions.  Each partition covers an inode range, and maintains two in-memory btrees: inode btree and dentry btree.

At lease 3 meta nodes are required in respect to high availability.

.. csv-table:: Properties
   :header: "Key", "Type", "Description", "Mandatory"
 
   "role", "string", "Role of process and must be set to *metanode*", "Yes"
   "listen", "string", "Listen and accept port of the server", "Yes"
   "prof", "string", "Pprof port", "Yes"
   "logLevel", "string", "Level operation for logging. Default is *error*", "No"
   "metadataDir", "string", MetaNode store snapshot directory", "Yes"
   "logDir", "string", "Log directory", "Yes",
   "raftDir", "string", "Raft wal directory",  "Yes",
   "raftHeartbeatPort", "string", "Raft heartbeat port", "Yes"
   "raftReplicaPort", "string", "Raft replicate port", "Yes"
   "consulAddr", "string", "Addresses of monitor system", "No" 
   "exporterPort", "string", "Port for monitor system", "No" 
   "masterAddrs", "string", "Addresses of master server", "Yes"
   "warnLogDir","string","Warn message directory","No"
   "totalMem","string","Max memory metadata used","No"




Example:

.. code-block:: json

   {
        "role": "metanode",
        "listen": "9021",
        "prof": "9092",
        "logLevel": "debug",
        "metadataDir": "/export/Data/metanode",
        "logDir": "/export/Logs/metanode",
        "raftDir": "/export/Data/metanode/raft",
        "warnLogDir":"/export/home/tomcat/UMP-Monitor/logs/",
        "raftHeartbeatPort": "9093",
        "raftReplicaPort": "9094",
        "consulAddr": "http://consul.prometheus-cfs.local",
        "exporterPort": 9511,
        "totalMem":  "17179869184",
        "masterAddrs": [
            "192.168.31.173:80",
            "192.168.31.141:80",
            "192.168.30.200:80"
        ]
    }
