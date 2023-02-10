Resource Manager (Master)
============================

The cluster contains dataNodes,metaNodes,vols,dataPartitions and metaPartitions,they are managed by master server. The master server caches the metadata in mem,persist to GoLevelDB,and ensure consistence by raft protocol.

The master server manages dataPartition id to dataNode server mapping,metaPartition id to metaNode server mapping.

At lease 3 master nodes are required in respect to high availability.

Features
--------

- Multi-tenant, Resource Isolation
- dataNodes,metaNodes shared,vol owns dataPartition and metaPartition exclusive
- Asynchronous communication with dataNode and metaNode

Configurations
--------------

CubeFS use **JSON** as configuration file format.

.. csv-table:: Properties
   :header: "Key", "Type", "Description", "Mandatory"
   
   "role", "string", "Role of process and must be set to master", "Yes"
   "ip", "string", "host ip", "Yes"
   "listen", "string", "Http port which api service listen on", "Yes"
   "prof", "string", "golang pprof port", "Yes"
   "id", "string", "identy different master node", "Yes"
   "peers", "string", "the member information of raft group", "Yes"
   "logDir", "string", "Path for log file storage", "Yes"
   "logLevel", "string", "Level operation for logging. Default is *error*.", "No"
   "ebsAddr","string","blobStore addr, need config if use blobStore subsystem","否", ""
   "retainLogs", "string", "the number of raft logs will be retain.", "Yes"
   "walDir", "string", "Path for raft log file storage.", "Yes"
   "storeDir", "string", "Path for RocksDB file storage,path must be exist", "Yes"
   "clusterName", "string", "The cluster identifier", "Yes"
   "exporterPort", "int", "The prometheus exporter port", "No"
   "consulAddr", "string", "The consul register addr for prometheus exporter", "No"
   "metaNodeReservedMem","string","If the metanode memory is below this value, it will be marked as read-only. Unit: byte. 1073741824 by default.", "No"
   "heartbeatPort","string","Raft heartbeat port,5901 by default","No"
   "replicaPort","string","Raft replica Port,5902 by default","No"
   "nodeSetCap","string","the capacity of node set,18 by default","No"
   "missingDataPartitionInterval","string","how much time it has not received the heartbeat of replica,the replica is considered  missing ,24 hours by default","No"
   "dataPartitionTimeOutSec","string","how much time it has not received the heartbeat of replica, the replica is considered not alive ,10 minutes by default","No"
   "numberOfDataPartitionsToLoad","string","the maximum number of partitions to check at a time,40  by default","No"
   "secondsToFreeDataPartitionAfterLoad","string","the task that release the memory occupied by loading data partition task can be start, only after secondsToFreeDataPartitionAfterLoad seconds
  ,300 by default","No"
    "tickInterval","string","the interval of timer which check heartbeat and election timeout,500 ms by default","No"
    "electionTick","string","how many times the tick timer has reset,the election is timeout,5 by default","No"


**Example:**

.. code-block:: json

   {
    "role": "master",
    "id":"1",
    "ip": "10.196.59.198",
    "listen": "17010",
    "prof":"17020",
    "peers": "1:10.196.59.198:17010,2:10.196.59.199:17010,3:10.196.59.200:17010",
    "retainLogs":"20000",
    "logDir": "/cfs/master/log",
    "logLevel":"info",
    "walDir":"/cfs/master/data/wal",
    "storeDir":"/cfs/master/data/store",
    "exporterPort": 9500,
    "consulAddr": "http://consul.prometheus-cfs.local",
    "clusterName":"cubefs01",
    "metaNodeReservedMem": "1073741824"
   }


Start Service
-------------

.. code-block:: bash

   nohup ./master -c config.json > nohup.out &
