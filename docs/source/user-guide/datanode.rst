Data Subsystem
======================

How To Start DataNode
---------------------

Start a DataNode process by execute the server binary of CFS you built with ``-c`` argument and specify configuration file.

.. code-block:: bash

   nohup cfs-datanode -c datanode.json &


Configurations
--------------

.. csv-table:: Properties
   :header: "Key", "Type", "Description", "Mandatory"

   "role", "string", "Role of process and must be set to *datanode*", "Yes"
   "port", "string", "Port of TCP network to be listen", "Yes"
   "localIP", "string", "IP of network to be choose", "No"
   "prof", "string", "Port of HTTP based prof and api service", "Yes"
   "logDir", "string", "Path for log file storage", "Yes"
   "logLevel", "string", "Level operation for logging. Default is *error*", "No"
   "raftHeartbeat", "string", "Port of raft heartbeat TCP network to be listen", "Yes"
   "raftReplica", "string", "Port of raft replicate TCP network to be listen", "Yes"
   "raftDir", "string", "Path for raft log file storage", "No"
   "consulAddr", "string", "Addresses of monitor system", "No"
   "exporterPort", "string", "Port for monitor system", "No"
   "masterAddr", "string slice", "Addresses of master server", "Yes"
   "rack", "string", "Identity of rack", "No"
   "disks", "string slice", "PATH:MAX_ERRS:REST_SIZE", "Yes"
   "warnLogDir","string","Warn message directory","No"


**Example:**

.. code-block:: json

   {
       "role": "datanode",
       "port": "6000",
       "prof": "6001",
       "logDir": "/export/Logs/datanode",
       "logLevel": "debug",
       "raftHeartbeat": "9095",
       "warnLogDir":"/export/home/tomcat/UMP-Monitor/logs/",
       "raftReplica": "9096",
       "raftDir": "/export/Logs/datanode/raft",
       "consulAddr": "http://consul.prometheus-cfs.local",
       "exporterPort": 9512,    
       "masterAddr": [
           "10.196.30.200:80",
           "10.196.31.141:80",
           "10.196.31.173:80"
       ],
       "rack": "main",
        "disks": [
           "/data0:107374182400",
           "/data1:107374182400"
       ]
   }

