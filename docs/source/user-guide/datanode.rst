Data Subsystem
======================

How To Start DataNode
---------------------

Start a DataNode process by execute the server binary of CubeFS you built with ``-c`` argument and specify configuration file. Usually, the number of datanode instances needs to be more than three, but in a production environment, in order to consider disaster recovery backup, at least four datanode instances are required.

.. code-block:: bash

   nohup cfs-server -c datanode.json &


Configurations
--------------

.. csv-table:: Properties
   :header: "Key", "Type", "Description", "Mandatory"

   "role", "string", "Role of process and must be set to *datanode*", "Yes"
   "listen", "string", "Port of TCP network to be listen", "Yes"
   "localIP", "string", "IP of network to be choose", "No,If not specified, the ip address used to communicate with the master is used."
   "prof", "string", "Port of HTTP based prof and api service", "Yes"
   "logDir", "string", "Path for log file storage", "Yes"
   "logLevel", "string", "Level operation for logging. Default is *error*", "No"
   "raftHeartbeat", "string", "Port of raft heartbeat TCP network to be listen", "Yes"
   "raftReplica", "string", "Port of raft replicate TCP network to be listen", "Yes"
   "raftDir", "string", "Path for raft log file storage", "No"
   "consulAddr", "string", "Addresses of monitor system", "No"
   "exporterPort", "string", "Port for monitor system", "No"
   "masterAddr", "string slice", "Addresses of master server", "Yes"
   "zoneName", "string", "Specified zone. ``default`` by default.", "No"
   "disks", "string slice", "
   | Format: *PATH:RETAIN*.
   | PATH: Disk mount point. RETAIN: Retain space. (Ranges: 20G-50G.)", "Yes"


**Example:**

.. code-block:: json

   {
       "role": "datanode",
       "listen": "17310",
       "prof": "17320",
       "logDir": "/cfs/datanode/log",
       "logLevel": "info",
       "raftHeartbeat": "17330",
       "raftReplica": "17340",
       "raftDir": "/cfs/datanode/log",
       "consulAddr": "http://consul.prometheus-cfs.local",
       "exporterPort": 9502,
       "masterAddr": [
            "10.196.59.198:17010",
            "10.196.59.199:17010",
            "10.196.59.200:17010"
       ],
        "disks": [
           "/data0:10737418240",
           "/data1:10737418240"
       ]
   }


Notice
-------------

  * Since datanode uses **SEEK_HOLE** and **SEEK_DATA** operations which is supported by XFS (since Linux 3.5) and ext4 (since Linux 3.8), users should pay attention to the Linux kernel version on which datanodes are deployed.
  * `listen`, `raftHeartbeat`, `raftReplica` can't be modified after boot startup first time.
  * Above config would be stored under directory `raftDir` in `constcfg` file. If need modified forcely, you must delete this file manually.
  * These configuration items associated with master's datanode infomation. If they have been modified, master would't be found old datanode.
