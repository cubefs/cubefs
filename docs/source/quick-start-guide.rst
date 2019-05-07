Quick Start Guide
=================

Building
--------

Build Servers
^^^^^^^^^^^^^

In ChubaoFS, the server consists of the resource manager, metanode and datanode, which are compiled to a single binary for deployment convenience.

Building of ChubaoFS server depends on RocksDB, `build RocksDB v5.9.2+ <https://github.com/facebook/rocksdb/blob/master/INSTALL.md>`_ .
Recommended installation uses `make static_lib` .

ChubaoFS server is built with the following command:

.. code-block:: bash

   cd cmd; sh build.sh

Build Client
^^^^^^^^^^^^

.. code-block:: bash

   cd client; sh build.sh

Deployment
----------

Start Resource Manager
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   nohup ./cmd -c master.json &


Sample *master.json* is shown as follows,

.. code-block:: json

   {
     "role": "master",
     "ip": "192.168.31.173",
     "port": "80",
     "prof":"10088",
     "id":"1",
     "peers": "1:192.168.31.173:80,2:192.168.31.141:80,3:192.168.30.200:80",
     "retainLogs":"20000",
     "logDir": "/export/Logs/master",
     "logLevel":"info",
     "walDir":"/export/Data/master/raft",
     "storeDir":"/export/Data/master/rocksdbstore",
     "warnLogDir":"/export/home/tomcat/UMP-Monitor/logs/",
     "consulAddr": "http://consul.prometheus-cfs.local",
     "exporterPort": 9510,
     "clusterName":"cfs"
   }

   
For detailed explanations of *master.json*, please refer to :doc:`user-guide/master`.

Start Metanode
^^^^^^^^^^^^^^

.. code-block:: bash

   nohup ./cmd -c meta.json &

Sample *meta.json is* shown as follows,

.. code-block:: json

   {
       "role": "metanode",
       "listen": "9021",
       "prof": "9092",
       "logLevel": "debug",
       "metaDir": "/export/Data/metanode",
       "logDir": "/export/Logs/metanode",
       "raftDir": "/export/Data/metanode/raft",
       "raftHeartbeatPort": "9093",
       "raftReplicatePort": "9094",
       "totalMem":  "17179869184",
       "warnLogDir":"/export/home/tomcat/UMP-Monitor/logs/",
       "consulAddr": "http://consul.prometheus-cfs.local",
       "exporterPort": 9511,
       "masterAddrs": [
           "192.168.31.173:80",
           "192.168.31.141:80",
           "192.168.30.200:80"
       ]
   }


For detailed explanations of *meta.json*, please refer to :doc:`user-guide/metanode`.

Start Datanode
^^^^^^^^^^^^^^

1. Prepare data directories

   **Recommendation** Using independent disks can reach better performance.

   **Disk preparation**

    1.1 Check available disks

        .. code-block:: bash

           fdisk -l

    1.2 Build local Linux file system on the selected devices

        .. code-block:: bash

           mkfs.xfs -f /dev/sdx

    1.3 Make mount point

        .. code-block:: bash

           mkdir /data0

    1.4 Mount the device on mount point

        .. code-block:: bash

           mount /dev/sdx /data0

2. Start datanode

   .. code-block:: bash
   
      nohup ./cmd -c datanode.json &

   Sample *datanode.json* is shown as follows,
   
   .. code-block:: json

      {
        "role": "datanode",
        "port": "6000",
        "prof": "6001",
        "logDir": "/export/Logs/datanode",
        "logLevel": "info",
        "raftHeartbeat": "9095",
        "raftReplica": "9096",
        "warnLogDir":"/export/home/tomcat/UMP-Monitor/logs/",
        "consulAddr": "http://consul.prometheus-cfs.local",
        "exporterPort": 9512,
        "masterAddr": [
        "192.168.31.173:80",
        "192.168.31.141:80",
        "192.168.30.200:80"
        ],
        "rack": "",
        "disks": [
           "/data0:21474836480",
           "/data1:21474836480"
        ]
      }

For detailed explanations of *datanode.json*, please refer to :doc:`user-guide/datanode`.

Create Volume
^^^^^^^^^^^^^

By decault, there are only a few data partitions allocated upon volume creation, and will be dynamically expanded according to actual usage. For performance evaluation, it is better to preallocate enough data partitions.

.. code-block:: bash

   curl -v "http://127.0.0.1/admin/createVol?name=test&capacity=100&owner=cfs"



Mount Client
------------

1. Run ``modprobe fuse`` to insert FUSE kernel module.
2. Run ``yum install -y fuse`` to install libfuse.
3. Run ``nohup client -c fuse.json &`` to start a client.

   Sample *fuse.json* is shown as follows,
   
   .. code-block:: json
   
      {
        "mountPoint": "/mnt/fuse",
        "volName": "test",
        "owner": "cfs",
        "masterAddr": "192.168.31.173:80,192.168.31.141:80,192.168.30.200:80",
        "logDir": "/export/Logs/client",
        "warnLogDir":"/export/home/tomcat/UMP-Monitor/logs/",
        "profPort": "10094",
        "logLevel": "info"
      }


For detailed explanations of *fuse.json*, please refer to :doc:`user-guide/client`.

Note that end user can start more than one client on a single machine, as long as mountpoints are different.

Upgrading
---------

1. freeze the cluster

.. code-block:: bash

   curl -v "http://127.0.0.1/cluster/freeze?enable=true"

2. upgrade each module

3. closed freeze flag

.. code-block:: bash

   curl -v "http://127.0.0.1/cluster/freeze?enable=false"
