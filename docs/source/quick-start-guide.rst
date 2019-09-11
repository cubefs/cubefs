Quick Start Guide
=================

Building
--------

Use following command to build server and client:

.. code-block:: bash

   make build

If the build is successful, `cfs-server` and `cfs-client` will be found in directory `build/bin`

Deployment
----------

Start Resource Manager (Master)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   nohup ./cfs-server -c master.json &


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
     "consulAddr": "http://consul.prometheus-cfs.local",
     "exporterPort": 9510,
     "clusterName":"cfs"
   }


For detailed explanations of *master.json*, please refer to :doc:`user-guide/master`.

Start Metanode
^^^^^^^^^^^^^^

.. code-block:: bash

   nohup ./cfs-server -c meta.json &

Sample *meta.json is* shown as follows,

.. code-block:: json

   {
       "role": "metanode",
       "listen": "9021",
       "prof": "9092",
       "logLevel": "info",
       "metadataDir": "/export/Data/metanode",
       "logDir": "/export/Logs/metanode",
       "raftDir": "/export/Data/metanode/raft",
       "raftHeartbeatPort": "9093",
       "raftReplicaPort": "9094",
       "totalMem":  "17179869184",
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

      nohup ./cfs-server -c datanode.json &

   Sample *datanode.json* is shown as follows,

   .. code-block:: json

      {
        "role": "datanode",
        "port": "6000",
        "prof": "6001",
        "logDir": "/export/Logs/datanode",
        "raftDir": "/export/Data/datanode/raft",
        "logLevel": "info",
        "raftHeartbeat": "9095",
        "raftReplica": "9096",
        "consulAddr": "http://consul.prometheus-cfs.local",
        "exporterPort": 9512,
        "masterAddr": [
        "192.168.31.173:80",
        "192.168.31.141:80",
        "192.168.30.200:80"
        ],
        "disks": [
           "/data0:21474836480",
           "/data1:21474836480"
        ]
      }

For detailed explanations of *datanode.json*, please refer to :doc:`user-guide/datanode`.

Create Volume
^^^^^^^^^^^^^

By default, there are only a few data partitions allocated upon volume creation, and will be dynamically expanded according to actual usage.

.. code-block:: bash

   curl -v "http://127.0.0.1/admin/createVol?name=test&capacity=10000&owner=cfs"

For performance evaluation, extra data partitions shall be pre-created according to the amount of data nodes and disks to reach maximum performance.

.. code-block:: bash

    curl -v "http://127.0.0.1/dataPartition/create?name=test&count=120"

Mount Client
------------

1. Run ``modprobe fuse`` to insert FUSE kernel module.
2. Run ``yum install -y fuse`` to install libfuse.
3. Run ``cfs-client -c fuse.json`` to start a client daemon.

   Sample *fuse.json* is shown as follows,

   .. code-block:: json

      {
        "mountPoint": "/mnt/fuse",
        "volName": "test",
        "owner": "cfs",
        "masterAddr": "192.168.31.173:80,192.168.31.141:80,192.168.30.200:80",
        "logDir": "/export/Logs/client",
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
