Run Cluster Manually
=====================

Compile Dependencies
--------

.. csv-table::
   :header: "Dependencies", "Version Requirement"

   "gcc-c++","4.8.5 and above"
   "CMake","3.1 and above"
   "Go","1.16 and above"
   "bzip2-devel","1.0.6 and above"
   "mvn","3.8.4 and above"


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

   ./cfs-server -c master.json


Sample *master.json* is shown as follows,

.. code-block:: json

   {
     "role": "master",
     "ip": "10.196.59.198",
     "listen": "17010",
     "prof":"17020",
     "id":"1",
     "peers": "1:10.196.59.198:17010,2:10.196.59.199:17010,3:10.196.59.200:17010",
     "retainLogs":"2000",
     "logDir": "/cfs/master/log",
     "logLevel":"info",
     "walDir":"/cfs/master/data/wal",
     "storeDir":"/cfs/master/data/store",
     "consulAddr": "http://consul.prometheus-cfs.local",
     "clusterName":"cubefs01",
     "metaNodeReservedMem": "1073741824"
   }


For detailed explanations of *master.json*, please refer to :doc:`user-guide/master`.

Start Metanode
^^^^^^^^^^^^^^

.. code-block:: bash

   ./cfs-server -c meta.json

Sample *meta.json is* shown as follows,

.. code-block:: json

   {
       "role": "metanode",
       "listen": "17210",
       "prof": "17220",
       "logLevel": "info",
       "metadataDir": "/cfs/metanode/data/meta",
       "logDir": "/cfs/metanode/log",
       "raftDir": "/cfs/metanode/data/raft",
       "raftHeartbeatPort": "17230",
       "raftReplicaPort": "17240",
       "totalMem":  "8589934592",
       "consulAddr": "http://consul.prometheus-cfs.local",
       "exporterPort": 9501,
       "masterAddr": [
           "10.196.59.198:17010",
           "10.196.59.199:17010",
           "10.196.59.200:17010"
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

      ./cfs-server -c datanode.json

   Sample *datanode.json* is shown as follows,

   .. code-block:: json

      {
        "role": "datanode",
        "listen": "17310",
        "prof": "17320",
        "logDir": "/cfs/datanode/log",
        "raftDir": "/cfs/datanode/log",
        "logLevel": "info",
        "raftHeartbeat": "17330",
        "raftReplica": "17340",
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

For detailed explanations of *datanode.json*, please refer to :doc:`user-guide/datanode`.

Start BlobStore
^^^^^^^^^^^^^^^^^^^^^

Please refer to :doc:`user-guide/blobstore` 。

Start ObjectNode
^^^^^^^^^^^^^^^^

.. code-block:: bash

   ./cfs-server -c objectnode.json

Sample *objectnode.json is* shown as follows,

.. code-block:: json

    {
        "role": "objectnode",
        "domains": [
            "object.cfs.local"
        ],
        "listen": 17410,
        "masterAddr": [
           "10.196.59.198:17010",
           "10.196.59.199:17010",
           "10.196.59.200:17010"
        ],
        "logLevel": "info",
        "logDir": "/cfs/Logs/objectnode"
    }


For detailed explanations of *objectnode.json*, please refer to :doc:`user-guide/objectnode`.


Create Volume
------------

By default, there are only a few data partitions allocated upon volume creation, and will be dynamically expanded according to actual usage.

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/admin/createVol?name=ltptest&capacity=10000&owner=ltptest"
For detailed explanations of volume creation, please refer to :doc:`admin-api/master/volume`.

For performance evaluation, extra data partitions shall be pre-created according to the amount of data nodes and disks to reach maximum performance.

.. code-block:: bash

    curl -v "http://10.196.59.198:17010/dataPartition/create?name=test&count=120"

Mount Client
------------

1. Run ``modprobe fuse`` to insert FUSE kernel module.
2. Run ``yum install -y fuse`` to install libfuse.
3. Run ``cfs-client -c fuse.json`` to start a client daemon.

   Sample *fuse.json* is shown as follows,

   .. code-block:: json

      {
        "mountPoint": "/cfs/mountpoint",
        "volName": "ltptest",
        "owner": "ltptest",
        "masterAddr": "10.196.59.198:17010,10.196.59.199:17010,10.196.59.200:17010",
        "logDir": "/cfs/client/log",
        "profPort": "17510",
        "exporterPort": "9504",
        "logLevel": "info"
      }


For detailed explanations of *fuse.json*, please refer to :doc:`user-guide/client`.

Note that end user can start more than one client on a single machine, as long as mountpoints are different.

Upgrading
---------

1. freeze the cluster

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/cluster/freeze?enable=true"

2. upgrade each module

3. closed freeze flag

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/cluster/freeze?enable=false"
