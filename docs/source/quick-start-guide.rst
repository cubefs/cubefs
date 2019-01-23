Quick Start Guide
=================

Building
--------

Build Servers
^^^^^^^^^^^^^

In the CFS design, servers refer to master, metanode and datanode, and are compiled into a single binary for deployment convenience.

You'll need to `build RocksDB v5.9.2+ <https://github.com/facebook/rocksdb/blob/master/INSTALL.md>`_ on your machine.
Recommended for installation use make static_lib command. After that, you can build cfs using the following command.

.. code-block:: bash

   cd cmd; go build cmd.go

Build Client
^^^^^^^^^^^^

.. code-block:: bash

   cd client; go build

Deployment
----------

Start Master
^^^^^^^^^^^^

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
     "logDir": "/export/Logs/cfs/master",
     "logLevel":"DEBUG",
     "walDir":"/export/Logs/cfs/raft",
     "storeDir":"/export/cfs/rocksdbstore",
     "consulAddr": "http://cbconsul-cfs01.cbmonitor.svc.ht7.n.jd.local",
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


For detailed explanations of *meta.json*, please refer to :doc:`user-guide/metanode`.

Start Datanode
^^^^^^^^^^^^^^

1. Prepare data directories

   **TODO**

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
        "raftReplicate": "9096",
        "consulAddr": "http://cbconsul-cfs01.cbmonitor.svc.ht7.n.jd.local",
        "exporterPort": 9512,
        "masterAddr": [
        "192.168.31.173:80",
        "192.168.31.141:80",
        "192.168.30.200:80"
        ],
        "rack": "",
        "disks": [
        "/data0:1:40000"
        ]  
      }

For detailed explanations of *datanode.json*, please refer to :doc:`user-guide/datanode`.

Create Volume
^^^^^^^^^^^^^
.. code-block:: bash

   curl -v "http://127.0.0.1/admin/createVol?name=test&capacity=100"

Mount Client
------------

1. Run ``modprobe fuse`` to insert FUSE kernel module.
2. Run ``yum install -y fuse`` to install libfuse.
3. Run ``nohup client -c fuse.json &`` to start a client.

   Sample *fuse.json* is shown as follows,
   
   .. code-block:: json
   
      {
        "mountpoint": "/mnt/fuse",
        "volname": "test",
        "master": "192.168.31.173:80,192.168.31.141:80,192.168.30.200:80",
        "logpath": "/export/Logs/cfs",
        "profport": "10094",
        "loglvl": "info"
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
