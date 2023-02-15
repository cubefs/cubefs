BlobStore Manual
================

Build
-----

.. code-block:: bash

   $ git clone https://github.com/cubefs/cubefs.git
   $ cd cubefs/blobstore
   $ source env.sh
   $ ./build.sh

If  ``build`` successful, the following executable files will be generated in the ``bin`` directory

    1. clustermgr
    2. proxy
    3. scheduler
    4. blobnode
    5. access
    6. cli

Cluster Deployment
------------------

Since modules are related to a certain extent, they need to be deployed in the following order to avoid deployment failure due to service dependencies.

Basic Environment
:::::::::::::::::

  1. Platform Support

      `Linux`

  2. Component

      `Kafka <https://kafka.apache.org/documentation/#basic_ops>`_

      `Consul <https://learn.hashicorp.com/tutorials/consul/get-started-install?in=consul/getting-started>`_ (optional, support for consul)

  3. **Dev**

      `Go <https://go.dev/>`_ (1.17.x)

Clustermgr
::::::::::

At least three nodes are required to deploy clustermgr to ensure service availability.  When starting a node, you need to change the corresponding configuration file and ensure that the associated configuration between the cluster nodes is consistent.

1. Start Service (three-node cluster)

.. code-block:: bash

   nohup ./clustermgr -f clustermgr.conf
   nohup ./clustermgr -f clustermgr1.conf
   nohup ./clustermgr -f clustermgr2.conf

2. Example: ``clustermgr.conf``

.. code-block:: json

   {
        "bind_addr":":9998",
        "cluster_id":1,
        "idc":["z0"],
        "chunk_size": 16777216, # set the chunk size for blobnode
        "log": {
            "level": "info",
            "filename": "./run/logs/clustermgr.log"# running log for clustermgr
         },
        "auth": {# auth config
            "enable_auth": false,
            "secret": "testsecret"
        },
        "region": "test-region",
        "db_path":"./run/db0",
        "code_mode_policies": [ # code mode
            {"mode_name":"EC3P3","min_size":0,"max_size":50331648,"size_ratio":0.2,"enable":true}
        ],
        "raft_config": { # raft cluster config
            "snapshot_patch_num": 64,
            "server_config": {
                "nodeId": 1,
                "listen_port": 10110, # Consistent with the host port of the nodeID in the member
                "raft_wal_dir": "./run/raftwal0"
            },
            "raft_node_config":{
                "flush_num_interval": 10000,
                "flush_time_interval_s": 10,
                "truncate_num_interval": 10,
                "node_protocol": "http://",
                "members": [ # raft member list
                        {"id":1, "host":"127.0.0.1:10110", "learner": false, "node_host":"127.0.0.1:9998"},
                        {"id":2, "host":"127.0.0.1:10111", "learner": false, "node_host":"127.0.0.1:9999"},
                        {"id":3, "host":"127.0.0.1:10112", "learner": false, "node_host":"127.0.0.1:10000"}]
                ]},
        "disk_mgr_config": { # disk manager config
            "refresh_interval_s": 10,
            "rack_aware":false,
            "host_aware":false
        }
   }

Proxy
:::::

1. Based on ``kafka``, need to create blob_delete_topic, shard_repair_topic, shard_repair_priority_topic corresponding topics in advance

.. code-block:: bash

    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic blob_delete shard_repair shard_repair_priority

2. Start Service

.. code-block:: bash

    # To ensure availability, each computer room ``idc`` needs to deploy at least one proxy node
    nohup ./proxy -f proxy.conf &

3. ``proxy.conf``:

.. code-block:: json

   {
      "bind_addr": ":9600",
      "host": "http://127.0.0.1:9600",
      "idc": "z0",
      "cluster_id": 1,
      "default_alloc_vols_num" : 2,
      "heartbeat_interval_s": 3,
      "clustermgr": { # clustermgr service addr
        "hosts": [
          "http://127.0.0.1:9998",
          "http://127.0.0.1:9999",
          "http://127.0.0.1:10000"
          ]
      },
      "auth": {
          "enable_auth": false,
          "secret": "test"
      },
      "mq": { # kafka config
        "blob_delete_topic": "blob_delete",
        "shard_repair_topic": "shard_repair",
        "shard_repair_priority_topic": "shard_repair_prior",
        "msg_sender": {
          "broker_list": ["127.0.0.1:9092"]
        }
      },
      "log": {
        "level": "info",
        "filename": "./run/logs/proxy.log"
      }
   }

Scheduler
:::::::::

1. Start service

.. code-block:: bash

   nohup ./scheduler -f scheduler.conf &

2. ``scheduler.conf``:

.. code-block:: json

   {
      "bind_addr": ":9800",
      "cluster_id": 1,
      "services": {
        "leader": 1,
        "node_id": 1,
        "members": {"1": "127.0.0.1:9800"}
      },
      "service_register": {
        "host": "http://127.0.0.1:9800",
        "idc": "z0"
      },
      "clustermgr": { # clustermgr addr
        "hosts": ["http://127.0.0.1:9998", "http://127.0.0.1:9999", "http://127.0.0.1:10000"]
      },
      "kafka": { # kafka service
        "broker_list": ["127.0.0.1:9092"]
      },
      "blob_delete": {
        "delete_log": {
          "dir": "./run/delete_log"
        }
      },
      "shard_repair": {
        "orphan_shard_log": {
          "dir": "./run/orphan_shard_log"
        }
      },
      "log": {
        "level": "info",
        "filename": "./run/logs/scheduler.log"
      },
      "task_log": {
        "dir": "./run/task_log"
      }
   }

Blobnode
::::::::

1. Create related directories under the compiled blobnode binary directory

.. code-block:: bash

   # This directory corresponds to the path of the configuration file
   mkdir -p ./run/disks/disk{1..6} # Each directory needs to be mounted on a disk to ensure the accuracy of data collection
   mkdir -p ./run/auditlog

2. Start Service

.. code-block:: bash

   nohup ./blobnode -f blobnode.conf

3. Example of  ``blobnode.conf``:

.. code-block:: json

   {
      "bind_addr": ":8899",
      "cluster_id": 1,
      "idc": "z0",
      "rack": "testrack",
      "host": "http://127.0.0.1:8899",
      "dropped_bid_record": {
        "dir": "./run/logs/blobnode_dropped"
      },
      "disks": [
        {
          "path": "./run/disks/disk1",
          "auto_format": true,
          "max_chunks": 1024 # Chunk size is as defined in clustermgr configuration
        },
        {
          "path": "./run/disks/disk2",
          "auto_format": true,
          "max_chunks": 1024
        },
        {
          "path": "./run/disks/disk3",
          "auto_format": true,
          "max_chunks": 1024
        },
        {
          "path": "./run/disks/disk4",
          "auto_format": true,
          "max_chunks": 1024
        },
        {
          "path": "./run/disks/disk5",
          "auto_format": true,
          "max_chunks": 1024
        },
        {
          "path": "./run/disks/disk6",
          "auto_format": true,
          "max_chunks": 1024
        },
        {
          "path": "./run/disks/disk7",
          "auto_format": true,
          "max_chunks": 1024
        },
        {
          "path": "./run/disks/disk8",
          "auto_format": true,
          "max_chunks": 1024
        }
      ],
      "clustermgr": {
        "hosts": [
          "http://127.0.0.1:9998",
          "http://127.0.0.1:9999",
          "http://127.0.0.1:10000"
        ]
      },
      "disk_config":{
        "disk_reserved_space_B":1
      },
      "log": {
        "level": "info",
        "filename": "./run/logs/blobnode.log"
      }
   }

Access
::::::

1. Start Service

.. code-block:: bash

   # The access module is a stateless single node deployment
   nohup ./access -f access.conf

2. Example of ``access.conf``:

.. code-block:: json

   {
        "bind_addr": ":9500", # service port
        "log": {
            "level": "info",
            "filename": "./run/logs/access.log"
         },
        "stream": {
            "idc": "z0",
            "cluster_config": {
                "region": "test-region",
                "clusters":[
                    {"cluster_id":1,"hosts":["http://127.0.0.1:9998","http://127.0.0.1:9999","http://127.0.0.1:10000"]}]
            }
        }
   }

Test
----

Start Cli
:::::::::

1. After starting ``cli`` on any machine in the cluster, set the access address by issuing the following command:

.. code-block:: bash

   $> cd ./blobstore
   $> ./bin/cli -c cli/cli/cli.conf # use cli with default config


Verification
::::::::::::

.. code-block:: bash

   # Upload file， response the location of the file，（-d,  the actual content of the file）
   $> access put -v -d "test -data-"
   # Response
   {"cluster_id":1,"code_mode":10,"size":11,"blob_size":8388608,"crc":2359314771,"blobs":[{"min_bid":1844899,"vid":158458,"count":1}]}

   # Download file，need the location of the file
   $> access get -v -l '{"cluster_id":1,"code_mode":10,"size":11,"blob_size":8388608,"crc":2359314771,"blobs":[{"min_bid":1844899,"vid":158458,"count":1}]}'

   # Delete file，-l represent location；Confirm manually
   $> access del -v -l '{"cluster_id":1,"code_mode":10,"size":11,"blob_size":8388608,"crc":2359314771,"blobs":[{"min_bid":1844899,"vid":158458,"count":1}]}'

Tips
----

1.  For clustermgr and blobnode deployment failures, redeployment needs to clean up residual data to avoid registration disk failure or data display errors by issuing the following command:

.. code-block:: bash

   # blobnode example
   rm -f -r ./run/disks/disk*/.*
   rm -f -r ./run/disks/disk*/*

   # clustermgr example
   rm -f -r /tmp/raft*
   rm -f -r /tmp/volume*
   rm -f -r /tmp/clustermgr*
   rm -f -r /tmp/normal*

2. After all modules are successfully deployed, upload verification needs to be delayed for a period of time, waiting for the successful volume creation.


Single Deployment
-----------------

1. physical machine
:::::::::::::::::::

blobstore supports stand-alone deployment, just run the one-click start command. When start blobstore service successfully is displayed, the deployment is successful. The specific operations are as follows:

.. code-block:: bash

    $> cd blobstore
    $> ./run.sh
    ...
    start blobstore service successfully, wait minutes for internal state preparation
    $>

2. Container
::::::::::::

blobstore support docker container too：

1. pull image on docker cloud[``recommend``]

.. code-block:: bash

    $> docker pull cubefs/cubefs:blobstore-v3.2.0 # pull
    $> docker run cubefs/cubefs:blobstore-v3.2.0 # run
    $> ./run_docker.sh -r # run
    $> docker container ls # list running containers
       CONTAINER ID        IMAGE                                  COMMAND                  CREATED             STATUS              PORTS               NAMES
       76100321156b        blobstore:v3.2.0                      "/bin/sh -c /apps/..."   4 minutes ago       Up 4 minutes                            thirsty_kare
    $> docker exec -it thirsty_kare /bin/bash # enter container

2. local build with shell script

    tips: it maybe need some time to build whole system

.. code-block:: bash

    $> cd blobstore
    $> ./run_docker.sh -b # build
    &> Successfully built 0b29fda1cd22
       Successfully tagged blobstore:v3.2.0
    $> ./run_docker.sh -r # run
    $> ... # same with step 1

cli tools in container

   tips: cli is a command manager of blobstore, can be more convenient to use after initial configuration, and get more help information via ``./bin/cli --help``

.. code-block:: bash

   $> ./bin/cli -c conf/cli.conf


Appendix
--------

1. Code Mode Policies

.. csv-table::
   :header: "Type", "Descriptions"

   "EC15P12", "{N: 15, M: 12, L: 0, AZCount: 3, PutQuorum: 24, GetQuorum: 0, MinShardSize: 2048}"
   "EC6P6", "{N: 06, M: 06, L: 0, AZCount: 3, PutQuorum: 11, GetQuorum: 0, MinShardSize: 2048}"
   "EC16P20L2", "{N: 16, M: 20, L: 2, AZCount: 2, PutQuorum: 34, GetQuorum: 0, MinShardSize: 2048}"
   "EC6P10L2", "{N: 06, M: 10, L: 2, AZCount: 2, PutQuorum: 14, GetQuorum: 0, MinShardSize: 2048}"
   "EC12P4", "{N: 12, M: 04, L: 0, AZCount: 1, PutQuorum: 15, GetQuorum: 0, MinShardSize: 2048}"
   "EC3P3", "{N: 6, M: 3, L: 3, AZCount: 3, PutQuorum: 9, GetQuorum: 0, MinShardSize: 2048}"

*Where N: the number of data blocks, M: number of check blocks,, L: Number of local check blocks, AZCount: the count of AZ,  PutQuorum: (N + M) / AZCount + N <= PutQuorum <= M + N， MinShardSize: Minimum shard size, fill data into 0-N shards continuously, if the data size is less than MinShardSize*N, it will be aligned with zero bytes*, see `details <https://github.com/cubefs/cubefs/blob/release-3.2.0/blobstore/common/codemode/codemode.go>`_ .