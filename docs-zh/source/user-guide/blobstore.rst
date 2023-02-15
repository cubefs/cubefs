BlobStore部署文档
=================


编译构建
--------

.. code-block:: bash

   $ git clone https://github.com/cubefs/cubefs.git
   $ cd cubefs/blobstore
   $ source env.sh
   $ sh build.sh

构建成功后，将在 ``bin`` 目录中生成以下可执行文件：

    1. clustermgr
    2. proxy
    3. scheduler
    4. blobnode
    5. access
    6. cli

集群部署
--------

由于模块之间具有一定的关联性，需按照以下顺序进行部署，避免服务依赖导致部署失败。

基础环境
::::::::

1. **支持平台**

    Linux

2. **依赖组件**

    `Kafka <https://kafka.apache.org/documentation/#basic_ops>`_

    `Consul <https://learn.hashicorp.com/tutorials/consul/get-started-install?in=consul/getting-started>`_ （可选，支持consul）

3. **语言环境**

    `Go <https://go.dev/>`_ (1.17.x)

启动clustermgr
::::::::::::::

部署clustermgr至少需要三个节点，以保证服务可用性。启动节点示例如下，节点启动需要更改对应配置文件，并保证集群节点之间的关联配置是一致。

1. 启动（三节点集群）

.. code-block:: bash

   nohup ./clustermgr -f clustermgr.conf
   nohup ./clustermgr -f clustermgr1.conf
   nohup ./clustermgr -f clustermgr2.conf

2. 三个节点的集群配置，示例节点一：``clustermgr.conf``

.. code-block:: json

   {
        "bind_addr":":9998",
        "cluster_id":1,
        "idc":["z0"],#集群部署模式，可以制定多个AZ
        "chunk_size": 16777216, # blobnode中对应的chunk的大小
        "log": {
            "level": "info",
            "filename": "./run/logs/clustermgr.log"#运行日志输出路径
         },
        "auth": {# 鉴权配置
            "enable_auth": false,
            "secret": "testsecret"
        },
        "region": "test-region",
        "db_path":"./run/db0",
        "code_mode_policies": [ # 编码模式
            {"mode_name":"EC3P3","min_size":0,"max_size":50331648,"size_ratio":0.2,"enable":true}
        ],
        "raft_config": { # raft 集群配置
            "server_config": {
                "nodeId": 1,
                "listen_port": 10110, #与menber中对应nodeID的host端口保持一致
                "raft_wal_dir": "./run/raftwal0"
            },
            "raft_node_config":{
                "node_protocol": "http://",
                "members": [ # raft成员列表
                        {"id":1, "host":"127.0.0.1:10110", "learner": false, "node_host":"127.0.0.1:9998"},
                        {"id":2, "host":"127.0.0.1:10111", "learner": false, "node_host":"127.0.0.1:9999"},
                        {"id":3, "host":"127.0.0.1:10112", "learner": false, "node_host":"127.0.0.1:10000"}]
                ]},
        "disk_mgr_config": { # 磁盘管理配置
            "refresh_interval_s": 10,
            "rack_aware":false,
            "host_aware":false
        }
   }

启动proxy
::::::::::::

1. ``proxy`` 依赖kafka组件，需要提前创建blob_delete_topic、shard_repair_topic、shard_repair_priority_topic对应主题

.. code-block:: bash

    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic blob_delete shard_repair shard_repair_priority


2. 启动服务

.. code-block:: bash

    # 保证可用性，每个机房 ``idc`` 至少需要部署一个proxy节点
    nohup ./proxy -f proxy.conf &

3. 示例 ``proxy.conf``:

.. code-block:: json

   {
      "bind_addr": ":9600",
      "host": "http://127.0.0.1:9600",
      "idc": "z0",
      "cluster_id": 1,
      "clustermgr": { # clustermgr 服务地址
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
      "mq": { # kafka配置
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

启动scheduler
:::::::::::::

1. 启动服务

.. code-block:: bash

   nohup ./scheduler -f scheduler.conf &

2. 示例 ``scheduler.conf``: 注意scheduler模块单节点部署

.. code-block:: json

   {
      "bind_addr": ":9800",
      "cluster_id": 1,
      "services": { # scheduler服务
        "leader": 1,
        "node_id": 1,
        "members": {"1": "127.0.0.1:9800"}
      },
      "service_register": {
        "host": "http://127.0.0.1:9800",
        "idc": "z0"
      },
      "clustermgr": { # clustermgr服务地址
        "hosts": ["http://127.0.0.1:9998", "http://127.0.0.1:9999", "http://127.0.0.1:10000"]
      },
      "kafka": { # kafka服务
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

启动blobnode
:::::::::::

1. 在编译好的 ``blobnode`` 二进制目录下**创建相关目录**

.. code-block:: bash

    # 该目录对应配置文件的路径
    mkdir -p ./run/disks/disk{1..8} # 每个目录需要挂载磁盘，保证数据收集准确性

2. 启动服务

.. code-block:: bash

   nohup ./blobnode -f blobnode.conf

3. 示例 ``blobnode.conf``:

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
          "max_chunks": 1024 # chunk大小以clustermgr配置中的定义为准
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

启动access
::::::::::

1. 启动服务

.. code-block:: bash

   # access模块为无状态单节点部署
   nohup ./access -f access.conf

2. 示例 ``access.conf``:

.. code-block:: json

   {
        "bind_addr": ":9500", # 服务端口
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

配置说明
:::::::::

1. clustermgr
    1) code_mode_policies(编码模式策略)
    示例:

    .. code-block:: json

        {
           "code_mode" : "EC3P3" # 具体策略方案，详见附录
           "min_size" : 0 # 最小上传对象大小为0
           "max_size" : 1024 # 最大上传对象大小为1024
           "size_ratio" : 1 # 不同策略的存储空间比列
           "enable" : true # 是否启用这个策略,ture代表启用，false不启用
        }


集群验证
--------

启动CLI
:::::::

在集群中任一台机器启动命令行工具 ``cli`` 后，设置access访问地址即可。

.. code-block:: bash

   $> cd ./blobstore
   $>./bin/cli -c cli/cli/cli.conf # 采用默认配置启动cli 工具进入命令行

验证
::::

.. code-block:: bash

   # 上传文件，成功后会返回一个location，（-d 参数为文件实际内容）
   $> access put -v -d "test -data-"
   # 返回结果
   #"code_mode":11是clustermgr配置文件中制定的编码模式，11就是EC3P3编码模式
   {"cluster_id":1,"code_mode":10,"size":11,"blob_size":8388608,"crc":2359314771,"blobs":[{"min_bid":1844899,"vid":158458,"count":1}]}

   # 下载文件，用上述得到的location作为参数（-l），即可下载文件内容
   $> access get -v -l '{"cluster_id":1,"code_mode":10,"size":11,"blob_size":8388608,"crc":2359314771,"blobs":[{"min_bid":1844899,"vid":158458,"count":1}]}'

   # 删除文件，用上述location作为参数（-l）；删除文件需要手动确认
   $> access del -v -l '{"cluster_id":1,"code_mode":10,"size":11,"blob_size":8388608,"crc":2359314771,"blobs":[{"min_bid":1844899,"vid":158458,"count":1}]}'


部署提示
--------

1. 对于clustermgr和blobnode部署失败后，重新部署需清理残留数据，避免注册盘失败或者数据显示错误，命令如下：

.. code-block:: bash

   # blobnode示例
   rm -f -r ./run/disks/disk*/.*
   rm -f -r ./run/disks/disk*/*

   # clustermgr示例
   rm -f -r /tmp/raftdb0
   rm -f -r /tmp/volumedb0
   rm -f -r /tmp/clustermgr
   rm -f -r /tmp/normaldb0
   rm -f -r /tmp/normalwal0

2. 所有模块部署成功后，上传验证需要延缓一段时间，等待创建卷成功。

单机部署
------

一、物理机部署
:::::::::::

blobstore支持单机部署，运行一键启动命令即可，当显示有start blobstore service successfully便表示部署成功，具体操作如下：

.. code-block:: bash

    $> cd blobstore
    $> ./run.sh
    ...
    start blobstore service successfully, wait minutes for internal state preparation
    $>

二、容器部署
:::::::::::

blobstore支持以下docker镜像部署方式：

1. 远端拉取构建【``推荐``】

.. code-block:: bash

    $> docker pull cubefs/cubefs:blobstore-v3.2.0 # 拉取镜像
    $> docker run cubefs/cubefs:blobstore-v3.2.0 # 运行镜像
    $> docker container ls # 查看运行中的容器
       CONTAINER ID        IMAGE                                  COMMAND                  CREATED             STATUS              PORTS               NAMES
       76100321156b        blobstore:v3.2.0                       "/bin/sh -c /apps/..."   4 minutes ago       Up 4 minutes                            thirsty_kare
    $> docker exec -it thirsty_kare /bin/bash # 进入容器


2. 本地脚本编译构建

  小提示：整个初始编译过程可能需要些时间

.. code-block:: bash

    $> cd blobstore
    $> ./run_docker.sh -b # 编译构建
    &> Successfully built 0b29fda1cd22
       Successfully tagged blobstore:v3.2.0
    $> ./run_docker.sh -r # 运行镜像
    $> ... # 后续步骤同1

cli工具使用

   小提示: cli 是为 blobstore 提供的交互式命令行管理工具, 配置 cli 后能够更方便地使用, 用 help 可以查看帮助信息

.. code-block:: bash

   $> ./bin/cli -c conf/cli.conf


附录
-----

1. 编码策略

.. csv-table:: 常用策略表
   :header: "类别", "描述"

   "EC15P12", "{N: 15, M: 12, L: 0, AZCount: 3, PutQuorum: 24, GetQuorum: 0, MinShardSize: 2048}"
   "EC6P6", "{N: 06, M: 06, L: 0, AZCount: 3, PutQuorum: 11, GetQuorum: 0, MinShardSize: 2048}"
   "EC16P20L2", "{N: 16, M: 20, L: 2, AZCount: 2, PutQuorum: 34, GetQuorum: 0, MinShardSize: 2048}"
   "EC6P10L2", "{N: 06, M: 10, L: 2, AZCount: 2, PutQuorum: 14, GetQuorum: 0, MinShardSize: 2048}"
   "EC12P4", "{N: 12, M: 04, L: 0, AZCount: 1, PutQuorum: 15, GetQuorum: 0, MinShardSize: 2048}"
   "EC3P3", "{N: 6, M: 3, L: 3, AZCount: 3, PutQuorum: 9, GetQuorum: 0, MinShardSize: 2048}"

*其中N: 数据块数量, M: 校验块数量, L: 本地校验块数量, AZCount: AZ数量,  PutQuorum: (N + M) / AZCount + N <= PutQuorum <= M + N， MinShardSize: 最小shard大小,将数据连续填充到 0-N 分片中，如果数据大小小于 MinShardSize*N，则与零字节对齐*，详见
`代码 <https://github.com/cubefs/cubefs/blob/release-3.2.0/blobstore/common/codemode/codemode.go>`_
。