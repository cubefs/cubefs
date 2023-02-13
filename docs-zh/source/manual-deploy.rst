手动部署集群
=================

编译依赖项
--------

.. csv-table::
   :header: "依赖项", "版本要求"

   "gcc-c++","4.8.5及以上"
   "CMake","3.1及以上"
   "Go","1.16及以上"
   "bzip2-devel","1.0.6及以上"
   "mvn","3.8.4及以上"


编译构建
--------

使用如下命令同时构建server，client及相关的依赖：

.. code-block:: bash

   $ git clone http://github.com/cubeFS/cubefs.git
   $ cd cubefs
   $ make build

如果构建成功，将在`build/bin` 目录中生成可执行文件`cfs-server`和`cfs-client`。

集群部署
----------

启动资源管理节点
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   ./cfs-server -c master.json


示例 ``master.json`` ：注意：master服务最少应该启动3个节点实例

.. code-block:: json

   {
     "role": "master",
     "ip": "10.196.59.198",
     "listen": "17010",
     "prof":"17020",
     "id":"1",
     "peers": 1:10.196.59.198:17010,2:10.196.59.199:17010,3:10.196.59.200:17010",
     "retainLogs":"20000",
     "logDir": "/cfs/master/log",
     "logLevel":"info",
     "walDir":"/cfs/master/data/wal",
     "storeDir":"/cfs/master/data/store",
     "consulAddr": "http://consul.prometheus-cfs.local",
     "clusterName":"cubefs01",
     "metaNodeReservedMem": "1073741824"
   }


详细配置参数请参考 :doc:`user-guide/master` 。

启动元数据节点
^^^^^^^^^^^^^^^^^^^^^
.. code-block:: bash


   ./cfs-server -c metanode.json

示例 ``meta.json`` ：注意：metanode服务最少应该启动3个节点实例

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


详细配置参数请参考 :doc:`user-guide/metanode`.

启动纠删码子系统
^^^^^^^^^^^^^^^^^^^^^

部署参考 :doc:`user-guide/blobstore` 。

启动 ObjectNode
^^^^^^^^^^^^^^^^

.. code-block:: bash

   ./cfs-server -c objectnode.json

示例 ``objectnode.json`` 内容如下

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


配置文件的详细信息 *objectnode.json*, 请参阅 :doc:`user-guide/objectnode`.

启动数据节点
^^^^^^^^^^^^^^

1. 准备数据目录

   **推荐** 使用单独磁盘作为数据目录，配置多块磁盘能够达到更高的性能。

   **磁盘准备**

    1.1 查看机器磁盘信息，选择给CubeFS使用的磁盘

        .. code-block:: bash

           fdisk -l

    1.2 格式化磁盘，建议格式化为XFS

        .. code-block:: bash

           mkfs.xfs -f /dev/sdx

    1.3 创建挂载目录

        .. code-block:: bash

           mkdir /data0

    1.4 挂载磁盘

        .. code-block:: bash

           mount /dev/sdx /data0

2. 启动数据节点

   .. code-block:: bash

      ./cfs-server -c datanode.json

   示例 ``datanode.json`` :注意：datanode服务最少应该启动4个节点实例

   .. code-block:: json

      {
        "role": "datanode",
        "listen": "17310",
        "prof": "17320",
        "logDir": "/cfs/datanode/log",
        "logLevel": "info",
        "raftHeartbeat": "17330",
        "raftReplica": "17340",
        "raftDir":"/cfs/datanode/log",
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

详细配置参数请参考 :doc:`user-guide/datanode`.


创建Volume卷
------------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/admin/createVol?name=ltptest&capacity=10000&owner=ltptest"

   如果执行性能测试，请调用相应的API，创建足够多的数据分片（data partition）,如果集群中有8块磁盘，那么需要创建80个datapartition
卷创建详细参数请参考 :doc:`admin-api/master/volume`.

挂载客户端
------------

1. 运行 ``modprobe fuse`` 插入FUSE内核模块。
2. 运行 ``yum install -y fuse`` 安装libfuse。
3. 运行 ``client -c fuse.json`` 启动客户端。

   样例 *fuse.json* ,

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


详细配置参数请参考 :doc:`user-guide/client`.

用户可以使用不同的挂载点在同一台机器上同时启动多个客户端

升级注意事项
---------------
集群数据节点和元数据节点升级前，请先禁止集群自动为卷扩容数据分片.

1. 冻结集群

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/cluster/freeze?enable=true"

2. 升级节点

3. 开启自动扩容数据分片

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/cluster/freeze?enable=false"

*注：升级节点时不能修改各节点配置文件的端口。*
