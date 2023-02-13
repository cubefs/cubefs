元数据节点
=================

元数据节点是由多个元数据分片(meta partition)和基于multiRaft的对应个数的raft实例组成；
每个元数据分片(meta partition)都是一个inode范围，且包含两个内存BTrees： inode BTree
和dentry BTree。注意：MetaNode的实例最少需要3个

.. csv-table:: Properties
   :header: "配置项", "类型", "描述", "是否必需"

   "role", "字符串", "进程角色： *MetaNode*", "是"
   "listen", "字符串", "监听和接受请求的端口", "是"
   "prof", "字符串", "调试和管理员API接口", "是"
   "logLevel", "字符串", "日志级别，默认: *error*", "否"
   "metadataDir", "字符串", "元数据快照存储目录", "是"
   "logDir", "字符串", "日志存储目录", "是"
   "raftDir", "字符串", "raft wal日志目录",  "是"
   "raftHeartbeatPort", "字符串", "raft心跳通信端口", "是"
   "raftReplicaPort", "字符串", "raft数据传输端口", "是"
   "consulAddr", "字符串", "prometheus注册接口", "否"
   "exporterPort", "字符串", "prometheus获取监控数据端口", "否"
   "masterAddr", "字符串", "master服务地址", "是"
   "totalMem","字符串","最大可用内存，此值需高于master配置中metaNodeReservedMem的值，单位：字节","是"
   "localIP","字符串","本机ip地址","否，如果不填写该选项，则使用和master通信的ip地址"
   "zoneName", "字符串", "指定区域", "否，默认分配至``default``区域"
   "deleteBatchCount","int64","一次性批量删除多少inode节点，默认``500``","否"



样例:

.. code-block:: json

   {
        "role": "metanode",
        "listen": "17210",
        "prof": "17220",
        "logLevel": "debug",
        "localIP":"10.196.59.202",
        "metadataDir": "/cfs/metanode/data/meta",
        "logDir": "/cfs/metanode/log",
        "raftDir": "/cfs/metanode/data/raft",
        "raftHeartbeatPort": "17230",
        "raftReplicaPort": "17240",
        "consulAddr": "http://consul.prometheus-cfs.local",
        "exporterPort": 9501,
        "totalMem":  "8589934592",
        "masterAddr": [
            "10.196.59.198:17010",
            "10.196.59.199:17010",
            "10.196.59.200:17010"
        ]
    }

启动服务
-------------

.. code-block:: bash

   nohup ./cfs-server -c metanode.json > nohup.out &


注意事项
-------------

  * listen、raftHeartbeatPort、raftReplicaPort这三个配置选项在程序首次配置启动后，不能修改；
  * 相关的配置信息被记录在metadataDir目录下的constcfg文件中，如果需要强制修改，需要手动删除该文件；
  * 上述三个配置选项和MetaNode在master的注册信息有关。如果修改，将导致master无法定位到修改前的metanode信息；

