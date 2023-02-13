客户端
======

环境依赖
------------

- 插入内核FUSE模
- 安装libfuse

.. code-block:: bash

   modprobe fuse
   yum install -y fuse

配置文件
-------------------

fuse.json

.. code-block:: json

   {
     "mountPoint": "/cfs/mountpoint",
     "volName": "ltptest",
     "owner": "ltptest",
     "masterAddr": "10.196.59.198:17010,10.196.59.199:17010,10.196.59.200:17010",
     "logDir": "/cfs/client/log",
     "logLevel": "info",
     "profPort": "27510"
   }

.. csv-table:: 配置选项
   :header: "名称", "类型", "描述", "必需"

    "mountPoint", "string", "挂载点", "是"
    "volName", "string", "卷名称", "是"
    "owner", "string", "所有者", "是"
    "masterAddr", "string", "Master节点地址", "是"
    "logDir", "string", "日志存放路径", "否"
    "logLevel", "string", "日志级别：debug, info, warn, error", "否"
    "profPort", "string", "golang pprof调试端口", "否"
    "exporterPort", "string", "prometheus获取监控数据端口", "否"
    "consulAddr", "string", "监控注册服务器地址", "否"
    "lookupValid", "string", "内核FUSE lookup有效期，单位：秒", "否"
    "attrValid", "string", "内核FUSE attribute有效期，单位：秒", "否"
    "icacheTimeout", "string", "客户端inode cache有效期，单位：秒", "否"
    "enSyncWrite", "string", "使能DirectIO同步写，即DirectIO强制数据节点落盘", "否"
    "autoInvalData", "string", "FUSE挂载使用AutoInvalData选项", "否"
    "rdonly", "bool", "以只读方式挂载，默认为false", "否"
    "writecache", "bool", "利用内核FUSE的写缓存功能，需要内核FUSE模块支持写缓存，默认为false", "否"
    "keepcache", "bool", "保留内核页面缓存。此功能需要启用writecache选项，默认为false", "否"
    "token", "string", "如果创建卷时开启了enableToken，此参数填写对应权限的token", "否"
    "readRate", "int", "限制每秒读取次数，默认无限制", "否"
    "writeRate", "int", "限制每秒写入次数，默认无限制", "否"
    "followerRead", "bool", "从follower中读取数据，默认为false", "否"
    "accessKey", "string", "卷所属用户的鉴权密钥", "否"
    "secretKey", "string", "卷所属用户的鉴权密钥", "否"
    "disableDcache", "bool", "禁用Dentry缓存，默认为false", "否"
    "subdir", "string", "设置子目录挂载", "否"
    "fsyncOnClose", "bool", "文件关闭后执行fsync操作，默认为true", "否"
    "maxcpus", "int", "最大可使用的cpu核数，可限制client进程cpu使用率", "否"
    "enableXattr", "bool", "是否使用*xattr*，默认是false", "否"
    "enableBcache", "bool", "是否开启本地一级缓存，默认false", "否"

挂载
---------------

执行如下命令挂载客户端：

.. code-block:: bash

   ./cfs-client -c fuse.json

如果使用示例的``fuse.json``，则客户端被挂载到``/mnt/fuse``。所有针对``/mnt/fuse``的操作都将被作用于CubeFS。

卸载
-------------

建议使用标准的Linux ``umount`` 命令终止挂载。

预热
-------------
执行如下命令预热文件或者目录：

.. code-block:: bash

   ./cfs-preload -c preload.json 

.. code-block:: json

   {
      "target":"/", 
      "volumeName": "cold4",
      "masterAddr": "10.177.69.105:17010,10.177.69.106:17010,10.177.117.108:17010",
      "logDir": "/mnt/hgfs/share/cfs-client-test",
      "logLevel": "debug",
      "ttl": "100",
      "replicaNum": "1",
      "zones": "",
      "action":"clear",
      "traverseDirConcurrency":"4",
      "preloadFileConcurrency":"10",
      "preloadFileSizeLimit":"10737418240",
      "readBlockConcurrency":"10"
      "prof":"27520"
   }

.. csv-table:: 配置选项
   :header: "名称", "类型", "描述", "必需"

    "target", "string", "要预热的目录或者文件", "是"
    "volName", "string", "卷名称", "是"
    "masterAddr", "string", "Master节点地址", "是"
    "logDir", "string", "日志存放路径", "是"
    "logLevel", "string", "日志级别：debug, info, warn, error", "是"
    "ttl", "string", "预热缓存生存期", "是"
    "action", "string", "预热行为:clear-清理预热缓存;preload-预热数据", "是"
    "replicaNum", "string", "预热数据副本数(1-16)", "否"
    "zones", "string", "预热副本所在zone", "否"
    "traverseDirConcurrency", "string", "遍历目录的任务并发数", "否"
    "preloadFileConcurrency", "string", "预热文件的并发数", "否"
    "preloadFileSizeLimit", "string", "预热文件的阈值，文件大小低于该阈值的文件才可以被预热", "否"
    "readBlockConcurrency", "string", "从纠删卷读取数据的并发数", "否"
    "prof", "string", "golang pprof调试端口", "否"

一级缓存
-------------

部署在用户客户端的本地读cache服务，对于数据集有修改写，需要强一致的场景不建议使用。
部署缓存后，客户端需要增加以下挂载参数，重新挂载后缓存才能生效。

.. code-block:: json
   {
       "maxStreamerLimit":"10000000",
       "bcacheDir":"/home/service/mnt"
   }

.. csv-table:: 客户端缓存配置参数
   :header: "名称", "类型", "描述", "必需"

    "maxStreamerLimit", "string", "文件元数据缓存数目", "是"
    "bcacheDir", "string", "需要开启读缓存的目录路径", "是"

启动命令
^^^^^^^^^^^

.. code-block:: bash

   ./cfs-bcache -c bcache.json

配置文件
^^^^^^^^^^^

.. code-block:: json

   {
      "cacheDir":"/home/service/var:1099511627776",
      "logDir":"/home/service/var/logs/cachelog",
      "logLevel":"warn"
   }

.. csv-table:: 缓存服务配置选项
   :header: "名称", "类型", "描述", "必需"

    "cacheDir", "string", "缓存数据的本地存储路径:分配空间（单位Byte)", "是"
    "logDir", "string", "日志路径", "是"
    "logLevel", "string", "日志级别", "是"
