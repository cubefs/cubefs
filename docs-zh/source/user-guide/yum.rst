自动部署集群
==================

可以使用yum工具在CentOS 7+操作系统中快速部署和启动CubeFS集群. 该工具的rpm依赖项可通过以下命令安装:

.. code-block:: bash

    $ yum install https://ocs-cn-north1.heytapcs.com/cubefs/rpm/3.2.0/cfs-install-3.2.0-el7.x86_64.rpm
    $ cd /cfs/install
    $ tree -L 3
     .
     ├── install_cfs.yml
     ├── install.sh
     ├── iplist
     ├── src
     └── template
         ├── client.json.j2
         ├── create_vol.sh.j2
         ├── datanode.json.j2
         ├── grafana
         │   ├── grafana.ini
         │   ├── init.sh
         │   └── provisioning
         ├── master.json.j2
         ├── metanode.json.j2
         └── objectnode.json.j2

可根据实际环境，在 **iplist** 文件中修改CubeFS集群的参数.

- **[master]** , **[datanode]** , **[metanode]** , **[objectnode]**, **[monitor]** , **[client]** 包含了每个模块的成员IP地址。

- **[cfs:vars]** 模块定义了所有节点的ssh登陆信息，需要事先将集群中所有节点的登录名和密码进行统一。

- **#master config** 模块定义了每个Master节点的启动参数。

.. csv-table::
   :header: "参数", "类型", "描述", "是否必需"

   "master_clusterName", "字符串", "集群名字", "是"
   "master_listen", "字符串", "http服务监听的端口号", "是"
   "master_prof", "字符串", "golang pprof 端口号", "是"
   "master_logDir", "字符串", "日志文件存储目录", "是"
   "master_logLevel", "字符串", "日志级别, 默认为 *info*", "否"
   "master_retainLogs", "字符串", "保留多少条raft日志", "是"
   "master_walDir", "字符串", "raft wal日志存储目录", "是"
   "master_storeDir", "字符串", "RocksDB数据存储目录.此目录必须存在，如果目录不存在，无法启动服务", "是"
   "master_exporterPort", "整型", "prometheus获取监控数据端口", "否"
   "master_metaNodeReservedMem","字符串","元数据节点预留内存大小，如果剩余内存小于该值，MetaNode变为只读。单位：字节， 默认值：1073741824", "否"

- **#datanode config** 模块定义了每个DataNode的启动参数。

.. csv-table::
   :header: "参数", "类型", "描述", "是否必需"

   "datanode_listen", "字符串", "数据节点作为服务端启动TCP监听的端口", "是"
   "datanode_prof", "字符串", "数据节点提供HTTP接口所用的端口", "是"
   "datanode_logDir", "字符串", "日志存放的路径", "是"
   "datanode_logLevel", "字符串", "日志的级别。默认是 *info*", "否"
   "datanode_raftHeartbeat", "字符串", "RAFT发送节点间心跳消息所用的端口", "是"
   "datanode_raftReplica", "字符串", "RAFT发送日志消息所用的端口", "是"
   "datanode_raftDir", "字符串", "RAFT调测日志存放的路径。默认在二进制文件启动路径", "否"
   "datanode_exporterPort", "字符串", "监控系统采集的端口", "否"
   "datanode_disks", "字符串数组", "
   | 格式: *PATH:RETAIN*.
   | PATH: 磁盘挂载路径.
   | RETAIN: 该路径下的最小预留空间，剩余空间小于该值即认为磁盘已满，单位：字节。（建议值：20G~50G)", "是"

- **#metanode config** 模块定义了MetaNode的启动参数。

.. csv-table::
   :header: "参数", "类型", "描述", "是否必需"

   "metanode_listen", "字符串", "监听和接受请求的端口", "是"
   "metanode_prof", "字符串", "调试和管理员API接口", "是"
   "metanode_logLevel", "字符串", "日志级别，默认: *info*", "否"
   "metanode_metadataDir", "字符串", "元数据快照存储目录", "是"
   "metanode_logDir", "字符串", "日志存储目录", "是",
   "metanode_raftDir", "字符串", "raft wal日志目录", "是",
   "metanode_raftHeartbeatPort", "字符串", "raft心跳通信端口", "是"
   "metanode_raftReplicaPort", "字符串", "raft数据传输端口", "是"
   "metanode_exporterPort", "字符串", "prometheus获取监控数据端口", "否"
   "metanode_totalMem","字符串", "最大可用内存，此值需高于master配置中metaNodeReservedMem的值，单位：字节", "是"

- **#objectnode config** 模块定义了ObjectNode的启动参数。

.. csv-table::
   :header: "参数", "类型", "描述", "是否必需"

   "objectnode_listen", "字符串", "http服务监听的IP地址和端口号", "是"
   "objectnode_domains", "字符串数组", "
   | 为S3兼容接口配置域名以支持DNS风格访问资源
   | 格式: ``DOMAIN``", "否"
   "objectnode_logDir", "字符串", "日志存放路径", "是"
   "objectnode_logLevel", "字符串", "
   | 日志级别.
   | 默认: ``error``", "否"
   "objectnode_exporterPort", "字符串", "prometheus获取监控数据端口", "No"
   "objectnode_enableHTTPS", "字符串", "是否支持 HTTPS协议", "Yes"

- **#client config** 模块定义了fuse客户端的启动参数

.. csv-table::
   :header: "参数", "类型", "描述", "是否必需"

   "client_mountPoint", "字符串", "挂载点", "是"
   "client_volName", "字符串", "卷名称", "否"
   "client_owner", "字符串", "卷所有者", "是"
   "client_SizeGB", "字符串", "如果卷不存在，则会创建一个该大小的卷，单位：GB", "否"
   "client_logDir", "字符串", "日志存放路径", "是"
   "client_logLevel", "字符串", "日志级别：*debug*, *info*, *warn*, *error*，默认 *info*", "否"
   "client_exporterPort", "字符串", "prometheus获取监控数据端口", "是"
   "client_profPort", "字符串", "golang pprof调试端口", "否"

.. code-block:: yaml

    [master]
    10.196.59.198
    10.196.59.199
    10.196.59.200
    [datanode]
    ...
    [cfs:vars]
    ansible_ssh_port=22
    ansible_ssh_user=root
    ansible_ssh_pass="password"
    ...
    #master config
    ...
    #datanode config
    ...
    datanode_disks =  '"/data0:10737418240","/data1:10737418240"'
    ...
    #metanode config
    ...
    metanode_totalMem = "28589934592"
    ...
    #objectnode config
    ...

更多配置介绍请参考 :doc:`master`; :doc:`metanode`; :doc:`datanode`; :doc:`objectnode`; :doc:`client`; :doc:`monitor`.。
CubeFS支持混部。如果采取混部的方式，注意修改各个模块的端口配置**避免端口冲突**。

用 **install.sh** 脚本启动CubeFS集群，并确保首先启动Master。

.. code-block:: bash

    $ bash install.sh -h
    Usage: install.sh -r | --role [datanode | metanode | master | objectnode | monitor | client | all | createvol ]
    $ bash install.sh -r master
    $ bash install.sh -r metanode
    $ bash install.sh -r datanode
    $ bash install.sh -r objectnode
    $ bash install.sh -r monitor
    $ bash install.sh -r createvol
    $ bash install.sh -r client

全部角色启动后，可以登录到 **client** 角色所在节点验证挂载点 **/cfs/mountpoint** 是否已经挂载CubeFS文件系统。

在浏览器中打开链接http://consul.prometheus-cfs.local 查看监控系统(监控系统的IP地址已在 **iplist** 文件的 **[monitor]** 模块定义).
