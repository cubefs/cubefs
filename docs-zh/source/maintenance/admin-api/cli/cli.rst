Cli工具使用
====================

使用命令行界面工具（CLI）可以实现方便快捷的集群管理。利用此工具，可以查看集群及各节点的状态，并进行各节点、卷及用户的管理。

随着CLI的不断完善，最终将会实现对于集群各节点接口功能的100%覆盖。

编译及配置
----------

下载CubeFS源码后，在 ``cubefs/cli`` 目录下，运行 ``build.sh`` 文件 ，即可生成 ``cfs-cli`` 可执行程序。

同时，在 ``root`` 目录下会生成名为 ``.cfs-cli.json`` 的配置文件，修改master地址为当前集群的master地址即可。也可使用命令 ``./cfs-cli config info`` 和 ``./cfs-cli config set`` 来查看和设置配置文件。

使用方法
---------

在 ``cubefs/cli`` 目录下，执行命令 ``./cfs-cli --help`` 或 ``./cfs-cli -h`` ，可获取CLI的帮助文档。

CLI主要分为六类管理命令：

.. csv-table::
   :header: "命令", "描述"

   "cfs-cli cluster", "集群管理"
   "cfs-cli metanode", "元数据节点管理"
   "cfs-cli datanode", "数据节点管理"
   "cfs-cli datapartition", "数据分片管理"
   "cfs-cli metapartition", "元数据分片管理"
   "cfs-cli config", "配置管理"
   "cfs-cli completion", "生成自动补全命令脚本"
   "cfs-cli volume, vol", "卷管理"
   "cfs-cli user", "用户管理"

集群管理命令
>>>>>>>>>>>>>

.. code-block:: bash

    ./cfs-cli cluster info          #获取集群信息，包括集群名称、地址、卷数量、节点数量及使用率等

.. code-block:: bash

    ./cfs-cli cluster stat          #按区域获取元数据和数据节点的使用量、状态等

.. code-block:: bash

    ./cfs-cli cluster freeze [true/false]        #是否冻结集群，设置为 `true` 冻结后，当partition写满，集群不会自动分配新的partition

.. code-block:: bash

    ./cfs-cli cluster threshold [float]     #设置集群中每个MetaNode的内存阈值

.. code-block:: bash

    ./cli cluster cluster set [flags]    #设置集群的参数.

元数据节点管理命令
>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cfs-cli metanode list         #获取所有元数据节点的信息，包括id、地址、读写状态及存活状态

.. code-block:: bash

    ./cfs-cli metanode info [Address]     #展示元数据节点基本信息，包括状态、使用量、承载的partition ID等，

.. code-block:: bash

    ./cfs-cli metanode decommission [Address] #将该元数据节点下线，该节点上的partition将自动转移至其他可用节点

.. code-block:: bash

    ./cfs-cli metanode migrate [srcAddress] [dstAddress] #将源元数据节点上的meta partition转移至目标元数据节点

数据节点管理命令
>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cfs-cli datanode list         #获取所有数据节点的信息，包括id、地址、读写状态及存活状态

.. code-block:: bash

    ./cfs-cli datanode info [Address]     #展示数据节点基本信息，包括状态、使用量、承载的partition ID等，

.. code-block:: bash

    ./cfs-cli datanode decommission [Address] #将该数据节点下线，该节点上的data partition将自动转移至其他可用节点

.. code-block:: bash

    ./cfs-cli datanode migrate [srcAddress] [dstAddress] #将源数据节点上的data partition转移至目标数据节点


数据分片管理命令
>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cfs-cli datapartition info [Partition ID]        #获取指定data partition的信息

.. code-block:: bash

    ./cli datapartition decommission [Address] [Partition ID]   #将目标节点上的指定data partition分片下线，并自动转移至其他可用节点

.. code-block:: bash

    ./cfs-cli datapartition add-replica [Address] [Partition ID]    #在目标节点新增一个data partition分片

.. code-block:: bash

    ./cfs-cli datapartition del-replica [Address] [Partition ID]    #删除目标节点上的data partition分片

.. code-block:: bash

    ./cfs-cli datapartition check    #故障诊断，查找多半分片不可用和分片缺失的data partition


元数据分片管理命令
>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cfs-cli metapartition info [Partition ID]        #获取指定meta partition的信息

.. code-block:: bash

    ./cli metapartition decommission [Address] [Partition ID]   #将目标节点上的指定meta partition分片下线，并自动转移至其他可用节点

.. code-block:: bash

    ./cfs-cli metapartition add-replica [Address] [Partition ID]    #在目标节点新增一个meta partition分片

.. code-block:: bash

    ./cfs-cli metapartition del-replica [Address] [Partition ID]    #删除目标节点上的meta partition分片

.. code-block:: bash

    ./cfs-cli metapartition check    #故障诊断，查找多半分片不可用和分片缺失的meta partition

配置管理
>>>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cfs-cli config info     #展示配置信息

.. code-block:: bash

    ./cfs-cli config set [flags] #设置配置信息

    Flags:
        --addr string      Specify master address [{HOST}:{PORT}]
    -h, --help             help for set
        --timeout uint16   Specify timeout for requests [Unit: s]


自动补全管理
>>>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cfs-cli completion      #生成命令自动补全脚本

卷管理命令
>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cfs-cli volume create [VOLUME NAME] [USER ID] [flags]

    Flags:
         --cache-action int          Specify low volume cacheAction (default 0)
         --cache-capacity int        Specify low volume capacity[Unit: GB]
         --cache-high-water int       (default 80)
         --cache-low-water int        (default 60)
         --cache-lru-interval int    Specify interval expiration time[Unit: min] (default 5)
         --cache-rule-key string     Anything that match this field will be written to the cache
         --cache-threshold int       Specify cache threshold[Unit: byte] (default 10485760)
         --cache-ttl int             Specify cache expiration time[Unit: day] (default 30)
         --capacity uint             Specify volume capacity (default 10)
         --crossZone string          Disable cross zone (default "false")
         --description string        Description
         --ebs-blk-size int          Specify ebsBlk Size[Unit: byte] (default 8388608)
         --follower-read string      Enable read form replica follower (default "true")
     -h, --help                      help for create
         --mp-count int              Specify init meta partition count (default 3)
         --normalZonesFirst string   Write to normal zone first (default "false")
         --replica-num string        Specify data partition replicas number(default 3 for normal volume,1 for low volume)
         --size int                  Specify data partition size[Unit: GB] (default 120)
         --vol-type int              Type of volume (default 0)
     -y, --yes                       Answer yes for all questions
         --zone-name string          Specify volume zone name



.. code-block:: bash

    ./cfs-cli volume delete [VOLUME NAME] [flags]               #删除指定卷[VOLUME NAME], ec卷大小为0才能删除
    Flags:
        -y, --yes                                           #跳过所有问题并设置回答为"yes"

.. code-block:: bash

    ./cfs-cli volume info [VOLUME NAME] [flags]                 #获取卷[VOLUME NAME]的信息
    Flags:
        -d, --data-partition                                #显示数据分片的详细信息
        -m, --meta-partition                                #显示元数据分片的详细信息

.. code-block:: bash

    ./cfs-cli volume add-dp [VOLUME] [NUMBER]                   #创建并添加个数为[NUMBER]的数据分片至卷[VOLUME]

.. code-block:: bash

    ./cfs-cli volume list                                       #获取包含当前所有卷信息的列表

.. code-block:: bash

    ./cfs-cli volume transfer [VOLUME NAME] [USER ID] [flags]   #将卷[VOLUME NAME]转交给其他用户[USER ID]
    Flags：
        -f, --force                                         #强制转交
        -y, --yes                                           #跳过所有问题并设置回答为"yes"

.. code-block:: bash

    ./cli volume update                                     #更新集群的参数
    Flags:
        --cache-action string      Specify low volume cacheAction (default 0)
        --cache-capacity string    Specify low volume capacity[Unit: GB]
        --cache-high-water int      (default 80)
        --cache-low-water int       (default 60)
        --cache-lru-interval int   Specify interval expiration time[Unit: min] (default 5)
        --cache-rule string        Specify cache rule
        --cache-threshold int      Specify cache threshold[Unit: byte] (default 10M)
        --cache-ttl int            Specify cache expiration time[Unit: day] (default 30)
        --capacity uint            Specify volume datanode capacity [Unit: GB]
        --description string       The description of volume
        --ebs-blk-size int         Specify ebsBlk Size[Unit: byte]
        --follower-read string     Enable read form replica follower (default false)
        -y, --yes               Answer yes for all questions
        --zonename string   Specify volume zone name

用户管理命令
>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cfs-cli user create [USER ID] [flags]         #创建用户[USER ID]
    Flags：
        --access-key string                     #指定用户用于对象存储功能的access key
        --secret-key string                     #指定用户用于对象存储功能的secret key
        --password string                       #指定用户密码
        --user-type string                      #指定用户类型，可选项为normal或admin（默认为normal）
        -y, --yes                               #跳过所有问题并设置回答为"yes"

.. code-block:: bash

    ./cfs-cli user delete [USER ID] [flags]         #删除用户[USER ID]
    Flags：
        -y, --yes                               #跳过所有问题并设置回答为"yes"

.. code-block:: bash

    ./cfs-cli user info [USER ID]                   #获取用户[USER ID]的信息

.. code-block:: bash

    ./cfs-cli user list                             #获取包含当前所有用户信息的列表

.. code-block:: bash

    ./cfs-cli user perm [USER ID] [VOLUME] [PERM]   #更新用户[USER ID]对于卷[VOLUME]的权限[PERM]
                                                #[PERM]可选项为"只读"（READONLY/RO）、"读写"（READWRITE/RW）、"删除授权"（NONE）

.. code-block:: bash

    ./cfs-cli user update [USER ID] [flags]         #更新用户[USER ID]的信息
    Flags：
        --access-key string                     #更新后的access key取值
        --secret-key string                     #更新后的secret key取值
        --user-type string                      #更新后的用户类型，可选项为normal或admin
        -y, --yes                               #跳过所有问题并设置回答为"yes"

