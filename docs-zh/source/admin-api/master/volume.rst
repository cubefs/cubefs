卷管理命令
===================

创建
----------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/admin/createVol?name=test&capacity=100&owner=cfs&mpCount=3"


为用户创建卷，并分配一组数据分片和元数据分片. 在创建新卷时，默认分配10个数据分片和3个元数据分片。

CubeFS以 **Owner** 参数作为用户ID。在创建卷时，如果集群中没有与该卷的Owner同名的用户时，会自动创建一个用户ID为Owner的用户；如果集群中已存在用户ID为Owner的用户，则会自动将该卷的所有权归属于该用户。详情参阅： :doc:`/admin-api/master/user`

.. csv-table:: 参数列表
   :header: "参数", "类型", "描述", "是否必需", "默认值"

   "name", "string", "卷名称", "是", "无"
   "volType", "int", "卷类型：0：副本卷，1：纠删码卷", "否", "0"
   "capacity", "int", "卷的配额,单位是GB", "是", "无"
   "owner", "string", "卷的所有者，同时也是用户ID", "是", "无"
   "mpCount", "int", "初始化元数据分片个数", "否", "3"
   "replicaNum", "int", "初始化数据分片个数", "否", "副本卷默认3（支持1,3），纠删码卷默认1（支持1-16个）"
   "size", "int", "数据分片大小，单位GB", "否", "120"
   "enablePosixAcl", "bool", "是否配置posix权限限制", "否", "false"
   "followerRead", "bool", "允许从follower读取数据，纠删码卷默认true", "否", "false"
   "crossZone", "bool", "是否跨区域，如设为true，则不能设置zoneName参数", "否", "false"
   "normalZonesFirst", "bool", "是否优先写普通域", "否", "false"
   "zoneName", "string", "指定区域", "否", "如果crossZone设为false，则默认值为default"
   "cacheRuleKey", "string", "纠删码卷使用", "否", "非空时，匹配该字段的才会写入cache，空"
   "ebsBlkSize", "int", "每个块的大小，单位byte", "否", "默认8M"
   "cacheCap", "int", "纠删码卷 cache容量的大小,单位GB", "否", "纠删码卷必填"
   "cacheAction", "int", "纠删码卷写cache的场景，0-不写cache, 1-读数据回写cache, 2-读写数据都写到cache", "否", "0"
   "cacheThreshold", "int", "纠删码卷小于该值时，才写入到cahce中,单位byte", "否", "默认10M"
   "cacheTTL", "int", "纠删码卷cache淘汰时间，单位天", "否", "默认30"
   "cacheHighWater", "int", "纠删码卷cache淘汰的阈值，dp内容量淘汰上水位，达到该值时，触发淘汰", "否", "默认80，即120G*80/100=96G时，dp开始淘汰数据"
   "cacheLowWater", "int", "dp上容量淘汰下水位，达到该值时，不再淘汰，", "否", "默认60，即120G*60/100=72G，dp不再淘汰数据"
   "cacheLRUInterval", "int", "低容量淘汰检测周期，单位分钟", "否", "默认5分钟"

删除
-------------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/vol/delete?name=test&authKey=md5(owner)"


首先把卷标记为逻辑删除（status设为1）, 然后通过周期性任务删除所有数据分片和元数据分片,最终从持久化存储中删除。纠删码卷使用大小为0时才能删除

在删除卷的同时，将会在所有用户的信息中删除与该卷有关的权限信息。

.. csv-table:: 参数列表
   :header: "参数", "类型", "描述"

   "name", "string", "卷名称"
   "authKey", "string", "计算vol的所有者字段的32位MD5值作为认证信息"


查询卷详细信息
---------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/admin/getVol?name=test" | python -m json.tool


展示卷的基本信息，包括卷的名字、所有的数据分片和元数据分片信息等。

.. csv-table:: 参数列表
   :header: "参数", "类型", "描述"

   "name", "string", "卷名称"

响应示例

.. code-block:: json

   {
       "Authenticate": false,
        "CacheAction": 0,
        "CacheCapacity": 0,
        "CacheHighWater": 80,
        "CacheLowWater": 60,
        "CacheLruInterval": 5,
        "CacheRule": "",
        "CacheThreshold": 10485760,
        "CacheTtl": 30,
        "Capacity": 10,
        "CreateTime": "2022-03-31 16:08:31",
        "CrossZone": false,
        "DefaultPriority": false,
        "DefaultZonePrior": false,
        "DentryCount": 0,
        "Description": "",
        "DomainOn": false,
        "DpCnt": 0,
        "DpReplicaNum": 16,
        "DpSelectorName": "",
        "DpSelectorParm": "",
        "FollowerRead": true,
        "ID": 706,
        "InodeCount": 1,
        "MaxMetaPartitionID": 2319,
        "MpCnt": 3,
        "MpReplicaNum": 3,
        "Name": "abc",
        "NeedToLowerReplica": false,
        "ObjBlockSize": 8388608,
        "Owner": "cfs",
        "PreloadCapacity": 0,
        "RwDpCnt": 0,
        "Status": 0,
        "VolType": 1,
        "ZoneName": "default"
   }

查询卷数据分片详细信息
---------

.. code-block:: bash

   curl -v "http://192.168.0.12:17010/client/partitions?name=ltptest" | python -m json.tool


展示卷的所有的数据分片信息

.. csv-table:: 参数列表
   :header: "参数", "类型", "描述"

   "name", "string", "卷名称"

响应示例

.. code-block:: json

   {
       "Epoch": 0,
       "Hosts": [
           "192.168.0.34:17310",
           "192.168.0.33:17310",
           "192.168.0.32:17310"
       ],
       "IsRecover": false,
       "LeaderAddr": "192.168.0.33:17310",
       "PartitionID": 4,
       "ReplicaNum": 3,
       "Status": 2
   }


查询卷元数据分片详细信息
---------

.. code-block:: bash

   curl -v "http://192.168.0.12:17010/client/metaPartitions?name=ltptest" | python -m json.tool


展示卷的所有的元数据分片信息

.. csv-table:: 参数列表
   :header: "参数", "类型", "描述"

   "name", "string", "卷名称"

响应示例

.. code-block:: json

   {
       "DentryCount": 1,
       "End": 16777216,
       "InodeCount": 1,
       "IsRecover": false,
       "LeaderAddr": "192.168.0.23:17210",
       "MaxInodeID": 3,
       "Members": [
           "192.168.0.22:17210",
           "192.168.0.23:17210",
           "192.168.0.24:17210"
       ],
       "PartitionID": 1,
       "Start": 0,
       "Status": 2
   }


统计
-------

.. code-block:: bash

   curl -v http://10.196.59.198:17010/client/volStat?name=test


展示卷的总空间大小、已使用空间大小及是否开启读写token控制的信息。

.. csv-table:: 参数列表
   :header: "参数", "类型", "描述"

   "name", "string", "卷名称"
   "version", "int", "卷版本，0：副本卷， 1：ec-卷，默认0-副本卷，访问纠删码卷必填"

响应示例

.. code-block:: json

   {
       "CacheTotalSize": 0,
       "CacheUsedRatio": "",
       "CacheUsedSize": 0,
       "EnableToken": false,
       "InodeCount": 1,
       "Name": "abc-test",
       "TotalSize": 10737418240,
       "UsedRatio": "0.00",
       "UsedSize": 0
   }



更新
----------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/vol/update?name=test&capacity=100&authKey=md5(owner)"

增加卷的配额，也可调整其它相关参数。

.. csv-table:: 参数列表
   :header: "参数", "类型", "描述", "是否必需"

   "name", "string", "卷名称", "是"
   "description", "string", "卷描述信息", "否"
   "authKey", "string", "计算vol的所有者字段的32位MD5值作为认证信息", "是"
   "capacity", "int", "更新卷的datanode容量，单位G, 副本卷不能小于已使用容量", "否"
   "zoneName", "string", "更新后所在区域，若不设置将被更新至default区域", "是"
   "followerRead", "bool", "允许从follower读取数据", "否"
   "enablePosixAcl", "bool", "是否配置posix权限限制", "否", "false"
   "emptyCacheRule", "string", "是否置空cacheRule", "否", "默认为false, true代表设置cacheRule=''"
   "cacheRuleKey", "string", "缓存规则,纠删码卷使用，满足对应规则的才缓存", "否", "默认为空，不限制"
   "ebsBlkSize", "int", "纠删码卷的每个块的大小", "否", "默认8M"
   "cacheCap", "int", "纠删码卷使用二级cache时，cache的容量大小", "否", "0"
   "cacheAction", "int", "纠删码卷使用，0：不写cache, 1-读数据写cache, 2-读写数据都写到cache", "否", "默认0"
   "cacheThreshold", "int", "缓存文件大小限制，纠删码卷小于该值时，才会写到cache当中", "否", "默认10M"
   "cacheTTL", "int", "缓存过期时间，单位天", "否", "默认30天"
   "cacheHighWater", "int", "淘汰高水位", "否", "默认80, 即80%时，触发淘汰"
   "cacheLowWater", "int", "缓存淘汰低水位", "否", "默认40, 每次淘汰到40%停止淘汰"
   "cacheLRUInterval", "int", "缓存检测周期，单位分钟", "否", "默认5分钟"


获取卷列表
----------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/vol/list?keywords=test"

获取全部卷的列表信息，可按关键字过滤。

.. csv-table:: 参数列表
   :header: "参数", "类型", "描述", "是否必需"

   "keywords", "string", "获取卷名包含此关键字的卷信息", "否"

响应示例

.. code-block:: json

    [
       {
           "Name": "test1",
           "Owner": "cfs",
           "CreateTime": 0,
           "Status": 0,
           "TotalSize": 155515112832780000,
           "UsedSize": 155515112832780000
       },
       {
           "Name": "test2",
           "Owner": "cfs",
           "CreateTime": 0,
           "Status": 0,
           "TotalSize": 155515112832780000,
           "UsedSize": 155515112832780000
       }
    ]


扩容
----------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/vol/expand?name=test&capacity=100&authKey=md5(owner) "

对指定卷进行扩容到指定容量

.. csv-table:: 参数列表
   :header: "参数", "类型", "描述", "是否必需"

   "name", "string", "卷名称", "是"
   "authKey", "string", "计算vol的所有者字段的32位MD5值作为认证信息", "是"
   "capacity", "int", "扩充后卷的配额,单位是GB", "是"


缩容
----------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/vol/shrink?name=test&capacity=100&authKey=md5(owner) "

对指定卷进行缩小到指定容量

.. csv-table:: 参数列表
   :header: "参数", "类型", "描述", "是否必需"

   "name", "string", "卷名称", "是"
   "authKey", "string", "计算vol的所有者字段的32位MD5值作为认证信息", "是"
   "capacity", "int", "压缩后卷的配额,单位是GB", "是"


两副本
----------

主要事项
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

两个副本可以正常支持修改和写入（使用其他dp及其范围）

1. 支持已创建的3副本卷设置2副本，并在创建新dp生效，但不包括老的dp。
2. 两副本卷有一个副本崩溃然后没有leader的情况下，使用force raft del 接口删除2replicas 。

异常场景处理
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

例如存在一个dp，有两个副本A、B

- 两副本迁移的异常场景

迁移目标是C，我们实现的过程是先添加副本C，然后删除源A，迁移过程B crash

解决方式：如果B crash了，raft不可用，先删除B，等待迁移完成，删除A，再添加一个副本

- 正常运营过程某一个副本crash，例如B

没有leader，根据raft规则两副本不能删除B的，因为需要需要先commit 然后apply，但commit的条件是大多数存活。
 
解决方式:
   - 新命令
   强制删除B  /dataReplica/delete?....force=true。raft支持新接口del replcia直接不使用raft log commit（先备份dp数据）

   - datanode 将检查副本数（volume 和 dp 必须都是 2 个副本，以防使用不挡）和 force字段。

命令行
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. 两副本卷的创建

 .. code-block:: bash

      curl -v "http://192.168.0.11:17010/admin/createVol?name=2replica&capacity=100&owner=cfs&mpCount=3&replicaNum=2&followerRead=true"

2. 原有三副本卷降为两副本

- 存量的数据只读（建议批量脚本执行）

 .. code-block:: bash

      curl -v "http://192.168.0.13:17010/admin/setDpRdOnly?id=**&rdOnly=true

- 更新卷副本数量

 .. code-block:: bash

      curl -v "http://192.168.0.13:17010/vol/update?name=ltptest&replicaNum=2&followerRead=true&authKey=0e20229116d5a9a4a9e876806b514a85"

3. 强制删除(确定副本不可用使用）

 .. code-block:: bash

      curl "10.86.180.77:17010/dataReplica/delete?raftForceDel=true&addr=10.33.64.33:17310&id=47128"  

流控
----------

主要事项
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. 考虑到不区分volume的存储组件，在client端做volume限流

2. 分布式场景，需要中心控制client端流量，master做中心，保证iops，不增加额外流控server，可以减少运维压力

3. client采用幂函数控制流量增长，在流量在资源充足的场景下，可以快速增长

4. 保证volume整体流量调控下平稳

5. master可以均衡客户端流量，根据客户端请求情况自适应调节

配置项
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

无配置项，通过url命令设置

QOS流控参数字段及接口
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- 创建卷时启用QOS：

.. code-block:: bash

   curl -v "http://192.168.0.11:17010/admin/createVol?name=volName&capacity=100&owner=cfs&qosEnable=true&flowWKey=10000"

   启用qos，写流量设置为10000MB

- 获取卷的流量情况：

.. code-block:: bash

   curl  "http://192.168.0.11:17010/qos/getStatus?name=ltptest"

- 获取客户端数据：

.. code-block:: bash

   curl  "http://192.168.0.11:17010/qos/getClientsInfo?name=ltptest”

- 更新服务端参数，关闭、启用流控，调节读写流量值：

.. code-block:: bash

   curl  "http://192.168.0.11:17010/qos/update?name=ltptest&qosEnable=true&flowWKey=100000"|jq

涉及字段包括：
FlowWKey                = "flowWKey"   //写（卷）
FlowRKey                = "flowRKey"     //读（卷）   

一些系统参数说明
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. 默认单位

无论是client端还是datanode端，目前流量都是MB为单位
 
2. 最低参数流量和io，作用于datanode和volume的设置，如果设置值，则需要

MinFlowLimit    = 100 * util.MB

MinIoLimit      = 100

否则报错
 
3. 如果没有设置流量值，但启用限流，则使用默认值（Byte）

defaultIopsRLimit                     uint64 = 1 << 16

defaultIopsWLimit                     uint64 = 1 << 16

defaultFlowWLimit                     uint64 = 1 << 35

defaultFlowRLimit                     uint64 = 1 << 35


client和master通信
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. client长时间收不到master的流量控制日志会warn出来

2. client和master无法不通讯，会维持原有流量限制，也会warn出来

3.流量长时间为0则不会主动请求master流量，不上报给master，减少通信请求。master会清理长时间不上报客户端信息。

冷卷
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. 读一级缓存不算作流量

2. cache写不计入写流量控制

3. 其他都算作流量