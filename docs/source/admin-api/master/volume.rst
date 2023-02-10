Volume
======

Create
----------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/admin/createVol?name=test&capacity=100&owner=cfs&mpCount=3"


| Allocate a set of data partition and a meta partition to the user.
| Default create 10 data partition and 3 meta partition when create volume.
| CubeFS uses the **Owner** parameter as the user ID. When creating a volume, if there is no user named the owner of the volume, a user with the user ID same as **Owner** will be automatically created; if a user named Owner already exists in the cluster, the volume will be owned by the user. For details, please see:: doc: `/admin-api/master/user`

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description", "Mandatory", "Default"
   
   "name", "string", "volume name", "Yes", "None"
   "capacity", "int", "the quota of vol, unit is GB", "Yes", "None"
   "volType", "int", "volType: 0: replica-volume, 1:ec-volume", "Yes", "0"
   "owner", "string", "the owner of vol, and user ID of a user", "Yes", "None"
   "mpCount", "int", "the amount of initial meta partitions", "No", "3"
   "enablePosixAcl", "bool", "enable posix acl for vol", "no", "false"
   "replicaNum", "int", "the amount of initial data partitions", "No", "3 default for replicas volume(support 1,3)，1 default for EC volume（support 1-16）"
   "size", "int", "the size of data partitions, unit is GB", "No", "120"
   "followerRead", "bool", "enable read from follower, ec-volume default true", "No", "false"
   "crossZone", "bool", "cross zone or not. If it is true, parameter *zoneName* must be empty", "No", "false"
   "normalZonesFirst", "bool", "write to normal zones first or not", "No", "false"
   "zoneName", "string", "specified zone", "No", "default (if *crossZone* is false)"
   "cacheRuleKey", "string", "for ec volume", "No", "None"
   "ebsBlkSize", "int", "ec block size，unit is byte", "No", "8"
   "cacheCap", "int", "Cache capacity for ec volume,unit is GB", "No", "Yes for ec volume"
   "cacheAction", "int", "Cache scenario for ec volume，0-do not cache, 1-cache when reading, 2-cache when reading or writing", "No", "0"
   "cacheThreshold", "int", "When it is less than this value for ec volume, it is written to the cahce,unit is byte", "No", "10"
   "cacheTTL", "int", "Cache elimination time for ec volume，unit is day", "No", "30"
   "cacheHighWater", "int", "Threshold for cache elimination for ec volume，when this value is reached, trigger dp content elimination", "No", "80"
   "cacheLowWater", "int", "When this value is reached, dp content elimination will not be eliminated，", "No", "60"
   "cacheLRUInterval", "int", "Obsolescence detection cycle for ec volume，unit is minute", "No", "5"

Delete
-------------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/vol/delete?name=test&authKey=md5(owner)"


Mark the vol status to MarkDelete first, then delete data partition and meta partition asynchronous, finally delete meta data from persist store, ec-volume can be deleted only if used size is zero.

While deleting the volume, the policy information related to the volume will be deleted from all user information.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "name", "string", "volume name"
   "authKey", "string", "calculates the 32-bit MD5 value of the owner field as authentication information"

Get
---------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/admin/getVol?name=test" | python -m json.tool


Show the base information of the vol, such as name, the detail of data partitions and meta partitions and so on.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "name", "string", "volume name"

response

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



Stat
-------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/client/volStat?name=test"


Show the status information of volume.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "name", "string", "volume name"
   "version", "", "volume version, 0: replica-volume, 1: ec-volume, default 0"

response

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


Update
----------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/vol/update?name=test&capacity=100&authKey=md5(owner)"

Increase the quota of volume, or adjust other parameters.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description", "Mandatory"

   "name", "string", "volume name", "Yes"
   "authKey", "string", "calculates the 32-bit MD5 value of the owner field as authentication information", "Yes"
   "capacity", "int", "the quota of vol, has to be 20 percent larger than the used space, unit is GB", "Yes"
   "zoneName", "string", "update zone name", "Yes"
   "followerRead", "bool", "enable read from follower", "No"
   "enablePosixAcl", "bool", "enable posix acl for vol", "no", "false"
   "cacheRuleKey", "string", "for ec volume", "No", "None"
   "ebsBlkSize", "int", "ec block size，unit is byte", "No", "8"
   "cacheCap", "int", "Cache capacity for ec volume,unit is GB", "No", "Yes for ec volume"
   "cacheAction", "int", "Cache scenario for ec volume，0-do not cache, 1-cache when reading, 2-cache when reading or writing", "No"
   "cacheThreshold", "int", "When it is less than this value for ec volume, it is written to the cahce,unit is byte", "No"
   "cacheTTL", "int", "Cache elimination time for ec volume，unit is day", "No"
   "cacheHighWater", "int", "Threshold for cache elimination for ec volume，when this value is reached, trigger dp content elimination", "No"
   "cacheLowWater", "int", "When this value is reached, dp content elimination will not be eliminated，", "No"
   "cacheLRUInterval", "int", "Obsolescence detection cycle for ec volume，unit is minute", "No"
   "cacheRuleKey", "string", "modify cache rule", "No"
   "emptyCacheRule", "bool", "whether to empty cacheRule", "No"


List
--------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/vol/list?keywords=test"

List all volumes information, and can be filtered by keywords.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description", "Mandatory"

   "keywords", "string", "get volumes information which contains this keyword", "No"

response

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


Expand
----------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/vol/expand?name=test&capacity=100&authKey=md5(owner) "

Expand the volume to the specified capacity

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description", "Mandatory"

   "name", "string", "Volume name", "Yes"
   "authKey", "string", "Calculates the 32-bit MD5 value of the owner field as authentication information", "Yes"
   "capacity", "int", "Capacity after expaned,unit is GB", "Yes"


Shrink
----------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/vol/shrink?name=test&capacity=100&authKey=md5(owner) "

Shrink the volume to the specified capacity

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description", "Mandatory"

   "name", "string", "Volume name", "Yes"
   "authKey", "string", "Calculates the 32-bit MD5 value of the owner field as authentication information", "Yes"
   "capacity", "int", "Capacity after Shrinked,unit is GB", "Yes"

Two-Replica
----------

Main matters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Both replicas can support modification and write normally (with other dp and its range)

1. It supports setting up 2 replicas of the created 3 replicas volume, and it takes effect when a new dp is created, but does not include the old dp.
2. In the case of a two-replica volume with one replica crashing and no leader, use the force raft del interface to delete 2 replicas.

Exception scene handling
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For example, there is a dp, there are two replicas A, B

- Unusual scenarios of two replicas migration

The migration target is C. The process we implemented is to add replica C first, then delete source A, and the migration process B crashes

Solution: If B crashes and raft is unavailable, delete B first, wait for the migration to complete, delete A, and add a replica

- A certain replica crashes during normal operation, such as B

Without a leader, according to the raft rule, the two replicas cannot delete B's, because it needs to commit first and then apply, but the condition for commit is that most of them survive.
 
Solution:
   - new command
   Force delete B /dataReplica/delete?....force=true. raft supports the new interface del replcia directly without using raft log commit (back up dp data first)

   - The datanode will check the number of replicas (both volume and dp must be 2 replicas, in case it is not used) and the force field.

Command
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Creation of two replica volumes

 .. code-block:: bash

      curl -v "http://192.168.0.11:17010/admin/createVol?name=2replica&capacity=100&owner=cfs&mpCount=3&replicaNum=2&followerRead=true"

2. The original three-replica volume is reduced to two replicas

- The existing data is read-only (recommended for batch script execution)

 .. code-block:: bash

      curl -v "http://192.168.0.13:17010/admin/setDpRdOnly?id=**&rdOnly=true

- Update volume replica count

 .. code-block:: bash

      curl -v "http://192.168.0.13:17010/vol/update?name=ltptest&replicaNum=2&followerRead=true&authKey=0e20229116d5a9a4a9e876806b514a85"

3. Force delete (make sure the replica is not available for use)

 .. code-block:: bash

      curl "10.86.180.77:17010/dataReplica/delete?raftForceDel=true&addr=10.33.64.33:17310&id=47128"  

QOS
----------

Main matters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Considering the storage components that do not distinguish between volumes, perform volume current limiting on the client side

2. In distributed scenarios, the center needs to control the client-side traffic, and the master is the center to ensure iops without adding additional flow control servers, which can reduce operation and maintenance pressure

3. The client uses the power function to control the traffic growth, and the traffic can grow rapidly when the resources are sufficient.

4. Ensure that the overall flow of the volume is stable under the control of the overall flow

5. The master can balance client traffic and adjust adaptively according to client requests

Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

No configuration item, set by url command

Parameter fields and interfaces for QOS 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Enable QOS when creating volumes：

.. code-block:: bash

   curl -v "http://192.168.0.11:17010/admin/createVol?name=volName&capacity=100&owner=cfs&qosEnable=true&flowWKey=10000"

   Enable qos, write traffic is set to 10000MB

- Get the traffic situation of the volume：

.. code-block:: bash

   curl  "http://192.168.0.11:17010/qos/getStatus?name=ltptest"

- Get client data：

.. code-block:: bash

   curl  "http://192.168.0.11:17010/qos/getClientsInfo?name=ltptest”

- Update server parameters, close and enable QOS, and adjust read and write QOS values：

.. code-block:: bash

   curl  "http://192.168.0.11:17010/qos/update?name=ltptest&qosEnable=true&flowWKey=100000"|jq

The fields involved：
FlowWKey                = "flowWKey"   //Write（volume）
FlowRKey                = "flowRKey"     //Read（volume）   

Description of parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. default unit

Whether it is the client side or the datanode side, the current QOS is in MB
 
2. The minimum parameters flow and io, which act on the settings of datanode and volume, if the value is set, it is required

MinFlowLimit    = 100 * util.MB

MinIoLimit      = 100

Otherwise report an error
 
3. If no traffic value is set, but throttling is enabled, the default value is used（Byte）

defaultIopsRLimit                     uint64 = 1 << 16

defaultIopsWLimit                     uint64 = 1 << 16

defaultFlowWLimit                     uint64 = 1 << 35

defaultFlowRLimit                     uint64 = 1 << 35


client and master communication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. If the client fails to receive QOS log of the master for a long time, it will warn

2. The client and the master cannot communicate with each other, the original QOS limit will be maintained, and it will also warn.

3. If QOS is 0 for a long time, the master QOS will not be actively requested, and it will not be reported to the master to reduce communication requests. The master will clean up the client information that has not been reported for a long time。

Cold volume
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Reading block cache does not count as traffic

2. Write into cache is not included in QOS

3. Everything else counts as QOS