Clustermgr管理
===============

查看状态
--------

.. code-block:: bash

	curl "http://127.0.0.1:9998/stat" | python -m json.tool

展示集群状态，包括raft状态、空间状态、卷信息统计等等。

响应示例

.. code-block:: json

   {
	    "leader_host": "127.0.0.1:9998",
        "read_only": false,
        "raft_status": {
            "nodeId": 1,
            "term": 2,
            "vote": 1,
            "commit": 909,
            "leader": 1,
            "raftState": "StateLeader",
            "applied": 909,
            "raftApplied": 909,
            "transferee": 0,
            "peers": [
                {
                    "id": 1,
                    "host": "127.0.0.1:10110",
                    "match": 909,
                    "next": 910,
                    "state": "StateReplicate",
                    "paused": false,
                    "pendingSnapshot": 0,
                    "active": true,
                    "isLearner": false,
                    "isInflightFull": false,
                    "inflightCount": 0
                },
                {
                    "id": 2,
                    "host": "127.0.0.1:10111",
                    "match": 909,
                    "next": 910,
                    "state": "StateReplicate",
                    "paused": false,
                    "pendingSnapshot": 0,
                    "active": true,
                    "isLearner": false,
                    "isInflightFull": false,
                    "inflightCount": 0
                },
                {
                    "id": 3,
                    "host": "127.0.0.1:10112",
                    "match": 909,
                    "next": 910,
                    "state": "StateReplicate",
                    "paused": false,
                    "pendingSnapshot": 0,
                    "active": true,
                    "isLearner": false,
                    "isInflightFull": false,
                    "inflightCount": 0
                }
            ]
        },
        "space_stat": {
            "total_space": 314304364536,
            "free_space": 105832886264,
            "used_space": 208471478272,
            "writable_space": 52898562048,
            "total_blob_node": 1,
            "total_disk": 8,
            "disk_stat_infos": [
                {
                    "idc": "z0",
                    "total": 8,
                    "total_chunk": 18728,
                    "total_free_chunk": 6303,
                    "available": 8,
                    "readonly": 0,
                    "expired": 0,
                    "broken": 0,
                    "repairing": 0,
                    "repaired": 0,
                    "dropping": 0,
                    "dropped": 0
                }
            ]
        },
        "volume_stat": {
            "total_volume": 9,
            "idle_volume": 5,
            "can_alloc_volume": 5,
            "active_volume": 4,
            "lock_volume": 0,
            "unlocking_volume": 0
        }
   }
   

节点添加
---------

.. code-block:: bash

   curl -X POST --header 'Content-Type: application/json' -d '{"peer_id": 1, "host": "127.0.0.1:9998", "member_type": 2}' "http://127.0.0.1:9998/member/add" 
   
添加节点，指定节点类型，地址和id。

.. csv-table:: 参数列表
   :header: "参数", "类型", "描述"

   "peer_id", "uint64", "raft节点id，不可重复"
   "host", "string", "主机地址"
   "member_type", "uint8", "节点类型，1表示leaner，2表示normal"
   
成员移除
--------

.. code-block:: bash

   curl -X POST --header 'Content-Type: application/json' -d '{"peer_id": 1}' "http://127.0.0.1:9998/member/remove"

根据id移除节点。

.. csv-table:: 参数列表
   :header: "参数", "类型", "描述"

   "peer_id", "uint64", "raft节点id，不可重复"
   
切主
-----

.. code-block:: bash

   curl -X POST --header 'Content-Type: application/json' -d '{"peer_id": 1}' "http://127.0.0.1:9998/leadership/transfer"
   
根据id切换主节点。

.. csv-table:: 参数列表
   :header: "参数", "类型", "描述"

   "peer_id", "uint64", "raft节点id，不可重复"
   
启动或禁用后台任务
-----------------

.. csv-table::
   :header: "任务类型(type)", "任务名(key)", "开关(value)"

   "磁盘修复", "disk_repair", "Enable/Disable"
   "数据均衡", "balance", "Enable/Disable"
   "磁盘下线", "disk_drop", "Enable/Disable"
   "数据删除", "blob_delete", "Enable/Disable"
   "数据修补", "shard_repair", "Enable/Disable"
   "数据巡检", "vol_inspect", "Enable/Disable"
   
查看任务状态

.. code-block:: bash

   curl http://127.0.0.1:9998/config/get?key=balance

开启任务

.. code-block:: bash

   curl -X POST http://127.0.0.1:9998/config/set -d '{"key":"balance","value":"Enable"}' --header 'Content-Type: application/json'

关闭任务

.. code-block:: bash

   curl -X POST http://127.0.0.1:9998/config/set -d '{"key":"balance","value":"Disable"}' --header 'Content-Type: application/json'


