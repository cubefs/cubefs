Clustermgr
===============

Statistics
--------

.. code-block:: bash

	curl "http://127.0.0.1:9998/stat" | python -m json.tool

Show cluster statistics, including raft status, space status, volume information statistics, etc.

Response

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
   

Member Add
---------

.. code-block:: bash

   curl -X POST --header 'Content-Type: application/json' -d '{"peer_id": 1, "host": "127.0.0.1:9998", "member_type": 2}' "http://127.0.0.1:9998/member/add" 
   
Add a cluster node, specify the node type, address and id.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Descriptions"

   "peer_id", "uint64", "raft node id，unique"
   "host", "string", "host address"
   "member_type", "uint8", "node type，1(leaner) and 2(normal)"
   
Member Remove
--------

.. code-block:: bash

   curl -X POST --header 'Content-Type: application/json' -d '{"peer_id": 1}' "http://127.0.0.1:9998/member/remove"

Remove node by id.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Descriptions"

   "peer_id", "uint64", "raft node id，unique"
   
Leadership Transfer
-------------------

.. code-block:: bash

   curl -X POST --header 'Content-Type: application/json' -d '{"peer_id": 1}' "http://127.0.0.1:9998/leadership/transfer"
   
Transfer leadership by node id.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Descriptions"

   "peer_id", "uint64", "raft node id，unique"
   
Task Management
-----------------

.. csv-table::
   :header: "type", "key", "value"

   "Disk Repair", "disk_repair", "Enable/Disable"
   "Balance", "balance", "Enable/Disable"
   "Disk Drop", "disk_drop", "Enable/Disable"
   "Delete", "blob_delete", "Enable/Disable"
   "Shard Repair", "shard_repair",	"Enable/Disable"
   "Inspection", "vol_inspect", "Enable/Disable"
   
Task State

.. code-block:: bash

   curl http://127.0.0.1:9998/config/get?key=balance

Task Enable

.. code-block:: bash

   curl -X POST http://127.0.0.1:9998/config/set -d '{"key":"balance","value":"Enable"}' --header 'Content-Type: application/json'

Task Disable

.. code-block:: bash

   curl -X POST http://127.0.0.1:9998/config/set -d '{"key":"balance","value":"Disable"}' --header 'Content-Type: application/json'


