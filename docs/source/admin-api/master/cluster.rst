Cluster
=======

Overview
--------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/admin/getCluster" | python -m json.tool


Display the base information of the cluster, such as the detail of metaNode, dataNode, vol and so on.

response

.. code-block:: json

   {
       "Name": "test",
       "LeaderAddr": "10.196.59.198:17010",
       "DisableAutoAlloc": false,
       "Applied": 225,
       "MaxDataPartitionID": 100,
       "MaxMetaNodeID": 3,
       "MaxMetaPartitionID": 1,
       "DataNodeStatInfo": {},
       "MetaNodeStatInfo": {},
       "VolStatInfo": {},
       "BadPartitionIDs": {},
       "BadMetaPartitionIDs": {},
       "MetaNodes": {},
       "DataNodes": {}
   }


Freeze
------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/cluster/freeze?enable=true"

If cluster is freezed, the vol never allocates dataPartitions automatically.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"

   "enable", "bool", "if enable is true, the cluster is freezed"


Statistics
-----------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/cluster/stat"

Show cluster space information by zone.

response

.. code-block:: json

    {
        "DataNodeStatInfo": {
            "TotalGB": 1,
            "UsedGB": 0,
            "IncreasedGB": -2,
            "UsedRatio": "0.0"
        },
        "MetaNodeStatInfo": {
            "TotalGB": 1,
            "UsedGB": 0,
            "IncreasedGB": -8,
            "UsedRatio": "0.0"
        },
        "ZoneStatInfo": {
            "zone1": {
                "DataNodeStat": {
                    "TotalGB": 1,
                    "UsedGB": 0,
                    "AvailGB": 0,
                    "UsedRatio": 0,
                    "TotalNodes": 0,
                    "WritableNodes": 0
                },
                "MetaNodeStat": {
                    "TotalGB": 1,
                    "UsedGB": 0,
                    "AvailGB": 0,
                    "UsedRatio": 0,
                    "TotalNodes": 0,
                    "WritableNodes": 0
                }
            }
        }
    }

Topology
-----------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/topo/get"

Show cluster topology information by zone.

response

.. code-block:: json

    [
        {
            "Name": "zone1",
            "Status": "available",
            "NodeSet": {
                "700": {
                    "DataNodeLen": 0,
                    "MetaNodeLen": 0,
                    "MetaNodes": [],
                    "DataNodes": []
                }
            }
        },
        {
            "Name": "zone2",
            "Status": "available",
            "NodeSet": {
                "800": {
                    "DataNodeLen": 0,
                    "MetaNodeLen": 0,
                    "MetaNodes": [],
                    "DataNodes": []
                }
            }
        }
    ]

Update Zone
------------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/zone/update?name=zone1&enable=false"

Set the status of the zone to available or unavailable.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"

   "name", "string", "zone name"
   "enable", "bool", "if enable is true, the cluster is available"

Get Zone
-----------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/zone/list"

Get name and status of all zones.

response

.. code-block:: json

    [
        {
            "Name": "zone1",
            "Status": "available",
            "NodeSet": {}
        },
        {
            "Name": "zone2",
            "Status": "available",
            "NodeSet": {}
        }
    ]

Get Node Info
----------------------

.. code-block:: bash

   curl -v "http://192.168.0.11:17010/admin/getNodeInfo"

Get node info of cluster.

response

.. code-block:: json

    {
        "code": 0,
        "msg": "success",
        "data": {
            "batchCount": 0,
            "deleteWorkerSleepMs": 0,
            "markDeleteRate": 0
        }
    }

Set Node Info
-------------------

.. code-block:: bash

   curl -v "http://192.168.0.11:17010/admin/setNodeInfo?batchCount=100&markDeleteRate=100&deleteWorkerSleepMs=1000"

Set node info of cluster.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"

   "autoRepairRate", "uint64", "dataNode auto repair rate"
   "batchCount", "uint64", "metanode delete batch count"
   "deleteWorkerSleepMs", "uint64", "metanode delete worker sleep time with millisecond. if 0 for no sleep"
   "loadFactor", "uint64", "load factor"
   "markDeleteRate", "uint64", "datanode batch markdelete limit rate. if 0 for no infinity limit"
   "maxDpCntLimit", "uint64", "maximum number of dp on each datanode, default 3000, 0 represents setting to default"

