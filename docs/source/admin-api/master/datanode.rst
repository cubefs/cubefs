Datanode Related
================

GET
-----

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/dataNode/get?addr=10.196.59.201:17310"  | python -m json.tool


Show the base information of the dataNode, such as addr, disk total size, disk used size and so on.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "addr", "string", "the addr which communicate with master"

response

.. code-block:: json

   {
       "TotalWeight": 39666212700160,
       "UsedWeight": 2438143586304,
       "AvailableSpace": 37228069113856,
       "ID": 2,
       "Zone": "zone1",
       "Addr": "10.196.59.201:17310",
       "ReportTime": "2018-12-06T10:56:38.881784447+08:00",
       "IsActive": true
       "UsageRatio": 0.06146650815226848,
       "SelectTimes": 5,
       "Carry": 1.0655859145960367,
       "DataPartitionReports": {},
       "DataPartitionCount": 21,
       "NodeSetID": 3,
       "PersistenceDataPartitions": {},
       "BadDisks": {}
   }


Decommission
-------------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/dataNode/decommission?addr=10.196.59.201:17310"


Remove the dataNode from cluster, data partitions which locate the dataNode will be migrate other available dataNode asynchronous.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "addr", "string", "the addr which communicate with master"
