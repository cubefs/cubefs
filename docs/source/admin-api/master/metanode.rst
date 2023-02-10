Metanode Related
================

GET
---

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/metaNode/get?addr=10.196.59.202:17210"  | python -m json.tool


Show the base information of the metaNode, such as addr, total memory, used memory and so on.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"

   "addr", "string", "the addr which communicate with master"

response

.. code-block:: json

   {
       "ID": 3,
       "Addr": "10.196.59.202:17210",
       "IsActive": true,
       "Zone": "zone1",
       "MaxMemAvailWeight": 66556215048,
       "TotalWeight": 67132641280,
       "UsedWeight": 576426232,
       "Ratio": 0.008586377967698518,
       "SelectCount": 0,
       "Carry": 0.6645600532184904,
       "Threshold": 0.75,
       "ReportTime": "2018-12-05T17:26:28.29309577+08:00",
       "MetaPartitionCount": 1,
       "NodeSetID": 2,
       "PersistenceMetaPartitions": {}
   }


Decommission
-------------

.. code-block:: bash

   curl -v "http://127.0.0.1/metaNode/decommission?addr=127.0.0.1:9021"


Remove the metaNode from cluster, meta partitions which locate the metaNode will be migrate other available metaNode asynchronous.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"

   "addr", "string", "the addr which communicate with master"

Threshold
---------

.. code-block:: bash

   curl -v "http://127.0.0.1/threshold/set?threshold=0.75"


If the used memory percent arrives the threshold, the status of the meta partitions which locate the metaNode will be read only.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "threshold", "float64", "the max percent of memory which metaNode can use"


Migrate
---------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/metaNode/migrate?srcAddr=src&targetAddr=dst&count=3"

Migrate the specified number of metadata partitions from the source meta node to the target meta node.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "srcAddr", "string", "Source meta node"
   "targetAddr", "string", "Target meta node"
   "count", "int", "The number of meta partitions to migrate，default(50)"