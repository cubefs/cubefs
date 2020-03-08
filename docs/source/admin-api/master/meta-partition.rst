Meta Partition
==============

Create
---------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/metaPartition/create?name=test&start=10000"


split meta partition manually,if max meta partition of the vol which range is [0,end),end larger than start parameter,old meta partition range is[0,start], new meta partition is [start+1,end)

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "name", "string", "the name of vol"
   "start", "uint64", "the start value of meta partition which will be create"

Get
-------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/metaPartition/get?id=1" | python -m json.tool


show base information of meta partition,such as id,start,end and so on.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "id", "uint64", "the id of meta partition"

response

.. code-block:: json

   {
       "PartitionID": 1,
       "Start": 0,
       "End": 9223372036854776000,
       "MaxNodeID": 1,
       "Replicas": {},
       "ReplicaNum": 3,
       "Status": 2,
       "PersistenceHosts": {},
       "Peers": {},
       "MissNodes": {}
   }


Decommission
-------------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/metaPartition/decommission?id=13&addr=10.196.59.202:17210"


remove the replica of meta partition,and create new replica asynchronous

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "id", "uint64", "the id of meta partition"
   "addr", "string", "the addr of replica which will be decommission"

Load
-------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/metaPartition/load?id=1"


send load task to the metaNode which meta partition locate on,then check the crc of each replica in the meta partition

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"

   "id", "uint64", "the  id of data partition"
