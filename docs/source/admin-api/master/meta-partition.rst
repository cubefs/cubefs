Meta Partition
==============

Create
---------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/metaPartition/create?name=test&start=10000"


Split meta partition manually. If max meta partition of the vol which range is ``[begin,end)``:

if ``start`` larger than ``begin`` and less than ``end`` parameter, old meta partition range will be ``[begin,start]``, new meta partition will be ``[start+1,end)``;

if ``start`` less than ``begin``, ``max`` is the biggest inode num in current mp, old meta partition range will be ``[begin,max+16777216]``, new meta partition will be ``[max+16777217,inf)``;

if ``start`` bigger than ``end``, old meta partition range will be ``[begin,start]``, new meta partition will be ``[start+1,inf)``;
so, *if start is too big, that will cause inode count in one partition too many, which will cost large memory*.

when inode count is too big on the last meta partition, it will trigger partition range split automatically

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "name", "string", "the name of vol"
   "start", "uint64", "the start value of meta partition which will be create"

Get
-------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/metaPartition/get?id=1" | python -m json.tool


Show base information of meta partition, such as id, start, end and so on.

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
       "VolName": "test",
       "Replicas": {},
       "ReplicaNum": 3,
       "Status": 2,
       "IsRecover": true,
       "Hosts": {},
       "Peers": {},
       "Zones": {},
       "MissNodes": {},
       "LoadResponse": {}
   }


Decommission
-------------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/metaPartition/decommission?id=13&addr=10.196.59.202:17210"


Remove the replica of meta partition, and create new replica asynchronous.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "id", "uint64", "the id of meta partition"
   "addr", "string", "the addr of replica which will be decommission"

Load
-------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/metaPartition/load?id=1"


Send load task to the metaNode which meta partition locate on, then check the crc of each replica in the meta partition.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"

   "id", "uint64", "the  id of data partition"
