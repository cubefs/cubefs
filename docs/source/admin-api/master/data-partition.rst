Data Partition
==============

Create
-------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/dataPartition/create?count=400&name=test"


Create a set of data partition.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "count", "int", "the num of dataPartitions will be create"
   "name", "string", "the name of vol"

Get
-------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/dataPartition/get?id=100"  | python -m json.tool

Get information of the specified data partition.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "id", "uint64", "the id of data partition"

response

.. code-block:: json

   {
       "PartitionID": 100,
       "LastLoadedTime": 1544082851,
       "ReplicaNum": 3,
       "Status": 2,
       "Replicas": {},
       "Hosts": {},
       "Peers": {},
       "Zones": {},
       "MissingNodes": {},
       "VolName": "test",
       "VolID": 2,
       "FileInCoreMap": {},
       "FilesWithMissingReplica": {}
   }

Decommission
-------------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/dataPartition/decommission?id=13&addr=10.196.59.201:17310"


Remove the replica of data partition, and create new replica asynchronous.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"

   "id", "uint64", "the id of data partition"
   "addr", "string", "the addr of replica which will be decommission"

Load
-------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/dataPartition/load?id=1"


Send load task to the dataNode which data partition locate on, then check the crc of each file in the data partition asynchronous.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "id", "uint64", "the  id of data partition"