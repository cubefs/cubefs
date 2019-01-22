Data Partition
==============

Create
-------

.. code-block:: bash

   curl -v "http://127.0.0.1/dataPartition/create?count=40&name=test&type=extent"


create a set of data partition

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "count", "int", "the num of dataPartitions will be create"
   "name", "string", "the name of vol"
   "type", "string", "the type of data partition,now only support extent type"

Get
-------

.. code-block:: bash

   curl -v "http://127.0.0.1/dataPartition/get?id=100"  | python -m json.tool


.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "id", "uint64", "the id of data partition"

response

.. code-block:: json

   {
       "PartitionID": 100,
       "LastLoadTime": 1544082851,
       "ReplicaNum": 3,
       "Status": 2,
       "Replicas": {},
       "PartitionType": "extent",
       "PersistenceHosts": {},
       "Peers": {},
       "MissNodes": {},
       "VolName": "test",
       "RandomWrite": true,
       "FileInCoreMap": {}
   }


Load
-------

.. code-block:: bash

   curl -v "http://127.0.0.1/dataPartition/load?name=test&id=1"


send load task to the dataNode which data parition locate on,then check the crc of each file in the data parttion asynchronous

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "name", "string", "the name of vol"
   "id", "uint64", "the  id of data partition"
