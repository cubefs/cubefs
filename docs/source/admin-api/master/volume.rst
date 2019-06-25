Volume
======

Create
----------

.. code-block:: bash

   curl -v "http://127.0.0.1/admin/createVol?name=test&capacity=100&owner=cfs&mpCount=3"


| Allocate a set of data partition and a meta partition to the user.
| Default create 10 data partition and 3 meta partition when create volume.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "name", "string", ""
   "capacity", "int", "the quota of vol,unit is GB"
   "owner", "string", "the owner of vol"
   "mpCount", "int", "the amount of initial meta partitions"

Delete
-------------

.. code-block:: bash

   curl -v "http://127.0.0.1/vol/delete?name=test&authKey=md5(owner)"


Mark the vol status to MarkDelete first, then delete data partition and meta partition asynchronous,finally delete meta data from persist store

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "name", "string", ""
   "authKey", "string", "calculates the MD5 value of the owner field  as authentication information"

Get
---------

.. code-block:: bash

   curl -v "http://127.0.0.1/client/vol?name=test&authKey=md5(owner)" | python -m json.tool


show the base information of the vol,such as name,the detail of data partitions and meta partitions and so on.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "name", "string", ""
   "authKey", "string", "calculates the MD5 value of the owner field  as authentication information"

response

.. code-block:: json

   {
       "Name": "test",
       "VolType": "extent",
       "MetaPartitions": {},
       "DataPartitions": {}
   }


Stat
-------

.. code-block:: bash

   curl -v http://127.0.0.1/client/volStat?name=test


show vol stat information

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "name", "string", ""

response

.. code-block:: json

   {
       "Name": "test",
       "TotalSize": 322122547200000000,
       "UsedSize": 15551511283278
   }


Update
----------

.. code-block:: bash

   curl -v "http://127.0.0.1/vol/update?name=test&capacity=100&authKey=md5(owner)"

add the vol quota

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"

   "name", "string", ""
   "capacity", "int", "the quota of vol, unit is GB"
   "authKey", "string", "calculates the MD5 value of the owner field  as authentication information"