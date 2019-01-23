Volume
======

Create
----------

.. code-block:: bash

   curl -v "http://127.0.0.1/admin/createVol?name=test&capacity=100"


allocate a set of data partition and a meta partition to the user.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "name", "string", ""
   "capacity", "int", "the quota of vol,unit is GB"

Delete
-------------

.. code-block:: bash

   curl -v "http://127.0.0.1/vol/delete?name=test"


Mark the vol status to MarkDelete first, then delete data partition and meta partition asynchronous,finally delete meta data from persist store

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "name", "string", ""

Get
---------

.. code-block:: bash

   curl -v "http://127.0.0.1/client/vol?name=test" | python -m json.tool


show the base information of the vol,such as name,the detail of data partitions and meta partitions and so on.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "name", "string", ""

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

   curl -v "http://127.0.0.1/vol/update?name=test&capacity=100"

add the vol quota

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"

   "name", "string", ""
   "capacity", "int", "the quota of vol, unit is GB"