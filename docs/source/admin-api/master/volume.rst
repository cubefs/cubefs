Volume
======

Create
----------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/admin/createVol?name=test&capacity=100&owner=cfs&mpCount=3"


| Allocate a set of data partition and a meta partition to the user.
| Default create 10 data partition and 3 meta partition when create volume.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "name", "string", ""
   "capacity", "int", "the quota of vol,unit is GB"
   "owner", "string", "the owner of vol"
   "mpCount", "int", "the amount of initial meta partitions"
   "enableToken","bool","whether to enable the token mechanism to control client permissions"

Delete
-------------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/vol/delete?name=test&authKey=md5(owner)"


Mark the vol status to MarkDelete first, then delete data partition and meta partition asynchronous,finally delete meta data from persist store

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "name", "string", ""
   "authKey", "string", "calculates the MD5 value of the owner field  as authentication information"

Get
---------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/client/vol?name=test&authKey=md5(owner)" | python -m json.tool


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
        "DataPartitions": {},
        "OSSSecure": {
            "AccessKey": "l9aC5S9OzlneHguZ",
            "SecretKey": "4sL0xJfHVVHKOC3MDwPfaF1AcGm5a2XL"
        }
    }


Stat
-------

.. code-block:: bash

   curl -v http://10.196.59.198:17010/client/volStat?name=test


show vol stat information

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "name", "string", ""

response

.. code-block:: json

   {
       "Name": "test",
       "TotalSize": 322122547200000000,
       "UsedSize": 15551511283278,
       "EnableToken": false
   }


Update
----------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/vol/update?name=test&capacity=100&authKey=md5(owner)"

add the vol quota

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"

   "name", "string", ""
   "capacity", "int", "the quota of vol, unit is GB"
   "authKey", "string", "calculates the MD5 value of the owner field  as authentication information"
   "enableToken","bool","whether to enable the token mechanism to control client permissions"

Add Token
------------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/token/add?name=test&tokenType=1&authKey=md5(owner)"

add the token

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"

   "name", "string", "the name of vol"
   "tokenType", "int", "1 is readonly token, 2 is readWrite token"
   "authKey", "string", "calculates the MD5 value of the owner field  as authentication information"

Update Token
---------------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/token/update?name=test&token=xx&tokenType=1&authKey=md5(owner)"

update token type

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"

   "name", "string", "the name of vol"
   "token", "string","the token value"
   "tokenType", "int", "1 is readonly token, 2 is readWrite token"
   "authKey", "string", "calculates the MD5 value of the owner field  as authentication information"

Delete Token
---------------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/token/delete?name=test&token=xx&authKey=md5(owner)"

delete token

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"

   "name", "string", "the name of vol"
   "token", "string","the token value"
   "authKey", "string", "calculates the MD5 value of the owner field  as authentication information"

Get Token
------------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/token/get?name=test&token=xx"

show token information

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"

   "name", "string", "the name of vol"
   "token", "string","the token value"
