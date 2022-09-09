Volume
======

Create
----------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/admin/createVol?name=test&capacity=100&owner=cfs&mpCount=3"


| Allocate a set of data partition and a meta partition to the user.
| Default create 10 data partition and 3 meta partition when create volume.
| CubeFS uses the **Owner** parameter as the user ID. When creating a volume, if there is no user named the owner of the volume, a user with the user ID same as **Owner** will be automatically created; if a user named Owner already exists in the cluster, the volume will be owned by the user. For details, please see:: doc: `/admin-api/master/user`

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description", "Mandatory", "Default"
   
   "name", "string", "volume name", "Yes", "None"
   "capacity", "int", "the quota of vol, unit is GB", "Yes", "None"
   "owner", "string", "the owner of vol, and user ID of a user", "Yes", "None"
   "mpCount", "int", "the amount of initial meta partitions", "No", "3"
   "size", "int", "the size of data partitions, unit is GB", "No", "120"
   "followerRead", "bool", "enable read from follower", "No", "false"
   "crossZone", "bool", "cross zone or not. If it is true, parameter *zoneName* must be empty", "No", "false"
   "zoneName", "string", "specified zone", "No", "default (if *crossZone* is false)"

Delete
-------------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/vol/delete?name=test&authKey=md5(owner)"


Mark the vol status to MarkDelete first, then delete data partition and meta partition asynchronous, finally delete meta data from persist store.

While deleting the volume, the policy information related to the volume will be deleted from all user information.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "name", "string", "volume name"
   "authKey", "string", "calculates the 32-bit MD5 value of the owner field as authentication information"

Get
---------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/client/vol?name=test&authKey=md5(owner)" | python -m json.tool


Show the base information of the vol, such as name, the detail of data partitions and meta partitions and so on.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "name", "string", "volume name"
   "authKey", "string", "calculates the 32-bit MD5 value of the owner field as authentication information"

response

.. code-block:: json

    {
        "Name": "test",
        "Owner": "user",
        "Status": "0",
        "FollowerRead": "true",
        "MetaPartitions": {},
        "DataPartitions": {},
        "DataPartitions": {},
        "CreateTime": 0
    }


Stat
-------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/client/volStat?name=test"


Show the status information of volume.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "name", "string", "volume name"

response

.. code-block:: json

   {
       "Name": "test",
       "TotalSize": 322122547200000000,
       "UsedSize": 155515112832780000,
       "UsedRatio": "0.48",
       "EnableToken": false
   }


Update
----------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/vol/update?name=test&capacity=100&authKey=md5(owner)"

Increase the quota of volume, or adjust other parameters.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description", "Mandatory"

   "name", "string", "volume name", "Yes"
   "authKey", "string", "calculates the 32-bit MD5 value of the owner field as authentication information", "Yes"
   "capacity", "int", "the quota of vol, has to be 20 percent larger than the used space, unit is GB", "Yes"
   "zoneName", "string", "update zone name", "Yes"
   "followerRead", "bool", "enable read from follower", "No"

List
--------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/vol/list?keywords=test"

List all volumes information, and can be filtered by keywords.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description", "Mandatory"

   "keywords", "string", "get volumes information which contains this keyword", "No"

response

.. code-block:: json

    [
       {
           "Name": "test1",
           "Owner": "cfs",
           "CreateTime": 0,
           "Status": 0,
           "TotalSize": 155515112832780000,
           "UsedSize": 155515112832780000
       },
       {
           "Name": "test2",
           "Owner": "cfs",
           "CreateTime": 0,
           "Status": 0,
           "TotalSize": 155515112832780000,
           "UsedSize": 155515112832780000
       }
    ]

