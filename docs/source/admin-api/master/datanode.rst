Datanode Related
================

GET
-----

.. code-block:: bash

   curl -v "http://127.0.0.1/dataNode/get?addr=127.0.0.1:5000"  | python -m json.tool


show the base information of the dataNode,such as addr,disk total size,disk used size and so on.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "addr", "string", "the addr which communicate with master"

response

.. code-block:: json

   {
       "MaxDiskAvailWeight": 3708923232256,
       "CreatedVolWeights": 2705829396480,
       "RemainWeightsForCreateVol": 36960383303680,
       "TotalWeight": 39666212700160,
       "UsedWeight": 2438143586304,
       "Available": 37228069113856,
       "Rack": "default",
       "Addr": "10.196.30.231:6000",
       "ReportTime": "2018-12-06T10:56:38.881784447+08:00",
       "Ratio": 0.06146650815226848,
       "SelectCount": 5,
       "Carry": 1.0655859145960367,
       "Sender": {
           "TaskMap": {}
       },
       "DataPartitionCount": 21
   }


Decommission
-------------

.. code-block:: bash

   curl -v "http://127.0.0.1/dataNode/decommission?addr=127.0.0.1:5000"


remove the dataNode from cluster, data partitions which locate the dataNode will be migrate other available dataNode asynchronous

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "addr", "string", "the addr which communicate with master"