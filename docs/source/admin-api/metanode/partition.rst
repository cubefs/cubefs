Meta Partition
==================

Get Partitions
----------------

.. code-block:: bash

   curl -v  http://10.196.59.202:17210/getPartitions

Get all meta-partition base information of the metanode.

Get Partition by ID
---------------------

.. code-block:: bash

   curl -v http://10.196.59.202:17210/getPartitionById?pid=100

Get the specified partition information, this result contains: leader address, raft group peer and cursor.
    
.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "pid", "integer", "meta-partition id"
    
    
    
