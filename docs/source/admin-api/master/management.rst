Master Management
=================

Add
-----

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/raftNode/add?addr=10.196.59.197:17010&id=3"


Add a new master node to master raft group.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "addr", "string", "the addr of master server, format is ip:port"
   "id", "uint64", "the node id of master server"

Remove
---------

.. code-block:: bash

   curl -v "http://10.196.59.198:17010/raftNode/remove?addr=10.196.59.197:17010&id=3"


Remove the master node from master raft group.

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "addr", "string", "the addr of master server, format is ip:port"
   "id", "uint64", "the node id of master server"
