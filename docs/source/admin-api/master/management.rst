Master Management
=================

Add
-----

.. code-block:: bash

   curl -v "http://127.0.0.1/raftNode/add?addr=127.0.0.1:80&id=3"


add new master  node to master raft group

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "addr", "string", "the addr of master server, format is ip:port"
   "id", "uint64", "the node id of master server"

Remove
---------

.. code-block:: bash

   curl -v "http://127.0.0.1/raftNode/remove?addr=127.0.0.1:80&id=3"


remove the master node from master raft group

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "addr", "string", "the addr of master server, format is ip:port"
   "id", "uint64", "the node id of master server"