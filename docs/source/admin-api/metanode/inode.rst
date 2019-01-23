Inode
=====

Get Inode
------------

.. code-block:: bash

   curl -v http://127.0.0.1:9092/getInode?pid=100&ino=1024

Get inode information
    
.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "pid", "integer", "meta-partition id"
   "ino", "integer", "inode id"

Get Extents by Inode
---------------------

.. code-block:: bash

   curl -v http://127.0.0.1:9092/getExtentsByInode?pid=100&ino=1024

Get inode all extents information
    
.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "pid", "integer", "meta-partition id"
   "ino", "integer", "inode id"
    
Get All Inodes
---------------

.. code-block:: bash

   curl -v http://127.0.0.1:9092/getAllInodes?pid=100

Get all inodes of the specified partition

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "pid", "integer", "meta-partition id"
    
