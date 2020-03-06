Inode
=====

Get Inode
------------

.. code-block:: bash

   curl -v http://10.196.59.202:17210/getInode?pid=100&ino=1024

Get inode information
    
.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "pid", "integer", "meta-partition id"
   "ino", "integer", "inode id"

Get Extents by Inode
---------------------

.. code-block:: bash

   curl -v http://10.196.59.202:17210/getExtentsByInode?pid=100&ino=1024

Get inode all extents information
    
.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "pid", "integer", "meta-partition id"
   "ino", "integer", "inode id"
    
Get All Inodes
---------------

.. code-block:: bash

   curl -v http://10.196.59.202:17210/getAllInodes?pid=100

Get all inodes of the specified partition

.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "pid", "integer", "meta-partition id"
    
