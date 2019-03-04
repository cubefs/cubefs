Dentry
======

Get Dentry
-----------

.. code-block:: bash

   curl -v 'http://127.0.0.1:9092/getDentry?pid=100&name=""&parentIno=1024'


Get dentry information


.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "pid", "integer", "meta partition id"
   "name", "string", "file or directory name"
   "parentIno", "integer", "file or directory parent directory inode"
    
Get Directory
--------------

.. code-block:: bash

   curl -v "http://127.0.0.1:9092/getDirectory?pid=100&parentIno=1024"


Get all files of the parent inode is 1024


.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "pid", "integer", "partition id"
   "ino", "integer", "inode id" 

Get All Dentry
--------------

.. code-block:: bash

   curl -v "http://127.0.0.1:9092/getAllDentry?pid=100"



.. csv-table:: Parameters
   :header: "Parameter", "Type", "Description"
   
   "pid", "integer", "partition id"