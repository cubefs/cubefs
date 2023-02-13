Inode管理命令
=====================

获取指定Inode基本信息
-----------------------------

.. code-block:: bash

   curl -v http://192.168.0.22:17210/getInode?pid=100&ino=1024

.. csv-table:: 请求参数说明：
   :header: "参数", "类型", "描述"
   
   "pid", "整型", "分片id"
   "ino", "整型", "inode的id"

获取指定Inode的数据存储信息
----------------------------------

.. code-block:: bash

   curl -v http://192.168.0.22:17210/getExtentsByInode?pid=100&ino=1024
    
.. csv-table:: 请求参数：
   :header: "参数", "类型", "描述"
   
   "pid", "整型", "分片id"
   "ino", "整型", "inode id"
    
获取指定元数据分片的全部inode信息
---------------------------------------

.. code-block:: bash

   curl -v http://192.168.0.22:17210/getAllInodes?pid=100

.. csv-table:: 请求参数：
   :header: "参数", "类型", "描述"
   
   "pid", "整型", "分片 id"
    
    
获取inode上的ebs分片信息
---------------------------------------

.. code-block:: bash

   curl -v '192.168.0.22:17220/getEbsExtentsByInode?pid=282&ino=16797167'

.. csv-table:: 请求参数：
   :header: "参数", "类型", "描述"
   
   "pid", "整型", "分片 id"
   "ino", "整型", "inode的id"
    
