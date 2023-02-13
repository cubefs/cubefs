Blobnode管理
===============

查看磁盘状态
--------

.. code-block:: bash

   curl http://127.0.0.1:8889/stat


查看指定管理的所有磁盘可用空间，已用空间，读写状态，已用chunk数等状态信息。

响应示例

.. code-block:: json

   [
	    {
		"cluster_id": 100, 
		"idc": "z2",
		"rack": "testrack",
		"host": "http://10.52.140.66:8889",
		"path": "/home/service/var/data11",
		"status": 1,
		"readonly": false,
		"create_time": "2021-10-12T17:31:59.139122319+08:00",
		"last_update_time": "2021-10-12T17:31:59.139122319+08:00",
		"disk_id": 249,
		"used": 185613819904,
		"free": 7648276848640,
		"size": 7833890668544,
		"max_chunk_cnt": 0,
		"free_chunk_cnt": 0,
		"used_chunk_cnt": 111
	    },
	    {
		"cluster_id": 100,
		"idc": "z2",
		"rack": "testrack",
		"host": "http://10.52.140.66:8889",
		"path": "/home/service/var/data3",
		"status": 1,
		"readonly": false,
		"create_time": "2021-10-12T17:31:59.164906608+08:00",
		"last_update_time": "2021-10-12T17:31:59.164906608+08:00",
		"disk_id": 262,
		"used": 146654310400,
		"free": 7687236358144,
		"size": 7833890668544,
		"max_chunk_cnt": 0,
		"free_chunk_cnt": 0,
		"used_chunk_cnt": 122
	    }
   ]

查看指定磁盘信息
---------------

.. code-block:: bash

   curl http://127.0.0.1:8889/disk/stat/diskid/262

响应示例

.. code-block:: json

   {
        "cluster_id": 100,
    	"idc": "z2",
    	"rack": "testrack",
    	"host": "http://10.52.140.66:8889",
    	"path": "/home/service/var/data3",
    	"status": 1,
    	"readonly": false,
    	"create_time": "2021-10-12T17:31:59.164906608+08:00",
   	    "last_update_time": "2021-10-12T17:31:59.164906608+08:00",
   	    "disk_id": 262,
    	"used": 146549026816,
    	"free": 7687341641728,
    	"size": 7833890668544,
    	"max_chunk_cnt": 0,
    	"free_chunk_cnt": 0,
    	"used_chunk_cnt": 122
   }

注册磁盘
-------

.. code-block:: bash

   curl -X POST --header 'Content-Type: application/json' -d '{"path":"/home/service/disks/data11"}' "http://127.0.0.1:8889/disk/probe" 


