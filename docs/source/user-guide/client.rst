Client
======

Prerequisite
------------

Insert FUSE kernel module and install libfuse.

.. code-block:: bash

   modprobe fuse
   yum install -y fuse

Prepare Config File
-------------------

fuse.json

.. code-block:: json

   {
     "mountPoint": "/mnt/fuse",
     "volName": "test",
     "owner": "cfs",
     "masterAddr": "192.168.31.173:80,192.168.31.141:80,192.168.30.200:80",
     "warnLogDir":"/export/home/tomcat/UMP-Monitor/logs/",
     "logDir": "/export/Logs/client",
     "logLevel": "info",
     "profPort": "10094"
   }

.. csv-table:: Supported Configurations
   :header: "Name", "Type", "Description", "Mandatory"

   "mountPoint", "string", "Mount point", "Yes"
   "volName", "string", "Volume name", "Yes"
   "owner", "string", "Owner name as authentication", "Yes"
   "masterAddr", "string", "Resource manager IP address", "Yes"
   "logDir", "string", "Path to store log files", "No"
   "logLevel", "string", "Log level：debug, info, warn, error", "No"
   "profPort", "string", "Golang pprof port", "No"
   "exporterPort", "string", "Performance monitor port", "No"
   "consulAddr", "string", "Performance monitor server address", "No"
   "lookupValid", "string", "Lookup valid duration in FUSE kernel module, unit: sec", "No"
   "attrValid", "string", "Attr valid duration in FUSE kernel module, unit: sec", "No"
   "icacheTimeout", "string", "Inode cache valid duration in client", "No"
   "enSyncWrite", "string", "Enable DirectIO sync write, i.e. make sure data is fsynced in data node", "No"
   "autoInvalData", "string", "Use AutoInvalData FUSE mount option", "No"
   "warnLogDir","string","Warn message directory","No"

Mount
-----

Use the example *fuse.json*, and client is mounted on the directory */mnt/fuse*. All operations to */mnt/fuse* would be performed on the backing distributed file system.

.. code-block:: bash

   nohup ./cfs-client -c fuse.json &
