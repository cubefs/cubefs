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
     "mountPoint": "/cfs/mountpoint",
     "volName": "ltptest",
     "owner": "ltptest",
     "masterAddr": "10.196.59.198:17010,10.196.59.199:17010,10.196.59.200:17010",
     "logDir": "/cfs/client/log",
     "logLevel": "info",
     "profPort": "27510"
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
   "rdonly", "bool", "Mount as read-only file system", "No"
   "writecache", "bool", "Leverage the write cache feature of kernel FUSE. Requires the kernel FUSE module to support write cache.", "No"
   "keepcache", "bool", "Keep kernel page cache. Requires the writecache option is enabled.", "No"
   "readRate", "int", "Read Rate Limit. Unlimited by default.", "No"
   "writeRate", "int", "Write Rate Limit. Unlimited by default.", "No"
   "followerRead", "bool", "Enable read from follower. False by default.", "No"
   "accessKey", "string", "Access key of user who owns the volume.", "No"
   "secretKey", "string", "Secret key of user who owns the volume.", "No"
   "disableDcache", "bool", "Disable Dentry Cache. False by default.", "No"
   "subdir", "string", "Mount sub directory.", "No"
   "fsyncOnClose", "bool", "Perform fsync upon file close. True by default.", "No"
   "maxcpus", "int", "The maximum number of available CPU cores. Limit the CPU usage of the client process.", "No"
   "enableXattr", "bool", "Enable xattr support. False by default.", "No"
   "nearRead", "bool", "Enable read from the nearer datanode. True by default, but only take effect when followerRead is enabled.", "No"
   "enablePosixACL", "bool", "Enable posix ACL support. False by default.", "No"
   "enableSummary", "bool", "Enable content summary. False by default.", "No"
   "enableUnixPermission", "bool", "Enable unix permission check support. False by default.", "No"

Mount
-----

Use the example *fuse.json*, and client is mounted on the directory */mnt/fuse*. All operations to */mnt/fuse* would be performed on the backing distributed file system.

.. code-block:: bash

   ./cfs-client -c fuse.json

Unmount
--------

It is recommended to use standard Linux ``umount`` command to terminate the mount.

DataPartitionSelector
---------------------

``dpSelectorName`` and ``dpSelectorParm`` allow custom to specify the desired dataPartition selection strategy to write. If custom does not set any strategy or set a wrong strategy, the default strategy(``DefaultRandomSelector``) will take effect.

Custom can modify ``DataPartitionSelector`` through master API, and do not need to remount client.

.. code-block:: bash

    curl 'http://masterIP:Port/vol/update?name=volName&authKey=VolKey&dpSelectorName=a&dpSelectorParm=b'

``dpSelectorName`` and ``dpSelectorParm`` must be modified at the same time.
