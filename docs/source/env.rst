
Environment Requirements and Capacity Planning
=========================================================

Environment Requirements
--------------------------

The following table lists the system and hardware requirements of the performance test environment and production environment. You can also refer to the capacity planning chapter to accurately customize the deployment plan based on your cluster's actual capacity planning.
Note that since the DataNode used Punch Hole features of linux kernal, so that the kernel version of servers which used for deploy DataNode are recommended as follow.

.. csv-table::
   :header: "FileSystem", "Kernel Version Requirement"

   "XFS",">=2.6.38"
   "ext4",">=3.0"
   "Btrfs",">=3.7"
   "tmpfs(5)",">=3.5"
   "gfs2(5)",">=4.16"

In order to speed up read and write of meta data, the meta data is stored in memory, while the DataNode mainly occupies disk resources. To maximize the use of node resources, you can mix-deploy DataNode and MetaNode on the same node.

.. csv-table::
   :file: csv/env.csv

Capacity Planning
-----------------------------

First of all, you have to assess the highest expected number of files and storage capacity of the cluster in the future.
Secondly, you need to know the machine resources you currently have, and the total memory, CPU cores, and disks on each machine.
If you have been clear about those statistics, you can use the empirical reference values ​​given in the second section to see which scale your current environment belongs to, what file size it can carry,or you need to prepare for the current file experience requirements How many resources to prevent frequent expansion of machine resources.

.. csv-table::
   :header: "Total File Count", "Total File Size", "Total memory", "Total Disk Space"

   "1,000,000,000", "10PB", "2048 GB", "10PB"

The higher the proportion of large files, the greater the MetaNode pressure.

Of course, if you feel that the current resources are adequately used, you don't need to meet the capacity growth requirements all at once. Then you can pay attention to the capacity warning information of MetaNode/DataNode in time. When the memory or disk is about to run out, dynamically increase MetaNode/DataNode to adjust the capacity. In other words, if you find that the disk space is not enough, you can increase the disk or increase DataNode. If you find that all MetaNode memory is too full, you can increase MetaNode to relieve memory pressure.


Multi-Zone Deploy
-----------------------


If you want the cluster to support fault tolerance in the computer room, you can deploy a CubeFS cluster across computer rooms. At the same time, it should be noted that since the communication delay between computer rooms is higher than that of a single computer room, if the requirements for high availability are greater than low latency, you can choose a cross-computer room deployment solution. If you have higher performance requirements, it is recommended to deploy clusters in a single computer room.
Configuration scheme: Modify the zoneName parameter in the DataNode/MetaNode configuration file, specify the name of the computer room where you are, and then start the DataNode/MetaNode process, the computer room will be stored and recorded by the Master along with the registration of DataNode/MetaNode.


Create a single zone volume:

.. code-block:: bash

    $ cfs-cli volume create {name} --zone-name={zone}

In order to prevent volume initialization failure in a single computer room, please ensure that the DataNode of a single computer room is not less than 3 and MetaNode is not less than 4.

Create a cross-zone volume:

.. code-block:: bash

    $ cfs-cli volume create {name} --cross-zone=true
