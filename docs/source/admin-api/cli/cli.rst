CLI
====================

Using the Command Line Interface (CLI) can manage the cluster conveniently. With this tool, you can view the status of the cluster and each node, and manage each node, volumes, or users.

As the CLI continues to improve, it will eventually achieve 100% coverage of the APIs of each node in the cluster.

Compile and Configure
-----------------------

After downloading the ChubaoFS source code, execute the command ``go build`` in the directory ``chubaofs/cli`` to generate ``cli``.

At the same time, a configuration file named ``.cfs-cli.json`` will be generated in the directory ``root``, and the master address can be changed to the current cluster master address. You can also get or set the master address by executing the command ``./cli config info`` or ``./cli config set``.

Usage
---------

In the directory ``chubaofs/cli``, execute the command ``./cli --help`` or ``./cli -h`` to get the CLI help document.

CLI is mainly divided into six types of management commands.

.. csv-table:: Commands List
   :header: "Command", "description"

   "cli cluster", "Manage cluster components"
   "cli metanode", "Manage meta nodes"
   "cli datanode", "Manage data nodes"
   "cli datapartition", "Manage data partitions"
   "cli volume, vol", "Manage cluster volumes"
   "cli user", "Manage cluster users"

Cluster Management
>>>>>>>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cli cluster info          #Show cluster summary information

.. code-block:: bash

    ./cli cluster stat          #Show cluster status information

MetaNode Management
>>>>>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cli metanode list         #List information of meta nodes

DataNode Management
>>>>>>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cli datanode list         #List information of meta nodes

DataPartition Management
>>>>>>>>>>>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cli datapartition info [VOLUME] [Partition ID]        #Get information of data partition

Volume Management
>>>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cli volume create [VOLUME NAME] [USER ID] [flags]     #Create a new volume
    Flags:
        --capacity uint                                     #Specify volume capacity [Unit: GB] (default 10)
        --dp-size  uint                                     #Specify size of data partition size [Unit: GB] (default 120)
        --follower-read                                     #Enable read form replica follower (default true)
        --mp-count int                                      #Specify init meta partition count (default 3)
        --replicas int                                      #Specify volume replicas number (default 3)
        -y, --yes                                           #Answer yes for all questions

.. code-block:: bash

    ./cli volume delete [VOLUME NAME] [flags]               #Delete a volume from cluster
    Flags:
        -y, --yes                                           #Answer yes for all questions

.. code-block:: bash

    ./cli volume info [VOLUME NAME] [flags]                 #Show volume information
    Flags:
        -d, --data-partition                                #Display data partition detail information
        -m, --meta-partition                                #Display meta partition detail information

.. code-block:: bash

    ./cli volume add-dp [VOLUME] [NUMBER]                   #Create and add more data partition to a volume

.. code-block:: bash

    ./cli volume list                                       #List cluster volumes

.. code-block:: bash

    ./cli volume transfer [VOLUME NAME] [USER ID] [flags]   #Transfer volume to another user. (Change owner of volume)
    Flags：
        -f, --force                                         #Force transfer without current owner check
        -y, --yes                                           #Answer yes for all questions


User Management
>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cli user create [USER ID] [flags]         #Create a new user
    Flags：
        --access-key string                     #Specify user access key for object storage interface authentication
        --secret-key string                     #Specify user secret key for object storage interface authentication
        --password string                       #Specify user password
        --user-type string                      #Specify user type [normal | admin] (default "normal")
        -y, --yes                               #Answer yes for all questions

.. code-block:: bash

    ./cli user delete [USER ID] [flags]         #Delete specified user
    Flags：
        -y, --yes                               #Answer yes for all questions

.. code-block:: bash

    ./cli user info [USER ID]                   #Show detail information about specified user

.. code-block:: bash

    ./cli user list                             #List cluster users

.. code-block:: bash

    ./cli user perm [USER ID] [VOLUME] [PERM]   #Setup volume permission for a user
                                                #The value of [PERM] is READONLY, RO, READWRITE, RW or NONE

.. code-block:: bash

    ./cli user update [USER ID] [flags]         #Update information about specified user
    Flags：
        --access-key string                     #Update user access key
        --secret-key string                     #Update user secret key
        --user-type string                      #Update user type [normal | admin]
        -y, --yes                               #Answer yes for all questions

