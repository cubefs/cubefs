CLI
====================

Using the Command Line Interface (CLI) can manage the cluster conveniently. With this tool, you can view the status of the cluster and each node, and manage each node, volumes, or users.

As the CLI continues to improve, it will eventually achieve 100% coverage of the APIs of each node in the cluster.

Compile and Configure
-----------------------

After downloading the CubeFS source code, execute the command ``go build`` in the directory ``cubefs/cli`` to generate ``cli``.

At the same time, a configuration file named ``.cfs-cli.json`` will be generated in the directory ``root``, and the master address can be changed to the current cluster master address. You can also get or set the master address by executing the command ``./cli config info`` or ``./cli config set``.

Bug Shooting
-----------------------

The logs of ``cfs-cli`` tool are in the directory ``/tmp/cfs/cli``, which offer detail running information for bug shooting.

Usage
---------

In the directory ``cubefs/cli``, execute the command ``./cli --help`` or ``./cli -h`` to get the CLI help document.

CLI is mainly divided into seven types of management commands.

.. csv-table:: Commands List
   :header: "Command", "description"

   "cli cluster", "Manage cluster components"
   "cli metanode", "Manage meta nodes"
   "cli datanode", "Manage data nodes"
   "cli datapartition", "Manage data partitions"
   "cli metapartition", "Manage meta partitions"
   "cli config", "Manage configuration for cli tool"
   "cli completion", "Generating bash completions "
   "cli volume, vol", "Manage cluster volumes"
   "cli user", "Manage cluster users"

Cluster Management
>>>>>>>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cli cluster info          #Show cluster summary information

.. code-block:: bash

    ./cli cluster stat          #Show cluster status information

.. code-block:: bash

    ./cli cluster freeze [true/false]        #Turn on or turn off the automatic allocation of the data partitions.

.. code-block:: bash

    ./cli cluster threshold [float]     #Set the threshold of memory on each meta node.

.. code-block:: bash

    ./cli cluster cluster set [flags]    #Set the parameters for clustedr.

MetaNode Management
>>>>>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cli metanode list          #List information of meta nodes

.. code-block:: bash

    ./cli metanode info [Address]     #Show detail information of a meta node

.. code-block:: bash

    ./cli metanode decommission [Address] #Decommission partitions in a meta node to other nodes

.. code-block:: bash

    ./cfs-cli metanode migrate [srcAddress] [dstAddress] #Migrate meta partitions from source metanode to target metanode

DataNode Management
>>>>>>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cli datanode list          #List information of data nodes

.. code-block:: bash

    ./cli datanode info [Address]         #Show detail information of a data node

.. code-block:: bash

   ./cli datanode decommission [Address]   #Decommission partitions in a data node to other nodes

.. code-block:: bash

    ./cfs-cli datanode migrate [srcAddress] [dstAddress] #Migrate data partitions from source datanode to target datanode

DataPartition Management
>>>>>>>>>>>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cli datapartition info [Partition ID]        #Display detail information of a data partition

.. code-block:: bash

    ./cli datapartition decommission [Address] [Partition ID]   #Decommission a replication of the data partition to a new address

.. code-block:: bash

    ./cli datapartition add-replica [Address] [Partition ID]    #Add a replication of the data partition on a new address

.. code-block:: bash

    ./cli datapartition del-replica [Address] [Partition ID]    #Delete a replication of the data partition from a fixed address

.. code-block:: bash

    ./cli datapartition check    #Diagnose partitions, display the partitions those are corrupt or lack of replicas

MetaPartition Management
>>>>>>>>>>>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cli metapartition info [Partition ID]        #Display detail information of a meta partition

.. code-block:: bash

    ./cli metapartition decommission [Address] [Partition ID]   #Decommission a replication of the meta partition to a new address

.. code-block:: bash

    ./cli metapartition add-replica [Address] [Partition ID]    #Add a replication of the meta partition on a new address

.. code-block:: bash

    ./cli metapartition del-replica [Address] [Partition ID]    #Delete a replication of the meta partition from a fixed address

.. code-block:: bash

    ./cli metapartition check    #Diagnose partitions, display the partitions those are corrupt or lack of replicas

Config Management
>>>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cli config info     #Show configurations of cli

.. code-block:: bash

    ./cfs-cli config set [flags] #set config info
    Flags:
        --addr string      Specify master address [{HOST}:{PORT}]
    -h, --help             help for set
        --timeout uint16   Specify timeout for requests [Unit: s]

Completion Management
>>>>>>>>>>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cli completion      #Generate bash completions

Volume Management
>>>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cli volume create [VOLUME NAME] [USER ID] [flags]     #Create a new volume
    Flags:
         --cache-action int          Specify low volume cacheAction (default 0)
         --cache-capacity int        Specify low volume capacity[Unit: GB]
         --cache-high-water int       (default 80)
         --cache-low-water int        (default 60)
         --cache-lru-interval int    Specify interval expiration time[Unit: min] (default 5)
         --cache-rule-key string     Anything that match this field will be written to the cache
         --cache-threshold int       Specify cache threshold[Unit: byte] (default 10485760)
         --cache-ttl int             Specify cache expiration time[Unit: day] (default 30)
         --capacity uint             Specify volume capacity (default 10)
         --crossZone string          Disable cross zone (default "false")
         --description string        Description
         --ebs-blk-size int          Specify ebsBlk Size[Unit: byte] (default 8388608)
         --follower-read string      Enable read form replica follower (default "true")
     -h, --help                      help for create
         --mp-count int              Specify init meta partition count (default 3)
         --normalZonesFirst string   Write to normal zone first (default "false")
         --replica-num int           Specify data partition replicas number (default 3)
         --size int                  Specify data partition size[Unit: GB] (default 120)
         --vol-type int              Type of volume (default 0)
     -y, --yes                       Answer yes for all questions
         --zone-name string          Specify volume zone name


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
    Flags:
        -f, --force                                         #Force transfer without current owner check
        -y, --yes                                           #Answer yes for all questions

.. code-block:: bash

    ./cli volume update                                     #Update cluster volumes parameters
    Flags:
        --cache-action string      Specify low volume cacheAction (default 0)
        --cache-capacity string    Specify low volume capacity[Unit: GB]
        --cache-high-water int      (default 80)
        --cache-low-water int       (default 60)
        --cache-lru-interval int   Specify interval expiration time[Unit: min] (default 5)
        --cache-rule string        Specify cache rule
        --cache-threshold int      Specify cache threshold[Unit: byte] (default 10M)
        --cache-ttl int            Specify cache expiration time[Unit: day] (default 30)
        --capacity uint            Specify volume datanode capacity [Unit: GB]
        --description string       The description of volume
        --ebs-blk-size int         Specify ebsBlk Size[Unit: byte]
        --follower-read string     Enable read form replica follower (default false)
        -y, --yes               Answer yes for all questions
        --zonename string   Specify volume zone name

User Management
>>>>>>>>>>>>>>>>>

.. code-block:: bash

    ./cli user create [USER ID] [flags]         #Create a new user
    Flags:
        --access-key string                     #Specify user access key for object storage interface authentication
        --secret-key string                     #Specify user secret key for object storage interface authentication
        --password string                       #Specify user password
        --user-type string                      #Specify user type [normal | admin] (default "normal")
        -y, --yes                               #Answer yes for all questions

.. code-block:: bash

    ./cli user delete [USER ID] [flags]         #Delete specified user
    Flags:
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
    Flags:
        --access-key string                     #Update user access key
        --secret-key string                     #Update user secret key
        --user-type string                      #Update user type [normal | admin]
        -y, --yes                               #Answer yes for all questions

