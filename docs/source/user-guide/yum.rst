Run Cluster by Yum Tools
=========================

Yum tools to run a CubeFS cluster for CentOS 7+ is provided. The list of RPM packages dependencies can be installed with:

.. code-block:: bash

    $ yum install https://ocs-cn-north1.heytapcs.com/cubefs/rpm/3.2.0/cfs-install-3.2.0-el7.x86_64.rpm
    $ cd /cfs/install
    $ tree -L 2
    .
    ├── install_cfs.yml
    ├── install.sh
    ├── iplist
    ├── src
    └── template
        ├── client.json.j2
        ├── console.json.j2
        ├── create_vol.sh.j2
        ├── datanode.json.j2
        ├── grafana
        │   ├── grafana.ini
        │   ├── init.sh
        │   └── provisioning
        ├── master.json.j2
        ├── metanode.json.j2
        └── objectnode.json.j2

Set parameters of the CubeFS cluster in **iplist**.

- **[master]**, **[datanode]** , **[metanode]** , **[monitor]** , **[client]** modules includes member IP addresses of each role.

- **[cfs:vars]** module includes parameters for SSH connection. So make sure the port, username and password for SSH connection is unified before start.

- **#master config** module includes parameters of Master.

.. csv-table:: Properties
   :header: "Key", "Type", "Description", "Mandatory"

   "master_clusterName", "string", "The cluster identifier", "Yes"
   "master_listen", "string", "Http port which api service listen on", "Yes"
   "master_prof", "string", "golang pprof port", "Yes"
   "master_logDir", "string", "Path for log file storage", "Yes"
   "master_logLevel", "string", "Level operation for logging. Default is *info*.", "No"
   "master_retainLogs", "string", "the number of raft logs will be retain.", "Yes"
   "master_walDir", "string", "Path for raft log file storage.", "Yes"
   "master_storeDir", "string", "Path for RocksDB file storage,path must be exist", "Yes"
   "master_exporterPort", "int", "The prometheus exporter port", "No"
   "master_metaNodeReservedMem","string","If the metanode memory is below this value, it will be marked as read-only. Unit: byte. 1073741824 by default.", "No"

- **#datanode config** module includes parameters of DataNodes.

.. csv-table:: Properties
   :header: "Key", "Type", "Description", "Mandatory"

   "datanode_listen", "string", "Port of TCP network to be listen", "Yes"
   "datanode_prof", "string", "Port of HTTP based prof and api service", "Yes"
   "datanode_logDir", "string", "Path for log file storage", "Yes"
   "datanode_logLevel", "string", "Level operation for logging. Default is *info*", "No"
   "datanode_raftHeartbeat", "string", "Port of raft heartbeat TCP network to be listen", "Yes"
   "datanode_raftReplica", "string", "Port of raft replicate TCP network to be listen", "Yes"
   "datanode_raftDir", "string", "Path for raft log file storage", "No"
   "datanode_exporterPort", "string", "Port for monitor system", "No"
   "datanode_disks", "string slice", "
   | Format: *PATH:RETAIN*.
   | PATH: Disk mount point, must exists. RETAIN: Retain space. (Ranges: 20G-50G.)
   | RETAIN: The minimum free space(Bytes) reserved for the path.", "Yes"

- **#metanode config** module includes parameters of MetaNodes.

.. csv-table:: Properties
   :header: "Key", "Type", "Description", "Mandatory"

   "metanode_listen", "string", "Listen and accept port of the server", "Yes"
   "metanode_prof", "string", "Pprof port", "Yes"
   "metanode_logLevel", "string", "Level operation for logging. Default is *info*", "No"
   "metanode_metadataDir", "string", "MetaNode store snapshot directory", "Yes"
   "metanode_logDir", "string", "Log directory", "Yes",
   "metanode_raftDir", "string", "Raft wal directory", "Yes",
   "metanode_raftHeartbeatPort", "string", "Raft heartbeat port", "Yes"
   "metanode_raftReplicaPort", "string", "Raft replicate port", "Yes"
   "metanode_exporterPort", "string", "Port for monitor system", "No"
   "metanode_totalMem","string", "Max memory metadata used. The value needs to be higher than the value of *metaNodeReservedMem* in the master configuration. Unit: byte", "Yes"

- **#objectnode config** module includes parameters of ObjectNodes.

.. csv-table::
   :header: "Key", "Type", "Description", "Mandatory"

   "objectnode_listen", "string", "Listen and accept port of the server", "Yes"
   "objectnode_domains", "string slice", "
   | Domain of S3-like interface which makes wildcard domain support
   | Format: ``DOMAIN``", "No"
   "objectnode_logDir", "string", "Log directory", "Yes"
   "objectnode_logLevel", "string", "
   | Level operation for logging.
   | Default: ``error``", "No"
   "objectnode_exporterPort", "string", "Port for monitor system", "No"
   "objectnode_enableHTTPS", "string", "Enable HTTPS", "Yes"

- **#client config** module includes parameters of Client.

.. csv-table:: Properties
   :header: "Key", "Type", "Description", "Mandatory"

   "client_mountPoint", "string", "Mount point of the client", "Yes"
   "client_volName", "string", "Volume name", "No"
   "client_owner", "string", "Owner id", "Yes"
   "client_SizeGB", "string", "Initial size to create the volume if it not exists", "No"
   "client_logDir", "string", "Path for log file storage", "Yes"
   "client_logLevel", "string", "Level operation for logging. Default is *info*", "No"
   "client_exporterPort", "string", "Port exposed to monitor system", "Yes"
   "client_profPort", "string", "Pprof port", "No"

.. code-block:: yaml

    [master]
    10.196.59.198
    10.196.59.199
    10.196.59.200
    [datanode]
    ...
    [cfs:vars]
    ansible_ssh_port=22
    ansible_ssh_user=root
    ansible_ssh_pass="password"
    ...
    #master config
    ...
    #datanode config
    ...
    datanode_disks =  '"/data0:10737418240","/data1:10737418240"'
    ...
    #metanode config
    ...
    metanode_totalMem = "28589934592"
    ...
    #objectnode config
    ...

For more configurations, please refer to :doc:`master`; :doc:`metanode`; :doc:`datanode`; :doc:`client`; :doc:`monitor`; :doc:`console`.
CubeFS supports depoly multi-module on the same server. Pay attention to modifying the port configuration of each module to avoid port conflicts in this way.

Start the resources of CubeFS cluster with script **install.sh** . (make sure the Master is started first)

.. code-block:: bash

    $ bash install.sh -h
    Usage: install.sh -r | --role [datanode | metanode | master | objectnode | monitor | client | all | createvol ]
    $ bash install.sh -r master
    $ bash install.sh -r metanode
    $ bash install.sh -r datanode
    $ bash install.sh -r objectnode
    $ bash install.sh -r monitor
    $ bash install.sh -r createvol
    $ bash install.sh -r client

Check mount point at **/cfs/mountpoint** on **client** node defined in **iplist** .

Open http://consul.prometheus-cfs.local in browser for monitoring system(the IP of monitoring system is defined in **iplist** ).
