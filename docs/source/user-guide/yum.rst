Run Cluster by Yum Tools
=========

Yum tools to run a ChubaoFS cluster for CentOS 7+ is provided. The list of RPM packages dependencies can be installed with:

.. code-block:: bash

    $ yum install http://storage.jd.com/chubaofsrpm/latest/cfs-install-latest-el7.x86_64.rpm
    $ cd /cfs/install
    $ tree -L 2
    .
    ├── install_cfs.yml
    ├── install.sh
    ├── iplist
    ├── src
    └── template
        ├── client.json.j2
        ├── create_vol.sh.j2
        ├── datanode.json.j2
        ├── grafana
        ├── master.json.j2
        └── metanode.json.j2

Set parameters of the ChubaoFS cluster in **iplist**.

- Set IP addresses in **[master]**, **[datanode]** , **[metanode]** , **[monitor]** , **[client]** field;

- Set **datanode_disks** in  **#datanode config** field. Make sure the path exists on each DataNode and has at least 30GB of space.

- Unify the username and password of each node, and set the username and password in **[cfs:vars]** field.

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
    #datanode config
    ...
    datanode_disks =  '"/data0:10737418240","/data1:10737418240"'
    ...

Start the resources of ChubaoFS cluster with script **install.sh** . (make sure the Master is started first)

.. code-block:: bash

    $ bash install.sh -h
    Usage: install.sh [-r --role datanode or metanode or master or monitor or client or all ] [-v --version 1.5.1 or latest]
    $ bash install.sh -r master
    $ bash install.sh -r metanode
    $ bash install.sh -r datanode
    $ bash install.sh -r monitor
    $ bash install.sh -r client

Check mount point at **/cfs/mountpoint** on **client** node defined in **iplist** .

Open http://consul.prometheus-cfs.local in browser for monitoring system(the IP of monitoring system is defined in **iplist** ).
