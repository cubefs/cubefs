Monitor
-----------------------

CubeFS use prometheus as metrics collector. It simply config as follow in master, metanode, datanode, client's config file：

.. code-block:: json

   {
       "exporterPort": 9505,
       "consulAddr": "http://consul.prometheus-cfs.local",
       "consulMeta": "k1=v1;k2=v2",
       "ipFilter": "10.17.*",
       "enablePid": "false"
   }


* exporterPort：prometheus exporter Port. when set, can export prometheus metrics from URL(http://$hostip:$exporterPort/metrics). If not set, prometheus exporter will unavailable；
* consulAddr: consul register address, it can work with prometheus to auto discover deployed CubeFS nodes, if not set, consul register will not work.
* consulMeta: conusl meta config, not necessary, it can set meta info when register to consul.
* ipFilter: it's a ip regular filter, exposed to consul, not necessary, default empty, which is used for multiple ip machine. it can support positive or negative filter, for example:
    * ipFilter="10.17.*", means that ip, regular match ipFilter, is ok
    * ipFilter="!10.17.*" means that ip, not regular match ipFilter, is ok
* enablePid: whether to report partition id, default false; if you want to show dp or mp info in your cluster, you can set it true
Using grafana as prometheus metrics web front：

.. image:: ../pic/cfs-grafana-dashboard.png
   :align: center


Also, user can use prometheus alertmanager to capture and route alerts to the correct receiver. please visit prometheus alertmanger `web-doc <https://prometheus.io/docs/alerting/alertmanager/>`_

Metrics
>>>>>>>>>

- Cluster

    + The number of nodes:  ``MasterCount`` , ``MetaNodeCount`` , ``DataNodeCount`` , ``ObjectNodeCount``
    + The number of clients: ``ClientCount``
    + The number of volumes: ``VolumeCount``
    + The size of nodes: ``DataNodeSize`` , ``MetaNodeSize``
    + The used ratio of nodes: ``DataNodeUsedRatio`` , ``MetaNodeUsedRatio``
    + The number of inactive nodes: ``DataNodeInactive`` , ``MetaNodesInactive``
    + The capacity of volumes: ``VolumeTotalSize``
    + The used ratio of volumes: ``VolumeUsedRatio``
    + The number of error disks: ``DiskError``

- Volume

    + The used size of volumes: ``VolumeUsedSize``
    + The used ratio of volumes: ``VolumeUsedRatio``
    + The capacity change rate of volumes: ``VolumeSizeRate``

- Master

    + The number of invalid masters: ``master_nodes_invalid``
    + The number of invalid metanodes: ``metanode_inactive``
    + The number of invalid datanodes:： ``datanode_inactive``
    + The number of invalid clients:： ``fuseclient_inactive``

- MetaNode

    + The duration of each operation (Time) and the number of operations per second (Ops) on the metanode, which can be selected from the ``MetaNodeOp`` drop-down list.

- DataNode

    + The duration of each operation (Time) and the number of operations per second (Ops) on the datanode, which can be selected from the ``DataNodeOp`` drop-down list.

- ObjectNode

    + The duration of each operation (Time) and the number of operations per second (Ops) on the objectnode, which can be selected from the ``objectNodeOp`` drop-down list.

- FuseClient

    + The duration of each operation (Time) and the number of operations per second (Ops) on the client, which can be selected from the ``fuseOp`` drop-down list.

*Recommended focus metrics: cluster status, node or disk failure, total size, growth rate, etc.*


Grafana DashBoard Config
>>>>>>>>>>>>>>>>>>>>>>>>>>>

.. literalinclude:: cfs-grafana-dashboard.json
   :language: json
