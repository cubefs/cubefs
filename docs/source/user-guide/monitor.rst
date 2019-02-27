Monitor
-----------------------

CFS use prometheus as metrics collector. It simply config as follow in master，metanode，datanode，client's config file：

.. code-block:: json

   {
       "exporterPort": 9510,
       "consulAddr": "http://consul.prometheus-cfs.local"
   }


* exporterPort：prometheus exporter Port. when set，can export prometheus metrics from URL(http://$hostip:$exporterPort/metrics). If not set, prometheus exporter will unavailable；
* consulAddr: consul register address，it can work with prometheus to auto discover deployed cfs nodes, if not set, consul register will not work.

Using grafana as prometheus metrics web front：

.. image:: ../pic/cfs-grafana-dashboard.png
   :align: center   


Grafana DashBoard Config:

.. literalinclude:: cfs-grafana-dashboard.json
   :language: json
