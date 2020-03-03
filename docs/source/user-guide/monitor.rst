Monitor
-----------------------

ChubaoFS use prometheus as metrics collector. It simply config as follow in master，metanode，datanode，client's config file：

.. code-block:: json

   {
       "exporterPort": 9505,
       "consulAddr": "http://consul.prometheus-cfs.local"
   }


* exporterPort：prometheus exporter Port. when set，can export prometheus metrics from URL(http://$hostip:$exporterPort/metrics). If not set, prometheus exporter will unavailable；
* consulAddr: consul register address，it can work with prometheus to auto discover deployed chubaofs nodes, if not set, consul register will not work.

Using grafana as prometheus metrics web front：

.. image:: ../pic/cfs-grafana-dashboard.png
   :align: center


Also, user can use prometheus alertmanager to capture and route alerts to the correct receiver. please visit prometheus alertmanger `web-doc <https://prometheus.io/docs/alerting/alertmanager/>`_

Grafana DashBoard Config:

.. literalinclude:: cfs-grafana-dashboard.json
   :language: json
