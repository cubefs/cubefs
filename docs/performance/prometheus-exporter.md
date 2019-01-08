## prometheus exporter

Container File System（as：CFS） use prometheus as metrics collector. It config as follow in master，metanode，datanode，client：

```json
{
    "exporterPort": 9510,
    "consulAddr": "http://127.0.0.1:8500"
}
```

as：

* exporterPort：prometheus exporter Port. when set，can export prometheus metrics from URL(http://$hostip:$exporterPort/metrics). If not set, prometheus exporter will unavailable；
* consulAddr: consul register address，it can work with prometheus to auto discover deployed cfs nodes, if not set, consul register will not work.

You can use grafana as prometheus metrics web front as follow：

![cfs-grafana-dasshboard](../assert/cfs-grafana-dashboard.png)

import grafana DashBoard config [cfs-grafana-dashboard.json](cfs-grafana-dashboard.json).

