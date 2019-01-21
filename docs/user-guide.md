## Monitor

CFS use prometheus as metrics collector. It simply config as follow in master，metanode，datanode，client's config file：

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


## Build Servers

In the CFS design, servers refer to master, metanode and datanode, and are compiled into a single binary for deployment convenience.
You'll need to build RocksDB v5.9.2+ [https://github.com/facebook/rocksdb/blob/master/INSTALL.md] on your machine.
Recommended for installation use  make static_lib command.After that, you can build cfs using the following command

```bash
cd cmd; go build cmd.go
```

## Build Client

Client is compiled separately.

```bash
cd client; go build
```

## Start Master

Example master.json is shown below:
```json
{
  "role": "master",
  "ip": "192.168.31.173",
  "port": "80",
  "prof":"10088",
  "id":"1",
  "peers": "1:192.168.31.173:80,2:192.168.31.141:80,3:192.168.30.200:80",
  "retainLogs":"20000",
  "logDir": "/export/Logs/cfs/master",
  "logLevel":"DEBUG",
  "walDir":"/export/Logs/cfs/raft",
  "storeDir":"/export/cfs/rocksdbstore",
  "consulAddr": "http://cbconsul-cfs01.cbmonitor.svc.ht7.n.jd.local",
  "exporterPort": 9510,
  "clusterName":"cfs"
}
```
For detailed explanations of *master.json*, please refer to [master.md](user-guide/master.md).
```bash
./cmd -c master.json
```

## Start Metanode
Example meta.json is shown below:

```json
{
    "role": "metanode",
    "listen": "9021",
    "prof": "9092",
    "logLevel": "debug",
    "metaDir": "/export/cfs/metanode_meta",
    "logDir": "/export/Logs/cfs/metanode",
    "raftDir": "/export/cfs/metanode_raft",
    "raftHeartbeatPort": "9093",
    "raftReplicatePort": "9094",
    "consulAddr": "http://cbconsul-cfs01.cbmonitor.svc.ht7.n.jd.local",
    "exporterPort": 9511,
    "masterAddrs": [
        "192.168.31.173:80",
        "192.168.31.141:80",
        "192.168.30.200:80"
    ]
}
```
For detailed explanations of *meta.json*, please refer to [metanode.md](user-guide/metanode.md).
```bash
./cmd -c meta.json
```
## Start Datanode

1. Prepare config file

Example *datanode.json* is shown below:

```json
{
  "role": "datanode",
  "port": "6000",
  "prof": "6001",
  "logDir": "/export/Logs/datanode",
  "logLevel": "info",
  "raftHeartbeat": "9095",
  "raftReplicate": "9096",
  "consulAddr": "http://cbconsul-cfs01.cbmonitor.svc.ht7.n.jd.local",
  "exporterPort": 9512,
  "masterAddr": [
  "192.168.31.173:80",
  "192.168.31.141:80",
  "192.168.30.200:80"
  ],
  "rack": "",
  "disks": [
  "/data0:1:40000"
  ]  
}
```

2. Start the datanode

```bash
nohup ./cmd -c datanode.json &
```

For detailed explanations of *datanode.json*, please refer to [datanode.md](user-guide/datanode.md).

## Create Volume

## Allocate Logical Space

## Mount Client

1. Run "modprobe fuse" to insert FUSE kernel module.
2. Run "rpm -i fuse-2.9.2-8.el7.x86_64.rpm" to install libfuse.
3. Run "nohup client -c fuse.json &" to start a client.
4. Operations to the *mountpoint* specified in *fuse.json* will be performed on CFS.

Example *fuse.json* is shown below:

```text
{
  "mountpoint": "/mnt/fuse",
  "volname": "test",
  "master": "192.168.31.173:80,192.168.31.141:80,192.168.30.200:80",
  "logpath": "/export/Logs/cfs",
  "profport": "10094",
  "loglvl": "info"
}
```

For detailed explanations of *fuse.json*, please refer to [client.md](user-guide/client.md).

For optimizing FUSE kernel to achive better performance, please refer to [fuse.md](performance/fuse.md).

Note that end user can start more than one client on a single machine, as long as mountpoints are different.
