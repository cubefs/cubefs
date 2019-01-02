# Buiding and Deployment

## Building

### Building servers

In the CFS design, servers refer to master, metanode and datanode, and are compiled into a single binary for deployment convenience.

```bash
cd cmd; go build cmd.go
```

### Building client

Client is compiled separately.

```bash
cd client; go build
```

## Deployment

### Start master

### Start metanode
Example meta.json is shown below:

```json
{
    "role": "metanode",
    "listen": "9021",
    "prof": "9092",
    "logLevel": "debug",
    "metaDir": "/var/cfs/metanode_meta",
    "logDir": "/var/log/cfs",
    "raftDir": "/var/cfs/metanode_raft",
    "raftHeartbeatPort": "9093",
    "raftReplicatePort": "9094",
    "masterAddrs": [
         "192.168.1.10:80",
        "192.168.1.200:80"
    ]
}
```
For detailed explanations of *meta.json*, please refer to [meta.md](meta.md).
```bash
./cmd -c meta.json
```
### Start datanode

1. Prepare config file

Example *datanode.json* is shown below:

```json
{
  "role": "datanode",
  "port": "6000",
  "prof": "6001",
  "logDir": "/export/App/datanode/logs",
  "logLevel": "info",
  "raftHeartbeat": "9095",
  "raftReplicate": "9096",
  "masterAddr": [
  "11.3.26.134:80",
  "11.3.26.102:80",
  "11.3.26.104:80"
  ],
  "rack": "",
  "disks": [
  "/export/App/datanode/data:1:40000"
  ]  
}
```

2. Start the datanode

```bash
nohup ./cmd -c datanode.json &
```

For detailed explanations of *datanode.json*, please refer to [start_datanode.md](start_datanode.md).

### Create a volume

### Allocate logical space

### Mount a client

1. Run "modprobe fuse" to insert FUSE kernel module.
2. Run "rpm -i fuse-2.9.2-8.el7.x86_64.rpm" to install libfuse.
3. Run "nohup client -c fuse.json &" to start a client.
4. Operations to the *mountpoint* specified in *fuse.json* will be performed on CFS.

Example *fuse.json* is shown below:

```text
{
  "mountpoint": "/mnt/foo",
  "volname": "foo",
  "master": "11.3.26.134:80,11.3.26.102:80,11.3.26.104:80",
  "logpath": "/export/Logs/cfs",
  "profport": "10094",
  "loglvl": "info"
}
```

For detailed explanations of *fuse.json*, please refer to [client.md](client.md).

For optimizing FUSE kernel to achive better performance, please refer to [fuse.md](fuse.md).

Note that end user can start more than one client on a single machine, as long as mountpoints are different.
