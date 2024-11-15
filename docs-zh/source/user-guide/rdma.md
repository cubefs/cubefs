# 使用RDMA

服务端datanode, metanode的网络同时支持RDMA和TCP的侦听服务。其中TCP的服务是一直开启的，RDMA的服务可以通过配置项开启或者关闭。客户端fuse Client目前只能选择使用RDMA或者TCP。

### 客户端的配置
客户端的配置文件里面添加这些和RDMA相关的选项:
- enableDataRdma: client和datanode直接的数据通信采用RDMA方式。
- enableMetaRdma: client和metanode直接的数据通信采用RDMA方式。
- rdmaDpPort: datanode侦听的RDMA端口号。
- rdmaMpPort: metanode侦听的RDMA端口号。
- rdmaMemBlockNum: buddy所使用的内存池的块数量。
- rdmaMemBlockSize: buddy所使用的内存池的块大小。
- rdmaMemPoolLevel: buddy所能管理的最大块数量为2的rdmaMemPoolLevel次方。
- rdmaConnDataSize: 每个连接分配的发送和接收内存的大小。
- wqDepth: 一个RDMA链接中设置的WQ数量，这个值影响RDMA缓存的预分配和同时支持的并发数量。
- minCqeNum: 一个线程对应的完成队列设置的最大完成事件数量。
- rdmaLogLevel: 底层RDMA日志的级别。
- rdmaLogFile: 底层RDMA日志文件。
- workerNum: 工作线程的数量。
客户端配置的例子：
```json
{
  "masterAddr": "10.177.80.10:17010,10.177.80.11:17010,10.177.182.171:17010",
  "mountPoint": "/mnt/cubefs",
  "volName": "test",
  "owner": "test",
  "enableDataRdma": "true",
  "enableMetaRdma": "true",
  "rdmaDpPort": "17360",
  "rdmaMpPort": "17260",
  "rdmaMemBlockNum": "20971520",
  "rdmaMemBlockSize": "1025",
  "rdmaMemPoolLevel": "25",
  "rdmaConnDataSize": "4194304",
  "wqDepth": "32",
  "minCqeNum": "65536",
  "rdmaLogLevel": "error",
  "rdmaLogFile": "/home/service/client/logs/rdma.log",
  "workerNum": "4",
  "logDir": "/home/service/client/logs",
  "logLevel": "warn"
}
```

其中主要的参数配置是开启/关闭RDMA, 服务端RDMA的端口，以及日志的级别和存放位置。其它的参数可以使用默认配置。

### datanode的配置
datanode配置文件里面新增和RDMA相关的选项:
- enableRdma: 是否开启RDMA的侦听服务。true代表同时侦听RDMA和TCP服务。
- rdmaPort: 侦听的RDMA端口。
- rdmaIP: 侦听的RDMA地址，使用本机地址：127.0.0.1。
- rdmaMemBlockNum: buddy所使用的内存池的块数量。
- rdmaMemBlockSize: buddy所使用的内存池的块大小。
- rdmaMemPoolLevel: buddy所能管理的最大块数量为2的rdmaMemPoolLevel次方。
- rdmaConnDataSize: 每个连接分配的发送和接收内存的大小。
- wqDepth: 一个RDMA链接中设置的WQ数量，这个值影响RDMA缓存的预分配和同时支持的并发数量。
- minCqeNum: 一个线程对应的完成队列设置的最大完成事件数量。
- rdmaLogLevel: 底层RDMA日志的级别。
- rdmaLogFile: 底层RDMA日志文件。
- workerNum: 工作线程的数量。
datanode配置的例子：
```json
{
  "role": "datanode",
  "listen": "17310",
  "enableRdma": "true",
  "rdmaPort": "17360",
  "rdmaIP": "127.0.0.1",
  "rdmaMemBlockNum": "20971520",
  "rdmaMemBlockSize": "1025",
  "rdmaMemPoolLevel": "25",
  "rdmaConnDataSize": "4194304",
  "wqDepth": "32",
  "minCqeNum": "65536",
  "workerNum": "4",
  "rdmaLogLevel": "error",
  "rdmaLogFile": "/home/service/cfs-data/logs/rdma.log",
  "logDir": "./logs",
  "warnLogDir": "./logs",
  "logLevel": "warn",
  "disks":["/home/service/var/data:10737418240"],
  "masterAddr": ["master.wanyol.com:17010"]
}
```

### metanode的配置
metanode的RDMA相关选项和datanode是一样的:
metanode配置的例子：
```json
{
  "role": "metanode",
  "listen": "17210",
  "prof": "17220",
  "enableRdma": "true",
  "rdmaPort": "17260",
  "rdmaIP": "127.0.0.1",
  "rdmaMemBlockNum": "20971520",
  "rdmaMemBlockSize": "1025",
  "rdmaMemPoolLevel": "25",
  "rdmaConnDataSize": "4194304",
  "wqDepth": "32",
  "minCqeNum": "65536",
  "workerNum": "4",
  "rdmaLogLevel": "error",
  "rdmaLogFile": "/home/service/app/cfs-meta/logs/rdma.log",
  "raftHeartbeatPort": "17230",
  "raftReplicaPort": "17240",
  "exporterPort": 17250,
  "logLevel": "warn",
  "logDir": "./logs",
  "warnLogDir": "./logs",
  "memRatio": "70",
  "totalMem": "92122547200",
  "metadataDir": "/home/service/var/data/cfs/metanode/meta",
  "raftDir": "/home/service/var/data/cfs/metanode/raft",
  "consulAddr":"10.177.92.51:8500",
  "consulMeta":"dataset=custom;category=custom;app=cfs;appid=cfs-master;metric_path=/metrics;role=metanode;cluster=cfs_kerneltest1",
  "masterAddr": ["10.177.80.10:17010", "10.177.80.11:17010", "10.177.182.171:17010"]
}
```

### RDMA的使用条件
- 网卡设备支出RDMA，我们测试使用的网卡是：Mellanox Technologies MT27800 Family [ConnectX-5]
- 安装网卡驱动：MLNX_OFED_LINUX-5.6-2.0.9.0。建议用户安装MLNX_OFED_LINUX软件。
- 默认配置项里面设置RDMA缓存都比较大，一般达到4G。建议宿主机的内存在256G以上。

::: tip 提示
1. 如果客户端开启了RDMA功能，但是部分的网络报文还是TCP类型。这是因为这部分报文数据量很小，使用RDMA没有优势。
2. 每次切换客户端的网络模式都需要重启客户端。
3. 使用RDMA模式的客户端预先分配很大的内存，所以启动时会比TCP模式慢很多。
4. 如果enableRdma = true，并且宿主机上面没有支持RDMA的网卡，这个将会导致程序启动失败。这是因为初始化缓存时，会去寻找RDMA设备。

:::
