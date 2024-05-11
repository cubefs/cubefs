{
  "role": "datanode",
  "listen": "17310",
  "localIP": "_ip_",  
  "bindIp": "true",
  "enableRdma": "true",
  "rdmaPort": "18515",
  "rdmaIP": "_rdma_",
  "rdmaMemBlockNum": "40960",
  "rdmaMemBlockSize": "131072",
  "rdmaMemPoolLevel": "18",
  "rdmaHeaderBlockNum": "32768",
  "rdmaHeaderPoolLevel": "15",
  "rdmaResponseBlockNum": "32768",
  "rdmaResponsePoolLevel": "15",
  "wqDepth": "32",
  "minCqeNum": "1024",
  "raftHeartbeat": "17330",
  "raftReplica": "17340",
  "raftDir": "_dir_/raftlog/datanode",
  "logDir": "_dir_/logs",
  "warnLogDir": "_dir_/logs",
  "logLevel": "debug",
  "disks": [
  	"_dir_/disk:2048"
  ],
  "enableSmuxConnPool": "true",
  "masterAddr": [
      _master_addr_
]
}
