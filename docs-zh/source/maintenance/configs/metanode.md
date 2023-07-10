# MetaNode 配置

## 配置说明

| 配置项                 | 类型           | 描述                                               | 必需 |
|---------------------|--------------|--------------------------------------------------|----|
| role                | string       | 进程角色： *MetaNode*                                 | 是  |
| listen              | string       | 监听和接受请求的端口                                       | 是  |
| prof                | string       | 调试和管理员API接口                                      | 是  |
| logLevel            | string       | 日志级别，默认: *error*                                 | 否  |
| metadataDir         | string       | 元数据快照存储目录                                        | 是  |
| logDir              | string       | 日志存储目录                                           | 是  |
| raftDir             | string       | raft wal日志目录                                     | 是  |
| raftHeartbeatPort   | string       | raft心跳通信端口                                       | 是  |
| raftReplicaPort     | string       | raft数据传输端口                                       | 是  |
| consulAddr          | string       | prometheus注册接口                                   | 否  |
| exporterPort        | string       | prometheus获取监控数据端口                               | 否  |
| masterAddr          | string slice | master服务地址                                       | 是  |
| totalMem            | string       | 最大可用内存，此值需高于master配置中metaNodeReservedMem的值，单位：字节 | 是  |
| memRatio            | string       | 最大可用内存占主机总内存的比例。若填写该项，则计算出的值将会覆盖`totalMem`配置项    | 否  |
| localIP             | string       | 本机ip地址，如果不填写该选项，则使用和master通信的ip地址                | 否  |
| bindIp              | bool         | 是否仅在本机ip上监听连接，默认`false`                          | 否  |
| zoneName            | string       | 指定区域，默认分配至`default`区域                            | 否  |
| deleteBatchCount    | int64        | 一次性批量删除多少inode节点，默认`500`                         | 否  |
| tickInterval        | float64      | raft检查心跳和选举超时的间隔，单位毫秒，默认`300`                    | 否  |
| raftRecvBufSize     | int          | raft接收缓冲区大小，单位：字节，默认`2048`                       | 否  |
| nameResolveInterval | int          | raft节点地址解析间隔，单位：分钟，值应当介于[1-60]之间，默认`1`           | 否  |

## 配置示例

``` json
{
     "role": "metanode",
     "listen": "17210",
     "prof": "17220",
     "logLevel": "debug",
     "localIP":"127.0.0.1",
     "metadataDir": "/cfs/metanode/data/meta",
     "logDir": "/cfs/metanode/log",
     "raftDir": "/cfs/metanode/data/raft",
     "raftHeartbeatPort": "17230",
     "raftReplicaPort": "17240",
     "consulAddr": "http://consul.prometheus-cfs.local",
     "exporterPort": 9501,
     "totalMem":  "8589934592",
     "masterAddr": [
         "127.0.0.1:17010",
         "127.0.0.2:17010",
         "127.0.0.3:17010"
     ]
 }
```

## 注意事项

-   `listen`、`raftHeartbeatPort`、`raftReplicaPort`这三个配置选项在程序首次配置启动后，不能修改
-   相关的配置信息被记录在`metadataDir`目录下的`constcfg`文件中，如果需要强制修改，需要手动删除该文件
-   上述三个配置选项和`MetaNode`在`Master`的注册信息有关。如果修改，将导致`Master`无法定位到修改前的`MetaNode`信息