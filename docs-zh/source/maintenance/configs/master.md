# Master 配置
## 配置说明

CubeFS 使用 **JSON** 作为配置文件的格式.

| 配置项                                 | 类型     | 描述                                         | 必需 | 默认值        |
|-------------------------------------|--------|--------------------------------------------|----|------------|
| role                                | string | 进程的角色，值只能是 master                          | 是  |            |
| ip                                  | string | 主机ip                                       | 是  |            |
| listen                              | string | http服务监听的端口号                               | 是  |            |
| prof                                | string | golang pprof 端口号                           | 是  |            |
| id                                  | string | 区分不同的master节点                              | 是  |            |
| peers                               | string | raft复制组成员信息                                | 是  |            |
| logDir                              | string | 日志文件存储目录                                   | 是  |            |
| logLevel                            | string | 日志级别                                       | 否  | error      |
| retainLogs                          | string | 保留多少条raft日志.                               | 是  |            |
| walDir                              | string | raft wal日志存储目录.                            | 是  |            |
| storeDir                            | string | RocksDB数据存储目录.此目录必须存在，如果目录不存在，无法启动服务       | 是  |            |
| clusterName                         | string | 集群名字                                       | 是  |            |
| ebsAddr                             | string | 纠删码子系统的地址，使用纠删码子系统时需配置                     | 否  |            |
| exporterPort                        | int    | prometheus获取监控数据端口                         | 否  |            |
| consulAddr                          | string | consul注册地址，供prometheus exporter使用          | 否  |            |
| metaNodeReservedMem                 | string | 元数据节点预留内存大小，单位：字节                          | 否  | 1073741824 |
| heartbeatPort                       | string | raft心跳通信端口                                 | 否  | 5901       |
| replicaPort                         | string | raft数据传输端口                                 | 否  | 5902       |
| nodeSetCap                          | string | NodeSet的容量                                 | 否  | 18         |
| missingDataPartitionInterval        | string | 当此时间段内没有收到副本的心跳，该副本被认为已丢失，单位：s             | 否  | 24h        |
| dataPartitionTimeOutSec             | string | 当此时间段内没有收到副本的心跳，该副本被认为非存活，单位：s             | 否  | 10min      |
| numberOfDataPartitionsToLoad        | string | 一次最多检查多少数据分片                               | 否  | 40         |
| secondsToFreeDataPartitionAfterLoad | string | 在多少秒之后开始释放由加载数据分片任务占用的内存                   | 否  | 300        |
| tickInterval                        | string | 检查心跳和选举超时的计时器间隔，单位：ms                      | 否  | 500        |
| electionTick                        | string | 在计时器重置多少次时，选举超时                            | 否  | 5          |
| bindIp                              | bool   | 是否仅在主机ip上监听连接                              | 否  | false      |
| faultDomain                         | bool   | 是否启用故障域                                    | 否  | false      |
| faultDomainBuildAsPossible          | bool   | 若可用的故障域数量少于预期的故障域数量，是否仍尽可能地去构建nodeSetGroup | 否  | false      |
| faultDomainGrpBatchCnt              | string | 可用的故障域数量                                   | 否  | 3          |
| dpNoLeaderReportIntervalSec         | string | 数据分片没有leader时，多久上报一次，单位：s                  | 否  | 60         |
| mpNoLeaderReportIntervalSec         | string | 元数据分片没有leader时，多久上报一次，单位：s                 | 否  | 60         |
| maxQuotaNumPerVol                   | string | 单个卷最大的配额数                                  | 否  | 100        |

## 配置示例

``` json
{
 "role": "master",
 "id":"1",
 "ip": "127.0.0.1",
 "listen": "17010",
 "prof":"17020",
 "peers": "1:127.0.0.1:17010,2:127.0.0.2:17010,3:127.0.0.3:17010",
 "retainLogs":"20000",
 "logDir": "/cfs/master/log",
 "logLevel":"info",
 "walDir":"/cfs/master/data/wal",
 "storeDir":"/cfs/master/data/store",
 "exporterPort": 9500,
 "consulAddr": "http://consul.prometheus-cfs.local",
 "clusterName":"cubefs01",
 "metaNodeReservedMem": "1073741824"
}
```