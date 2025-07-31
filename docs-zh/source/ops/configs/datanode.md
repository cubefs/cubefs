# DataNode 配置
## 配置说明

| 关键字        | 参数类型     | 描述                                   | 必需   |
|:--------------|:-------------|:---------------------------------------|:-------|
| role          | string       | Role 必须配置为 “datanode”                   | 是   |
| listen        | string       | 数据节点作为服务端启动 TCP 监听的端口                   | 是   |
| localIP       | string       | 数据节点作为服务端选用的 IP                        | 否   |
| prof          | string       | 数据节点提供 HTTP 接口所用的端口                     | 是   |
| logDir        | string       | 调测日志存放的路径                             | 是   |
| logLevel      | string       | 调测日志的级别。默认是error                      | 否   |
| raftHeartbeat | string       | RAFT 发送节点间心跳消息所用的端口                    | 是   |
| raftReplica   | string       | RAFT 发送日志消息所用的端口                       | 是   |
| raftDir       | string       | RAFT 调测日志存放的路径。默认在二进制文件启动路径            | 否   |
| consulAddr    | string       | 监控系统的地址                               | 否   |
| exporterPort  | string       | 监控系统的端口                               | 否   |
| masterAddr    | string slice | 集群管理器的地址                              | 是   |
| localIP       | string       | 本机 ip 地址，如果不填写该选项，则使用和 master 通信的ip地址     | 否   |
| zoneName      | string       | 指定区域，默认分配至 `default` 区域                 | 否   |
| diskReadIocc  | int          | 限制单盘并发读操作,小于等于0表示不限制            | 否   |
| diskReadFlow  | int          | 限制单盘读流量,小于等于0表示不限制                | 否   |
| diskWriteIocc | int          | 限制单盘并发写操作,小于等于0表示不限制            | 否   |
| diskWriteFlow | int          | 限制单盘写流量,小于等于0表示不限制                | 否   |
| disks         | string slice | 格式：`磁盘挂载路径:预留空间` ，预留空间配置范围`[20G,50G]` | 是   |
| diskCurrentLoadDpLimit | int | 一个磁盘上并发加载的data partition的最大数量 | 否 |
| diskCurrentStopDpLimit | int | 一个磁盘上并发停止的data partition的最大数量 | 否 |
| enableLogPanicHook | bool | (实验性) Hook `panic` 函数以便在执行`panic`之前使日志落盘 | 否 |
| diskAsyncQosEnable | bool | 异步IO限制开关 | 否 |
| diskAsyncReadFlow | int | 限制单盘异步读流量,小于等于0表示不限制 | 否 |
| diskAsyncReadIocc | int | 限制单盘异步读并发,小于等于0表示不限制 | 否 |
| diskAsyncWriteFlow | int | 限制单盘异步写流量,小于等于0表示不限制 | 否 |
| diskAsyncWriteIocc | int | 限制单盘异步写并发,小于等于0表示不限制 | 否 |
| diskDeleteIocc | int | 限制单盘删除操作并发,小于等于0表示不限制 | 否 |
| diskDeleteIops | int | 限制单盘删除操作IOPS,小于等于0表示不限制 | 否 |
## 配置示例

``` json
{
     "role": "datanode",
     "listen": "17310",
     "prof": "17320",
     "logDir": "/cfs/datanode/log",
     "logLevel": "info",
     "raftHeartbeat": "17330",
     "raftReplica": "17340",
     "raftDir": "/cfs/datanode/log",
     "consulAddr": "http://consul.prometheus-cfs.local",
     "exporterPort": 9502,
     "masterAddr": [
         "10.196.59.198:17010",
         "10.196.59.199:17010",
         "10.196.59.200:17010"
     ],
     "diskReadIocc": 0,
     "diskReadFlow": 0,
     "diskWriteIocc": 0,
     "diskWriteFlow": 0,
     "diskAsyncQosEnable": true,
     "diskAsyncReadFlow": 0,
     "diskAsyncReadIocc": 0,
     "diskAsyncWriteFlow": 0,
     "diskAsyncWriteIocc": 0,
     "diskDeleteIocc": 0,
     "diskDeleteIops": 0,
     "disks": [
         "/data0:10737418240",
         "/data1:10737418240"
     ]
}
```

## 注意事项

-   listen、raftHeartbeat、raftReplica 这三个配置选项在程序首次配置启动后，不能修改
-   相关的配置信息被记录在 raftDir 目录下的 constcfg 文件中，如果需要强制修改，需要手动删除该文件
-   上述三个配置选项和 datanode 在 master 的注册信息有关。如果修改，将导致 master 无法定位到修改前的 datanode 信息
