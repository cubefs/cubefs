# DataNode 配置
## 配置说明

| 关键字           | 参数类型         | 描述                         | 是否必要                         |
|---------------|--------------|----------------------------|------------------------------|
| role          | string       | Role必须配置为“datanode”        | 是                            |
| listen        | string       | 数据节点作为服务端启动TCP监听的端口        | 是                            |
| localIP       | string       | 数据节点作为服务端选用的IP             | 否                            |
| prof          | string       | 数据节点提供HTTP接口所用的端口          | 是                            |
| logDir        | string       | 调测日志存放的路径                  | 是                            |
| logLevel      | string       | 调测日志的级别。默认是error           | 否                            |
| raftHeartbeat | string       | RAFT发送节点间心跳消息所用的端口         | 是                            |
| raftReplica   | string       | RAFT发送日志消息所用的端口            | 是                            |
| raftDir       | string       | RAFT调测日志存放的路径。默认在二进制文件启动路径 | 否                            |
| consulAddr    | string       | 监控系统的地址                    | 否                            |
| exporterPort  | string       | 监控系统的端口                    | 否                            |
| masterAddr    | string slice | 集群管理器的地址                   | 是                            |
| localIP       | string       | 本机ip地址                     | 否，如果不填写该选项，则使用和master通信的ip地址 |
| zoneName      | string       | 指定区域                       | 否，默认分配至`default`区域           |
| disks         | string slice | \                          | 格式：\*磁盘挂载路径:预留空间\* \         | 预留空间配置范围\[20G,50G\] | 是                                               |

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
     "disks": [
         "/data0:10737418240",
         "/data1:10737418240"
     ]
}
```

## 注意事项

> -   listen、raftHeartbeat、raftReplica这三个配置选项在程序首次配置启动后，不能修改；
> -   相关的配置信息被记录在raftDir目录下的constcfg文件中，如果需要强制修改，需要手动删除该文件；
> -   上述三个配置选项和datanode在master的注册信息有关。如果修改，将导致master无法定位到修改前的datanode信息；