# 日志处理

## 日志种类
1. MetaNode,DataNode,Master都存在两类日志，分别为服务运行日志和raft日志，client由于没用到raft，所以只有进程服务日志
2. 各个服务日志以及raft日志的日志路径都是可以配置的，具体在启动配置文件的如下字段:
```
{
    "logDir": "/cfs/log",
    "raftDir": "/cfs/log",
    ......
}
```
3. ObjectNode存在一种日志类型，即服务运行日志
4. 纠删码子系统的各个模块均存在两类日志，分别为服务运行日志与审计日志，审计日志默认关闭，如果开启[请参考](./configs/blobstore/base.md)

## 日志设置

1. 如果您是开发及测试人员，希望进行调试，可以将日志级别设置为Debug或者info  
2. 如果生产环境，可以将日志级别设置为warn或者error，将大大减少日志的量 
3. 支持的 log-level 有 debug、info、warn、error、fatal、critical（纠删码系统不支持critical级别）

日志的设置有2种方式：

- 在配置文件中设置，具体如下：
```
"logLevel": "debug"
```
- 可以通过命令动态的修改，命令如下
```
http://127.0.0.1:{profPort}/loglevel/set?level={log-level}
```

::: tip 提示
纠删码的日志设置稍有不同
:::

- 配置文件中设置，[请参考](./configs/blobstore/base.md)
- 通过命令修改，[请参考](./admin-api/blobstore/base.md)


## 日志格式

日志格式为如下格式
```text
[时间][日志级别][日志路径及行数][详细信息]
举例说明：
2023/03/08 18:38:06.628192 [ERROR] partition.go:664: action[LaunchRepair] partition(113300) err(no valid master).
```

::: tip
纠删码系统的格式稍有不同，这里分别介绍运行日志与审计日志
:::

服务运行日志格式如下

``test
[时间][日志级别][日志路径及行数][TraceID:SpanID][详细信息]
2023/03/15 18:59:10.350557 [DEBUG] scheduler/blob_deleter.go:540 [tBICACl6si0FREwX:522f47d329a9961d] delete shards: location[{Vuid:94489280515 Host:http://127.0.0.1:8889 DiskID:297}]
``

审计日志格式如下

```text
[请求][服务名][请求时间][请求类型][请求接口][请求头部][请求参数][响应状态码][响应长度][请求耗时，单位微秒]
REQ	SCHEDULER	16793641137770897	POST	/inspect/complete	{"Accept-Encoding":"gzip","Content-Length":"90","Content-Type":"application/json","User-Agent":"blobnode/cm_1.2.0/5616eb3c957a01d189765cf004cd2df50bc618a8 (linux/amd64; go1.16.13)}	{"task_id":"inspect-45800-cgch04ehrnv40jlcqio0","inspect_err_str":"","missed_shards":null}	200	{"Blobstore-Tracer-Traceid":"0c5ebc85d3dba21b","Content-Length":"0","Trace-Log":["SCHEDULER"],"Trace-Tags":["span.kind:server"]}		0	68
```