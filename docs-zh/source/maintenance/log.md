# 日志处理
## 日志种类
1.metaNode,dataNode,master都存在两类日志，分别为进程服务日志和raft日志，client由于没用到raft，所以只有进程服务日志
2.各个服务日志以及raft日志的日志路径都是可以配置的，具体在启动配置文件的如下字段:
```
{
    "logDir": "/cfs/log",
    "raftDir": "/cfs/log",
    ......
}
```


## 日志设置

1. 如果您是开发及测试人员，希望进行调试，可以将日志级别设置为Debug或者info。  
2. 如果生产环境，可以将日志级别设置为warn或者error,将大大减少日志的量。 
3. 支持的 log-level 有 debug,info,warn,error,fatal,critical。

日志的设置有2种方式：
- 在配置文件中设置，具体如下：
```
"logLevel": "debug"
```
- 可以通过命令动态的修改，命令如下
```
http://127.0.0.1:{profPort}/loglevel/set?level={log-level}
```
## 日志格式
日志格式为如下格式
```
[时间][日志级别][日志路径及行数][详细信息]
举例说明：
2023/03/08 18:38:06.628192 [ERROR] partition.go:664: action[LaunchRepair] partition(113300) err(no valid master).
```

