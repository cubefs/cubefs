# 纠删码基础配置

基础配置是每个模块共有的配置，主要包括服务端口、日志以及审计日志等

```json
{
  "max_procs": "最大cpu核心数",
  "bind_addr": "主机：端口",
  "shutdown_timeout_s": "停服务超时时间",
  "auditlog": {
    "logdir": "审计日志路径",
    "chunkbits": "审计日志文件大小，大小等于2^chunkbits个字节",
    "bodylimit": "缓存可读body信息大小",
    "rotate_new": "是否开启新的日志文件用于每次重启，true 或者 false",
    "log_file_suffix": "日志文件后缀，实例`.log`",
    "backup": "保留文件个数，不配或0，表示无限制",
    "log_format": "使用text或者json格式，默认text格式",
    "metric_config": {
      "idc": "机房编号",
      "service": "服务名",
      "tag": "标签",
      "team": "团队",
      "enable_req_length_cnt": "是否启用请求长度统计，true或者false,默认false",
      "enable_resp_length_cnt": "是否启用响应长度统计，true或者false,默认false",
      "enable_resp_duration": "是否启用响应时延，true或者false,默认false",
      "max_api_level": "api最大层级数，如/get/name为2"
    },
    "filters": "按照日志字段多条件组合匹配过滤日志",
    "metrics_filter": "是否开启metric filter",
    "log_format": "日志输出格式，有 text 和 json",
    "log_async_queue_size": "审计日志异步队列条数：0（默认）不开启异步，同步等待写盘返回（与历史行为一致）；<0 开启异步并用默认队列长度4096；>0 开启异步并用该值作为队列长度，最大1048576（超出会被截断并打告警日志）。修改后需重启。对 blobstore 服务、SDK 及 ObjectNode 本地审计（auditLog.local 段）生效。异步模式下 Close（含 SDK）最多阻塞 5 秒",
    "log_async_drop": "异步队列满时是否丢弃而非阻塞等待，默认false：不丢弃（兼容历史写盘，不丢日志），审计盘卡死时请求线程会被拖住（与同步模式同样的风险）；true：丢弃，业务不等待审计盘。仅 log_async_queue_size != 0 时生效"
  },
  "auth": {
    "enable_auth": "是否开启鉴权，true或者false，默认false",
    "secret": "鉴权密钥"
  },
  "log":{
    "level": "日志级别，debug,info,warn,error,panic,fatal", 
    "filename": "日志存放路径",
    "maxsize": "每个日志文件的大小",
    "maxage": "保留天数",
    "maxbackups": "保留日志文件个数",
    "localtime": "保留日志使用本地时间",
    "compress": "保留日志是否开启压缩",
    "queuesize": "异步日志缓存队列大小",
    "writetimeout": "异步日志单次写入超时时间",
    "drop": "队列满时丢弃日志级别小于warn的条目"
  }
}
```
