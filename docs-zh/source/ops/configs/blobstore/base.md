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
    "log_format": "日志输出格式，有 text 和 json"
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
    "compress": "保留日志是否开启压缩"
  }
}
```
