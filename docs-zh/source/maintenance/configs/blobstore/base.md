# 纠删码基础配置

基础配置是每个模块共有的配置，主要包括服务端口、日志以及审计日志等

```json
{
  "bind_addr": "主机：端口",
  "auditlog": {
    "logdir": "审计日志路径",
    "chunkbits": "审计日志文件大小，大小等于2^chunkbits个字节",
    "rotate_new": "是否开启新的日志文件用于每次重启，true 或者 false",
    "log_file_suffix": "日志文件后缀，实例`.log`",
    "backup": "保留文件个数，不配或0，表示无限制",
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
    "keywords_filter": "关键字过滤，目前支持url和请求方法过滤，如/download、get等"
  },
  "auth": {
    "enable_auth": "是否开启鉴权，true或者false，默认false",
    "secret": "鉴权密钥"
  },
  "shutdown_timeout_s": "停服务超时时间",
  "log":{
    "level": "日志级别，debug,info,warn,error,panic,fatal", 
    "filename": "日志存放路径",
    "maxsize": "每个日志文件的大小",
    "maxage": "保留天数",
    "maxbackups": "保留日志文件个数"
  }
}
```