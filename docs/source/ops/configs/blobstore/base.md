# Erasure Code Basic Configuration

The basic configuration is shared by each module and mainly includes server ports, logs, and audit logs.

```json
{
  "max_procs": "max processes",
  "bind_addr": "host:port",
  "shutdown_timeout_s": "service shutdown timeout",
  "auditlog": {
    "logdir": "audit log path",
    "chunkbits": "audit log file size, equal to 2^chunkbits bytes",
    "bodylimit": "body buffer size for readable body cache",
    "rotate_new": "whether to enable a new log file for each restart, true or false",
    "log_file_suffix": "log file suffix, for example `.log`",
    "backup": "number of files to keep, not set or 0 means no limit",
    "log_format": "Use text or JSON format, with text format being the default",
    "metric_config": {
      "idc": "IDC number",
      "service": "service name",
      "tag": "tag",
      "team": "team",
      "enable_req_length_cnt": "whether to enable request length statistics, true or false, default is false",
      "enable_resp_length_cnt": "whether to enable response length statistics, true or false, default is false",
      "enable_resp_duration": "whether to enable response latency, true or false, default is false",
      "max_api_level": "maximum API level, such as 2 for /get/name"
    },
    "filters": "Filter log by multi-criteria matching of log's fields",
    "metrics_filter": "enable metric filter or not",
    "log_format": "value is text or json, default is text"
  },
  "auth": {
    "enable_auth": "whether to enable authentication, true or false, default is false",
    "secret": "authentication key"
  },
  "log":{
    "level": "log level, debug, info, warn, error, panic, fatal",
    "filename": "log storage path",
    "maxsize": "maximum size of each log file",
    "maxage": "number of days to keep",
    "maxbackups": "number of log files to keep",
    "compress": "backup with compress"
  }
}
```
