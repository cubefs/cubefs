# 日志配置选项

## 描述

| 关键字           |参数类型| 描述                                                                 | 必需   |
|:-----------------|:-------|:---------------------------------------------------------------------|:-------|
| logDir           | string | 日志存放目录                                                         | 否     |
| logLevel         | string | 日志级别                                                             | 否     |
| logMaxSize       | int    | 单个日志文件最大大小(MB) (default 1024 in MB)                        | 否     |
| logMaxAge        | int    | 最大保留天数 (default 0)                                             | 否     |
| logMaxBackups    | int    | 最多保留日志数 (default 0)                                           | 否     |
| logReservedRatio | float  | 保留日志磁盘空闲率,`logReservedSize` 没设置时启用 (default 0.0)      | 否     |
| logReservedSize  | int    | 日志磁盘空闲容量 (default 4096)                                      | 否     |
| logLocalTime     | bool   | 是否启用本地时间 (default true)                                      | 否     |
| logCompress      | bool   | 是否启用gzip压缩 (default false)                                     | 否     |

## 示例

``` json
{
  "logDir": "/tmp/logs/app",
  "logLevel": "info",
  "logMaxSize": 1024,
  "logMaxAge": 30,
  "logMaxBackups": 10,
  "logReservedRatio": 0.2,
  "logReservedSize": 10240,
  "logLocalTime": true,
  "logCompress": true
}
```
