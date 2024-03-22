# Log Configuration Options

## Description

| Name             | Type   | Description                                                                           | Required |
|:-----------------|:-------|:--------------------------------------------------------------------------------------|:---------|
| logDir           | string | Log directory                                                                         | No       |
| logLevel         | string | Log level: debug, info, warn, error, panic, fatal                                     | No       |
| logMaxSize       | int    | The maximum size in megabytes of the log file. (default 1024 in MB)                   | No       |
| logMaxAge        | int    | The maximum number of days to retain old log files. (default 0)                       | No       |
| logMaxBackups    | int    | The maximum number of old log files to retain. (default 0)                            | No       |
| logReservedRatio | float  | The ratio left space of the store device, if no `logReservedSize`. (default 0.0)      | No       |
| logReservedSize  | int    | The minimum left space in megabytes of the store device. (default 4096 in MB)         | No       |
| logLocalTime     | bool   | The computer's local time used for formatting in backup files. (default true)         | No       |
| logCompress      | bool   | The rotated log files should be compressed using gzip. (default false)                | No       |

## Example

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
