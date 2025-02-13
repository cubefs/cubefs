# LcNode Configuration
## Configuration Description

| Parameter           | Type           | Description                                                              | Required  | Default Value     |
|:--------------|:--------------|:-----------------------------------------------------------------|:-------| :--------- |
| role         | string       | Process role, must be set to `lcnode`                                         | Yes   |       |
| listen       | string       | Port number for HTTP service listening. Format: `PORT`                   | Yes      |   80    |
| logDir       | string       | Path to store logs                                                          | Yes   |       |
| logLevel     | string       | Log level                                                                   | No   |   error    |
| masterAddr   | string slice | Format: `HOST:PORT`, HOST: Resource management node IP (Master), PORT: Resource management node service port (Master) | Yes   |       |
| prof         | string       | Debugging and administrator API interface                                   | No   |       |
| lcScanRoutineNumPerTask | int       | Number of concurrent file migration                                 | No   |     20     |
| lcScanLimitPerSecond    | int       | QPS limit of file migration                                         | No   |    0 (no limit)      |
| delayDelMinute | int       | Lifecycle Source data retention period after a file is migrated               | No   |     1440     |
| useCreateTime | bool       | Use file create time to determine expiration. By default, use file access time to determine expiration  | No   |     false     |

## Configuration Example

``` json
{
    "role": "lcnode",
    "listen": "17510",
    "logDir": "./logs",
    "logLevel": "info",
    "masterAddr": [
        "xxx",
        "xxx",
        "xxx"
    ]
}
```