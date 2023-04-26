# ObjectNode Configuration
## Configuration Description

| Parameter    | Type         | Description                                                                                                           | Required |
|--------------|--------------|-----------------------------------------------------------------------------------------------------------------------|----------|
| role         | string       | Process role, must be set to `objectnode`                                                                             | Yes      |
| listen       | string       | IP address and port number for HTTP service listening. Format: `IP:PORT` or `:PORT`, default: `:80`                   | Yes      |
| domains      | string slice | Configure domain names for S3-compatible interfaces to support DNS-style access to resources. Format: `DOMAIN`        | No       |
| logDir       | string       | Path to store logs                                                                                                    | Yes      |
| logLevel     | string       | Log level, default: `error`                                                                                           | No       |
| masterAddr   | string slice | Format: `HOST:PORT`, HOST: Resource management node IP (Master), PORT: Resource management node service port (Master) | Yes      |
| exporterPort | string       | Port for Prometheus to obtain monitoring data                                                                         | No       |
| prof         | string       | Debugging and administrator API interface                                                                             | Yes      |

## Configuration Example

``` json
{
     "role": "objectnode",
     "listen": "17410",
     "domains": [
         "object.cfs.local"
     ],
     "logDir": "/cfs/Logs/objectnode",
     "logLevel": "info",
     "masterAddr": [
         "10.196.59.198:17010",
         "10.196.59.199:17010",
         "10.196.59.200:17010"
     ],
     "exporterPort": 9503,
     "prof": "7013"
}
```