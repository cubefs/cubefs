# Client Configuration
## Configuration Description

| Name          | Type   | Description                                                                                                               | Required |
|---------------|--------|---------------------------------------------------------------------------------------------------------------------------|----------|
| mountPoint    | string | Mount point                                                                                                               | Yes      |
| volName       | string | Volume name                                                                                                               | Yes      |
| owner         | string | Owner                                                                                                                     | Yes      |
| masterAddr    | string | Master node address                                                                                                       | Yes      |
| logDir        | string | Log directory                                                                                                             | No       |
| logLevel      | string | Log level: debug, info, warn, error                                                                                       | No       |
| profPort      | string | Golang pprof debug port                                                                                                   | No       |
| exporterPort  | string | Prometheus monitoring data port                                                                                           | No       |
| consulAddr    | string | Monitoring registration server address                                                                                    | No       |
| lookupValid   | string | Kernel FUSE lookup validity period, in seconds                                                                            | No       |
| attrValid     | string | Kernel FUSE attribute validity period, in seconds                                                                         | No       |
| icacheTimeout | string | Client inode cache validity period, in seconds                                                                            | No       |
| enSyncWrite   | string | Enable DirectIO synchronous write, i.e., force data node to write to disk with DirectIO                                   | No       |
| autoInvalData | string | Use the AutoInvalData option for FUSE mount                                                                               | No       |
| rdonly        | bool   | Mount in read-only mode, default is false                                                                                 | No       |
| writecache    | bool   | Use the write cache function of kernel FUSE module, requires kernel FUSE module support for write cache, default is false | No       |
| keepcache     | bool   | Keep kernel page cache. This function requires the writecache option to be enabled, default is false                      | No       |
| token         | string | If enableToken is enabled when creating a volume, fill in the token corresponding to the permission                       | No       |
| readRate      | int    | Limit the number of reads per second, default is unlimited                                                                | No       |
| writeRate     | int    | Limit the number of writes per second, default is unlimited                                                               | No       |
| followerRead  | bool   | Read data from follower, default is false                                                                                 | No       |
| accessKey     | string | Authentication key of the user to whom the volume belongs                                                                 | No       |
| secretKey     | string | Authentication key of the user to whom the volume belongs                                                                 | No       |
| disableDcache | bool   | Disable Dentry cache, default is false                                                                                    | No       |
| subdir        | string | Set subdirectory mount                                                                                                    | No       |
| fsyncOnClose  | bool   | Perform fsync operation after file is closed, default is true                                                             | No       |
| maxcpus       | int    | Maximum number of CPUs that can be used, can limit the CPU usage of the client process                                    | No       |
| enableXattr   | bool   | Whether to use xattr, default is false                                                                                    | No       |
| enableBcache  | bool   | Whether to enable local level-1 cache, default is false                                                                   | No       |
| enableAudit   | bool   | Whether to enable local audit logs, default is false                                                                      | No       |

## Configuration Example

``` json
{
  "mountPoint": "/cfs/mountpoint",
  "volName": "ltptest",
  "owner": "ltptest",
  "masterAddr": "10.196.59.198:17010,10.196.59.199:17010,10.196.59.200:17010",
  "logDir": "/cfs/client/log",
  "logLevel": "info",
  "profPort": "27510"
}
```