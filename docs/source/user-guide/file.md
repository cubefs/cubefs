# Using File Storage

## Environment Dependencies
- Insert the kernel FUSE module
- Install `libfuse`

```bash
modprobe fuse
yum install -y fuse
```

## Mounting the File System
After successfully creating a replica volume, mount the volume by executing the following command:

```bash
cfs-client -c client.json
```

After successful execution, you can use the **df -h** command to view the mount point. If the execution fails, you can judge according to the content of **output.log** in the log directory.

The example configuration file is as follows:

```json
{
    "mountPoint":"/mnt/cfs",
    "subdir":"/",
    "volName":"vol_test",
    "owner":"test",
    "accessKey":"**********",
    "secretKey":"*********",
    "masterAddr":"192.168.0.1",
    "rdonly":"false",
    "logDir":"/home/service/var/logs/cfs/log",
    "logLevel":"warn",
    "profPort":"192.168.1.1"
}
```

The meaning of each parameter in the configuration file is shown in the following table:

| Parameter  | Type         | Meaning                                      | Required |
| ---------- | ------------ | -------------------------------------------- | -------- |
| mountPoint | string       | Mount point                                  | Yes      |
| subdir     | string       | Mount subdirectory                           | No       |
| volName    | string slice | Volume name                                  | Yes      |
| owner      | string       | Volume owner                                 | Yes      |
| accessKey  | string       | Authentication key of the user to which the volume belongs | Yes      |
| secretKey  | string       | Authentication key of the user to which the volume belongs | Yes      |
| masterAddr | string       | Master node address                           | Yes      |
| rdonly     | bool         | Mount as read-only, default is false          | No       |
| logDir     | string       | Log storage path                             | No       |
| logLevel   | string       | Log level: debug, info, warn, error           | No       |
| profPort   | string       | Golang pprof debugging port                  | No       |

Other configuration parameters can be set according to actual needs:

| Parameter         | Type   | Meaning                                                         | Required |
| ----------------- | ------ | --------------------------------------------------------------- | -------- |
| exporterPort      | string | Port for Prometheus to obtain monitoring data                     | No       |
| consulAddr        | string | Monitoring registration server address                            | No       |
| lookupValid       | string | Validity period of kernel FUSE lookup, in seconds                  | No       |
| attrValid         | string | Validity period of kernel FUSE attribute, in seconds               | No       |
| icacheTimeout     | string | Validity period of client inode cache, in seconds                  | No       |
| enSyncWrite       | string | Enable DirectIO synchronous write, that is, DirectIO forces data nodes to write to disk | No       |
| autoInvalData     | string | Use the AutoInvalData option for FUSE mounting                     | No       |
| writecache        | bool   | Use the write cache function of the kernel FUSE module. This function requires the kernel FUSE module to support write cache. The default is false. | No       |
| keepcache         | bool   | Keep the kernel page cache. This function requires the writecache option to be enabled. The default is false. | No       |
| token             | string | If enableToken is enabled when creating a volume, fill in the token corresponding to the permission. | No       |
| readRate          | int    | Limit the number of reads per second. The default is unlimited.   | No       |
| writeRate         | int    | Limit the number of writes per second. The default is unlimited.  | No       |
| followerRead      | bool   | Read data from the follower. The default is false.                | No       |
| disableDcache     | bool   | Disable Dentry cache. The default is false.                       | No       |
| fsyncOnClose      | bool   | Perform fsync operation after the file is closed. The default is true. | No       |
| maxcpus           | int    | The maximum number of CPUs that can be used, which can limit the CPU usage of the client process. | No       |
| enableXattr       | bool   | Whether to use xattr. The default is false.                       | No       |
| enableBcache      | bool   | Whether to enable local level 1 cache. The default is false.      | No       |
| maxStreamerLimit  | string | When local level 1 cache is enabled, the number of file metadata caches. | No       |
| bcacheDir         | string | The target directory for read cache when local level 1 cache is enabled. | No       |

## Unmounting the File System
Execute the following command to unmount the replica volume:

```bash
umount -l /path/to/mountPoint
```

`/path/to/mountPoint` is the mount path in the client configuration file.

## Enabling Level 1 Cache

The local read cache service deployed on the user client is not recommended for scenarios where the data set has modified writes and requires strong consistency. After deploying the cache, the client needs to add the following mount parameters, and the cache will take effect after remounting.

```bash
./cfs-bcache -c bcache.json
```

The meaning of each parameter in the configuration file is shown in the following table:

```json
{
   "cacheDir":"/home/service/var:1099511627776",
   "logDir":"/home/service/var/logs/cachelog",
   "logLevel":"warn"
}
```

| Parameter | Type   | Meaning                                                    | Required |
|-----------|--------|------------------------------------------------------------|----------|
| cacheDir  | string | Local storage path for cached data: allocated space (Byte) | Yes      |
| logDir    | string | Log path                                                   | Yes      |
| logLevel  | string | Log level                                                  | Yes      |
