# Client 配置
## 配置说明

| 名称            | 类型     | 描述                                      | 必需  |
|---------------|--------|-----------------------------------------|-----|
| mountPoint    | string | 挂载点                                     | 是   |
| volName       | string | 卷名称                                     | 是   |
| owner         | string | 所有者                                     | 是   |
| masterAddr    | string | Master节点地址                              | 是   |
| logDir        | string | 日志存放路径                                  | 否   |
| logLevel      | string | 日志级别：debug, info, warn, error           | 否   |
| profPort      | string | golang pprof调试端口                        | 否   |
| exporterPort  | string | prometheus获取监控数据端口                      | 否   |
| consulAddr    | string | 监控注册服务器地址                               | 否   |
| lookupValid   | string | 内核FUSE lookup有效期，单位：秒                   | 否   |
| attrValid     | string | 内核FUSE attribute有效期，单位：秒                | 否   |
| icacheTimeout | string | 客户端inode cache有效期，单位：秒                  | 否   |
| enSyncWrite   | string | 使能DirectIO同步写，即DirectIO强制数据节点落盘         | 否   |
| autoInvalData | string | FUSE挂载使用AutoInvalData选项                 | 否   |
| rdonly        | bool   | 以只读方式挂载，默认为false                        | 否   |
| writecache    | bool   | 利用内核FUSE的写缓存功能，需要内核FUSE模块支持写缓存，默认为false | 否   |
| keepcache     | bool   | 保留内核页面缓存。此功能需要启用writecache选项，默认为false   | 否   |
| token         | string | 如果创建卷时开启了enableToken，此参数填写对应权限的token    | 否   |
| readRate      | int    | 限制每秒读取次数，默认无限制                          | 否   |
| writeRate     | int    | 限制每秒写入次数，默认无限制                          | 否   |
| followerRead  | bool   | 从follower中读取数据，默认为false                 | 否   |
| accessKey     | string | 卷所属用户的鉴权密钥                              | 否   |
| secretKey     | string | 卷所属用户的鉴权密钥                              | 否   |
| disableDcache | bool   | 禁用Dentry缓存，默认为false                     | 否   |
| subdir        | string | 设置子目录挂载                                 | 否   |
| fsyncOnClose  | bool   | 文件关闭后执行fsync操作，默认为true                  | 否   |
| maxcpus       | int    | 最大可使用的cpu核数，可限制client进程cpu使用率           | 否   |
| enableXattr   | bool   | 是否使用\*xattr\*，默认是false                  | 否   |
| enableBcache  | bool   | 是否开启本地一级缓存，默认false                      | 否   |

## 配置示例

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