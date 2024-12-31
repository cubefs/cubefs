# Client 配置
## 配置说明

| 名称             | 类型     | 描述                                      | 必需  |
|:----------------|:--------|:-----------------------------------------|:-------|
| mountPoint     | string | 挂载点                                     | 是   |
| volName        | string | 卷名称                                     | 是   |
| owner          | string | 所有者                                     | 是   |
| masterAddr     | string | Master节点地址                              | 是   |
| logDir         | string | 日志存放路径                                  | 否   |
| logLevel       | string | 日志级别：debug, info, warn, error           | 否   |
| profPort       | string | golang pprof 调试端口                        | 否   |
| exporterPort   | string | prometheus 获取监控数据端口                      | 否   |
| consulAddr     | string | 监控注册服务器地址                               | 否   |
| lookupValid    | string | 内核 FUSE lookup 有效期，单位：秒                   | 否   |
| attrValid      | string | 内核 FUSE attribute 有效期，单位：秒                | 否   |
| icacheTimeout  | string | 客户端 inode cache 有效期，单位：秒                  | 否   |
| enSyncWrite    | string | 使能 DirectIO 同步写，即 DirectIO 强制数据节点落盘         | 否   |
| autoInvalData  | string | FUSE 挂载使用 AutoInvalData 选项                 | 否   |
| rdonly         | bool   | 以只读方式挂载，默认为false                        | 否   |
| writecache     | bool   | 利用内核 FUSE 的写缓存功能，需要内核 FUSE 模块支持写缓存，默认为 false | 否   |
| keepcache      | bool   | 保留内核页面缓存。此功能需要启用 writecache选项，默认为false   | 否   |
| token          | string | 如果创建卷时开启了 enableToken，此参数填写对应权限的token    | 否   |
| readRate       | int    | 限制每秒读取次数，默认无限制                          | 否   |
| writeRate      | int    | 限制每秒写入次数，默认无限制                          | 否   |
| followerRead   | bool   | 从 follower 中读取数据，默认为 false                 | 否   |
| accessKey      | string | 卷所属用户的鉴权密钥                              | 否   |
| secretKey      | string | 卷所属用户的鉴权密钥                              | 否   |
| disableDcache  | bool   | 禁用 Dentry 缓存，默认为 false                     | 否   |
| subdir         | string | 设置子目录挂载                                 | 否   |
| fsyncOnClose   | bool   | 文件关闭后执行 fsync 操作，默认为true                  | 否   |
| maxcpus        | int    | 最大可使用的 cpu 核数，可限制 client 进程 cpu 使用率           | 否   |
| enableXattr    | bool   | 是否使用 \*xattr\*，默认是 false                  | 否   |
| enableBcache   | bool   | 是否开启本地一级缓存，默认false                      | 否   |
| enableAudit    | bool   | 是否开启本地审计日志，默认false                      | 否   |

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