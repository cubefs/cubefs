# 使用文件存储

## 环境依赖
- 插入内核FUSE模
- 安装libfuse

```bash
modprobe fuse
yum install -y fuse
```

## 挂载文件系统
成功创建副本卷后，通过执行如下的命令来挂载副本卷:

```bash
cfs-client -c client.json 
```

执行成功后,可通过**df -h**命令查看挂载点。如果执行失败，可根据日志目录下的**output.log**内容进行判断。

配置文件的示例如下：

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

配置文件中各参数的含义如下表所示：

| 参数          | 类型               | 含义                                                     |必需 |
|--------------|--------------------|---------------------------------------------------------|-----|
| mountPoint   | string             | 挂载点                          |是   |
| subdir       | string             | 挂载子目录     |否  |
| volName      | string slice       | 卷名称                  |是   |
| owner       | string             | 卷所有者                                             |是   |
| accessKey     | string             | 卷所属用户的鉴权密钥                                  |是   |
| secretKey | string             | 卷所属用户的鉴权密钥                               |是   |
| masterAddr         | string             | master节点地址                                      |是   
| rdonly         | bool             | 以只读方式挂载，默认false                      |否   
| logDir         | string             | 日志存放路径                                      |否   
| logLevel         | string        | 日志级别：debug, info, warn,error                |否   
| profPort         | string             | golang pprof调试端口                               |否   

其他配置参数可根据实际场景需要进行设置:

| 参数          | 类型               | 含义                                                     |必需 |
|--------------|--------------------|---------------------------------------------------------|-----|
| exporterPort         | string             | prometheus获取监控数据端口                              |否  
| consulAddr         | string             | 监控注册服务器地址                              |否  
| lookupValid         | string             | 内核FUSE lookup有效期，单位：秒                               |否  
| attrValid         | string             | 内核FUSE attribute有效期，单位：秒                               |否  
| icacheTimeout         | string             | 客户端inode cache有效期，单位：秒                             |否  
| enSyncWrite         | string             | 使能DirectIO同步写，即DirectIO强制数据节点落盘                               |否  
| autoInvalData         | string             | FUSE挂载使用AutoInvalData选项                               |否  
| writecache         | bool             | 利用内核FUSE的写缓存功能，需要内核FUSE模块支持写缓存，默认为false                               |否  
| keepcache         | bool             | 保留内核页面缓存。此功能需要启用writecache选项，默认为false                               |否 
| token         | string             | 如果创建卷时开启了enableToken，此参数填写对应权限的token                              |否 
| readRate         | int             | 限制每秒读取次数，默认无限制                               |否 
| writeRate         | int             | 限制每秒写入次数，默认无限制                               |否 
| followerRead         | bool             | 从follower中读取数据，默认为false                               |否 
| disableDcache         | bool             | 禁用Dentry缓存，默认为false                              |否 
| fsyncOnClose         | bool             | 文件关闭后执行fsync操作，默认为true                               |否 
| maxcpus         | int             | 最大可使用的cpu核数，可限制client进程cpu使用率                               |否 
| enableXattr         | bool             | 是否使用xattr，默认是false                               |否 
| enableBcache         | bool             | 是否开启本地一级缓存，默认false                               |否 
| maxStreamerLimit         | string             | 开启本地一级缓存时，文件元数据缓存数目 |否
| bcacheDir         | string             | 开启本地一级缓存时，需要开启读缓存的目标目录路                 |否 


## 卸载文件系统
执行如下命令卸载副本卷:

```bash
umount -l /path/to/mountPoint
```

**/path/to/mountPoint** 为客户端配置文件中的挂载路径

## 开启一级缓存

部署在用户客户端的本地读cache服务，对于数据集有修改写，需要强一致的场景不建议使用。 部署缓存后，客户端需要增加以下挂载参数，重新挂载后缓存才能生效。

```bash
./cfs-bcache -c bcache.json
```

配置文件中各参数的含义如下表所示：

```json
{
   "cacheDir":"/home/service/var:1099511627776",
   "logDir":"/home/service/var/logs/cachelog",
   "logLevel":"warn"
}
```

| 参数          | 类型               | 含义                                                     |必需 |
|--------------|--------------------|---------------------------------------------------------|-----|
| cacheDir         | string             | 缓存数据的本地存储路径:分配空间（单位Byte)            |是 
| logDir         | string             | 日志路径   |是 
| logLevel         | string             | 日志级别   |是 