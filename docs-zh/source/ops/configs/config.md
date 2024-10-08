# 集群配置修改

## 在线配置修改
通过 CLI 工具，可以对以下集群配置项进行在线修改

### 修改卷配置
cfs-cli 的子命令 volume 的 help 选项，列举出了当前支持在线修改配置选项
```bash
cfs-cli volume -h

Usage:
  cfs-cli volume [command]

Aliases:
  volume, vol

Available Commands:
  add-dp      Create and add more data partition to a volume
  create      Create a new volume
  delete      Delete a volume from cluster
  expand      Expand capacity of a volume
  info        Show volume information
  list        List cluster volumes
  shrink      Shrink capacity of a volume
  transfer    Transfer volume to another user. (Change owner of volume)
  update      Update configuration of the volume
```
比如可通过 **cfs-cli volume transfer** 命令修改卷的 owner，具体使用方法可以使用 **cfs-cli volume transfer -h** 查询。

### 调整子系统日志等级
如果子系统的配置文件中，开启了 profPort 端口，则可以通过该端口进行日志等级的修改。
```bash
curl -v "http://127.0.0.1:{profPort}/loglevel/set?level={log-level}"
```
目前支持的日志等级 log-level 有 **debug,info,warn,error,critical,read,write,fatal**

### 调整纠删码日志等级

纠删码系统的所有模块均支持此方式，[详情参考](../../dev-guide/admin-api/blobstore/base.md)

| 级别    | 值   |
|:-------|:-----|
| Debug | 0   |
| Info  | 1   | 
| Warn  | 2   | 
| Error | 3   |
| Panic | 4   |
| Fatal | 5   |

```bash
# 以下为设置日志级别为warn
curl -XPOST -d 'level=2' http://127.0.0.1:9500/log/level
```

## 离线配置修改

::: tip 提示
集群中子系统的其他配置项，需要修改子系统的启动配置文件后重启才可生效。
:::

### 修改 DataNode 保留空间
DataNode 的配置文件中，disk 参数后半部分的数字即为预留空间，**单位byte**，修改完后**重新启动即可**。
```bash
{ ...
  "disks": [
   "/cfs/disk:10737418240" //10737418240为预留空间大小
  ],
  ...
}
```

### 修改 MetaNode 的最大可用内存
MetaNode 配置文件的 totalMem 指元数据节点可用总内存大小。当 MetaNode 的内存占用高于此值，MetaNode 变为只读状态。通常该值要小于节点内存，**如果元数据子系统和副本子系统混合部署，则需要给副本子系统预留内存空间**。


### 修改 DataNode/MetaNode 端口

::: danger 警告
不建议修改 DataNode/MetaNode 的端口。因为 DataNode/MetaNode 在 master 中是通过 ip:port 进行注册的。如果修改了端口，master 则会认为其为全新节点，旧节点是 Inactive 状态。
:::

### 纠删码其他配置修改

请参考[配置管理章节](blobstore/base.md)