# 集群配置修改

## 在线配置修改
通过cli工具，可以对以下集群配置项进行在线修改

### 修改卷配置
cfs-cli的子命令volume的help选项，列举出了当前支持在线修改配置选项
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
比如可通过**cfs-cli volume transfer**命令修改卷的owner，具体使用方法可以使用**cfs-cli volume transfer -h**查询。

### 调整子系统日志等级
如果子系统的配置文件中，开启了profPort端口，则可以通过该端口进行日志等级的修改。
```bash
curl -v "http://127.0.0.1:{profPort}/loglevel/set?level={log-level}"
```
目前支持的日志等级log-level有**debug,info,warn,error,critical,read,write,fatal**

## 离线配置修改
集群中子系统的其他配置项，需要修改子系统的启动配置文件后重启才可生效。

### 修改DataNode保留空间
DataNode的配置文件中，disk参数后半部分的数字即为保留空间，**单位byte**，修改完后**重新启动即可**。
```bash
{ ...
  "disks": [
   "/cfs/disk:10737418240" //10737418240为保留空间大小
  ],
  ...
}
```

### 修改MetaNode的最大可用内存
MetaNode配置文件的totalMem指元数据节点可用总内存大小。当MetaNode的内存占用高于此值，MetaNode变为只读状态。通常该值要小于节点内存，**如果元数据子系统和副本子系统混合部署，则需要给副本子系统预留内存空间**。


### 修改DataNode/MetaNode端口
**不建议修改DataNode/MetaNode的端口**。因为DataNode/MetaNode在master中是通过ip:port进行注册的。如果修改了端口，master则会认为其为全新节点，旧节点是 Inactive 状态。