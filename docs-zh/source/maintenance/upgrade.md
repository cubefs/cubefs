# 系统升级
这里分别讲述下CubeFS和BlobStore模块的升级方案。
## CubeFS 升级
### 获取二进制
+ 获取指定版本二进制， https://github.com/cubeFS/cubefs/releases 获取指定版本代码，编译生成二进制
### 冻结集群
```
$ cfs-cli cluster freeze true
```
### 注意事项
1. 确认启动配置文件，不要更改配置文件中的数据目录、端口等重要信息
   1. 配置文件其他参数修改参考配置说明，release notes等
2. 各组件升级顺序, 参考对应版本release notes 
   1. 如无特殊要求，一般可按照datanode->metanode->master->client的顺序升级各组件
### 升级datanode&metanode
下面以datanode为例描述
1. 停止旧的datanode进程
2. 启动新的datanode进程
3. 启动后检查节点状态，知道显示为active后，再升级下一台机器
```
$ cfs-cli datanode info 192.168.0.33:17310
[Data node info]
 ID                  : 9
 Address             : 192.168.0.33:17310
 Carry               : 0.06612836801123345
 Used ratio          : 0.0034684352702178426
 Used                : 96 GB
 Available           : 27 TB
 Total               : 27 TB
 Zone                : default
 IsActive            : Active
 Report time         : 2020-07-27 10:23:20
 Partition count     : 16
 Bad disks           : []
 Persist partitions  : [2 3 5 7 8 10 11 12 13 14 15 16 17 18 19 20]
```
### 升级master
1. 停止旧的master进程
2. 启动新的master进程
3. 观察监控是否正常
4. 查看master对应的raft状态是否正常
   1. 如下，查看对应重启master id对应的commit是否与其他副本一致，raft是否有主
```
curl 192.168.0.1:17010/get/raftStatus | python -m json.tool
{
    "code": 0,
    "data": {
        "AppQueue": 0,
        "Applied": 25168073,
        "Commit": 25168074,
        "ID": 1,
        "Index": 25168074,
        "Leader": 2,
        "NodeID": 2,
        "Replicas": {
            "1": {
                "Active": true,
                "Commit": 25168074,
                "Match": 25168074,
                "Next": 25168075,
            },
            "2": {
                "Active": true,
                "Commit": 25168074,
                "Match": 25168074,
                "Next": 25168075,
                "Paused": false,
            },
            "3": {
                "Active": true,
                "Commit": 25168074,
                "Match": 25168074,
                "Next": 25168075,
            }
        },
        "RestoringSnapshot": false,
        "State": "StateLeader",
        "Stopped": false,
        "Term": 292,
        "Vote": 2
    },
    "msg": "success"
}
```
### 升级client
1. 停止业务读写
2. umount 挂载点
   1. 若出现如下错误, 则需要执行 umount -l 挂载点
```
umount: /xxx/mnt: target is busy.
        (In some cases useful info about processes that use
         the device is found by lsof(8) or fuser(1)
```
3. 查看client进程若存在，则停止
4. 启动新的客户端进程即可，`df -h` 查看是否执行成功
