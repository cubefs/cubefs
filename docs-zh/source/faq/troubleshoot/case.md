# FAQ

## 问题1
_描述_：使用对象存储的接口create bucket时提示权限不足，请问帐号要怎么设置权限呢？用的是testuser这个帐号。java程序中的异常log是Access Denied。

**回答**：这个应该是需要先创建卷，卷就是桶。使用对象存储的接口去创建桶时，都是cubefs通过后台创建相应的卷，这个可能会卡住。建议先用cfs-cli创建好卷。


## 问题2
_描述_：硬盘坏了，副本无法正常下线。这种无主状态的data partition应该怎么修复？

**回答**：可以强制下线删除副本，然后再添加新的副本。
```bash
curl -v "http://192.168.1.1:17010/dataReplica/delete?raftForceDel=true&addr=192.168.1.2:17310&id=35455&force=true"
curl -v "http://192.168.0.11:17010/dataReplica/add?id=12&addr=192.168.0.33:17310"
```


## 问题3
_描述_：假如一个桶内只有一个对象，test1/path2/obj.jpg, 正常删除后，应该是test1/path2也同时删掉。但是cubefs删除的只有obj.jpg文件，不会自动删除test1目录和path2目录。

**回答**：这个本质是因为objectnode是基于fuse的path虚拟出来s3的key，metanode本身没有对应的语义。如果删除要把上层的空目录也递归删除的话，判断逻辑就复杂了，并且也会有并发问题。用户可以挂载客户端，写个脚本定期递归查询空目录，并且清理空目录。使用深度优先搜索算法可以解决搜索空目录的难题。

## 问题4
_描述_：数据节点datanode3节点坏了2个，还有救么？

**回答**：有救的。首先备份坏掉的dp副本，然后强制删除坏的副本，最后找两个好的datanode添加上去。
```bash
curl -v "127.0.0.1:17010/dataReplica/delete?raftForceDel=true&addr=datanodeAddr:17310&id=47128"
curl -v "http://192.168.0.11:17010/dataReplica/add?id=12&addr=192.168.0.33:17310"
```


## 问题5
_描述_：metanode上mp的目录被删了，有一个metanode报丢失partition。这个如何处理，可以从别的节点把数据拷贝过来吗？

**回答**：可以通过下线这个节点后，再重新启动。这样就触发meta partition迁移到其它节点，通过自动迁移就可以完成副本拷贝。


## 问题6
_描述_：客户端cfs-client默认使用data partition的方式会把负载高的机器弄的更高，尤其是最早扩容的，磁盘容量写到90%以上，造成部分机器的IO wait高。机器的容量越高，越容易出现客户端并发访问，导致磁盘IO吞吐不能匹配请求，形成局部热点机器。请问是否有办法处理这种问题？

**回答**：选择优先存放在剩余空间大的：
```bash
curl -v "http://127.0.0.1:17010/nodeSet/update?nodesetId=id&dataNodeSelector=AvailableSpaceFirst"
```
或者把data node设置为只读模式，禁止热点节点继续写入：
```bash
curl -v "masterip:17010/admin/setNodeRdOnly?addr=datanodeip:17310&nodeType=2&rdOnly=true"
```


## 问题7
_描述_：升级3.4后metanode的meta partition数量慢慢变多，mp数量限制必须跟着调大，好像随着时间增加，变得不够用。

**回答**：调整大meta partition的inode数量间隔为1亿，这样就不容易去创建新的meta partition。
```bash
curl -v "http://192.168.1.1:17010/admin/setConfig?metaPartitionInodeIdStep=100000000"
```


## 问题8
_描述_：blobstore的磁盘损坏与下线流程是咋样的？ blobstore设置为坏盘后，磁盘状态停留在repaired，一直也没下线。主动调用下线API，显示需要磁盘状态为normal，且为只读才能下线的响应。 这个就进入死胡同了。 正常盘才能下线么？ 那损坏盘咋操作？比如说disk3这个盘损坏了，换了新盘后，设置了坏盘后，重启后，disk3 这个盘有了一个新的磁盘ID，disk3 旧的磁盘ID还在，如何删掉旧的这个磁盘ID？

**回答**：旧盘这个记录一直会存在，用来追查换盘记录。也就是说我们不删除旧的磁盘ID。


## 问题9
_描述_：cubefs对大文件场景支持怎么样啊，就几十个g的大模型文件。

**回答**：没有问题，可以支持的。


## 问题10
_描述_：强制删除异常的副本后，剩下的副本没有自动成为leader，导致没法添加新的副本。用cfs-cli datapartition check命令返回结果，显示这个dp是no leader状态。请问如何处理这个异常的副本？

**回答**：看下raft日志，查询这个分区的选举信息，是不是有去副本组看不到的节点请求投票。有的话，需要把它从副本组强制删除。
```bash
curl -v "http://192.168.1.1:17010/dataReplica/delete?raftForceDel=true&addr=192.168.1.2:17310&id=35455&force=true"
```

## 问题11
_描述_：BlobStore 这个系统是可以不用部署的吧？如果部署了 BlobStore ，假如图片文件部分数据丢失了，如果通过S3接口获取图片，是否有自动纠错功能？

**回答**：如果不部署，就是使用3副本模式。如果部署使用，就是 EC 模式。对于磁盘损坏，3副本和EC 模式都有修复能力，不同的是3副本使用好的副本修复。EC是数据基于纠删码技术切分数据写入的，使用纠删码技术来修复。

## 问题12
_描述_：cubefs有商用版本吗？

**回答**：cubefs是开源项目，没有商用版本

## 问题13
_描述_：docker部署遇到错误信息：
`docker pull cubefs/cbfs-base:1.1-golang-1.17.13 Error response from daemon: Get "https://registry-1.docker.io/v2/": net/http: request canceled while waiting for connection (Client.Timeout exceeded while awaiting headers)`

**回答**：需要使用加速镜像

## 问题14
_描述_：怎么看到 cubefs 的创建的 volume 信息?

**回答**：可以使用 cfs-cli 工具查看，命令为 `./cfs-cli volume info volName`

## 问题15
_描述_：请问一下 lcnode 有什么作用？lc是什么缩写？

**回答**：是 life cycle 生命周期组件，用于系统维度周期性的任务执行

## 问题16
_描述_：请问下这个 mediaType 是什么意思，怎么配置？

**回答**：这个是存储介质的类型，比如ssd是1，hdd是2，作为基础字段，将在 3.5 正式启用。升级到 3.5 以后，这个是必须配置的，配置方式为：
- master配置文件中新增项：`"legacyDataMediaType" ：1`
- datanode配置文件中新增项：`"mediaType": 1`
- 终端运行 `./cfs-cli cluster set dataMediaType=1`

## 问题17
_描述_：有没有 cubefs 的性能数据呢?

**回答**：有的，在官网文档上
https://cubefs.io/zh/docs/master/evaluation/tiny.html

## 问题18
_描述_：cubefs 在生产环境中一般是用副本模式还是纠删码模式？

**回答**：都有，考虑成本因素就选择纠删码，考虑性能因素就选择副本模式

## 问题19
_描述_：对象存储中的 endpoint 对应的 cubefs 的地址是？桶名对应的是？

**回答**：endpoint 默认就是 objectnode 的地址和端口 17410，如 "127.0.0.1:17410"。cubefs 的卷对应着 S3 的桶。

## 问题20
_描述_：对象存储的 region 该怎么填？

**回答**：可以填写集群名称 clusterName，如 "cfs_dev"

## 问题21
_描述_：cubefs 支持同时挂载多个卷吗？

**回答**：不支持一个客户端进程挂多个卷，但支持同机上面启用多个客户端，每个客户端可以挂自己的卷，这样就可以挂多个卷（也可以是重复的卷）

## 问题22
_描述_：GUI平台的账号和密码是多少？

**回答**：GUI 后端部署好之后，会生成一个初始有最高权限的账号 admin/Admin@1234 第一次登录时，需要修改密码。具体看 https://cubefs.io/zh/docs/master/user-guide/gui.html

## 问题23
_描述_：容器里面的 master, meta, data, object 是否都可以只启动一台？

**回答**：objectnode是无状态的，可以只启动一台。其它的要组成raft组，需要启动多台。

## 问题24
_描述_：不同组件相关的raft状态怎么查询 ？

**回答**：可以使用命令查询，只有leader会显示group成员的信息，其他显示自己的信息
```bash
curl 127.0.0.1:17320/raftStatus?raftID=1624 // datanode
curl "127.0.0.1:17010/get/raftStatus" | python -m json.tool  //master
curl 127.0.0.1:17220/getRaftStatus?id=400 //metanode
```

## 问题25
_描述_： 使用命令 `cfs-cli user create` 报错，提示 `invalid access key`，怎么解决？

**回答**：一般是输入的 AK/SK 的长度有问题。AK的长度是16个字符，SK的长度是32个字符。如果不清楚，可以去掉AK/SK的设置，系统默认会给每个账号生成一个 AK/SK。