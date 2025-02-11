# FAQ

## 问题1
_描述_：用的minio的API用的minio的API , create bucket时提示权限不足，请问是帐号要怎么调置权限呢？用的是testuser这个帐号。java程序中的异常log是Access Denied。

**回答**：这个应该是需要先创建卷，卷就是桶。使用s3的接口去创建桶，对应的cubefs都是通过后台创建相应的卷，这个可能会卡住。使用s3cmd创建桶卡住也是同样的问题。建议先用cfs-cli创建好卷。


## 问题2
_描述_：硬盘坏了，副本无法正常下线。这种无主状态的data partition应该怎么修复吗？

**回答**：可以强制下线删除副本，然后再添加新的副本。
```bash
curl "http://192.168.1.1:17010/dataReplica/delete?raftForceDel=true&addr=192.168.1.2:17310&id=35455&force=true"
```


## 问题3
_描述_：遇到metanode的容器报错误信息：CrashLoopBackOff和OOMKilled。

**回答**：是metanode的OOM，内存调大点就好了。


## 问题4
_描述_：正常S3删除对象的时候，会删除这个key, 而cubefs是使用文件系统模拟的S3, 所以就算这个key对应的目录为空，也不会自动删掉。
举个例子来说： 假如一个桶内只有一个对象，test1/path2/obj.jpg, 正常S3删除后，应该是个空桶了，但是cubefs 删除的只有obj.jpg文件，不会自动删除test1目录和path2目录。

**回答**：这个本质是因为objectnode是基于fuse的path虚拟出来s3的key，metanode本身没有对应的语义，如果删除要把上层的空dentry也递归删除的话，判断逻辑就复杂了，并且也会有并发问题。用户可以挂载客户端，写个脚本定期递归查询空目录，并且清理空目录。使用深度优先搜索算法可以解决搜索空目录的难题。

## 问题5
_描述_：数据节点datanode3节点坏了2个，还有救么？

**回答**：有救的，备份好坏的dp副本。
```bash
用curl "127.0.0.1:17010/dataReplica/delete?raftForceDel=true&addr=datanodeAddr:17310&id=47128"
```
强制删除，之后找两个好的datanode添加。
```bash
curl -v "http://192.168.0.11:17010/dataReplica/add?id=12&addr=192.168.0.33:17310"
```


## 问题6
_描述_：目录被人误删了，有一个metanode报丢失partition，这个如何处理，可以从别的节点把数据拷贝过来吗？

**回答**：可以通过下线这个节点后，再启动。这样就触发meta partition迁移到其它节点了。


## 问题7
_描述_：客户端cfs-client默认的方式会把负载高的机器弄的更高尤其是最早扩容的，磁盘容量写到90%以上，造成部分机器的IO wait高。机器的容量越高，越容易出现客户端并发访问，导致磁盘IO吞吐不能匹配请求，形成局部热点机器。请问是否有办法处理这种问题？

**回答**：选择优先存放在剩余空间大的：
```bash
curl -v "http://127.0.0.1:17010/nodeSet/update?nodesetId=id&dataNodeSelector=AvailableSpaceFirst"
```
或者把data node设置为只读模式，禁止热点节点继续写入：
```bash
curl "masterip:17010/admin/setNodeRdOnly?addr=datanodeip:17310&nodeType=2&rdOnly=true"
```


## 问题8
_描述_：升级3.4后metanode的meta partition数量慢慢变多，mp数量限制必须跟着调大，好像随着时间增加，变得不够用。

**回答**：调整大meta partition的inode数量间隔为1亿，这样就不容易去创建新的meta partition。
```bash
curl -v "http://192.168.1.1:17010/admin/setConfig?metaPartitionInodeIdStep=100000000"
```


## 问题9
_描述_：如果我想把ceph或minio的文件迁移到cubefs，建议用什么工具和方法？

**回答**：让AI写个python脚本用s3接口并发拷贝，或者使用工具juicefs_sync，rclone


## 问题10
_描述_：blobstore的磁盘损坏与下线流程是咋样的？ blobstore设置为坏盘后，磁盘状态停留在repaired，一直也没下线。主动调用下线API，显示需要磁盘状态为normal，且为只读才能下线的响应。 这个就进入死胡同了。 正常盘才能下线么？ 那损坏盘咋操作？比如说disk3这个盘损坏了，换了新盘后，设置了坏盘后，重启后，disk3 这个盘有了一个新的磁盘ID，disk3 旧的磁盘ID还在，如何删掉旧的这个磁盘ID？

**回答**：旧盘这个记录一直会存在，用来追查换盘记录。也就是说我们不删除旧的磁盘ID。


## 问题11
_描述_：cubefs对大文件场景支持怎么样啊，就几十个g的大模型文件。

**回答**：没有问题，可以支持的。


## 问题12
_描述_：强制删除异常的副本后，剩下的副本没有自动成为leader，导致没法添加新的副本。用cfs-cli datapartition check命令返回结果，显示这个dp是no leader状态。请问如何处理这个异常的副本？

**回答**：看下raft日志，查询这个分区的选举信息，是不是有去副本组看不到的节点请求投票。有的话，需要把它从副本组强制删除。
```bash
curl "http://192.168.1.1:17010/dataReplica/delete?raftForceDel=true&addr=192.168.1.2:17310&id=35455&force=true"
```

