# kafka消费问题

## offset is outside the range

报错内容类似如下

```shell
The Requested offset is outside the range of offsets maintained by the server for the given topic/patition.
```

指定消费位移时，如果对应位移超过了Kafka对应分区的范围，则会出现类似如上报错，一般原因会有以下几种

- 消费能力跟不上，Kafka集群的数据有保护期，过期后的数据会被删除，因此提供的位移可能是低于Kafka服务端的最小位移，一般有以下解决办法
  - 调整消费参数，具体可以参考[Scheduler配置](../maintenance/configs/blobstore/scheduler.md)
  - 新增Scheduler节点

::: tip 提示
scheduler节点数最大不能大于kafka分区数
:::

- Kafka集群有更换主题或者清理主题数据，但是Clustermgr的消费位移未清理，导致指定offset大于Kafka集群对应分区范围
  - 清理Clustermgr中保存的消费位移或者重新设置对应消费位移

无论是哪种原因，当前Scheduler服务正常启动，都需要将Clustermgr中的消费位移设置正确才能启动，下面介绍如何通过blobstore-cli工具问题处理步骤

- **查看消费位移**

```shell
cm kv list --prefix blob_delete-consume_offset # 查看删除消息，默认返回10条，更多可以带上参数--count

Key   : blob_delete-consume_offset-blob_delete_10001-0
Value : {"topic":"blob_delete_10001","partition":0,"offset":170446922}
Key   : blob_delete-consume_offset-blob_delete_10001-1
Value : {"topic":"blob_delete_10001","partition":1,"offset":170445513}
Key   : blob_delete-consume_offset-blob_delete_10001-10
Value : {"topic":"blob_delete_10001","partition":10,"offset":170440299}
Key   : blob_delete-consume_offset-blob_delete_10001-11
Value : {"topic":"blob_delete_10001","partition":11,"offset":170465361}
Key   : blob_delete-consume_offset-blob_delete_10001-12
Value : {"topic":"blob_delete_10001","partition":12,"offset":170462346}
Key   : blob_delete-consume_offset-blob_delete_10001-13
Value : {"topic":"blob_delete_10001","partition":13,"offset":170453103}
Key   : blob_delete-consume_offset-blob_delete_10001-14
Value : {"topic":"blob_delete_10001","partition":14,"offset":170463101}
Key   : blob_delete-consume_offset-blob_delete_10001-15
Value : {"topic":"blob_delete_10001","partition":15,"offset":170439254}
Key   : blob_delete-consume_offset-blob_delete_10001-16
Value : {"topic":"blob_delete_10001","partition":16,"offset":170470119}
Key   : blob_delete-consume_offset-blob_delete_10001-17
Value : {"topic":"blob_delete_10001","partition":17,"offset":170463816}
next marker: blob_delete-consume_offset-blob_delete_10001-17

cm kv list --prefix shard_repair-consume_offset

Key   : shard_repair-consume_offset-shard_repair_10001-0
Value : {"topic":"shard_repair_10001","partition":0,"offset":1140969}
Key   : shard_repair-consume_offset-shard_repair_10001-1
Value : {"topic":"shard_repair_10001","partition":1,"offset":1141604}
Key   : shard_repair-consume_offset-shard_repair_10001-10
Value : {"topic":"shard_repair_10001","partition":10,"offset":1141553}
Key   : shard_repair-consume_offset-shard_repair_10001-11
Value : {"topic":"shard_repair_10001","partition":11,"offset":1140962}
Key   : shard_repair-consume_offset-shard_repair_10001-12
Value : {"topic":"shard_repair_10001","partition":12,"offset":1140533}
Key   : shard_repair-consume_offset-shard_repair_10001-13
Value : {"topic":"shard_repair_10001","partition":13,"offset":1143025}
Key   : shard_repair-consume_offset-shard_repair_10001-14
Value : {"topic":"shard_repair_10001","partition":14,"offset":1140768}
Key   : shard_repair-consume_offset-shard_repair_10001-15
Value : {"topic":"shard_repair_10001","partition":15,"offset":1140299}
Key   : shard_repair-consume_offset-shard_repair_10001-16
Value : {"topic":"shard_repair_10001","partition":16,"offset":1141417}
Key   : shard_repair-consume_offset-shard_repair_10001-17
Value : {"topic":"shard_repair_10001","partition":17,"offset":1141269}
next marker: shard_repair-consume_offset-shard_repair_10001-17
```

::: tip 提示
如果查看某个分区的消费位移，可以`cm kv list --prefix <task_type>-consume_offset-<topic>-<partition>`，如`cm kv list --prefix shard_repair-consume_offset-shard_repair_10001-20`
:::

- **比对Kafka服务端分区位移**

核对确认Clustermgr保存的消费位移是否不在Kafka集群的对应分区范围内

- **停止scheduler节点**

- **修正消费位移**

```shell
# scheduler kafka set --task_type <task_type> --topic <topic> --partition <partition> <offset>
# 示例如下
scheduler kafka set --task_type shard_repair --topic shard_repair_10001 --partition 20 332053
```

- **启动Scheduler**