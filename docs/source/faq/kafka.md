# Kafka consumption issues

## offset is outside the range

The error message is similar to the following:

```shell
The Requested offset is outside the range of offsets maintained by the server for the given topic/patition.
```

When specifying the consumption offset, if the corresponding offset exceeds the range of the partition maintained by Kafka, an error message similar to the above will appear. Generally, the reasons for this error are as follows:

- Insufficient consumption capacity. Kafka cluster data has a retention period, and expired data will be deleted. Therefore, the provided offset may be lower than the minimum offset on the Kafka server. Solutions include:
  - Adjusting the consumption parameters. For more information, please refer to the [Scheduler configuration](../maintenance/configs/blobstore/scheduler.md)
  - Adding a Scheduler node.

::: tip Note
The maximum number of Scheduler nodes cannot be greater than the number of Kafka partitions.
:::

- The Kafka cluster has replaced or cleaned up topic data, but the consumption offset in Clustermgr has not been cleaned up, resulting in the specified offset being greater than the corresponding partition range in the Kafka cluster.
  - Clean up the consumption offset saved in Clustermgr or reset the corresponding consumption offset.

Regardless of the reason, if the Scheduler service is started normally, the consumption offset in Clustermgr needs to be set correctly before starting. The following section explains how to use the blobstore-cli tool to troubleshoot the problem.

- **Viewing the Consumption Offset**

```shell
cm kv list --prefix blob_delete-consume_offset # To view deleted messages, the default return is 10 messages. You can include the parameter --count to retrieve more messages.

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
If you want to view the consumption offset of a specific partition, you can use the following command: `cm kv list --prefix <task_type>-consume_offset-<topic>-<partition>`，such as `cm kv list --prefix shard_repair-consume_offset-shard_repair_10001-20`
:::

- **Comparing Kafka Server Partition Offsets**

Verify that the consumption offset saved in Clustermgr is within the corresponding partition range of the Kafka cluster.

- **Stopping Scheduler Nodes**

- **Correcting Consumption Offsets**

```shell
# scheduler kafka set --task_type <task_type> --topic <topic> --partition <partition> <offset>
# demo
scheduler kafka set --task_type shard_repair --topic shard_repair_10001 --partition 20 332053
```

- **Starting Scheduler Nodes**