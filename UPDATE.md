# CubeFS
## blobstore
- add batch consume kafka msg for scheduler, to solve handle blob delete msg too slow.
- remove kafka client manage offset local, let kafka server manage offset.
### scheduler
** upgrade action **

1.Back up old version

- stop switch
- check status
- get current kafka offset

``` shell
# 1. stop switch, $ip: clustermgr ip, $port: clustermgr port
curl -XPOST --header 'Content-Type: application/json' http://$ip:$port/config/set -d '{"key":"blob_delete", "value":"false"}'
curl -XPOST --header 'Content-Type: application/json' http://$ip:$port/config/set -d '{"key":"shard_repair", "value":"false"}'

# 2. check status
curl http://$ip:$port/config/get?key=blob_delete

# 3. get kafka offset, $ip2: scheduler ip, $port2: scheduler port
curl $ip2:$port2/metrics | grep kafka_topic_partition_offset | grep consume > metric_offset.log
```

e.g. metric_offset.log: It has 4 kafka partitions
> kafka_topic_partition_offset{cluster_id="102",module_name="SCHEDULER",partition="1",topic="blob_delete_102",type="consume"} 1.2053985e+07
> kafka_topic_partition_offset{cluster_id="102",module_name="SCHEDULER",partition="2",topic="blob_delete_102",type="consume"} 1.2051438e+07
> kafka_topic_partition_offset{cluster_id="102",module_name="SCHEDULER",partition="3",topic="blob_delete_102",type="consume"} 1234
> kafka_topic_partition_offset{cluster_id="102",module_name="SCHEDULER",partition="4",topic="blob_delete_102",type="consume"} 1025

2.Set kafka offset

- batch set kafka offset

``` shell
#!/bin/bash

DIR="/xxx/kafka_2.12-2.3.1-host/bin"  # kafka path
EXEC="kafka-consumer-groups.sh"
HOST="192.168.0.3:9095"  # kafka host
TOPIC="blob_delete_102"
GROUP="SCHEDULER-$TOPIC"
array_offset=(12053985  12051438  1234  1025)  # From the file above(metric_offset.log) 
  
echo "set consumer offset... ${TOPIC}  ${GROUP}"

cd $DIR
for i in {0..63}  # number of kafka partitions
do
    echo " $i :  ${array_offset[$i]}" 
    ./$EXEC --bootstrap-server $HOST --reset-offsets --topic ${TOPIC}:$i --group ${GROUP} --to-offset ${array_offset[$i]} --execute
done

echo "all done..."
cd -

```

3.update new version

- config file

- stop old version, and install new version

- open switch

``` json
# ebs-scheduler.conf
{
    "blob_delete": {
        "max_batch_size": 10,  # batch consumption size of kafka messages, default is 10. If the batch is full or the time interval is reached, consume the Kafka messages accumulated during this period
        "batch_interval_s": 2, # time interval for consuming kafka messages, default is 2s
        "message_slow_down_time_s": 3,  # slow down when overload, default is 3s
    }
}

```

``` shell
# 1. open switch, $ip: clustermgr ip, $port: clustermgr port
curl -XPOST --header 'Content-Type: application/json' http://$ip:$port/config/set -d '{"key":"blob_delete", "value":"true"}'
curl -XPOST --header 'Content-Type: application/json' http://$ip:$port/config/set -d '{"key":"shard_repair", "value":"true"}'

# 2. check status
curl http://$ip:$port/config/get?key=blob_delete
```