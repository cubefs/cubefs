#!/bin/bash

# set -ex
echo "prepare to create configuration file"
CBFS_HOSTNAME_INDEX=""
CBFS_ID=""
CBFS_HOSTNAME_INDEX=`echo $POD_NAME | awk -F '-' '{print $2}'`
CBFS_ID=$(($CBFS_HOSTNAME_INDEX+1))

HOST="http://"$POD_IP$BIND_ADDR

clustermgrs="["
for((i=1;i<=$CLUSTERMGR_REPLICAS;i++));
do
   tmp=`expr $i - 1`
   member="\"http://clustermgr-$tmp.clustermgr-service$CLUSTERMGR_BIND_ADDR\""
   clustermgrs="$clustermgrs""$member"
   if [[ $i = $CLUSTERMGR_REPLICAS ]]; then
       clustermgrs="$clustermgrs""]"
   else 
       clustermgrs="$clustermgrs"","
   fi
done

schedulers="{"
for((i=1;i<=$SCHEDULER_REPLICAS;i++));
do
   tmp=`expr $i - 1`
   
   member="\"$i\": \"scheduler-$tmp.scheduler-service$BIND_ADDR\""
   schedulers="$schedulers""$member"
   if [[ $i = $SCHEDULER_REPLICAS ]]; then
       schedulers="$schedulers""}"
   else 
       schedulers="$schedulers"","
   fi
done

jq -n \
  --arg node_id $CBFS_ID \
  --arg bind_addr $BIND_ADDR \
  --arg cluster_id $CLUSTER_ID \
  --argjson members "$schedulers" \
  --arg idc $IDC \
  --arg log_level $LOG_LEVEL \
  --arg host $HOST \
  --argjson clustermgrs "$clustermgrs" \
  --arg kafka_addrs $KAFKA_ADDRS \
  '{
  "bind_addr": $bind_addr,
  "cluster_id": ($cluster_id|tonumber),
  "services": {
    "leader": 1,
    "node_id": ($node_id|tonumber),
    "members": $members
  },
  "service_register": {
    "host": $host,
    "idc": $idc
  },
  "clustermgr": {
    "hosts": $clustermgrs
  },
  "kafka": {
    "broker_list": [$kafka_addrs]
  },
  "blob_delete": {
    "delete_log": {
      "dir": "/cfs/log/delete_log"
    }
  },
  "shard_repair": {
    "orphan_shard_log": {
      "dir": "/cfs/log/orphan_shard_log"
    }
  },
  "log": {
    "level": $log_level,
    "filename": "/cfs/log/scheduler.log"
  },
  "auditlog":{
    "logdir":"/cfs/log/auditlog/scheduler"
  },
  "task_log": {
    "dir": "/cfs/log/task_log"
  }
}' > /cfs/conf/scheduler.conf

echo "configuration finished"