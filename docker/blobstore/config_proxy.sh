#!/bin/bash

# set -ex
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

jq -n \
  --arg bind_addr $BIND_ADDR \
  --arg cluster_id $CLUSTER_ID \
  --arg default_alloc_vols_num $ALLOC_VOLS_NUM \
  --arg hb_interval $HB_INTERVAL \
  --arg idc $IDC \
  --arg log_level $LOG_LEVEL \
  --arg host $HOST \
  --argjson clustermgrs "$clustermgrs" \
  --arg kafka_addrs $KAFKA_ADDRS \
  '{
  "bind_addr": $bind_addr,
  "host": $host,
  "idc": $idc,
  "cluster_id": ($cluster_id|tonumber),
  "default_alloc_vols_num" : ($default_alloc_vols_num|tonumber),
  "heartbeat_interval_s": ($hb_interval|tonumber),
  "diskv_base_path": "/cfs/data/proxycache",
  "clustermgr": {
    "hosts": $clustermgrs
  },
  "mq": {
    "blob_delete_topic": "blob_delete",
    "shard_repair_topic": "shard_repair",
    "shard_repair_priority_topic": "shard_repair_prior",
    "msg_sender": {
      "broker_list": [$kafka_addrs]
    }
  },
  "log": {
    "level": $log_level,
    "filename": "/cfs/log/proxy.log"
  },
  "auditlog":{
    "logdir":"/cfs/log/auditlog/proxy"
  }
}' > /cfs/conf/proxy.conf

echo "configuration finished"