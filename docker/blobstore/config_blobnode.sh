#!/bin/bash

# set -ex
echo "prepare to create configuration file"

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

disks="["
disk_num=0
for var in "${!DISK_DEVICE_@}"; do
  disk_num=`expr $disk_num + 1`
done
cnt=0
for var in "${!DISK_DEVICE_@}"; do
  cnt=`expr $cnt + 1`
  member="{\"path\": \"${!var}\", \"auto_format\": true, \"max_chunks\": 1024}"
  disks="$disks""$member"
  if [[ $cnt = $disk_num ]]; then
    disks="$disks""]"
  else 
    disks="$disks"","
  fi
done
echo $disks

jq -n \
  --arg bind_addr $BIND_ADDR \
  --arg cluster_id $CLUSTER_ID \
  --arg idc $IDC \
  --arg rack $RACK \
  --arg log_level $LOG_LEVEL \
  --arg host $HOST \
  --argjson clustermgrs "$clustermgrs" \
  --argjson disks "$disks" \
  --arg disk_reserved $DISK_RESERVED \
  --arg compact_reserved $COMPACT_RESERVED \
  '{
  "bind_addr": $bind_addr,
  "cluster_id": ($cluster_id|tonumber),
  "idc": $idc,
  "rack": $rack,
  "host": $host,
  "dropped_bid_record": {
    "dir": "/cfs/log/dropped"
  },
  "disks": $disks,
  "clustermgr": {
    "hosts": $clustermgrs
  },
  "disk_config":{
    "disk_reserved_space_B": ($disk_reserved|tonumber),
    "compact_reserved_space_B": ($compact_reserved|tonumber)
  },
  "log": {
    "level": $log_level,
    "filename": "/cfs/log/blobnode.log"
  },
  "auditlog":{
    "logdir":"/cfs/log/auditlog/blobnode"
  }
}' > /cfs/conf/blobnode.conf

echo "configuration finished"