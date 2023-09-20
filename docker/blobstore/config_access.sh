#!/bin/bash

# set -ex
echo "prepare to create configuration file"

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
  --arg idc $IDC \
  --arg region $REGION \
  --arg log_level $LOG_LEVEL \
  --argjson clustermgrs "$clustermgrs" \
  '{
    "bind_addr": $bind_addr,
    "log": {
        "level": $log_level,
        "filename": "/cfs/log/access.log"
    },
    "auditlog":{
        "logdir":"/cfs/log/auditlog/access"
    },
    "stream": {
        "idc": $idc,
        "cluster_config": {
            "region": $region,
            "clusters": [
                {"cluster_id":($cluster_id|tonumber),"hosts":$clustermgrs}
            ]
        }
    }
}' > /cfs/conf/access.conf

echo "configuration finished"