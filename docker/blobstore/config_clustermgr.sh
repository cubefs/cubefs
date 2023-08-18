#!/bin/bash

# set -ex
echo "prepare to create configuration file"
CBFS_HOSTNAME_INDEX=""
CBFS_ID=""
CBFS_HOSTNAME_INDEX=`echo $POD_NAME | awk -F '-' '{print $2}'`
CBFS_ID=$(($CBFS_HOSTNAME_INDEX+1))

raft_members="["
for((i=1;i<=$CLUSTERMGR_REPLICAS;i++));
do
   tmp=`expr $i - 1`
   member="{\"id\":$i, \"host\":\"clustermgr-$tmp.clustermgr-service:$LISTEN_PORT\", \"learner\": false, \"node_host\":\"clustermgr-$tmp.clustermgr-service$BIND_ADDR\"}"
   raft_members="$raft_members""$member"
   if [[ $i = $CLUSTERMGR_REPLICAS ]]; then
       raft_members="$raft_members""]"
   else 
       raft_members="$raft_members"","
   fi
done

echo $raft_members

jq -n \
  --arg node_id $CBFS_ID \
  --arg bind_addr $BIND_ADDR \
  --arg cluster_id $CLUSTER_ID \
  --argjson idc "$IDC" \
  --arg chunk_size $CHUNK_SIZE \
  --arg log_level $LOG_LEVEL \
  --arg enable_auth $ENABLE_AUTH \
  --arg auth_secret $AUTH_SECRET \
  --arg region $REGION \
  --argjson policies "$CODE_POLICIES" \
  --arg listen_port $LISTEN_PORT \
  --argjson members "$raft_members" \
  --arg allocatable_size $ALLOC_SIZE \
  --arg raft_snapshot_num $RAFT_SNAPSHOT_NUM \
  --arg raft_flush_num $RAFT_FLUSH_NUM \
  --arg raft_flush_interval $RAFT_FLUSH_INTERVAL \
  --arg raft_trunc_num $RAFT_TRUNC_NUM \
  --arg disk_refresh_interval $DISK_REFRESH_INTERVAL \
  --arg disk_rack_aware $DISK_RACK_AWARE \
  --arg disk_host_aware $DISK_HOST_AWARE \
  '{
    "bind_addr":$bind_addr,
    "cluster_id":($cluster_id|tonumber),
    "idc":$idc,
    "chunk_size": ($chunk_size|tonumber),
    "log": {
        "level": $log_level,
        "filename": "/cfs/log/clustermgr.log"
    },
    "auditlog":{
        "logdir":"/cfs/log/auditlog/clustermgr"
    },
    "auth": {
        "enable_auth": $enable_auth| test("true"),
        "secret": $auth_secret
    },
    "region": $region,
    "db_path": "/cfs/data/clustermgr/db",
    "code_mode_policies": $policies,
    "raft_config": {
        "snapshot_patch_num": ($raft_snapshot_num|tonumber),
        "server_config": {
            "nodeId": ($node_id|tonumber),
            "listen_port": ($listen_port|tonumber),
            "raft_wal_dir": "/cfs/data/clustermgr/raftwal"
        },
        "raft_node_config":{
            "flush_num_interval": ($raft_flush_num|tonumber),
            "flush_time_interval_s": ($raft_flush_interval|tonumber),
            "truncate_num_interval": ($raft_trunc_num|tonumber),
            "node_protocol": "http://",
            "members": $members
        }
    },
    "volume_mgr_config":{
        "allocatable_size":($allocatable_size|tonumber)
    },
    "disk_mgr_config": {
        "refresh_interval_s": ($disk_refresh_interval|tonumber),
        "rack_aware":$disk_rack_aware| test("true"),
        "host_aware":$disk_host_aware| test("true")
    }
}' > /cfs/conf/clustermgr.conf

echo "configuration finished"