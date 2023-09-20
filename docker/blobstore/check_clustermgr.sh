#!/bin/bash

# set -ex
GET_CLUSTERMGR_URL="http://$CLUSTERMGR_SERVICE/stat"
code=1
while ((code!=0))
do
  clusterMgrInfo=`curl -s "$GET_CLUSTERMGR_URL" | jq .leader_host`
  if [ "$clusterMgrInfo" = "" ]; then
    code=1
    echo "waiting clustermgr service"
    sleep 2s
  else
    echo "leader: "$clusterMgrInfo
    code=0
  fi
done
echo "clustermgr service is ok"
