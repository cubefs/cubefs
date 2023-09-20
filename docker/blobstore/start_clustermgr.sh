#!/bin/bash

set -ex

/cfs/bin/config_clustermgr.sh

cat /cfs/conf/clustermgr.conf

nohup /cfs/bin/clustermgr -f /cfs/conf/clustermgr.conf  2>&1 &

while true
do
  echo "clustermgr running"
  sleep 600
done