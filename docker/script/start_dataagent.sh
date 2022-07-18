#!/bin/sh
mkdir -p /cfs/bin /cfs/log

echo "start datanode agent"
/cfs/bin/cfs-server -f -c /cfs/conf/dataAgent.json