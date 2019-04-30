#!/bin/sh

mkdir -p /cfs/bin /cfs/log /cfs/disk

sleep 10
echo "start datanode"
/cfs/bin/cfs-server -c /cfs/conf/datanode.json

