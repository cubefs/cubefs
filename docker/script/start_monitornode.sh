#!/bin/sh
mkdir -p /cfs/bin /cfs/log

echo "start monitornode"
/cfs/bin/cfs-server -f -c /cfs/conf/monitornode.json

