#!/bin/sh
mkdir -p /cfs/bin /cfs/log
echo "start lcnode"
/cfs/bin/cfs-server -f -c /cfs/conf/lcnode.json
