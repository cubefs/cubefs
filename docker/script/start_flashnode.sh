#!/bin/sh
mkdir -p /cfs/bin /cfs/log
echo "start flashnode"
/cfs/bin/cfs-server -f -c /cfs/conf/flashnode.json
