#!/bin/sh
mkdir -p /cfs/bin /cfs/log

echo "start convertnode"
/cfs/bin/cfs-server -f -c /cfs/conf/convertnode.json

