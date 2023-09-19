#!/bin/sh
mkdir -p /cfs/bin /cfs/log

echo "start objectnode"
/cfs/bin/cfs-server -f -c /cfs/conf/objectnode.json

