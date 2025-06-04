#!/bin/sh
mkdir -p /cfs/log /cfs/data/wal /cfs/data/store /cfs/bin
echo "start flashgroupmanager"
/cfs/bin/cfs-server -f -c /cfs/conf/flashgroupmanager.json

