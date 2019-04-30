#!/bin/sh

mkdir -p /cfs/log /cfs/data/wal /cfs/data/store /cfs/bin

echo "start master"
/cfs/bin/cfs-server -c /cfs/conf/master.json

