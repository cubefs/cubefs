#!/bin/sh
rm -rf /cfs/disk/* /cfs/log/*
mkdir -p /cfs/bin /cfs/log /cfs/disk
sleep 10
echo "start datanode"
/cfs/bin/cfs-server -f -c /cfs/conf/datanode.json && sleep 99999999d

