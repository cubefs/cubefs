#!/bin/sh

mkdir -p /cfs/bin /cfs/log /cfs/data/meta /cfs/data/raft

echo "start metanode"
/cfs/bin/cfs-server -c /cfs/conf/metanode.json

