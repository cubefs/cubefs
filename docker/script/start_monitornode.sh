#!/bin/sh
mkdir -p /cfs/bin /cfs/log

echo "start hbase thrift2server"
sh /root/hbase/hbase-2.3.6/start-thrift2.sh

echo "start monitornode"
/cfs/bin/cfs-server -f -c /cfs/conf/monitornode.json

