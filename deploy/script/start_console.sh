#!/bin/sh
rm -rf /cfs/disk/* /cfs/log/*
mkdir -p /cfs/bin /cfs/log

echo "start console"
/cfs/bin/cfs-server -f -c /cfs/conf/console.json

