#!/bin/bash

set -ex

/cfs/bin/config_proxy.sh

cat /cfs/conf/proxy.conf

nohup /cfs/bin/proxy -f /cfs/conf/proxy.conf  2>&1 &

while true
do
  echo "proxy running"
  sleep 600
done