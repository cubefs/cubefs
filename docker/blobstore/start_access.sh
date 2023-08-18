#!/bin/bash

set -ex

/cfs/bin/config_access.sh

cat /cfs/conf/access.conf

nohup /cfs/bin/access -f /cfs/conf/access.conf  2>&1 &

while true
do
  echo "access running"
  sleep 600
done