#!/bin/bash

set -ex

/cfs/bin/config_scheduler.sh

cat /cfs/conf/scheduler.conf

nohup /cfs/bin/scheduler -f /cfs/conf/scheduler.conf  2>&1 &

while true
do
  echo "scheduler running"
  sleep 600
done