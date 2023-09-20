#!/bin/bash

# set -ex

ps -ef | grep "/cfs/bin/scheduler -f /cfs/conf/scheduler.conf" |grep -v grep

status=$?

if [[ $status = 0 ]]; then
  exit 0
else
  exit 1
fi
