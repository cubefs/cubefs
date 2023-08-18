#!/bin/bash

# set -ex

ps -ef | grep "/cfs/bin/blobnode -f /cfs/conf/blobnode.conf" |grep -v grep

status=$?

if [[ $status = 0 ]]; then
  exit 0
else
  exit 1
fi
