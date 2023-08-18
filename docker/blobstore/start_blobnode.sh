#!/bin/bash

set -ex

/cfs/bin/config_blobnode.sh

cat /cfs/conf/blobnode.conf

nohup /cfs/bin/blobnode -f /cfs/conf/blobnode.conf  2>&1 &

while true
do
  echo "blobnode running"
  sleep 600
done