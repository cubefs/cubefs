#!/bin/bash

set -e

if [ $# -ne 1 ]; then
    echo "usage: start object: $0 <baseDir>"
    exit 1
fi

confFile=${1}/conf/object.conf
logDir=${1}/object/logs

if [ ! -f "${confFile}" ]; then
  echo "Error: ${confFile} not exist. Ensure 'sh ./shell/deploy.sh <baseDir> <bond0>' is run before 'sh ./shell/deploy_object.sh <baseDir>', with the same <baseDir> in both commands."
  exit 1
fi

echo "mkdir -p $logDir"
mkdir -p $logDir

echo "start checking whether the volume exists"
if ./build/bin/cfs-cli volume list | grep -q "objtest"; then
  echo "volume 'objtest' exists, skipping creation"
else
  echo "begin create volume 'objtest'"
  ./build/bin/cfs-cli volume create objtest obj -y
fi

echo "begin start objectnode service"
./build/bin/cfs-server -c ${confFile}
echo "start objectnode service success"

