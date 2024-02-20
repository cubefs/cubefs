#!/bin/bash

set -e

if [ $# -ne 1 ]; then
    echo "usage: start client: $0 <baseDir>"
    exit 1
fi

confFile=${1}/conf/client.conf
mntDir=${1}/client/mnt

if [ ! -f "${confFile}" ]; then
  echo "Error: ${confFile} not exist. Ensure 'sh ./shell/deploy.sh <baseDir> <bond0>' is run before 'sh ./shell/deploy_client.sh <baseDir>', with the same <baseDir> in both commands."
  exit 1
fi

echo "mkdir -p $mntDir"
mkdir -p $mntDir

echo "start checking whether the volume exists"
if ./build/bin/cfs-cli volume list | grep -q "ltptest"; then
  echo "volume 'ltptest' exists, skipping creation"
else
  echo "begin create volume 'ltptest'"
  ./build/bin/cfs-cli volume create ltptest ltp -y
fi

echo "begin start client"
./build/bin/cfs-client -c ${confFile}
echo "start client success"

