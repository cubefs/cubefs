#!/bin/bash

source ./init.sh

# build blobstore and get consul kafka
INIT

docker --version
if [ $? -ne 0 ]; then
  echo " 'docker' not found on your computer"
  exit 1
fi

docker build -t "blobstore:v1.2.0" -f Dockerfile .
docker run blobstore:v1.2.0 -d  > ./run/logs/docker.log 2>&1 &
