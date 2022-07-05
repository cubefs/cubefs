#!/usr/bin/env bash
echo "This script is used to build docker image with private golang runtime"

RootPath=$(cd $(dirname $0) ; pwd)
IMAGE_TAG="chubaofs/cfs-runtime-alter:1.0"

docker build -t ${IMAGE_TAG} -f ${RootPath}/Dockerfile-runtime-alter ${RootPath}
docker save ${IMAGE_TAG} > cfs-runtime-alter-1.0.tar