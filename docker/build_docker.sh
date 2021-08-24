#!/usr/bin/env bash

RootPath=$(cd $(dirname $BASH_SOURCE) ; pwd)
CfsBase="ghcr.io/chubaofs/cbfs-base:1.0"
CfsImages=${CBFS_DOCKER_IMAGE:-ghcr.io/chubaofs/cbfs-base:1.1}

if [ `docker images -q $CfsImages` ]; then
  exit 0
fi

if [ `docker pull -q $CfsImages 2>/dev/null ` ] ; then
  exit 0
fi

if [ ! `docker images -q $CfsBase` ]; then
  docker build -t ${CfsBase} -f ${RootPath}/Dockerfile-base ${RootPath}
fi

echo "build $CfsImages"
docker build -t ${CfsImages} -f ${RootPath}/Dockerfile-test ${RootPath}/s3tests
