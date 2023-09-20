#!/usr/bin/env bash

RootPath=$(cd $(dirname $0) ; pwd)
CfsBase="cubefs/blobstore:3.3.0"

docker build -t ${CfsBase} -f ${RootPath}/blobstore/Dockerfile ${RootPath}/..