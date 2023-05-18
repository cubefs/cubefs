#!/usr/bin/env bash

RootPath=$(cd $(dirname $0) ; pwd)
CfsDev="cubefs/cfs-base:dev-1.0"

docker build -t ${CfsDev} -f ${RootPath}/Dockerfile-dev ${RootPath}
