#!/usr/bin/env bash

RootPath=$(cd $(dirname $0) ; pwd)
CfsBase="chubaofs/cfs-base:1.4"

docker build -t ${CfsBase} -f ${RootPath}/Dockerfile ${RootPath}
