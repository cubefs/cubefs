#!/usr/bin/env bash

RootPath=$(cd $(dirname $0) ; pwd)
CfsBase="ghcr.io/chubaofs/cbfs-base:1.1"

docker build -t ${CfsBase} -f ${RootPath}/Dockerfile ${RootPath}
