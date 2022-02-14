#!/usr/bin/env bash

RootPath=$(cd $(dirname $0) ; pwd)
CfsBase="ghcr.io/cubefs/cbfs-base:1.0-golang-1.16.12"

docker build -t ${CfsBase} -f ${RootPath}/Dockerfile ${RootPath}
