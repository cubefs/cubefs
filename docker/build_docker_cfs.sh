#!/usr/bin/env bash

Version="1.0.0"
if [[ -n "$1" ]] ;then
	# docker image tag of CubeFS
    Version=$1
fi

RootPath=$(cd $(dirname $0)/..; pwd)

source ${RootPath}/build/build.sh
CfsServer="cubefs/cfs-server:$Version"
CfsClient="cubefs/cfs-client:$Version"
docker build -t ${CfsServer} -f ${RootPath}/docker/Dockerfile-cfs --target server ${RootPath}
docker build -t ${CfsClient} -f ${RootPath}/docker/Dockerfile-cfs --target client ${RootPath}

