#!/usr/bin/env bash

Version="1.0.0"
if [[ -n "$1" ]] ;then
	# docker image tag of ChubaoFS
    Version=$1
fi

RootPath=$(cd $(dirname $0)/..; pwd)

source ${RootPath}/build/build.sh
CfsServer="chubaofs/cfs-server:$Version"
CfsClient="chubaofs/cfs-client:$Version"
docker build -t ${CfsServer} -f ${RootPath}/docker/Dockerfile-cfs --target server ${RootPath}
docker build -t ${CfsClient} -f ${RootPath}/docker/Dockerfile-cfs --target client ${RootPath}

