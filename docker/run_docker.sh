#! /bin/sh

RootPath=$(cd $(dirname $0)/..; pwd)
GOPATH=/go
GoAppName=github.com/chubaofs/chubaofs
GoAppSrcPath=$GOPATH/src/$GoAppName
ServerBuildDockerImage="chubaofs/cfs-build:1.0"
ClientBuildDockerImage="chubaofs/centos-ltp:1.0"

# test & build
docker run --rm -v ${RootPath}:/go/src/github.com/chubaofs/chubaofs $ServerBuildDockerImage /bin/bash -c "cd $GoAppSrcPath && make build_server"
docker run --rm -v ${RootPath}:/go/src/github.com/chubaofs/chubaofs $ClientBuildDockerImage /bin/bash -c "cd $GoAppSrcPath && make build_client"

#docker-compose -f ${RootPath}/docker/docker-compose-demo.yml down

# start server
docker-compose -f ${RootPath}/docker/docker-compose.yml up -d servers

# start client
#docker-compose -f ${RootPath}/docker/docker-compose.yml run client
docker-compose -f ${RootPath}/docker/docker-compose.yml run --name cfs-client -d client bash -c "/cfs/script/start_client.sh  ; sleep 10000000000"

docker exec -it cfs-client /bin/bash
