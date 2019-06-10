#! /bin/bash

RootPath=$(cd $(dirname $0)/..; pwd)
GOPATH=/go
GoAppName=github.com/chubaofs/chubaofs
GoAppSrcPath=$GOPATH/src/$GoAppName
ServerBuildDockerImage="chubaofs/cfs-build:1.0"
ClientBuildDockerImage="chubaofs/centos-ltp:1.0"
DataNodeNum=4
export DiskPath="./disk"

help() {
    cat <<EOF

Usage: ./run_docker.sh [ -h | --help ] [ -d | --disk </disk/path> ] [ -l | --ltptest ]
    -h, --help              show help info
    -d, --disk </disk/path>     set datanode local disk path
    -c, --clear             clear old docker image
    -l, --ltptest           run ltp test
    -r, --run               run
EOF
    exit 0
}


clean() {
    docker-compose -f ${RootPath}/docker/docker-compose.yml down
}

# test & build
build() {
    docker run --rm -v ${RootPath}:/go/src/github.com/chubaofs/chubaofs $ServerBuildDockerImage /bin/bash -c "cd $GoAppSrcPath && make build_server"
    docker run --rm -v ${RootPath}:/go/src/github.com/chubaofs/chubaofs $ClientBuildDockerImage /bin/bash -c "cd $GoAppSrcPath && make build_client"
}

# start server
start_servers() {
    mkdir -p ${DiskPath}/{1..4}
    docker-compose -f ${RootPath}/docker/docker-compose.yml up -d servers
}

start_client() {
    docker-compose -f ${RootPath}/docker/docker-compose.yml run --name cfs-client -d client bash -c "/cfs/script/start_client.sh"
}

start_ltptest() {
    docker-compose -f ${RootPath}/docker/docker-compose.yml run --name cfs-client client
}

run_ltptest() {
    build
    start_servers
    start_ltptest
}

run() {
    build
    start_servers
    start_client
}

cmd="run"

ARGS=( "$@" )
for opt in ${ARGS[*]} ; do
    case "$opt" in
        -h|--help)
            help
            ;;
        -l|--ltptest)
            cmd=run_ltptest
            ;;
        -r|--run)
            cmd=run
            ;;
        -c|--clean)
            cmd=clean
            ;;
        *)
            ;;
    esac
done

for opt in ${ARGS[*]} ; do
    case "-$1" in
        --d|---disk)
            shift
            export DiskPath=${1:?"need disk dir path"}
            shift
            ;;
        -)
            break
            ;;
        *)
            shift
            ;;
    esac
done

case "-$cmd" in
    -help) help ;;
    -run) run ;;
    -run_ltptest) run_ltptest ;;
    -clean) clean ;;
    *) help ;;
esac

