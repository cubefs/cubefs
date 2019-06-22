#! /bin/bash

RootPath=$(cd $(dirname $0)/..; pwd)
GOPATH=/go
export DiskPath="$RootPath/docker/disk"

help() {
    cat <<EOF

Usage: ./run_docker.sh [ -h | --help ] [ -d | --disk </disk/path> ] [ -l | --ltptest ]
    -h, --help              show help info
    -d, --disk </disk/path>     set datanode local disk path
    -b, --build             build chubaofs server and cliente
    -s, --server            start chubaofs servers docker image
    -c, --client            start chubaofs client docker image
    -m, --monitor           start monitor web ui
    -l, --ltptest           run ltp test
    -r, --run               run servers, client and monitor
    --clear             clear old docker image
EOF
    exit 0
}


clean() {
    docker-compose -f ${RootPath}/docker/docker-compose.yml down
}

# test & build
build() {
    docker-compose -f ${RootPath}/docker/docker-compose.yml run build
}

# start server
start_servers() {
    mkdir -p ${DiskPath}/{1..4}
    docker-compose -f ${RootPath}/docker/docker-compose.yml up -d servers
}

start_client() {
    docker-compose -f ${RootPath}/docker/docker-compose.yml run client bash -c "/cfs/script/start_client.sh ; /bin/bash"
}

start_monitor() {
    docker-compose -f ${RootPath}/docker/docker-compose.yml up -d monitor
}

start_ltptest() {
    docker-compose -f ${RootPath}/docker/docker-compose.yml run client
}

run_ltptest() {
    build
    start_servers
    start_ltptest
}

run() {
    build
    start_monitor
    start_servers
    start_client
}

cmd="help"

ARGS=( "$@" )
for opt in ${ARGS[*]} ; do
    case "$opt" in
        -h|--help)
            help
            ;;
        -b|--build)
            cmd=build
            ;;
        -l|--ltptest)
            cmd=run_ltptest
            ;;
        -r|--run)
            cmd=run
            ;;
        -s|--server)
            cmd=run_servers
            ;;
        -c|--client)
            cmd=run_client
            ;;
        -m|--monitor)
            cmd=run_monitor
            ;;
        -clear|--clear)
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
            [[ -d $DiskPath ]] || { echo "error: $DiskPath must be exist and at least 30GB free size"; exit 1; }
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
    -build) build ;;
    -run_servers) start_servers ;;
    -run_client) start_client ;;
    -run_monitor) start_monitor ;;
    -run_ltptest) run_ltptest ;;
    -clean) clean ;;
    *) help ;;
esac

