#! /bin/bash

RootPath=$(cd $(dirname $0)/..; pwd)
GOPATH=/go
export DiskPath="$RootPath/docker/disk"

MIN_DNDISK_AVAIL_SIZE_GB=10

help() {
    cat <<EOF

Usage: ./run_docker_auth.sh [ -h | --help ] [ -d | --disk </disk/path> ] [ -l | --ltptest ]
    -h, --help              show help info
    -d, --disk </disk/path>     set datanode local disk path
    -b, --build             build cubefs server and cliente
    -s, --server            start cubefs servers docker image
    -c, --client            start cubefs client docker image
    -s3, --s3node           start cubefs s3node
    -m, --monitor           start monitor web ui
    -l, --ltptest           run ltp test
    -r, --run               run servers, client and monitor
    --clear             clear old docker image
EOF
    exit 0
}


clean() {
    docker-compose -f ${RootPath}/docker/docker-compose-auth.yml down
}

# test & build
build() {
    docker-compose -f ${RootPath}/docker/docker-compose-auth.yml run build
}

# start server
start_servers() {
    isDiskAvailable $DiskPath
    mkdir -p ${DiskPath}/{1..4}
    docker-compose -f ${RootPath}/docker/docker-compose-auth.yml up -d servers
}

start_client() {
    docker-compose -f ${RootPath}/docker/docker-compose-auth.yml run client bash -c "/cfs/script/start_client.sh ; /bin/bash"
}

start_s3node() {
    docker-compose -f ${RootPath}/docker/docker-compose-auth.yml up -d s3node1
}

start_monitor() {
    docker-compose -f ${RootPath}/docker/docker-compose-auth.yml up -d monitor
}

start_ltptest() {
    docker-compose -f ${RootPath}/docker/docker-compose-auth.yml run client
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
        -s3|--s3node)
            cmd=run_s3node
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

function isDiskAvailable() {
    Disk=${1:-"need diskpath"}
    [[ -d $Disk ]] || mkdir -p $Disk
    if [[ ! -d $Disk ]] ; then
        echo "error: $DiskPath must be exist and at least 10GB free size"
        exit 1
    fi
    avail_sectors=$(df  $Disk | tail -1 | awk '{print $4}')
    avail_GB=$(( $avail_sectors / 1024 / 1024 / 2  ))
    if (( $avail_GB < $MIN_DNDISK_AVAIL_SIZE_GB )) ; then
        echo "$Disk: avaible size $avail_GB GB < Min Disk avaible size $MIN_DNDISK_AVAIL_SIZE_GB GB" ;
        exit 1
    fi
}

for opt in ${ARGS[*]} ; do
    case "-$1" in
        --d|---disk)
            shift
            export DiskPath=${1:?"need disk dir path"}
            isDiskAvailable $DiskPath
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
    -run_s3node) start_s3node ;;
    -run_monitor) start_monitor ;;
    -run_ltptest) run_ltptest ;;
    -clean) clean ;;
    *) help ;;
esac
