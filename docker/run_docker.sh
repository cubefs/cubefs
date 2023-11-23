#! /bin/bash
set -e
RootPath=$(cd $(dirname $0)/..; pwd)
GOPATH=/go
export DiskPath="$RootPath/docker/docker_data"

MIN_DNDISK_AVAIL_SIZE_GB=10

help() {
    cat <<EOF

Usage: ./run_docker.sh [ -h | --help ] [ -d | --disk </disk/path> ] [ -l | --ltptest ]
    -h, --help              show help info
    -d, --disk </disk/path>     set CubeFS DataNode local disk path
    -b, --build             build CubeFS server and client
    -s, --server            start CubeFS servers docker image
    -c, --client            start CubeFS client docker image
    -m, --monitor           start monitor web ui
    -l, --ltptest           run ltp test
    -r, --run               run servers, client and monitor
    -f, --format            run gofmt to format source code
    --clean                 cleanup old docker image
EOF
    exit 0
}

compose="docker-compose --env-file ${RootPath}/docker/run_docker.env -f ${RootPath}/docker/docker-compose.yml"


clean() {
    ${compose} down
}

prepare() {
    ${compose} run prepare
}

# unit test
run_unit_test() {
    ${compose} run unit_test
}

# go format
run_format() {
    prepare
    ${compose} run format
}

run_bsgofumpt() {
    prepare
    ${compose} run bs_gofumpt
}

run_bsgolint() {
    prepare
    ${compose} run bs_golint
}

# build
build() {
    prepare
    ${compose} run build
}

# build
build_s3() {
    prepare
    ${compose} run build bash -c "/bin/bash /cfs/script/build.sh -s3"
}

# start server
start_servers() {
    isDiskAvailable $DiskPath
    mkdir -p ${DiskPath}/disk/{1..4}
    ${compose} up -d servers
}

start_client() {
    ${compose} run client bash -c "/cfs/script/start_client.sh ; /bin/bash"
}

start_monitor() {
    ${compose} up -d monitor
}

start_s3test() {
    ${compose} run client
}

start_ltptest() {
    ${compose} run client bash -c "/cfs/script/start.sh -ltp"
}

run_s3test() {
    build_s3
    start_servers
    start_s3test
    clean
}

run_ltptest() {
    build
    start_servers
    start_ltptest
    clean
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
        -t|--test)
            cmd=run_test
            ;;
        -l|--ltptest)
            cmd=run_ltptest
            ;;
        -n|--s3test)
            cmd=run_s3test
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
        -f|--format)
            cmd=run_format
            ;;
        --bsgofumpt)
            cmd=run_bsgofumpt
            ;;
        --bsgolint)
            cmd=run_bsgolint
            ;;
        -clean|--clean)
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
    avail_kb=$(df -k $Disk | tail -1 | awk '{print $4}')
    avail_GB=$(( $avail_kb / 1000 / 1000 ))
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
    -run_monitor) start_monitor ;;
    -run_ltptest) run_ltptest ;;
    -run_test) run_unit_test ;;
    -run_format) run_format ;;
    -run_s3test) run_s3test ;;
    -run_bsgofumpt) run_bsgofumpt ;;
    -run_bsgolint) run_bsgolint ;;
    -clean) clean ;;
    *) help ;;
esac

