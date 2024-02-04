#! /bin/bash
set -e
RootPath=$(cd $(dirname $0)/..; pwd)
export DiskPath="$RootPath/docker/docker_data"

MIN_DNDISK_AVAIL_SIZE_GB=10

help() {
    cat <<EOF

Usage: ./run_docker.sh {cmd} [ -h|--help ]  [ -d|--disk </disk/path> ]
    -h, --help              show help info
    -d, --disk </disk/path> set CubeFS DataNode local disk path

    -b, --build             build binaries of server and so on
    -t, --test              run all unit testing
    -r, --run               start servers, client and monitor
    -s, --server            start server docker images
    -c, --client            start client docker image
    -m, --monitor           start monitor web ui
    -f, --format            check format of source code
        --s3test            run s3 testing
        --ltptest           run ltp testing
        --buildlibsdkpre    build libcfs.so
        --goreleaser        release using goreleaser
        --bsgofumpt         run blobstore gofumpt
        --bsgolint          run blobstore golangci-lint
        --gosec             run gosec of source code
        --clean             cleanup all old docker images

EOF
    exit 0
}

compose="docker-compose --env-file ${RootPath}/docker/run_docker.env -f ${RootPath}/docker/docker-compose.yml"

has_go() {
    if command -v go &> /dev/null
    then
        echo "1"
    else
        echo "0"
    fi
}

get_go_build_cache() {
    tmp=`go env | grep GOCACHE | awk -F= '{print $2}'`
    echo ${tmp} | sed 's/\"//g' | sed "s/'//g"
}

check_is_podman() {
    if docker --version | grep -i -q 'podman'
    then
        echo "1"
    else
        echo "0"
    fi
    return 0
}

prepare_for_podman() {
    start=`sysctl net.ipv4.ip_unprivileged_port_start | awk -F= '{print $2}'`
    if test $start -gt 80
    then
        if [ $(whoami) = "root" ]
        then
            echo "Using rootful container"
            return 0
        fi
        echo "Please allow us to use 80 port"
        echo "Execute command 'sudo sysctl net.ipv4.ip_unprivileged_port_start=80' to set 'net.ipv4.ip_unprivileged_port_start' value temporarily"
        sudo sysctl net.ipv4.ip_unprivileged_port_start=80
    fi
}

clean() {
    ${compose} down
}

prepare() {
    ${compose} run --rm prepare
}

# unit test
run_unit_test() {
    prepare
    ${compose} run --rm unit_test
}

# go format
run_format() {
    prepare
    ${compose} run --rm format
}

run_build_libsdkpre() {
    prepare
    ${compose} run --rm build_libsdkpre
}

run_goreleaser() {
    prepare
    ${compose} run --rm goreleaser
}

run_bsgofumpt() {
    prepare
    ${compose} run --rm bs_gofumpt
}

run_bsgolint() {
    prepare
    ${compose} run --rm bs_golint
}

run_gosec() {
    prepare
    ${compose} run --rm gosec
}

# build
build() {
    prepare
    has=`has_go`
    build_cache_opt=""
    if test ${has} -eq 1
    then
        go_cache_path=`get_go_build_cache`
        echo "Mount ${go_cache_path} as go build cache dir"
        build_cache_opt="--volume ${go_cache_path}:/root/.cache/go-build"
    fi
    ${compose} run --rm ${build_cache_opt} build
}

# build
build_s3() {
    prepare
    has=`has_go`
    build_cache_opt=""
    if test ${has} -eq 1
    then
        go_cache_path=`get_go_build_cache`
        echo "Mount ${go_cache_path} as go build cache dir"
        build_cache_opt="--volume ${go_cache_path}:/root/.cache/go-build"
    fi
    ${compose} run --rm ${build_cache_opt} build bash -c "/bin/bash /cfs/script/build.sh -s3"
}

# start server
start_servers() {
    isDiskAvailable $DiskPath
    mkdir -p ${DiskPath}/disk/{1..4}
    ${compose} up -d servers
}

start_client() {
    ${compose} run --rm client bash -c "/cfs/script/start_client.sh ; /bin/bash"
}

start_monitor() {
    ${compose} up -d monitor
}

start_s3test() {
    ${compose} run --rm client
}

start_ltptest() {
    ${compose} run --rm client_ltp
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
        -r|--run)
            cmd=run
            ;;
        -s|--server)
            cmd=run_server
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
        --s3test)
            cmd=run_s3test
            ;;
        --ltptest)
            cmd=run_ltptest
            ;;
        --buildlibsdkpre)
            cmd=run_build_libsdkpre
            ;;
        --goreleaser)
            cmd=run_goreleaser
            ;;
        --bsgofumpt)
            cmd=run_bsgofumpt
            ;;
        --bsgolint)
            cmd=run_bsgolint
            ;;
        --gosec)
            cmd=run_gosec
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
        --d|--disk)
            shift
            export DiskPath=${1:?"need disk dir path"}
            isDiskAvailable "${DiskPath}"
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

if [ "$cmd" != "help" ] && [ "$cmd" != "build" ] && [ "$cmd" != "clean" ]
then
    is_podman=`check_is_podman`
    if test $is_podman -eq 1
    then
        prepare_for_podman
    fi
fi

case "-$cmd" in
    -help) help ;;
    -run) run ;;
    -build) build ;;
    -run_server) start_servers ;;
    -run_client) start_client ;;
    -run_monitor) start_monitor ;;
    -run_ltptest) run_ltptest ;;
    -run_test) run_unit_test ;;
    -run_format) run_format ;;
    -run_s3test) run_s3test ;;
    -run_build_libsdkpre) run_build_libsdkpre ;;
    -run_goreleaser) run_goreleaser ;;
    -run_bsgofumpt) run_bsgofumpt ;;
    -run_bsgolint) run_bsgolint ;;
    -run_gosec) run_gosec ;;
    -clean) clean ;;
    *) help ;;
esac
