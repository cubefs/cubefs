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
    -d, --disk </disk/path>     set ChubaoFS DataNode local disk path
    -b, --build             build ChubaoFS server and client
    --build-test            build ChubaoFS server and client in testing mode
    -s, --server            start ChubaoFS servers docker image
    -c, --client            start ChubaoFS client docker image
    -m, --monitor           start monitor web ui
    --ci-tests              run ci tests, include ltp tests and s3 tests
    -r, --run               run servers, client and monitor
    --clean                 cleanup old docker image
EOF
    exit 0
}

clean() {
    docker-compose -f ${RootPath}/docker/docker-compose.yml down
    docker-compose -f ${RootPath}/docker/docker-compose-test.yml down
}

# unit test
run_unit_test() {
    docker-compose -f ${RootPath}/docker/docker-compose-test.yml run unit_test
}

# build
build() {
    if [[ `docker images | grep "chubaofs/cfs-runtime-alter" | grep "1.0" | wc -l` -eq 0 ]]; then
        wget "http://storage.jd.local/dpgimage/cfs_spark/myy/client_upgrade/cfs-runtime-alter-1.0.tar" -O cfs-runtime-alter-1.0.tar;
        docker load < cfs-runtime-alter-1.0.tar;
        rm cfs-runtime-alter-1.0.tar;
    fi
    docker-compose -f ${RootPath}/docker/docker-compose.yml run build
}

# Build for CI tests
# In this mode, application ELF will be built by using 'go test -c' command instead of orginal 'go build' command
build_test() {
    if [[ `docker images | grep "chubaofs/cfs-runtime-alter" | grep "1.0" | wc -l` -eq 0 ]]; then
        wget "http://storage.jd.local/dpgimage/cfs_spark/myy/client_upgrade/cfs-runtime-alter-1.0.tar" -O cfs-runtime-alter-1.0.tar;
        docker load < cfs-runtime-alter-1.0.tar;
        rm cfs-runtime-alter-1.0.tar;
    fi
    docker-compose -f ${RootPath}/docker/docker-compose-test.yml run build_test
}

# start server for ci tests
start_servers_test() {
    isDiskAvailable $DiskPath
    mkdir -p ${DiskPath}/disk/{1..4}
    docker-compose -f ${RootPath}/docker/docker-compose-test.yml up -d servers
}

# start server
start_servers() {
    isDiskAvailable $DiskPath
    mkdir -p ${DiskPath}/disk/{1..4}
    docker-compose -f ${RootPath}/docker/docker-compose.yml up -d servers
}

start_ecservers_test() {
    isDiskAvailable $DiskPath
    docker-compose -f ${RootPath}/docker/docker-compose-test.yml up -d ec_servers
}

start_client() {
    docker-compose -f ${RootPath}/docker/docker-compose.yml run client bash -c "/cfs/script/start_client.sh ; /bin/bash"
}

start_monitor() {
    docker-compose -f ${RootPath}/docker/docker-compose.yml up -d monitor
}

start_convert() {
    docker-compose -f ${RootPath}/docker/docker-compose.yml up -d convert
}

start_ci_test() {
    docker-compose -f ${RootPath}/docker/docker-compose-test.yml run client
}

start_ci_ectest() {
    docker-compose -f ${RootPath}/docker/docker-compose-test.yml run ec_client
}

cover_analyze() {
    docker-compose -f ${RootPath}/docker/docker-compose-test.yml run cover_analyze
}

run_ci_tests() {
    build_test
    start_servers_test
    ls $RootPath/ecnode &>/dev/null
    if [[ $? -eq 0 ]];then
      start_ecservers_test
      start_ci_ectest
    else
      start_ci_test
    fi
    clean
    cover_analyze
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
    	--build-test)
    	    cmd=build_test
	    ;;	    
        -t|--test)
            cmd=run_test
            ;;
        --ci-tests)
            cmd=run_ci_tests
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
        -convert|--convert)
            cmd=run_convert
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
    -build_test) build_test ;;
    -run_servers) start_servers ;;
    -run_client) start_client ;;
    -run_monitor) start_monitor ;;
    -run_convert) start_convert ;;
    -run_ci_tests) run_ci_tests ;;
    -run_test) run_unit_test ;;
    -clean) clean ;;
    *) help ;;
esac

