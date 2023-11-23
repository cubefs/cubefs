#!/bin/bash

set -e

docker --version > /dev/null || exit 1
tag="$(git describe --tags --always)"

help() {
    cat <<EOF

Usage: $0 [ -h | --help ]
    -h, --help              show usage
    -b, --build             build blobstore docker image
    -r, --run               run blobstore in docker
    --clean                 cleanup image
EOF
    exit 0
}

source ./init.sh
if [ ! -d ./run/logs ];then
  mkdir -p ./run/logs
fi

RootPath=$(cd $(dirname $0)/..; pwd)
log=./run/logs/docker.log
name="blobstore:${tag}"


# build
build() {
    INIT
    echo  "start"
    docker build -t "${name}" -f blobstore/Dockerfile ${RootPath} || return 1
    return 0
}

# run
run() {
    docker run "${name}" -d > ${log} 2>&1 &
    sleep 3

    local ps=$(docker ps | grep ${tag})
    echo "$ps"
    if [ "x${ps}" == "x" ]; then
        echo "start to run failed. see more in ${log}"
        return 1
    fi
    return 0
}

#
clean() {
    local imageid=$(docker images | grep ${tag} | awk '{print $3}' | sort -f)
    echo "remove image ${name}  id[ ${imageid} ]"
    docker rmi "${imageid}" || exit 1
    exit 0
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
        -r|--run)
            cmd=run
            ;;
        --clean)
            cmd=clean
            ;;
    esac
done

case "-$cmd" in
    -help) help ;;
    -build)
        build || exit 1
        ;;
    -run)
        run || exit 1
        ;;
    -clean) clean ;;
    *) help ;;
esac
