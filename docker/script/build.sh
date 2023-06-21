#!/bin/bash
mkdir -p /go/src/github.com/cubefs/cubefs/docker/bin
failed=0

export GO111MODULE=off

echo -n 'Building CubeFS ... '

if [ "$1"x = "-scenario"x ]; then
    pushd /go/src/github.com/cubefs/cubefs
    make server
    make client
    make cli
    if [[ $? -eq 0 ]]; then
        echo -e "\033[32mdone\033[0m"
        cp build/bin/cfs-server /go/src/github.com/cubefs/cubefs/docker/bin/cfs-server
        cp build/bin/cfs-client /go/src/github.com/cubefs/cubefs/docker/bin/cfs-client
        cp build/bin/cfs-cli /go/src/github.com/cubefs/cubefs/docker/bin/cfs-cli
    else
        echo -e "\033[31mfail\033[0m"
        failed=1
    fi
    popd

    if [[ ${failed} -eq 1 ]]; then
        exit 1
    fi
    exit 0
fi

pushd /go/src/github.com/cubefs/cubefs
make
if [[ $? -eq 0 ]]; then
    echo -e "\033[32mdone\033[0m"
    cp build/bin/cfs-server /go/src/github.com/cubefs/cubefs/docker/bin/cfs-server
    cp build/bin/cfs-client /go/src/github.com/cubefs/cubefs/docker/bin/cfs-client
    cp build/bin/cfs-cli /go/src/github.com/cubefs/cubefs/docker/bin/cfs-cli
    cp build/bin/libcfs.so /go/src/github.com/cubefs/cubefs/docker/bin/libcfs.so
    cp build/bin/cfs-bcache /go/src/github.com/cubefs/cubefs/docker/bin/cfs-bcache
else
    echo -e "\033[31mfail\033[0m"
    failed=1
fi
popd

if [[ ${failed} -eq 1 ]]; then
    exit 1
fi
exit 0
